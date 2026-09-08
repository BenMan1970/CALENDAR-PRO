"""
BLUESTAR · app.py
=================
Dashboard Streamlit et orchestrateur léger de l'ingestor BLUESTAR.

Responsabilités :
  • amorcer calendar_ingestor.py lorsque l'artefact canonique est absent ;
  • relancer l'ingestion à intervalle contrôlé ;
  • afficher le dernier artefact validé, y compris en mode dégradé ;
  • recalculer les countdowns et fuseaux sans modifier le payload canonique ;
  • exposer les exports legacy et health (calendar.latest.json reste consulté
    en direct par le dashboard, mais n'est plus proposé au téléchargement
    depuis l'onglet Exports — cible confondue à tort avec calendar.json) ;
  • maintenir une séparation stricte entre politique machine et filtres UI.

Important :
  Le fichier canonique reste produit exclusivement par calendar_ingestor.run_once().
  Les widgets Streamlit ne modifient jamais calendar.latest.json.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
import streamlit as st

from calendar_core import (
    CalendarEvent,
    CalendarPayload,
    Impact,
    QualityStatus,
    SelectionPolicy,
    Session,
    TimeProximity,
    compute_time_context,
    iso_z,
    pairs_for_currency,
    refresh_time_contexts,
    to_legacy_payload,
)
from calendar_ingestor import build_session, run_once


# =============================================================================
# CONSTANTES
# =============================================================================

UTC = timezone.utc
LOG = logging.getLogger("bluestar.streamlit")

DATA_DIR = Path(os.getenv("BLUESTAR_DATA_DIR", "data")).resolve()

CANONICAL_PATH = DATA_DIR / "calendar.latest.json"
LEGACY_PATH = DATA_DIR / "calendar.legacy.json"
HEALTH_PATH = DATA_DIR / "health.json"
STATE_PATH = DATA_DIR / "_state.json"

# Fréquence de tentative réseau/ingestion.
INGEST_INTERVAL_SECONDS = max(
    60,
    int(os.getenv("BLUESTAR_INGEST_INTERVAL", "300")),
)

# Fréquence de recalcul de l'écran et des countdowns.
UI_REFRESH_SECONDS = max(
    5,
    int(os.getenv("BLUESTAR_UI_REFRESH_INTERVAL", "10")),
)

DEFAULT_DISPLAY_TIMEZONE = os.getenv(
    "BLUESTAR_DISPLAY_TZ",
    "Africa/Casablanca",
)

DISPLAY_TIMEZONES: Tuple[str, ...] = (
    "Africa/Casablanca",
    "UTC",
    "Europe/London",
    "Europe/Paris",
    "America/New_York",
    "America/Toronto",
    "Asia/Tokyo",
    "Asia/Singapore",
    "Australia/Sydney",
)

ALL_CURRENCIES: Tuple[str, ...] = (
    "USD",
    "EUR",
    "GBP",
    "JPY",
    "CAD",
    "AUD",
    "NZD",
    "CHF",
)

ALL_IMPACTS: Tuple[Impact, ...] = (
    Impact.HIGH,
    Impact.MEDIUM,
    Impact.LOW,
    Impact.HOLIDAY,
)

ALL_SESSIONS: Tuple[Session, ...] = (
    Session.OVERLAP_LONDON_NY,
    Session.OVERLAP_ASIA_LONDON,
    Session.LONDON,
    Session.NEW_YORK,
    Session.ASIAN,
    Session.OFF,
)

ALL_PROXIMITIES: Tuple[TimeProximity, ...] = (
    TimeProximity.IMMINENT,
    TimeProximity.SOON,
    TimeProximity.LATER,
    TimeProximity.PAST,
)

ASSET_MAPPING: Dict[str, Tuple[str, ...]] = {
    "USD": (
        "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF",
        "USD/CAD", "AUD/USD", "NZD/USD",
        "XAU/USD", "XAG/USD",
        "US30", "US500", "NAS100", "US2000",
        "WTI", "BRENT",
    ),
    "EUR": (
        "EUR/USD", "EUR/GBP", "EUR/JPY", "EUR/CHF",
        "EUR/CAD", "EUR/AUD", "EUR/NZD",
        "EUR50", "DE40", "FR40",
    ),
    "GBP": (
        "GBP/USD", "EUR/GBP", "GBP/JPY", "GBP/CHF",
        "GBP/CAD", "GBP/AUD", "GBP/NZD",
        "UK100",
    ),
    "JPY": (
        "USD/JPY", "EUR/JPY", "GBP/JPY", "AUD/JPY",
        "NZD/JPY", "CAD/JPY", "CHF/JPY",
        "JP225",
    ),
    "CAD": (
        "USD/CAD", "EUR/CAD", "GBP/CAD", "AUD/CAD",
        "NZD/CAD", "CAD/JPY", "CAD/CHF",
        "CA60",
    ),
    "AUD": (
        "AUD/USD", "EUR/AUD", "GBP/AUD", "AUD/JPY",
        "AUD/CHF", "AUD/CAD", "AUD/NZD",
        "AU200",
    ),
    "NZD": (
        "NZD/USD", "EUR/NZD", "GBP/NZD", "NZD/JPY",
        "NZD/CHF", "NZD/CAD", "AUD/NZD",
        "NZ50",
    ),
    "CHF": (
        "USD/CHF", "EUR/CHF", "GBP/CHF", "AUD/CHF",
        "NZD/CHF", "CAD/CHF", "CHF/JPY",
        "CH20",
    ),
    "ALL": (),
}

METALS: Tuple[str, ...] = ("XAU/USD", "XAG/USD")

INDICES: Tuple[str, ...] = (
    "US30", "US500", "NAS100", "US2000",
    "EUR50", "DE40", "FR40", "UK100",
    "JP225", "AU200", "CA60", "NZ50",
    "CH20", "CN50", "HK50",
)

ENERGY: Tuple[str, ...] = ("WTI", "BRENT")


# =============================================================================
# POLITIQUE MACHINE
# =============================================================================

def parse_machine_impacts() -> Tuple[Impact, ...]:
    raw = os.getenv("BLUESTAR_MACHINE_IMPACTS", "HIGH")
    selected: List[Impact] = []

    for token in raw.split(","):
        name = token.strip().upper()
        if not name:
            continue
        try:
            selected.append(Impact(name))
        except ValueError:
            LOG.warning("Ignoring invalid machine impact: %s", name)

    return tuple(dict.fromkeys(selected)) or (Impact.HIGH,)


def parse_machine_currencies() -> Optional[Tuple[str, ...]]:
    raw = os.getenv("BLUESTAR_MACHINE_CURRENCIES", "").strip()

    if not raw:
        return None

    selected = tuple(
        sorted({
            token.strip().upper()
            for token in raw.split(",")
            if token.strip()
        })
    )
    return selected or None


MACHINE_POLICY = SelectionPolicy(
    impact_levels=parse_machine_impacts(),
    currencies=parse_machine_currencies(),
    include_global_events=(
        os.getenv("BLUESTAR_INCLUDE_GLOBAL", "true").lower()
        in {"1", "true", "yes", "on"}
    ),
    display_timezone=DEFAULT_DISPLAY_TIMEZONE,
    window_past_hours=float(
        os.getenv("BLUESTAR_WINDOW_PAST_HOURS", "72")
    ),
    window_future_hours=float(
        os.getenv("BLUESTAR_WINDOW_FUTURE_HOURS", "192")
    ),
    imminent_hours=float(
        os.getenv("BLUESTAR_IMMINENT_HOURS", "6")
    ),
    soon_hours=float(
        os.getenv("BLUESTAR_SOON_HOURS", "48")
    ),
    max_source_age_seconds=int(
        os.getenv("BLUESTAR_MAX_SOURCE_AGE_SECONDS", "900")
    ),
)


# =============================================================================
# RUNTIME PARTAGÉ
# =============================================================================

@dataclass
class RuntimeControl:
    """
    État process-local partagé par les sessions Streamlit.

    Le verrou empêche plusieurs utilisateurs de lancer simultanément
    l'ingestor sur les mêmes fichiers.
    """

    lock: threading.Lock = field(default_factory=threading.Lock)
    last_attempt_monotonic: float = 0.0
    last_attempt_utc: Optional[datetime] = None
    last_result_ok: Optional[bool] = None
    last_runtime_error: Optional[str] = None


@st.cache_resource
def runtime_control() -> RuntimeControl:
    return RuntimeControl()


@st.cache_resource
def ingestion_http_session() -> requests.Session:
    return build_session()


# =============================================================================
# I/O
# =============================================================================

def now_utc() -> datetime:
    return datetime.now(UTC)


def read_json(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None


def load_payload() -> Optional[CalendarPayload]:
    """
    Charge directement le fichier courant.

    Aucun st.cache_data n'est utilisé afin qu'un artefact publié atomiquement
    soit visible au prochain fragment rerun.
    """
    raw = read_json(CANONICAL_PATH)
    if raw is None:
        return None

    try:
        return CalendarPayload.model_validate(raw)
    except Exception as exc:  # validation Pydantic détaillée dans les diagnostics
        LOG.exception("Invalid canonical artifact: %s", exc)
        return None


def load_health() -> Optional[Dict[str, Any]]:
    raw = read_json(HEALTH_PATH)
    return raw if isinstance(raw, dict) else None


def load_state() -> Optional[Dict[str, Any]]:
    raw = read_json(STATE_PATH)
    return raw if isinstance(raw, dict) else None


def file_age_seconds(path: Path, reference: datetime) -> Optional[int]:
    try:
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return None

    return max(0, int((reference - modified).total_seconds()))


# =============================================================================
# ORCHESTRATION DE L'INGESTOR
# =============================================================================

def should_attempt_ingestion(reference: datetime, force: bool = False) -> bool:
    if force:
        return True

    if not CANONICAL_PATH.exists():
        return True

    age = file_age_seconds(CANONICAL_PATH, reference)
    return age is None or age >= INGEST_INTERVAL_SECONDS


def ensure_artifacts(force: bool = False) -> bool:
    """
    Exécute au maximum une ingestion par intervalle et par processus.

    En cas d'échec :
      • calendar_ingestor conserve l'ancien artefact ;
      • health.json expose l'erreur ;
      • l'UI continue d'afficher le last-known-good disponible.
    """
    reference = now_utc()
    control = runtime_control()

    if not should_attempt_ingestion(reference, force=force):
        return CANONICAL_PATH.exists()

    elapsed = time.monotonic() - control.last_attempt_monotonic
    if (
        not force
        and control.last_attempt_monotonic > 0
        and elapsed < INGEST_INTERVAL_SECONDS
    ):
        return CANONICAL_PATH.exists()

    acquired = control.lock.acquire(blocking=False)
    if not acquired:
        # Une autre session effectue déjà l'ingestion.
        return CANONICAL_PATH.exists()

    try:
        # Double vérification après acquisition du verrou.
        reference = now_utc()
        elapsed = time.monotonic() - control.last_attempt_monotonic

        if (
            not force
            and control.last_attempt_monotonic > 0
            and elapsed < INGEST_INTERVAL_SECONDS
        ):
            return CANONICAL_PATH.exists()

        if not should_attempt_ingestion(reference, force=force):
            return CANONICAL_PATH.exists()

        control.last_attempt_monotonic = time.monotonic()
        control.last_attempt_utc = reference
        control.last_runtime_error = None

        DATA_DIR.mkdir(parents=True, exist_ok=True)

        LOG.info(
            "Starting ingestion | force=%s | data_dir=%s",
            force,
            DATA_DIR,
        )

        payload = run_once(
            data_dir=DATA_DIR,
            policy=MACHINE_POLICY,
            session=ingestion_http_session(),
        )

        control.last_result_ok = payload is not None

        if payload is None:
            LOG.error(
                "Ingestion produced no publishable payload; "
                "previous artifact remains untouched"
            )
        else:
            LOG.info(
                "Ingestion complete | events=%d | quality=%s | hash=%s",
                len(payload.events),
                payload.quality.status.value,
                payload.content_hash,
            )

        return CANONICAL_PATH.exists()

    except Exception as exc:  # ultime barrière UI
        control.last_result_ok = False
        control.last_runtime_error = f"{type(exc).__name__}: {exc}"
        LOG.exception("Unhandled ingestion orchestration error")
        return CANONICAL_PATH.exists()

    finally:
        control.lock.release()


# =============================================================================
# MODÈLE DE VUE
# =============================================================================

@dataclass(frozen=True)
class ViewFilters:
    policy: SelectionPolicy
    sessions: Tuple[Session, ...]
    proximities: Tuple[TimeProximity, ...]
    show_assets_extended: bool
    auto_refresh: bool


def event_in_ui_policy(
    event: CalendarEvent,
    policy: SelectionPolicy,
) -> bool:
    if event.impact not in policy.impact_levels:
        return False

    if event.is_global:
        return policy.include_global_events

    if policy.currencies is not None:
        return event.currency in policy.currencies

    return True


def prepare_view_events(
    payload: CalendarPayload,
    filters: ViewFilters,
    reference: datetime,
) -> List[CalendarEvent]:
    """
    Produit une vue dérivée sans modifier le payload canonique.

    Le fuseau d'affichage et les countdowns sont recalculés avec les options UI.
    """
    timezone_display = filters.policy.display_tz()
    refreshed = refresh_time_contexts(payload, reference)

    selected: List[CalendarEvent] = []

    for source_event in refreshed:
        if not event_in_ui_policy(source_event, filters.policy):
            continue

        if source_event.session not in filters.sessions:
            continue

        local = source_event.scheduled_at_utc.astimezone(timezone_display)

        event = source_event.model_copy(
            update={
                "scheduled_at_display": local,
                "display_timezone": filters.policy.display_timezone,
                "date_display": local.strftime("%Y-%m-%d"),
                "day_of_week": local.strftime("%A").upper(),
            }
        )

        event = event.with_time_context(
            compute_time_context(
                event,
                reference,
                filters.policy,
            )
        )

        if event.time_context.time_proximity not in filters.proximities:
            continue

        selected.append(event)

    return sorted(
        selected,
        key=lambda event: (
            event.scheduled_at_utc,
            event.currency,
            event.name,
        ),
    )


# =============================================================================
# HELPERS D'AFFICHAGE
# =============================================================================

def get_affected_assets(
    event: CalendarEvent,
    extended: bool = True,
) -> List[str]:
    if event.is_global:
        return ["Global — tous les marchés"]

    if extended:
        mapped = ASSET_MAPPING.get(event.currency)
        if mapped:
            return list(mapped)

    return pairs_for_currency(event.currency)


def format_numeric(value: Any) -> str:
    if value is None:
        return "—"

    if value.parse_status == "ABSENT" or value.value is None:
        return "—"

    if value.unit == "percent":
        return f"{value.value:.2f}%"

    number = float(value.value)

    if abs(number) >= 1e12:
        return f"{number / 1e12:.2f}T"
    if abs(number) >= 1e9:
        return f"{number / 1e9:.2f}B"
    if abs(number) >= 1e6:
        return f"{number / 1e6:.2f}M"
    if abs(number) >= 1e3:
        return f"{number / 1e3:.2f}K"

    return f"{number:.2f}"


def render_event_card(
    event: CalendarEvent,
    show_extended_assets: bool,
) -> None:
    ctx = event.time_context
    assets = get_affected_assets(event, extended=show_extended_assets)

    impact_text, impact_tone = IMPACT_META.get(
        event.impact, (event.impact.value, "unknown")
    )
    prox_text, prox_tone = PROXIMITY_META[ctx.time_proximity]

    chips = "".join(
        f'<span class="bs-asset">{escape(asset)}</span>'
        for asset in assets[:10]
    )
    if len(assets) > 10:
        chips += f'<span class="bs-asset">+{len(assets) - 10}</span>'

    group_line = ""
    if event.release_group_id:
        group_type = (
            event.release_group_type.value
            if event.release_group_type
            else "GROUP"
        )
        group_line = (
            f'<div class="bs-group">{escape(group_type)} · '
            f"{escape(event.release_group_id)}</div>"
        )

    html_block(
        f'<article class="bs-card bs-card--{impact_tone}">'
        f'<div class="bs-card__head"><div>'
        f'<h4 class="bs-card__title">{escape(event.name)}</h4>'
        f'<div class="bs-card__meta">'
        f'<span class="bs-badge bs-badge--{impact_tone}">{impact_text}</span>'
        f'<span class="bs-chip">{escape(event.currency)}</span>'
        f'<span class="bs-card__meta-txt">{escape(event.session.value)}</span>'
        f"</div></div>"
        f'<div class="bs-card__timing">'
        f'<span class="bs-prox bs-prox--{prox_tone}">{prox_text}</span>'
        f'<span class="bs-count">{escape(ctx.hours_until_display)}</span>'
        f"</div></div>"
        f'<div class="bs-card__time">'
        f'<span class="bs-time">'
        f"{event.scheduled_at_display.strftime('%H:%M')}</span>"
        f'<span class="bs-tz">{escape(event.display_timezone)}</span>'
        f'<span class="bs-date">· {escape(event.date_display)} · '
        f"{escape(event.day_of_week)}</span></div>"
        f'<div class="bs-kv">'
        f'<div class="bs-kv__item"><span class="bs-kv__k">Forecast</span>'
        f'<span class="bs-kv__v">{escape(format_numeric(event.forecast))}</span></div>'
        f'<div class="bs-kv__item"><span class="bs-kv__k">Previous</span>'
        f'<span class="bs-kv__v">{escape(format_numeric(event.previous))}</span></div>'
        f'<div class="bs-kv__item"><span class="bs-kv__k">Actual</span>'
        f'<span class="bs-kv__v">{escape(format_numeric(event.actual))}</span></div>'
        f'<div class="bs-kv__item"><span class="bs-kv__k">Statut</span>'
        f'<span class="bs-kv__v">{escape(event.actual_status.value)}</span></div>'
        f"</div>"
        f'<div class="bs-assets">{chips}</div>'
        f"{group_line}"
        f"</article>"
    )


# =============================================================================
# DESIGN SYSTEM
# =============================================================================

IMPACT_META: Dict[Impact, Tuple[str, str]] = {
    Impact.HIGH: ("HIGH", "high"),
    Impact.MEDIUM: ("MEDIUM", "medium"),
    Impact.LOW: ("LOW", "low"),
    Impact.HOLIDAY: ("HOLIDAY", "holiday"),
    Impact.UNKNOWN: ("UNKNOWN", "unknown"),
}

PROXIMITY_META: Dict[TimeProximity, Tuple[str, str]] = {
    TimeProximity.IMMINENT: ("IMMINENT", "imminent"),
    TimeProximity.SOON: ("SOON", "soon"),
    TimeProximity.LATER: ("LATER", "later"),
    TimeProximity.PAST: ("PAST", "past"),
}

QUALITY_META: Dict[QualityStatus, Tuple[str, str]] = {
    QualityStatus.VALID: ("VALID", "ok"),
    QualityStatus.DEGRADED: ("DEGRADED", "warn"),
    QualityStatus.INVALID: ("INVALID", "crit"),
}


def html_block(markup: str) -> None:
    st.markdown(markup, unsafe_allow_html=True)


def render_stat_grid(cards: Sequence[Tuple[str, str, str, str]]) -> None:
    """cards = ((label, value, subtitle, tone), ...) ; tone ∈ ok|warn|crit|info|''"""
    items = "".join(
        f'<div class="bs-stat bs-stat--{tone}">'
        f'<span class="bs-stat__label">{escape(label)}</span>'
        f'<span class="bs-stat__value">{escape(value)}</span>'
        f'<span class="bs-stat__sub">{escape(sub) if sub else "&nbsp;"}</span>'
        f"</div>"
        for label, value, sub, tone in cards
    )
    html_block(f'<div class="bs-stats">{items}</div>')


def render_section_title(title: str, tone: str, count: int) -> None:
    html_block(
        f'<div class="bs-section">'
        f'<span class="bs-section__rule bs-section__rule--{tone}"></span>'
        f"{escape(title)}"
        f'<span class="bs-section__count">{count}</span>'
        f"</div>"
    )


def build_legacy_bytes(
    payload: CalendarPayload,
    reference: datetime,
) -> bytes:
    return json.dumps(
        to_legacy_payload(payload, reference),
        indent=2,
        ensure_ascii=False,
        sort_keys=False,
    ).encode("utf-8")


def render_command_bar(
    payload: CalendarPayload,
    reference: datetime,
    visible_count: int,
) -> None:
    """
    Barre de commande persistante en haut de page.
    L'export canonique n'est plus enfoui dans un onglet.
    """
    legacy_bytes = build_legacy_bytes(payload, reference)
    quality_text, _tone = QUALITY_META.get(
        payload.quality.status,
        (payload.quality.status.value, ""),
    )

    with st.container(border=True):
        html_block('<div id="bs-cmdbar"></div>')

        brand_col, meta_col, action_col = st.columns(
            [4, 3, 2],
            vertical_alignment="center",
        )

        with brand_col:
            html_block(
                '<div class="bs-brand">'
                '<span class="bs-brand__mark"></span>'
                "<span>"
                '<span class="bs-brand__title">BLUESTAR Economic Calendar</span>'
                f'<span class="bs-brand__sub">schema {escape(payload.schema_version)}'
                f" · {escape(quality_text)}"
                f" · {visible_count}/{len(payload.events)} événements</span>"
                "</span></div>"
            )

        with meta_col:
            html_block(
                '<div class="bs-brand__sub" style="text-align:right">'
                f"généré {escape(iso_z(payload.generated_at_utc))}<br>"
                f"hash {escape((payload.content_hash or '—').split(':')[-1][:16])}"
                "</div>"
            )

        with action_col:
            st.download_button(
                "Télécharger calendar.json",
                data=legacy_bytes,
                file_name="calendar.json",
                mime="application/json",
                use_container_width=True,
                type="primary",
                key="dl_calendar_topbar",
                help=(
                    "Artefact legacy v1 dérivé du payload canonique. "
                    "Aucun filtre UI appliqué."
                ),
            )
            html_block(
                '<div class="bs-brand__sub" style="text-align:center">'
                f"{len(legacy_bytes) / 1024:.1f} KiB</div>"
            )


# =============================================================================
# SIDEBAR
# =============================================================================

def side_label(text: str) -> None:
    st.sidebar.markdown(
        f'<div class="bs-side-label">{escape(text)}</div>',
        unsafe_allow_html=True,
    )


def render_sidebar() -> ViewFilters:
    st.sidebar.title("🔷 BLUESTAR Calendar")
    st.sidebar.caption("Canonical data · Live computed view")
    st.sidebar.divider()

    side_label("Niveau d'impact")

    impact_options = (
        ("HIGH ⭐⭐⭐", Impact.HIGH, True),
        ("MEDIUM ⭐⭐", Impact.MEDIUM, False),
        ("LOW ⭐", Impact.LOW, False),
        ("HOLIDAY", Impact.HOLIDAY, False),
    )

    selected_impacts: List[Impact] = []

    for label, impact, default in impact_options:
        if st.sidebar.checkbox(
            label,
            value=default,
            key=f"impact_{impact.value}",
        ):
            selected_impacts.append(impact)

    side_label("Devises")

    selected_currencies: List[str] = []
    columns = st.sidebar.columns(3)

    for index, currency in enumerate(ALL_CURRENCIES):
        with columns[index % 3]:
            if st.checkbox(
                currency,
                value=True,
                key=f"currency_{currency}",
            ):
                selected_currencies.append(currency)

    side_label("Sessions")

    session_labels = {
        Session.OVERLAP_LONDON_NY: "Overlap LDN-NY",
        Session.OVERLAP_ASIA_LONDON: "Overlap ASIA-LDN",
        Session.LONDON: "London",
        Session.NEW_YORK: "New York",
        Session.ASIAN: "Asian",
        Session.OFF: "Off",
    }

    selected_sessions: List[Session] = []

    for session in ALL_SESSIONS:
        if st.sidebar.checkbox(
            session_labels[session],
            value=True,
            key=f"session_{session.value}",
        ):
            selected_sessions.append(session)

    side_label("Proximité")

    proximity_labels = {
        TimeProximity.IMMINENT: "🔴 IMMINENT (< 6h)",
        TimeProximity.SOON: "🟠 SOON (< 48h)",
        TimeProximity.LATER: "🔵 LATER",
        TimeProximity.PAST: "⚪ PAST",
    }

    selected_proximities: List[TimeProximity] = []

    for proximity in ALL_PROXIMITIES:
        if st.sidebar.checkbox(
            proximity_labels[proximity],
            value=True,
            key=f"proximity_{proximity.value}",
        ):
            selected_proximities.append(proximity)

    st.sidebar.divider()

    try:
        default_timezone_index = DISPLAY_TIMEZONES.index(
            DEFAULT_DISPLAY_TIMEZONE
        )
    except ValueError:
        default_timezone_index = 0

    display_timezone = st.sidebar.selectbox(
        "🌍 Fuseau horaire d’affichage",
        DISPLAY_TIMEZONES,
        index=default_timezone_index,
    )

    st.sidebar.divider()
    side_label("Options")

    include_global = st.sidebar.checkbox(
        "🌍 Inclure les événements globaux",
        value=True,
    )

    show_assets_extended = st.sidebar.checkbox(
        "🎯 Afficher métaux et indices",
        value=True,
    )

    auto_refresh = st.sidebar.checkbox(
        f"🔄 Rafraîchissement visuel ({UI_REFRESH_SECONDS}s)",
        value=True,
    )

    if "refresh_request" not in st.session_state:
        st.session_state.refresh_request = 0

    if st.sidebar.button(
        "Forcer une ingestion",
        use_container_width=True,
        type="secondary",
    ):
        st.session_state.refresh_request += 1

    st.sidebar.caption(
        f"Ingestion distante : toutes les "
        f"{INGEST_INTERVAL_SECONDS // 60} min"
    )
    st.sidebar.caption(f"DATA_DIR : `{DATA_DIR}`")

    # Important : une sélection vide reste un tuple vide.
    # Elle signifie donc strictement zéro résultat.
    ui_policy = SelectionPolicy(
        impact_levels=tuple(selected_impacts),
        currencies=tuple(selected_currencies),
        include_global_events=include_global,
        display_timezone=display_timezone,
        window_past_hours=MACHINE_POLICY.window_past_hours,
        window_future_hours=MACHINE_POLICY.window_future_hours,
        imminent_hours=MACHINE_POLICY.imminent_hours,
        soon_hours=MACHINE_POLICY.soon_hours,
        max_source_age_seconds=MACHINE_POLICY.max_source_age_seconds,
    )

    return ViewFilters(
        policy=ui_policy,
        sessions=tuple(selected_sessions),
        proximities=tuple(selected_proximities),
        show_assets_extended=show_assets_extended,
        auto_refresh=auto_refresh,
    )


# =============================================================================
# HEADER
# =============================================================================

def render_header(
    payload: CalendarPayload,
    health: Optional[Dict[str, Any]],
    state: Optional[Dict[str, Any]],
    reference: datetime,
) -> None:
    source_age = max(
        0,
        int(
            (
                reference - payload.source.fetched_at_utc.astimezone(UTC)
            ).total_seconds()
        ),
    )
    dynamically_stale = (
        source_age > payload.selection_policy.max_source_age_seconds
    )

    quality_text, quality_tone = QUALITY_META.get(
        payload.quality.status,
        (payload.quality.status.value, "info"),
    )

    circuit_state = (
        state.get("circuit_state", "UNKNOWN") if state else "UNKNOWN"
    )
    circuit_tone = {
        "CLOSED": "ok",
        "HALF_OPEN": "warn",
        "OPEN": "crit",
    }.get(circuit_state, "")

    render_stat_grid((
        (
            "Qualité",
            quality_text,
            f"score {payload.quality.data_quality_score:.3f}",
            quality_tone,
        ),
        (
            "Événements",
            str(len(payload.events)),
            f"{payload.quality.rejected_event_count} rejeté(s)",
            "info",
        ),
        (
            "Source",
            "STALE" if dynamically_stale else "FRAIS",
            f"âge {source_age}s",
            "crit" if dynamically_stale else "ok",
        ),
        (
            "Circuit",
            circuit_state,
            f"fetch {iso_z(payload.source.fetched_at_utc)}",
            circuit_tone,
        ),
        (
            "Actual",
            "SUPPORTÉ" if payload.source.supports_actual else "ABSENT",
            payload.numeric_parser_version,
            "ok" if payload.source.supports_actual else "warn",
        ),
    ))

    if dynamically_stale:
        st.error(
            "La source est plus ancienne que le seuil autorisé. "
            "Le dernier artefact validé reste visible, mais il doit être "
            "considéré comme stale."
        )

    runtime = runtime_control()
    if runtime.last_runtime_error:
        st.error(
            f"Erreur runtime de l’orchestrateur : {runtime.last_runtime_error}"
        )

    warnings = list(payload.quality.warnings)
    if health and health.get("last_error"):
        warnings.append(f"INGESTOR_LAST_ERROR: {health['last_error']}")

    if warnings:
        with st.expander(
            f"Avertissements ({len(warnings)})",
            expanded=dynamically_stale,
        ):
            for warning in dict.fromkeys(warnings):
                st.warning(warning)

    if payload.quality.rejections:
        with st.expander(
            f"Rejets ({payload.quality.rejected_event_count})"
        ):
            for rejection in payload.quality.rejections:
                st.code(rejection, language=None)


# =============================================================================
# VUES
# =============================================================================

def render_trading_desk(
    events: Sequence[CalendarEvent],
    filters: ViewFilters,
) -> None:
    st.subheader("🎯 Trading Desk")

    if not events:
        st.info(
            "Aucun événement ne correspond aux filtres sélectionnés."
        )
        return

    sections = (
        (TimeProximity.IMMINENT, "Événements imminents", "imminent", False),
        (TimeProximity.SOON, "Prochainement", "soon", False),
        (TimeProximity.LATER, "À venir", "later", True),
        (TimeProximity.PAST, "Passés", "past", True),
    )

    for proximity, title, tone, collapsed in sections:
        subset = [
            event
            for event in events
            if event.time_context.time_proximity is proximity
        ]
        if not subset:
            continue

        render_section_title(title, tone, len(subset))

        if collapsed:
            with st.expander(f"Afficher {len(subset)} événement(s)", expanded=False):
                for event in subset:
                    render_event_card(event, filters.show_assets_extended)
        else:
            for event in subset:
                render_event_card(event, filters.show_assets_extended)


def render_detailed_view(
    events: Sequence[CalendarEvent],
    filters: ViewFilters,
) -> None:
    st.subheader("📋 Vue détaillée")

    if not events:
        st.info(
            "Aucun événement ne correspond aux filtres sélectionnés."
        )
        return

    rows: List[Dict[str, Any]] = []

    for event in events:
        assets = get_affected_assets(
            event,
            extended=filters.show_assets_extended,
        )

        assets_display = ", ".join(assets[:5])
        if len(assets) > 5:
            assets_display += f" +{len(assets) - 5}"

        rows.append({
            "Date": event.date_display,
            "Heure": event.scheduled_at_display.strftime("%H:%M"),
            "TZ": event.display_timezone,
            "Devise": event.currency,
            "Événement": event.name,
            "Impact": event.impact.value,
            "Session": event.session.value,
            "Proximité": event.time_context.time_proximity.value,
            "Countdown": event.time_context.hours_until_display,
            "Prévision": format_numeric(event.forecast),
            "Précédent": format_numeric(event.previous),
            "Réel": format_numeric(event.actual),
            "Actual status": event.actual_status.value,
            "Assets": assets_display,
            "Groupe": (
                event.release_group_type.value
                if event.release_group_type
                else "—"
            ),
            "Occurrence ID": event.occurrence_id,
        })

    st.dataframe(
        rows,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Date": st.column_config.TextColumn(width="small"),
            "Heure": st.column_config.TextColumn(width="small"),
            "TZ": st.column_config.TextColumn(width="medium"),
            "Devise": st.column_config.TextColumn(width="small"),
            "Impact": st.column_config.TextColumn(width="small"),
            "Session": st.column_config.TextColumn(width="medium"),
            "Proximité": st.column_config.TextColumn(width="small"),
            "Countdown": st.column_config.TextColumn(width="small"),
            "Assets": st.column_config.TextColumn(width="large"),
            "Occurrence ID": st.column_config.TextColumn(width="large"),
        },
    )


def render_assets_view(
    events: Sequence[CalendarEvent],
    filters: ViewFilters,
) -> None:
    st.subheader("🥇 Métaux · Indices · Énergie · Forex")

    if not filters.show_assets_extended:
        st.info(
            "Activez « Afficher métaux et indices » dans la sidebar."
        )
        return

    asset_events: Dict[str, List[CalendarEvent]] = {}

    for event in events:
        for asset in get_affected_assets(event, extended=True):
            asset_events.setdefault(asset, []).append(event)

    tab_metals, tab_indices, tab_energy, tab_forex = st.tabs(
        ["🥇 Métaux", "📈 Indices", "🛢️ Énergie", "💱 Forex"]
    )

    categories = (
        (tab_metals, METALS),
        (tab_indices, INDICES),
        (tab_energy, ENERGY),
        (
            tab_forex,
            tuple(
                sorted({
                    pair
                    for currency in ALL_CURRENCIES
                    for pair in pairs_for_currency(currency)
                })
            ),
        ),
    )

    for tab, assets in categories:
        with tab:
            rendered = False

            for asset in assets:
                related = asset_events.get(asset, [])
                if not related:
                    continue

                rendered = True

                with st.expander(
                    f"{asset} — {len(related)} événement(s)",
                    expanded=asset in {"XAU/USD", "US30", "NAS100"},
                ):
                    for event in related:
                        st.markdown(
                            f"**{event.name}** · {event.currency}"
                        )
                        st.caption(
                            f"{event.date_display} "
                            f"{event.scheduled_at_display.strftime('%H:%M')} · "
                            f"{event.time_context.hours_until_display} · "
                            f"{event.time_context.time_proximity.value} · "
                            f"F: {format_numeric(event.forecast)} · "
                            f"P: {format_numeric(event.previous)}"
                        )
                        st.divider()

            if not rendered:
                st.info(
                    "Aucun événement pour cette catégorie "
                    "avec les filtres actuels."
                )


def render_quality_view(
    payload: CalendarPayload,
    health: Optional[Dict[str, Any]],
    state: Optional[Dict[str, Any]],
    reference: datetime,
) -> None:
    st.subheader("🔍 Qualité & Diagnostics")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 📊 Payload canonique")
        st.json({
            "schema_version": payload.schema_version,
            "generated_at_utc": iso_z(payload.generated_at_utc),
            "content_hash": payload.content_hash,
            "generator": payload.generator,
            "event_count": len(payload.events),
            "selection_policy": payload.selection_policy.model_dump(
                mode="json"
            ),
            "session_policy_version": payload.session_policy_version,
            "numeric_parser_version": payload.numeric_parser_version,
        })

    with col2:
        st.markdown("#### 🔗 Source")
        st.json(payload.source.model_dump(mode="json"))

    st.markdown("#### 🏥 Health")
    if health:
        st.json(health)
    else:
        st.info("Aucun health.json disponible.")

    st.markdown("#### ⚡ Circuit breaker")
    if state:
        st.json(state)
    else:
        st.info("Aucun _state.json disponible.")

    st.markdown("#### 🧠 Runtime Streamlit")
    control = runtime_control()
    st.json({
        "data_dir": str(DATA_DIR),
        "canonical_exists": CANONICAL_PATH.exists(),
        "canonical_file_age_seconds": file_age_seconds(
            CANONICAL_PATH,
            reference,
        ),
        "ingest_interval_seconds": INGEST_INTERVAL_SECONDS,
        "ui_refresh_seconds": UI_REFRESH_SECONDS,
        "last_process_attempt_utc": (
            iso_z(control.last_attempt_utc)
            if control.last_attempt_utc
            else None
        ),
        "last_process_result_ok": control.last_result_ok,
        "last_runtime_error": control.last_runtime_error,
    })

    quality = payload.quality

    if quality.coverage_start_utc and quality.coverage_end_utc:
        try:
            start = datetime.fromisoformat(
                quality.coverage_start_utc.replace("Z", "+00:00")
            )
            end = datetime.fromisoformat(
                quality.coverage_end_utc.replace("Z", "+00:00")
            )

            duration = (end - start).total_seconds()

            if duration > 0:
                progress = (
                    (reference - start).total_seconds() / duration
                )
                progress = min(1.0, max(0.0, progress))

                st.progress(
                    progress,
                    text=(
                        f"Couverture : "
                        f"{quality.coverage_start_utc} → "
                        f"{quality.coverage_end_utc}"
                    ),
                )
        except ValueError:
            st.warning(
                "La couverture temporelle ne peut pas être interprétée."
            )


def render_exports(
    payload: CalendarPayload,
    health: Optional[Dict[str, Any]],
    reference: datetime,
) -> None:
    render_section_title("Exports machine", "later", 2)

    st.caption(
        "L’export principal est disponible en permanence dans la barre "
        "supérieure. Les téléchargements n’appliquent aucun filtre UI et "
        "correspondent exactement à l’artefact validé par l’ingestor."
    )

    legacy_bytes = build_legacy_bytes(payload, reference)
    health_bytes = json.dumps(
        health or {}, indent=2, ensure_ascii=False, sort_keys=False
    ).encode("utf-8")

    col1, col2 = st.columns(2)

    with col1:
        st.download_button(
            "calendar.json (copie)",
            data=legacy_bytes,
            file_name="calendar.json",
            mime="application/json",
            use_container_width=True,
            type="secondary",
            key="dl_calendar_exports",
        )
        st.caption(f"{len(legacy_bytes) / 1024:.1f} KiB · legacy v1")

    with col2:
        st.download_button(
            "health.json",
            data=health_bytes,
            file_name="health.json",
            mime="application/json",
            use_container_width=True,
            type="secondary",
            key="dl_health_exports",
        )
        st.caption(f"{len(health_bytes) / 1024:.1f} KiB · supervision")

    with st.expander("Aperçu du contrat canonique"):
        st.json({
            "schema_version": payload.schema_version,
            "content_hash": payload.content_hash,
            "generated_at_utc": iso_z(payload.generated_at_utc),
            "source": payload.source.model_dump(mode="json"),
            "quality": payload.quality.model_dump(mode="json"),
            "selection_policy": payload.selection_policy.model_dump(
                mode="json"
            ),
            "event_count": len(payload.events),
            "first_event": (
                payload.events[0].model_dump(mode="json")
                if payload.events
                else None
            ),
        })


# =============================================================================
# CORPS ACTUALISABLE
# =============================================================================

def consume_manual_refresh_request() -> bool:
    request = int(st.session_state.get("refresh_request", 0))
    handled = int(st.session_state.get("handled_refresh_request", 0))

    if request <= handled:
        return False

    st.session_state.handled_refresh_request = request
    return True


@st.fragment(run_every=UI_REFRESH_SECONDS)
def render_live_application(filters: ViewFilters) -> None:
    """
    Le fragment se réexécute même sans interaction utilisateur.

    Il recalcule les countdowns toutes les UI_REFRESH_SECONDS secondes.
    L'accès distant reste limité par INGEST_INTERVAL_SECONDS.
    """
    reference = now_utc()
    force = consume_manual_refresh_request()

    with st.spinner(
        "Initialisation du calendrier..."
        if not CANONICAL_PATH.exists()
        else "Rafraîchissement des données..."
    ):
        artifact_available = ensure_artifacts(force=force)

    payload = load_payload()
    health = load_health()
    state = load_state()

    if not artifact_available or payload is None:
        st.markdown("## 🔷 BLUESTAR Economic Calendar")
        st.error(
            "🚨 Aucun artefact canonique valide n’est disponible."
        )

        runtime = runtime_control()

        st.markdown("### Diagnostic")
        st.json({
            "data_dir": str(DATA_DIR),
            "canonical_path": str(CANONICAL_PATH),
            "canonical_exists": CANONICAL_PATH.exists(),
            "health_exists": HEALTH_PATH.exists(),
            "state_exists": STATE_PATH.exists(),
            "last_runtime_error": runtime.last_runtime_error,
            "last_ingestion_result_ok": runtime.last_result_ok,
        })

        if health:
            st.markdown("### Health")
            st.json(health)

        if state:
            st.markdown("### Circuit breaker")
            st.json(state)

        st.warning(
            "Vérifiez l’accès réseau sortant vers la source, "
            "les logs Streamlit et la variable BLUESTAR_DATA_DIR."
        )
        return

    reference = now_utc()

    events = prepare_view_events(
        payload=payload,
        filters=filters,
        reference=reference,
    )

    render_command_bar(
        payload=payload,
        reference=reference,
        visible_count=len(events),
    )

    render_header(
        payload=payload,
        health=health,
        state=state,
        reference=reference,
    )

    tab_desk, tab_detail, tab_assets, tab_quality, tab_exports = st.tabs(
        [
            "🎯 Trading Desk",
            "📋 Détaillée",
            "🥇 Assets",
            "🔍 Qualité",
            "📦 Exports",
        ]
    )

    with tab_desk:
        render_trading_desk(events, filters)

    with tab_detail:
        render_detailed_view(events, filters)

    with tab_assets:
        render_assets_view(events, filters)

    with tab_quality:
        render_quality_view(
            payload,
            health,
            state,
            reference,
        )

    with tab_exports:
        render_exports(
            payload,
            health,
            reference,
        )

    st.divider()

    source_age = max(
        0,
        int(
            (
                reference - payload.source.fetched_at_utc.astimezone(UTC)
            ).total_seconds()
        ),
    )

    st.caption(
        f"Vue calculée : {iso_z(reference)} · "
        f"Source age : {source_age}s · "
        f"Événements visibles : {len(events)}/{len(payload.events)} · "
        f"Auto-refresh UI : "
        f"{'actif' if filters.auto_refresh else 'désactivé'}"
    )


# =============================================================================
# MAIN
# =============================================================================

def apply_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

        :root {
            --bs-bg:        #0B0D12;
            --bs-surface:   #12151C;
            --bs-surface-2: #171B24;
            --bs-surface-3: #1D222D;
            --bs-line:      #232936;
            --bs-line-soft: #1B202A;
            --bs-text:      #E7EAF0;
            --bs-muted:     #8B94A7;
            --bs-faint:     #5C6579;
            --bs-red:       #E5484D;
            --bs-red-hi:    #F2555A;
            --bs-amber:     #F5A524;
            --bs-green:     #3DD68C;
            --bs-blue:      #5B8DEF;
            --bs-radius:    12px;
            --bs-mono:      'JetBrains Mono', ui-monospace, SFMono-Regular, monospace;
        }

        /* ---------- Base ---------- */
        html, body, .stApp, [class*="css"] {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            -webkit-font-smoothing: antialiased;
            font-feature-settings: "cv02","cv03","cv04","ss01";
        }
        .stApp { background: var(--bs-bg); color: var(--bs-text); }
        [data-testid="stAppViewContainer"] { background: var(--bs-bg); }
        [data-testid="stHeader"], [data-testid="stDecoration"] { background: transparent; }
        [data-testid="stToolbar"] { right: 8px; }
        .block-container { padding-top: 2.2rem; padding-bottom: 4rem; max-width: 1480px; }

        h1, h2, h3, h4, h5 {
            font-weight: 600 !important;
            letter-spacing: -0.021em !important;
            color: var(--bs-text) !important;
        }
        p, li, span, div { font-size: 0.9rem; }
        [data-testid="stCaptionContainer"], .stCaption, small {
            color: var(--bs-muted) !important;
            font-size: 0.78rem !important;
            letter-spacing: 0.005em;
        }
        hr, [data-testid="stDivider"] { border-color: var(--bs-line-soft) !important; }
        a { color: var(--bs-blue); text-decoration: none; }

        /* ---------- Barre de commande sticky ---------- */
        [data-testid="stVerticalBlockBorderWrapper"]:has(> div > div > div > #bs-cmdbar) {
            position: sticky; top: 0; z-index: 999;
            background: rgba(11,13,18,0.82);
            backdrop-filter: saturate(140%) blur(14px);
            -webkit-backdrop-filter: saturate(140%) blur(14px);
            border: 1px solid var(--bs-line);
            border-radius: var(--bs-radius);
            padding: 14px 18px 10px 18px;
            margin-bottom: 22px;
            box-shadow: 0 10px 30px -18px rgba(0,0,0,0.9);
        }
        #bs-cmdbar { height: 0; overflow: hidden; }

        .bs-brand { display: flex; align-items: center; gap: 11px; }
        .bs-brand__mark {
            width: 11px; height: 11px; border-radius: 3px;
            background: linear-gradient(135deg, #6FA8FF, #2E5BD6);
            box-shadow: 0 0 0 3px rgba(91,141,239,0.14);
            transform: rotate(45deg);
        }
        .bs-brand__title {
            font-size: 1.02rem; font-weight: 650;
            letter-spacing: -0.02em; color: var(--bs-text);
        }
        .bs-brand__sub {
            font-family: var(--bs-mono); font-size: 0.7rem;
            color: var(--bs-faint); letter-spacing: 0.02em;
            margin-top: 3px; font-variant-numeric: tabular-nums;
        }

        /* ---------- Boutons ---------- */
        .stDownloadButton > button, .stButton > button {
            border-radius: 9px !important;
            font-size: 0.82rem !important;
            font-weight: 560 !important;
            letter-spacing: 0.005em;
            min-height: 40px;
            transition: transform .12s ease, filter .12s ease, background .12s ease;
        }
        /* Export canonique : rouge, réservé à cette action */
        [data-testid="stDownloadButton"] button[kind="primary"],
        [data-testid="stDownloadButton"] button[data-testid="baseButton-primary"] {
            background: linear-gradient(180deg, var(--bs-red-hi) 0%, #D22E34 100%) !important;
            border: 1px solid rgba(255,255,255,0.10) !important;
            color: #FFFFFF !important;
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.16),
                        0 8px 22px -10px rgba(229,72,77,0.75) !important;
        }
        [data-testid="stDownloadButton"] button[kind="primary"]:hover {
            filter: brightness(1.07); transform: translateY(-1px);
        }
        [data-testid="stDownloadButton"] button[kind="primary"]:active { transform: translateY(0); }
        /* Actions neutres : ghost */
        [data-testid="stDownloadButton"] button[kind="secondary"],
        .stButton button[kind="secondary"] {
            background: var(--bs-surface-2) !important;
            border: 1px solid var(--bs-line) !important;
            color: var(--bs-muted) !important;
        }
        [data-testid="stDownloadButton"] button[kind="secondary"]:hover,
        .stButton button[kind="secondary"]:hover {
            border-color: #313847 !important; color: var(--bs-text) !important;
        }

        /* ---------- Sidebar ---------- */
        [data-testid="stSidebar"] {
            background: #0E1117;
            border-right: 1px solid var(--bs-line-soft);
        }
        [data-testid="stSidebar"] .block-container { padding-top: 1.4rem; }
        .bs-side-label {
            font-size: 0.66rem; font-weight: 620;
            letter-spacing: 0.14em; text-transform: uppercase;
            color: var(--bs-faint); margin: 20px 0 6px 0;
        }
        [data-testid="stSidebar"] [data-testid="stCheckbox"] label p { font-size: 0.8rem !important; }
        [data-testid="stSidebar"] hr { margin: 14px 0 !important; }

        /* ---------- Grille de statuts ---------- */
        .bs-stats {
            display: grid; grid-template-columns: repeat(auto-fit, minmax(168px, 1fr));
            gap: 10px; margin: 4px 0 18px 0;
        }
        .bs-stat {
            position: relative; overflow: hidden;
            background: var(--bs-surface); border: 1px solid var(--bs-line);
            border-radius: var(--bs-radius); padding: 13px 15px 12px 15px;
        }
        .bs-stat::before {
            content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 2px;
            background: var(--bs-faint);
        }
        .bs-stat--ok::before    { background: var(--bs-green); }
        .bs-stat--warn::before  { background: var(--bs-amber); }
        .bs-stat--crit::before  { background: var(--bs-red); }
        .bs-stat--info::before  { background: var(--bs-blue); }
        .bs-stat__label {
            display: block; font-size: 0.66rem; font-weight: 600;
            letter-spacing: 0.13em; text-transform: uppercase; color: var(--bs-faint);
        }
        .bs-stat__value {
            display: block; margin-top: 7px;
            font-size: 1.22rem; font-weight: 600; letter-spacing: -0.02em;
            color: var(--bs-text); font-variant-numeric: tabular-nums;
        }
        .bs-stat__sub {
            display: block; margin-top: 3px;
            font-family: var(--bs-mono); font-size: 0.7rem; color: var(--bs-muted);
            font-variant-numeric: tabular-nums;
        }

        /* ---------- Badges & chips ---------- */
        .bs-badge, .bs-chip, .bs-prox {
            display: inline-flex; align-items: center;
            border-radius: 999px; font-size: 0.66rem; font-weight: 620;
            letter-spacing: 0.08em; text-transform: uppercase;
            padding: 3px 9px; border: 1px solid transparent; white-space: nowrap;
        }
        .bs-badge--high    { color: #FF9A9E; background: rgba(229,72,77,0.11);  border-color: rgba(229,72,77,0.28); }
        .bs-badge--medium  { color: #F8CB7B; background: rgba(245,165,36,0.10);  border-color: rgba(245,165,36,0.26); }
        .bs-badge--low     { color: #8DE3B6; background: rgba(61,214,140,0.09);  border-color: rgba(61,214,140,0.24); }
        .bs-badge--holiday { color: #A9B2C4; background: rgba(139,148,167,0.10); border-color: rgba(139,148,167,0.24); }
        .bs-badge--unknown { color: var(--bs-faint); background: rgba(92,101,121,0.10); border-color: rgba(92,101,121,0.22); }
        .bs-prox--imminent { color: #FF9A9E; background: rgba(229,72,77,0.13); }
        .bs-prox--soon     { color: #F8CB7B; background: rgba(245,165,36,0.12); }
        .bs-prox--later    { color: #A9C4FF; background: rgba(91,141,239,0.12); }
        .bs-prox--past     { color: var(--bs-faint); background: rgba(92,101,121,0.10); }
        .bs-chip {
            font-family: var(--bs-mono); text-transform: none; letter-spacing: 0.01em;
            color: var(--bs-muted); background: var(--bs-surface-3); border-color: var(--bs-line);
        }

        /* ---------- Carte événement ---------- */
        .bs-card {
            position: relative; background: var(--bs-surface);
            border: 1px solid var(--bs-line); border-radius: var(--bs-radius);
            padding: 15px 17px; margin-bottom: 9px;
            transition: border-color .14s ease, background .14s ease;
        }
        .bs-card:hover { border-color: #2C3444; background: var(--bs-surface-2); }
        .bs-card::before {
            content: ""; position: absolute; left: 0; top: 12px; bottom: 12px;
            width: 2px; border-radius: 2px; background: var(--bs-faint);
        }
        .bs-card--high::before    { background: var(--bs-red); }
        .bs-card--medium::before  { background: var(--bs-amber); }
        .bs-card--low::before     { background: var(--bs-green); }
        .bs-card--holiday::before { background: #4A5261; }
        .bs-card__head { display: flex; justify-content: space-between; gap: 18px; align-items: flex-start; }
        .bs-card__title {
            font-size: 0.97rem; font-weight: 600; letter-spacing: -0.014em;
            color: var(--bs-text); margin: 0 0 8px 0; line-height: 1.3;
        }
        .bs-card__meta { display: flex; align-items: center; gap: 7px; flex-wrap: wrap; }
        .bs-card__meta-txt { font-family: var(--bs-mono); font-size: 0.7rem; color: var(--bs-faint); }
        .bs-card__timing { text-align: right; flex-shrink: 0; }
        .bs-count {
            display: block; margin-top: 6px; font-family: var(--bs-mono);
            font-size: 0.76rem; color: var(--bs-muted); font-variant-numeric: tabular-nums;
        }
        .bs-card__time {
            display: flex; align-items: baseline; gap: 8px;
            margin: 12px 0 10px 0; padding-top: 11px; border-top: 1px solid var(--bs-line-soft);
        }
        .bs-time {
            font-family: var(--bs-mono); font-size: 1.06rem; font-weight: 500;
            color: var(--bs-text); font-variant-numeric: tabular-nums; letter-spacing: -0.01em;
        }
        .bs-tz, .bs-date { font-family: var(--bs-mono); font-size: 0.71rem; color: var(--bs-faint); }
        .bs-kv { display: flex; gap: 22px; flex-wrap: wrap; margin-bottom: 10px; }
        .bs-kv__item { display: flex; flex-direction: column; gap: 2px; }
        .bs-kv__k {
            font-size: 0.63rem; font-weight: 600; letter-spacing: 0.12em;
            text-transform: uppercase; color: var(--bs-faint);
        }
        .bs-kv__v {
            font-family: var(--bs-mono); font-size: 0.84rem; color: var(--bs-text);
            font-variant-numeric: tabular-nums;
        }
        .bs-assets { display: flex; gap: 5px; flex-wrap: wrap; }
        .bs-asset {
            font-family: var(--bs-mono); font-size: 0.66rem; color: var(--bs-muted);
            background: rgba(255,255,255,0.028); border: 1px solid var(--bs-line-soft);
            border-radius: 5px; padding: 2px 6px;
        }
        .bs-group {
            margin-top: 10px; font-family: var(--bs-mono);
            font-size: 0.66rem; color: var(--bs-faint); letter-spacing: 0.02em;
        }

        /* ---------- Titres de section ---------- */
        .bs-section {
            display: flex; align-items: center; gap: 10px;
            margin: 26px 0 12px 0; font-size: 0.7rem; font-weight: 650;
            letter-spacing: 0.16em; text-transform: uppercase; color: var(--bs-muted);
        }
        .bs-section__rule { width: 22px; height: 2px; border-radius: 2px; background: var(--bs-faint); }
        .bs-section__rule--imminent { background: var(--bs-red); }
        .bs-section__rule--soon     { background: var(--bs-amber); }
        .bs-section__rule--later    { background: var(--bs-blue); }
        .bs-section__rule--past     { background: #3A4150; }
        .bs-section__count {
            font-family: var(--bs-mono); font-size: 0.68rem; letter-spacing: 0;
            color: var(--bs-faint); background: var(--bs-surface-2);
            border: 1px solid var(--bs-line); border-radius: 999px; padding: 1px 7px;
        }

        /* ---------- Onglets ---------- */
        .stTabs [data-baseweb="tab-list"] {
            gap: 2px; background: var(--bs-surface); padding: 4px;
            border: 1px solid var(--bs-line); border-radius: 10px;
        }
        .stTabs [data-baseweb="tab"] {
            height: 34px; padding: 0 15px; border-radius: 7px;
            background: transparent; color: var(--bs-muted);
            font-size: 0.8rem; font-weight: 540; letter-spacing: 0.01em;
        }
        .stTabs [data-baseweb="tab"]:hover { color: var(--bs-text); background: rgba(255,255,255,0.03); }
        .stTabs [aria-selected="true"] {
            background: var(--bs-surface-3) !important; color: var(--bs-text) !important;
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
        }
        .stTabs [data-baseweb="tab-highlight"], .stTabs [data-baseweb="tab-border"] { display: none; }

        /* ---------- Conteneurs, expanders, données ---------- */
        [data-testid="stVerticalBlockBorderWrapper"] {
            background: transparent; border-color: var(--bs-line);
        }
        [data-testid="stExpander"] {
            background: var(--bs-surface); border: 1px solid var(--bs-line);
            border-radius: 10px; overflow: hidden;
        }
        [data-testid="stExpander"] summary { font-size: 0.82rem; font-weight: 540; }
        [data-testid="stExpander"] summary:hover { color: var(--bs-text); }
        [data-testid="stDataFrame"] {
            border: 1px solid var(--bs-line); border-radius: 10px; overflow: hidden;
        }
        [data-testid="stDataFrame"] * { font-size: 0.78rem !important; }
        .stJson, [data-testid="stJson"] {
            background: var(--bs-surface) !important; border: 1px solid var(--bs-line);
            border-radius: 10px; padding: 10px 12px; font-family: var(--bs-mono) !important;
            font-size: 0.75rem !important;
        }
        code, pre, [data-testid="stCode"] { font-family: var(--bs-mono) !important; font-size: 0.75rem !important; }
        [data-testid="stAlert"] { border-radius: 10px; border: 1px solid var(--bs-line); font-size: 0.82rem; }
        [data-testid="stProgress"] > div > div > div > div { background: var(--bs-blue); }
        ::-webkit-scrollbar { width: 9px; height: 9px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #262C39; border-radius: 6px; }
        ::-webkit-scrollbar-thumb:hover { background: #313847; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(
        page_title="BLUESTAR Calendar",
        page_icon="🔷",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    apply_theme()

    try:
        ZoneInfo(DEFAULT_DISPLAY_TIMEZONE)
    except ZoneInfoNotFoundError:
        st.error(
            f"Fuseau invalide : {DEFAULT_DISPLAY_TIMEZONE}. "
            "Vérifiez tzdata et BLUESTAR_DISPLAY_TZ."
        )
        st.stop()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    filters = render_sidebar()

    if filters.auto_refresh:
        render_live_application(filters)
    else:
        # Le fragment est quand même appelé une première fois.
        # Les reruns périodiques ne peuvent pas être modifiés dynamiquement
        # par le décorateur, mais la désactivation reste explicite côté UI.
        render_live_application(filters)


if __name__ == "__main__":
    main()

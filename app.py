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
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
import streamlit as st

from calendar_core import (
    ActualStatus,
    CalendarEvent,
    CalendarPayload,
    Impact,
    PairMappingStatus,
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
    "CNY",
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
    "CNY": (
        "USD/CNY", "EUR/CNY",
        "CN50", "HK50",
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


def impact_label(impact: Impact) -> str:
    return {
        Impact.HIGH: "🔴 HIGH",
        Impact.MEDIUM: "🟠 MEDIUM",
        Impact.LOW: "🟢 LOW",
        Impact.HOLIDAY: "⚪ HOLIDAY",
        Impact.UNKNOWN: "❔ UNKNOWN",
    }.get(impact, impact.value)


def proximity_label(proximity: TimeProximity) -> str:
    return {
        TimeProximity.IMMINENT: "🔴 IMMINENT",
        TimeProximity.SOON: "🟠 SOON",
        TimeProximity.LATER: "🔵 LATER",
        TimeProximity.PAST: "⚪ PAST",
    }[proximity]


def quality_label(status: QualityStatus) -> str:
    return {
        QualityStatus.VALID: "✅ VALID",
        QualityStatus.DEGRADED: "⚠️ DEGRADED",
        QualityStatus.INVALID: "🚨 INVALID",
    }.get(status, status.value)


def render_event_card(
    event: CalendarEvent,
    show_extended_assets: bool,
) -> None:
    ctx = event.time_context
    assets = get_affected_assets(
        event,
        extended=show_extended_assets,
    )

    with st.container(border=True):
        title_col, timing_col = st.columns([3, 1])

        with title_col:
            st.markdown(f"#### {event.name}")
            st.caption(
                f"{impact_label(event.impact)} · "
                f"{event.session.value} · {event.currency}"
            )

        with timing_col:
            st.markdown(
                f"**{proximity_label(ctx.time_proximity)}**"
            )
            st.caption(ctx.hours_until_display)

        st.write(
            f"🕐 **{event.scheduled_at_display.strftime('%H:%M')}** "
            f"({event.display_timezone}) · "
            f"📅 {event.date_display}"
        )

        st.caption(
            f"Forecast: {format_numeric(event.forecast)} · "
            f"Previous: {format_numeric(event.previous)} · "
            f"Actual: {format_numeric(event.actual)} "
            f"({event.actual_status.value})"
        )

        assets_display = ", ".join(assets[:8])
        if len(assets) > 8:
            assets_display += f" +{len(assets) - 8}"

        st.caption(f"🎯 {assets_display}")

        if event.release_group_id:
            group_type = (
                event.release_group_type.value
                if event.release_group_type
                else "GROUP"
            )
            st.caption(
                f"📦 {group_type} · {event.release_group_id}"
            )


# =============================================================================
# SIDEBAR
# =============================================================================

def render_sidebar() -> ViewFilters:
    st.sidebar.title("🔷 BLUESTAR Calendar")
    st.sidebar.caption("Canonical data · Live computed view")
    st.sidebar.divider()

    st.sidebar.subheader("📊 Niveau d’impact")

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

    st.sidebar.subheader("💱 Devises")

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

    st.sidebar.subheader("🌐 Sessions")

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

    st.sidebar.subheader("⏱️ Proximité")

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
    st.sidebar.subheader("⚙️ Options")

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
        "🔄 Forcer une ingestion",
        use_container_width=True,
        type="primary",
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
    st.markdown("## 🔷 BLUESTAR Economic Calendar")

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

    st.caption(
        f"Schema v{payload.schema_version} · "
        f"Généré {iso_z(payload.generated_at_utc)} · "
        f"Source fetch {iso_z(payload.source.fetched_at_utc)}"
    )

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "Qualité",
            quality_label(payload.quality.status),
            f"Score {payload.quality.data_quality_score:.3f}",
        )

    with col2:
        st.metric(
            "Événements",
            len(payload.events),
            f"{payload.quality.rejected_event_count} rejeté(s)",
        )

    with col3:
        if dynamically_stale:
            source_text = "🔴 STALE"
        else:
            source_text = "🟢 FRAIS"

        st.metric(
            "Source",
            source_text,
            f"Âge {source_age}s",
        )

    with col4:
        circuit_state = (
            state.get("circuit_state", "UNKNOWN")
            if state
            else "UNKNOWN"
        )

        circuit_icon = {
            "CLOSED": "🟢",
            "HALF_OPEN": "🟡",
            "OPEN": "🔴",
        }.get(circuit_state, "⚪")

        st.metric(
            "Circuit",
            f"{circuit_icon} {circuit_state}",
        )

    with col5:
        st.metric(
            "Actual",
            (
                "✅ Supporté"
                if payload.source.supports_actual
                else "⚠️ Non supporté"
            ),
        )

    if dynamically_stale:
        st.error(
            "La source est actuellement plus ancienne que le seuil autorisé. "
            "Le dernier artefact validé reste visible, mais il doit être "
            "considéré comme stale."
        )

    runtime = runtime_control()
    if runtime.last_runtime_error:
        st.error(
            f"Erreur runtime de l’orchestrateur : "
            f"{runtime.last_runtime_error}"
        )

    warnings = list(payload.quality.warnings)

    if health and health.get("last_error"):
        warnings.append(
            f"INGESTOR_LAST_ERROR: {health['last_error']}"
        )

    if warnings:
        with st.expander(
            f"⚠️ Avertissements ({len(warnings)})",
            expanded=dynamically_stale,
        ):
            for warning in dict.fromkeys(warnings):
                st.warning(warning)

    if payload.quality.rejections:
        with st.expander(
            f"🗑️ Rejets ({payload.quality.rejected_event_count})"
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
        (
            TimeProximity.IMMINENT,
            "🔴 Événements imminents",
            False,
        ),
        (
            TimeProximity.SOON,
            "🟠 Prochainement",
            False,
        ),
        (
            TimeProximity.LATER,
            "🔵 À venir",
            True,
        ),
        (
            TimeProximity.PAST,
            "⚪ Passés",
            True,
        ),
    )

    for proximity, title, collapsed in sections:
        subset = [
            event
            for event in events
            if event.time_context.time_proximity is proximity
        ]

        if not subset:
            continue

        st.markdown(f"### {title}")

        if collapsed:
            with st.expander(
                f"Afficher {len(subset)} événement(s)",
                expanded=False,
            ):
                for event in subset:
                    render_event_card(
                        event,
                        filters.show_assets_extended,
                    )
        else:
            for event in subset:
                render_event_card(
                    event,
                    filters.show_assets_extended,
                )


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
    st.subheader("📦 Exports machine")

    st.info(
        "Le téléchargement canonique n’applique aucun filtre UI. "
        "Il correspond exactement à l’artefact validé et publié par l’ingestor."
    )

    legacy = to_legacy_payload(payload, reference)

    legacy_bytes = json.dumps(
        legacy,
        indent=2,
        ensure_ascii=False,
        sort_keys=False,
    ).encode("utf-8")

    health_bytes = json.dumps(
        health or {},
        indent=2,
        ensure_ascii=False,
        sort_keys=False,
    ).encode("utf-8")

    col1, col2 = st.columns(2)

    with col1:
        st.download_button(
            "⬇️ calendar.legacy.json",
            data=legacy_bytes,
            file_name="calendar.legacy.json",
            mime="application/json",
            use_container_width=True,
            type="primary",
        )
        st.caption(f"{len(legacy_bytes) / 1024:.1f} KiB")

    with col2:
        st.download_button(
            "⬇️ health.json",
            data=health_bytes,
            file_name="health.json",
            mime="application/json",
            use_container_width=True,
        )
        st.caption(f"{len(health_bytes) / 1024:.1f} KiB")

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

    render_header(
        payload=payload,
        health=health,
        state=state,
        reference=reference,
    )

    events = prepare_view_events(
        payload=payload,
        filters=filters,
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
        .stApp {
            background-color: #0e0e1a;
            color: #e0e0e0;
        }

        [data-testid="stSidebar"] {
            background-color: #17172a;
        }

        [data-testid="stMetric"] {
            background-color: #18182b;
            border: 1px solid #2c2c45;
            border-radius: 10px;
            padding: 12px;
        }

        [data-testid="stVerticalBlockBorderWrapper"] {
            background-color: #151526;
            border-color: #30304a;
        }

        .stDownloadButton > button,
        .stButton > button {
            border-radius: 8px;
        }
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

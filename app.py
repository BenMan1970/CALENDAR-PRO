"""
BLUESTAR Pipeline Calendar — UI « Midnight Luxe »
=================================================
Orchestre pipeline_calendar.py et expose les news + le JSON, avec le design
system maison (ui.py : tokens, styles, composants et charts).

Doctrine :
  • le JSON produit est celui de calendar_core.py, INCHANGÉ (parité de
    content_hash inter-apps) ;
  • aucun widget n'alimente la SelectionPolicy : les filtres de cette page
    sont des filtres de VUE, et les exports téléchargent les OCTETS DU
    DISQUE — l'artefact consommé par le merge ne peut pas être contaminé ;
  • ingestion en thread de fond non bloquante, fallback seed au cold-start,
    cache invalidé par (mtime_ns, size) du fichier.
"""
from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

import pipeline_calendar as pc
import ui as U

UTC = timezone.utc

# ── Chemins ─────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parent / "data"
CANONICAL_PATH = DATA_DIR / "calendar.latest.json"
LEGACY_PATH = DATA_DIR / "calendar.json"
HEALTH_PATH = DATA_DIR / "health.json"
SEED_PATH = DATA_DIR / "seed" / "calendar.latest.seed.json"

IMPACT_LABELS = ("HIGH", "MEDIUM", "LOW", "HOLIDAY", "UNKNOWN")


# ═══════════════════════════════════════════════════════════════════════════
# INGESTION DE FOND (non bloquante, une seule à la fois par process)
# ═══════════════════════════════════════════════════════════════════════════
class Ingestion:
    """Superviseur d'ingestion : jamais plus d'un fetch concurrent, jamais de
    fetch pendant le cooldown 429, jamais de fetch en mode lecteur."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._running = False
        self.last_error: Optional[str] = None
        self.last_finished_at: Optional[float] = None

    @property
    def running(self) -> bool:
        return self._running

    def kick(self, *, force: bool = False) -> str:
        """Retourne l'état : started | running | fresh | cooldown | disabled."""
        if pc.ingest_disabled():
            return "disabled"

        with self._lock:
            if self._running:
                return "running"
            # Tests disque à l'intérieur du verrou : sans cela, deux sessions
            # concurrentes passent la garde (cooldown + fraîcheur) en même
            # temps puis se sérialisent ; si le worker de la première s'achève
            # dans la fenêtre, la seconde relance un fetch devenu inutile.
            if pc.is_rate_limited(DATA_DIR):
                return "cooldown"
            if not force and CANONICAL_PATH.exists():
                age = time.time() - CANONICAL_PATH.stat().st_mtime
                if age < pc.MIN_FETCH_SPACING_S:
                    return "fresh"
            self._running = True

        threading.Thread(target=self._worker, daemon=True, name="bs-ingest").start()
        return "started"

    def _worker(self) -> None:
        try:
            # [B4] session HTTP jetable par cycle : pas de pool partagé entre
            # threads Streamlit (sockets recyclés à froid = 429 fantômes).
            pc.run_once(DATA_DIR, session=None)
            self.last_error = None
        except Exception as exc:                              # noqa: BLE001
            self.last_error = f"{type(exc).__name__}: {exc}"
        finally:
            self.last_finished_at = time.time()
            self._running = False

    def wait_done(self, timeout: float = 30.0) -> bool:
        """Attend la fin du cycle en cours. L'UI l'appelle juste après
        ``kick`` pour que le rerun qui suit serve des octets frais — sans elle,
        le rerun est immédiat et les boutons de téléchargement embarquent
        l'artefact d'avant le cycle."""
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                if not self._running:
                    return True
            if time.monotonic() >= deadline:
                return not self._running
            time.sleep(0.2)


@st.cache_resource(show_spinner=False)
def _new_ingestion() -> Ingestion:
    """Instance persistante : sans ce cache, elle est réinstanciée à chaque
    rerun complet (filtre sidebar, onglet), ce qui réinitialise ``_running``
    à False et permet à deux run_once() de tourner en parallèle — la garantie
    « jamais plus d'un fetch concurrent » de la classe ne tient plus."""
    return Ingestion()


INGESTION = _new_ingestion()

# Messages partagés des boutons « Relancer l'ingestion » et « Scanner ».
_KICK_MSGS = {
    "started": ("Cycle lancé.", "ok"),
    "running": ("Un cycle est déjà en cours.", "warn"),
    "cooldown": ("Cooldown 429 actif — artefact précédent servi.", "warn"),
    "disabled": ("Mode lecteur (BLUESTAR_DISABLE_INGEST).", "warn"),
    "fresh": ("Artefact récent — espacement minimal respecté.", "ok"),
}


def _scan_and_rerun(msg_key: str, *, timeout: float = 30.0) -> None:
    """Force un cycle d'ingestion, en attend la fin, puis relance la page.

    But : que les boutons de téléchargement servent les octets du cycle qui
    vient de tourner. Sans attente, le rerun est immédiat et embarque
    l'artefact précédent — sur Cloud, data/ est reverté à chaque redémarrage
    du conteneur, donc le téléchargement servirait l'artefact commité (stale)
    même après une ingestion réussie."""
    with st.spinner("Rafraîchissement en cours…"):
        state = INGESTION.kick(force=True)
        if state in ("started", "running"):
            INGESTION.wait_done(timeout=timeout)
    st.session_state[msg_key] = _KICK_MSGS.get(state, (state, "warn"))
    st.rerun()



# À l'ouverture : si l'artefact est plus vieux que l'espacement minimal de
# fetch, on attend la fin du cycle qu'on vient de lancer plutôt que de servir
# l'état stale (sur Cloud, data/ est reverté à chaque restart du conteneur).
LOAD_REFRESH_TIMEOUT_S = 12.0


def _load_refresh_due() -> bool:
    """L'artefact canonique est-il assez vieux pour justifier d'attendre le
    cycle en cours à l'ouverture de la page ?"""
    if pc.ingest_disabled() or not CANONICAL_PATH.exists():
        return False
    return (time.time() - CANONICAL_PATH.stat().st_mtime) > pc.MIN_FETCH_SPACING_S

# ═══════════════════════════════════════════════════════════════════════════
# CHARGEMENT (cache invalidé par les stats fichier)
# ═══════════════════════════════════════════════════════════════════════════
def _stats(path: Path) -> Tuple[int, int]:
    try:
        s = path.stat()
        return s.st_mtime_ns, s.st_size
    except OSError:
        return 0, 0


@st.cache_data(show_spinner=False, ttl=20)
def load_canonical(mt: int = 0, sz: int = 0) -> Tuple[Optional[dict], bool]:
    """JSON canonique v2 ; fallback seed si data/ vide (cold-start Cloud)."""
    data = pc.read_json(CANONICAL_PATH)
    if data is not None:
        return data, False
    seed = pc.read_json(SEED_PATH)
    return (seed, True) if seed is not None else (None, False)


@st.cache_data(show_spinner=False, ttl=20)
def load_legacy(mt: int = 0, sz: int = 0) -> Tuple[Optional[dict], bool]:
    """JSON legacy v1 ; reconstruit depuis le seed si absent du disque."""
    data = pc.read_json(LEGACY_PATH)
    if data is not None:
        return data, False
    seed = pc.read_json(SEED_PATH)
    if seed and "events" in seed:
        try:
            from calendar_core import CalendarPayload, to_legacy_payload
            payload = CalendarPayload.model_validate(seed)
            return to_legacy_payload(payload, datetime.now(UTC)), True
        except Exception:                                     # noqa: BLE001
            return None, False
    return None, False


@st.cache_data(show_spinner=False, ttl=20)
def load_health(mt: int = 0, sz: int = 0) -> Optional[dict]:
    return pc.read_json(HEALTH_PATH)


def canonical() -> Tuple[Optional[dict], bool]:
    return load_canonical(*_stats(CANONICAL_PATH))


def legacy() -> Tuple[Optional[dict], bool]:
    return load_legacy(*_stats(LEGACY_PATH))


def health() -> Optional[dict]:
    return load_health(*_stats(HEALTH_PATH))


def disk_bytes(path: Path, fallback: Optional[Any] = None) -> bytes:
    """Octets EXACTS du disque (audit : un export ne se re-sérialise pas)."""
    try:
        return path.read_bytes()
    except OSError:
        if fallback is None:
            return b"{}"
        return json.dumps(fallback, indent=2, ensure_ascii=False).encode("utf-8")


# ═══════════════════════════════════════════════════════════════════════════
# PROJECTION DE VUE (countdowns recalculés à l'instant du rendu)
# ═══════════════════════════════════════════════════════════════════════════
@dataclass
class Filters:
    currencies: Tuple[str, ...] = ()
    impacts: Tuple[str, ...] = ("HIGH", "MEDIUM")
    query: str = ""
    horizon_h: int = 168
    upcoming_only: bool = False
    show_pairs: bool = False
    limit: int = 400


def _fmt_delta(hours: float) -> str:
    total_min = int(round(abs(hours) * 60))
    d, rem = divmod(total_min, 1440)
    h, m = divmod(rem, 60)
    if d:
        body = f"{d}j {h:02d}h"
    elif h:
        body = f"{h}h {m:02d}m"
    else:
        body = f"{m}m"
    return body if hours > 0 else f"−{body}"


def enrich_events(payload: Optional[dict], now: datetime) -> List[dict]:
    """Aplati le payload canonique en lignes de VUE. Le time_context stocké
    dans l'artefact est daté de l'ingestion : les countdowns sont TOUJOURS
    recalculés ici, sinon l'écran mentirait de plusieurs minutes."""
    if not payload:
        return []
    out: List[dict] = []
    for ev in payload.get("events", []):
        raw_dt = ev.get("scheduled_at_utc") or ""
        try:
            dt_utc = datetime.fromisoformat(str(raw_dt).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        display = str(ev.get("scheduled_at_display") or "")
        hm = display.split("T")[-1][:5] if "T" in display else dt_utc.strftime("%H:%M")
        hours = (dt_utc - now).total_seconds() / 3600.0
        out.append({
            "occurrence_id": ev.get("occurrence_id"),
            "name": ev.get("name") or "",
            "currency": (ev.get("currency") or "—").upper(),
            "impact": (ev.get("impact") or "UNKNOWN").upper(),
            "session": (ev.get("session") or "").replace("_", " ").title(),
            "dt_utc": dt_utc,
            "hm": hm,
            "tz_label": (ev.get("display_timezone") or "UTC").split("/")[-1].replace("_", " "),
            "date_display": ev.get("date_display") or dt_utc.strftime("%Y-%m-%d"),
            "day_of_week": (ev.get("day_of_week") or "").title(),
            "forecast": (ev.get("forecast") or {}).get("raw"),
            "previous": (ev.get("previous") or {}).get("raw"),
            "actual": (ev.get("actual") or {}).get("raw"),
            "actual_status": ev.get("actual_status"),
            "hours_until": round(hours, 4),
            "countdown": _fmt_delta(hours),
            "pairs": list(ev.get("pairs_with_currency_exposure") or []),
        })
    out.sort(key=lambda e: e["dt_utc"])
    return out


def apply_filters(events: List[dict], f: Filters, now: datetime) -> List[dict]:
    q = f.query.strip().lower()
    kept: List[dict] = []
    for e in events:
        if f.impacts and e["impact"] not in f.impacts:
            continue
        if f.currencies and e["currency"] not in f.currencies:
            continue
        if f.upcoming_only and e["hours_until"] <= 0:
            continue
        if e["hours_until"] > f.horizon_h:
            continue
        if q and q not in e["name"].lower() and q not in e["currency"].lower():
            continue
        kept.append(e)
    return kept[: f.limit]


# ═══════════════════════════════════════════════════════════════════════════
# BLOCS D'INTERFACE
# ═══════════════════════════════════════════════════════════════════════════
def serving_state(payload: Optional[dict], is_seed: bool) -> Tuple[str, str, bool]:
    """(label, couleur, pulsation) — état de service lisible en un coup d'œil."""
    if pc.ingest_disabled():
        return "MODE LECTEUR", U.MUTED, False
    if pc.is_rate_limited(DATA_DIR):
        return "COOLDOWN 429", U.WARN, False
    if INGESTION.running:
        return "INGESTION", U.ACCENT, True
    if is_seed:
        return "SEED · COLD START", U.WARN, False
    if payload:
        return "LIVE", U.OK, True
    return "INDISPONIBLE", U.BAD, False


def render_hero(payload: Optional[dict], is_seed: bool) -> None:
    label, color, live = serving_state(payload, is_seed)
    src = (payload or {}).get("source", {})
    quality = (payload or {}).get("quality", {})
    pills = [
        U.pill(label, color, live=live),
        U.pill(f"{len((payload or {}).get('events', []))} événements", U.MUTED),
        U.pill(quality.get("status", "—"),
               U.QUALITY_COLOR.get(quality.get("status", ""), U.GHOST)),
    ]
    U.render(U.hero(
        "Forex Factory · Fair Economy",
        "Pipeline",
        "Calendar",
        "Calendrier macroéconomique normalisé — fenêtre de veille glissante, "
        "horodatage UTC en backend, affichage en heure locale de place. "
        f"Source : {src.get('provider') or 'flux public hebdomadaire'}.",
        pills,
    ))


@st.fragment(run_every=20)
def render_live_strip() -> None:
    """Bandeau KPI auto-rafraîchi (20 s) : relit les stats fichier à chaque
    passe, donc capte l'écriture atomique du thread de fond SANS rerun global."""
    INGESTION.kick()
    payload, is_seed = canonical()
    now = datetime.now(UTC)
    if not payload:
        U.render(U.empty("Aucun artefact disponible",
                         "L'ingestion initiale est en cours — le bandeau se "
                         "remplira automatiquement dès le premier cycle publié."))
        return

    events = enrich_events(payload, now)
    upcoming = [e for e in events if e["hours_until"] > 0]
    high = [e for e in upcoming if e["impact"] == "HIGH"]
    nxt = high[0] if high else (upcoming[0] if upcoming else None)

    quality = payload.get("quality", {}) or {}
    score = float(quality.get("data_quality_score") or 0.0)
    status = quality.get("status", "—")

    src = payload.get("source", {}) or {}
    age_s: Optional[int] = None
    try:
        fetched = datetime.fromisoformat(str(src.get("fetched_at_utc", "")).replace("Z", "+00:00"))
        age_s = max(0, int((now - fetched).total_seconds()))
    except (ValueError, TypeError):
        pass

    imminent = sum(1 for e in upcoming if e["hours_until"] <= 6)
    cards = [
        U.kpi("Prochaine publication",
              nxt["countdown"] if nxt else "—",
              f"{nxt['currency']} · {nxt['name'][:38]}" if nxt else "aucun événement à venir",
              color=U.IMPACT.get(nxt["impact"], U.ACCENT) if nxt else None,
              mono=True, delay_ms=0),
        U.kpi("Fenêtre active", f"{len(upcoming)}",
              f"{imminent} dans les 6 h · {len(high)} à fort impact",
              bar=min(1.0, len(upcoming) / 40.0), delay_ms=40),
        U.kpi("Qualité artefact", str(status), f"score {score:.3f}",
              color=U.QUALITY_COLOR.get(status, U.GHOST), bar=score, delay_ms=80),
        U.kpi("Fraîcheur source",
              f"{age_s}s" if age_s is not None else "—",
              f"hash {(payload.get('content_hash') or '').split(':')[-1][:12] or '—'}",
              mono=True,
              color=U.OK if (age_s is not None and age_s < 900) else U.WARN,
              delay_ms=120),
    ]
    U.render(U.kpi_grid(cards))

    warnings = list(quality.get("warnings") or [])
    if is_seed:
        U.render(U.note(
            "<b>Données semence.</b> Le disque était vide au démarrage "
            "(cold-start Streamlit Cloud) : la vue s'appuie sur "
            "<code>seed/calendar.latest.seed.json</code> jusqu'au premier cycle frais.",
            "warn"))
    elif warnings:
        head = ", ".join(w.split(":")[0] for w in warnings[:4])
        U.render(U.note(
            f"<b>{len(warnings)} signal(aux) qualité.</b> {head} — détail dans "
            "l'onglet Diagnostics.",
            "warn" if status != "INVALID" else "bad"))

    # [refresh auto] Un artefact plus frais que celui servi au dernier rerun
    # complet vient d'arriver (cycle de fond ou scan lancé d'un autre onglet) :
    # un seul rerun global pour que les téléchargements de l'onglet Export
    # embarquent les octets frais — sans lui, ils serviraient l'artefact
    # précédent. Le premier passage se contente de mémoriser l'identifiant
    # servi (pas de rerun à froid).
    _fid = (src.get("fetched_at_utc")
            or payload.get("generated_at_utc")) if payload else None
    if _fid:
        _prev = st.session_state.get("_served_fid")
        st.session_state["_served_fid"] = _fid
        if _prev and _prev != _fid:
            st.rerun()


def render_sidebar(events: List[dict]) -> Filters:
    with st.sidebar:
        U.render(
            '<div style="display:flex;align-items:center;gap:10px;margin-bottom:2px;">'
            f'<div style="width:26px;height:26px;border-radius:8px;'
            f'background:linear-gradient(135deg,{U.ACCENT},#34D399);opacity:.9"></div>'
            '<div><div style="font-weight:750;letter-spacing:-.03em;font-size:.95rem">BLUESTAR</div>'
            f'<div style="font-size:.62rem;letter-spacing:.18em;color:{U.GHOST}">'
            'CALENDAR ENGINE</div></div></div>'
        )
        st.markdown("")

        ccy_pool = sorted({e["currency"] for e in events}) if events else []
        currencies = st.multiselect("Devises", ccy_pool, default=[],
                                    placeholder="Toutes les devises")
        # Options FIXES : dériver des événements déjà filtrés par la policy
        # fait planter le multiselect une semaine sans MEDIUM (ou sans HIGH) —
        # le default réclame alors une option absente (StreamlitAPIException).
        impacts = st.multiselect("Niveaux d'impact",
                                 list(IMPACT_LABELS),
                                 default=[i for i in ("HIGH", "MEDIUM") if i in IMPACT_LABELS],
                                 placeholder="Tous les niveaux")
        query = st.text_input("Recherche", value="", placeholder="CPI, NFP, rate…")
        horizon = st.slider("Horizon (heures)", 6, 168, 168, step=6)
        upcoming_only = st.toggle("À venir uniquement", value=False)
        show_pairs = st.toggle("Afficher les paires exposées", value=False)

        st.markdown("")
        if st.button("Relancer l'ingestion", type="primary", width="stretch"):
            _scan_and_rerun("_kick_msg")

        if msg := st.session_state.pop("_kick_msg", None):
            U.render(U.note(f"<b>{msg[0]}</b>", msg[1]))

        U.render(
            f'<div style="margin-top:14px;font-size:.66rem;color:{U.GHOST};'
            f'font-family:{U.MONO};line-height:1.7">'
            f'DATA_DIR · {DATA_DIR.name}/<br>'
            f'SPACING · {pc.MIN_FETCH_SPACING_S}s<br>'
            f'SCHEMA · {pc.SCHEMA_VERSION}</div>'
        )

    return Filters(
        currencies=tuple(currencies),
        impacts=tuple(impacts) if impacts else tuple(IMPACT_LABELS),
        query=query,
        horizon_h=int(horizon),
        upcoming_only=upcoming_only,
        show_pairs=show_pairs,
    )


def render_feed(events: List[dict], f: Filters) -> None:
    if not events:
        U.render(U.empty(
            "Aucun événement sur ce périmètre",
            "Élargissez l'horizon ou les niveaux d'impact dans le panneau latéral. "
            "Un calendrier creux est un fait de marché, pas une panne."))
        return

    groups: Dict[str, List[dict]] = {}
    for e in events:
        groups.setdefault(e["date_display"], []).append(e)

    blocks: List[str] = []
    for day, rows in groups.items():
        label = f"{rows[0]['day_of_week']} · {day}" if rows[0]["day_of_week"] else day
        cards = [U.event_card(e, show_pairs=f.show_pairs, delay_ms=min(i * 22, 260))
                 for i, e in enumerate(rows)]
        blocks.append(U.day_header(label, len(rows)) + U.feed(cards))
    U.render(*blocks)


def render_analytics(events: List[dict]) -> None:
    if not U.AVAILABLE:
        U.render(U.note("<b>Plotly absent de l'environnement.</b> "
                        "Ajoutez <code>plotly</code> à requirements.txt pour "
                        "activer les visualisations.", "warn"))
        return
    if not events:
        U.render(U.empty("Rien à visualiser", "Aucun événement sur le périmètre courant."))
        return

    c1, c2, c3 = st.columns([1.45, 1, 1])
    with c1, st.container(border=True):
        U.panel_title("Densité par jour", "volume · impact")
        fig = U.density_by_day(events)
        if fig:
            st.plotly_chart(fig, config=U.PLOTLY_CONFIG, width="stretch")
    with c2, st.container(border=True):
        U.panel_title("Répartition d'impact", "mix")
        fig = U.impact_donut(events)
        if fig:
            st.plotly_chart(fig, config=U.PLOTLY_CONFIG, width="stretch")
    with c3, st.container(border=True):
        U.panel_title("Charge par devise", "top 9")
        fig = U.currency_exposure(events)
        if fig:
            st.plotly_chart(fig, config=U.PLOTLY_CONFIG, width="stretch")

    with st.container(border=True):
        U.panel_title("Grille dense", "vue tabulaire")
        st.dataframe(
            [{
                "Date": e["date_display"], "Heure": e["hm"], "TZ": e["tz_label"],
                "Devise": e["currency"], "Impact": e["impact"], "Événement": e["name"],
                "Forecast": e["forecast"] or "—", "Previous": e["previous"] or "—",
                "Actual": e["actual"] or "—", "Session": e["session"],
                "Échéance": e["countdown"],
            } for e in events],
            column_config={
                "Date": st.column_config.TextColumn(width="small"),
                "Heure": st.column_config.TextColumn(width="small"),
                "TZ": st.column_config.TextColumn(width="small"),
                "Devise": st.column_config.TextColumn(width="small"),
                "Impact": st.column_config.TextColumn(width="small"),
                "Événement": st.column_config.TextColumn(width="large"),
                "Échéance": st.column_config.TextColumn(width="small"),
            },
            hide_index=True, height=420, width="stretch",
        )


def render_summary() -> None:
    lg, from_seed = legacy()
    if not lg:
        U.render(U.empty("Résumé indisponible",
                         "L'artefact legacy v1 n'a pas encore été publié."))
        return
    meta = lg.get("metadata", {}) or {}
    if from_seed:
        U.render(U.note("<b>Résumé reconstruit depuis le seed</b> — "
                        "le format legacy v1 est régénéré en mémoire.", "warn"))

    U.render(U.kpi_grid([
        U.kpi("Horizon servi", f"{meta.get('feed_horizon_h') or '—'} h",
              str(meta.get("feed_horizon_state") or "—"), mono=True),
        U.kpi("Mode de service", str(meta.get("serving_mode") or "—"),
              f"flux OK {meta.get('feeds_ok', '—')}/{meta.get('feeds_total', '—')}"),
        U.kpi("Fort impact", str(meta.get("high_impact_count", "—")),
              f"{meta.get('engine_events_count', '—')} lignes moteur"),
        U.kpi("Imminents", str(meta.get("imminent_count", "—")),
              f"{meta.get('upcoming_count', '—')} à venir"),
    ]))

    if note_txt := meta.get("coverage_note"):
        U.render(U.note(f"<b>Couverture.</b> {note_txt}"))

    summary = lg.get("summary_by_day", {}) or {}
    if not summary:
        U.render(U.empty("Aucun jour couvert", "Le résumé quotidien est vide."))
        return
    for day, items in sorted(summary.items()):
        with st.expander(f"{day}  ·  {len(items)} événement(s)"):
            U.render(
                '<div style="display:flex;flex-direction:column;gap:6px;">'
                + "".join(
                    f'<div style="font-size:.79rem;color:{U.MUTED};'
                    f'font-family:{U.MONO}">{item}</div>' for item in items
                ) + "</div>"
            )


def render_exports() -> None:
    # [scanner] « je scan, il me donne le calendrier » : force le cycle, attend
    # sa fin, puis rerun — les boutons ci-dessous embarquent alors les octets
    # du cycle. Sans scan, ils servent l'artefact du dernier rerun complet,
    # qui sur Cloud est l'artefact commité (data/ reverté à chaque restart).
    if msg := st.session_state.pop("_scan_msg", None):
        U.render(U.note(f"<b>{msg[0]}</b>", msg[1]))
    if st.button("\u25b6  Scanner et servir le calendrier frais",
                 type="primary", width="stretch", key="scan_exports"):
        _scan_and_rerun("_scan_msg")

    can, _ = canonical()
    lg, _ = legacy()
    hp = health()

    U.render(U.note(
        "<b>Exports fidèles au disque.</b> Les boutons servent les octets "
        "exacts des artefacts publiés par l'ingesteur : ni les filtres de "
        "cette page ni le fuseau d'affichage ne peuvent altérer un fichier "
        "consommé en aval."))
    st.markdown("")

    c1, c2, c3 = st.columns(3)
    specs = (
        (c1, "calendar.json", LEGACY_PATH, lg, "legacy v1 · format de merge", "primary"),
        (c2, "calendar.latest.json", CANONICAL_PATH, can, "canonique v2 · schéma riche", "primary"),
        (c3, "health.json", HEALTH_PATH, hp or {}, "supervision · forensique", "secondary"),
    )
    for col, name, path, fallback, hint, kind in specs:
        with col, st.container(border=True):
            data = disk_bytes(path, fallback)
            available = bool(fallback) or path.exists()
            U.panel_title(name, hint)
            if available:
                st.download_button(
                    f"Télécharger · {len(data) / 1024:.1f} KiB",
                    data=data, file_name=name, mime="application/json",
                    type=kind, width="stretch", key=f"dl_{name}",
                )
                U.render(f'<div style="font-size:.66rem;color:{U.GHOST};'
                         f'font-family:{U.MONO};margin-top:8px">'
                         f'{"disque" if path.exists() else "reconstruit"} · '
                         f'{len(data)} octets</div>')
            else:
                st.button("Indisponible", disabled=True, width="stretch", key=f"na_{name}")

    if can:
        with st.expander("Aperçu du schéma canonique (métadonnées, hors événements)"):
            st.json({k: v for k, v in can.items() if k != "events"}, expanded=False)


def render_diagnostics() -> None:
    can, is_seed = canonical()
    hp = health() or {}
    tz = pc.tz_environment()
    rl = pc.read_rate_limit(DATA_DIR) or {}

    c1, c2 = st.columns(2)
    with c1, st.container(border=True):
        U.panel_title("Service", "état d'exécution")
        U.render(U.kv_table([
            ("data_dir", str(DATA_DIR)),
            ("canonical présent", CANONICAL_PATH.exists()),
            ("seed présent", SEED_PATH.exists()),
            ("sert le seed", is_seed),
            ("ingestion en cours", INGESTION.running),
            ("mode lecteur", pc.ingest_disabled()),
            ("dernière erreur UI", INGESTION.last_error),
            ("cooldown 429 actif", pc.is_rate_limited(DATA_DIR)),
            ("cooldown jusqu'à", rl.get("blocked_until_utc")),
            ("espacement min", f"{pc.MIN_FETCH_SPACING_S}s"),
        ]))
    with c2, st.container(border=True):
        U.panel_title("Autorité des règles horaires", "audit B2")
        auth = bool(tz.get("pip_tzdata_authoritative"))
        U.render(U.note(
            "<b>tzdata pip prioritaire.</b> Les offsets affichés proviennent "
            "du paquet épinglé." if auth else
            "<b>tzdata système en tête de TZPATH.</b> Les offsets peuvent "
            "être périmés selon l'image d'exécution.",
            "ok" if auth else "warn"))
        st.markdown("")
        U.render(U.kv_table([
            ("tzdata (pip)", tz.get("tzdata_pip_version")),
            ("TZPATH[0]", tz.get("tzpath_head")),
            *[(f"Casablanca {k}", f"{v[0]:+.2f}h · {v[1]}")
              for k, v in (tz.get("casablanca_offsets") or {}).items()],
        ]))

    with st.container(border=True):
        U.panel_title("Flux sources", "statut par endpoint")
        feeds = ((can or {}).get("source", {}) or {}).get("feed_status", {}) or {}
        pills = [U.pill(f"{k} · {v}",
                        U.OK if str(v).startswith("ok")
                        else (U.WARN if "404" in str(v) else U.BAD))
                 for k, v in feeds.items()] or [U.pill("aucun statut publié", U.GHOST)]
        U.render(f'<div style="display:flex;gap:8px;flex-wrap:wrap">{"".join(pills)}</div>')
        st.markdown("")
        U.render(U.kv_table([(f"source #{i + 1}", u)
                             for i, u in enumerate(pc.SOURCE_URLS)]))

    warnings = ((can or {}).get("quality", {}) or {}).get("warnings") or []
    rejections = ((can or {}).get("quality", {}) or {}).get("rejections") or []
    if warnings or rejections:
        with st.container(border=True):
            U.panel_title("Signaux qualité", f"{len(warnings)} warning(s) · {len(rejections)} rejet(s)")
            U.render(*[U.note(f"<code>{w}</code>",
                              "bad" if "INVALID" in w or "EXPIRED" in w else "warn")
                       + '<div style="height:6px"></div>' for w in warnings])
            if rejections:
                with st.expander(f"Lignes rejetées ({len(rejections)})"):
                    st.code("\n".join(str(r) for r in rejections), language="text")

    with st.container(border=True):
        U.panel_title("health.json", "dernier cycle")
        st.json(hp, expanded=False)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
def main() -> None:
    st.set_page_config(
        page_title="BLUESTAR · Pipeline Calendar",
        page_icon="◆",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={"about": "BLUESTAR Pipeline Calendar — calendrier "
                             "macroéconomique normalisé (Forex Factory / Fair Economy)."},
    )
    U.inject()

    # [refresh à l'ouverture] « j'ouvre le lien, c'est frais ». kick()
    # lance le cycle en arrière-plan ; si l'artefact est stale (typiquement
    # après un restart du conteneur Cloud, qui revert data/ à l'état
    # commité), on en attend la fin avant de rendre la page — les
    # téléchargements de l'onglet Export servent alors l'artefact frais
    # dès le premier rendu. Borné : un cycle trop lent sert l'état
    # précédent et le bandeau ci-dessous rafraîchit seul.
    _state = INGESTION.kick()
    if _state in ("started", "running") and _load_refresh_due():
        with st.spinner("Rafraîchissement du calendrier…"):
            INGESTION.wait_done(timeout=LOAD_REFRESH_TIMEOUT_S)

    payload, is_seed = canonical()
    now = datetime.now(UTC)
    all_events = enrich_events(payload, now)

    filters = render_sidebar(all_events)
    render_hero(payload, is_seed)
    render_live_strip()
    st.markdown("")

    view = apply_filters(all_events, filters, now)

    tab_feed, tab_analytics, tab_summary, tab_export, tab_diag = st.tabs(
        ["Flux", "Analytique", "Résumé", "Export", "Diagnostics"]
    )
    with tab_feed:
        U.render(
            f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin:2px 0 4px">'
            f'{U.pill(f"{len(view)} / {len(all_events)} événements", U.MUTED)}'
            f'{U.pill(f"horizon {filters.horizon_h} h", U.MUTED)}'
            f'{U.pill("filtres de vue uniquement", U.ACCENT)}</div>'
        )
        render_feed(view, filters)
    with tab_analytics:
        render_analytics(view)
    with tab_summary:
        render_summary()
    with tab_export:
        render_exports()
    with tab_diag:
        render_diagnostics()

    gen = (payload or {}).get("generated_at_utc", "—")
    U.render(
        f'<div style="margin-top:34px;padding-top:14px;'
        f'border-top:1px solid {U.LINE};display:flex;justify-content:space-between;'
        f'flex-wrap:wrap;gap:10px;font-size:.68rem;color:{U.GHOST};font-family:{U.MONO}">'
        f'<span>BLUESTAR CALENDAR · core {pc.SCHEMA_VERSION}</span>'
        f'<span>généré {gen}</span>'
        f'<span>{"ingestion en cours" if INGESTION.running else "au repos"}</span></div>'
    )


if __name__ == "__main__":
    main()


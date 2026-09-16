"""
BLUESTAR Pipeline Calendar — Streamlit minimal
=============================================
UI ultra-légère qui orchestre pipeline_calendar.py et expose les news + le JSON.

Remplace l'ancien app.py (2 207 lignes de CSS, 5 onglets, asset mapping).
→ ~160 lignes. Même JSON produit (calendar_core.py inchangé).

Fallback seed : si data/ est vide (cold-start Streamlit Cloud), charge
seed/calendar.latest.seed.json tant que le fetch en arrière-plan n'a pas
produit de données fraîches.
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st

import pipeline_calendar as pc

UTC = timezone.utc

# ── Chemins ──────────────────────────────────────────────────────────────────
DATA_DIR = pc.Path(__file__).resolve().parent / "data"
CANONICAL_PATH = DATA_DIR / "calendar.latest.json"
LEGACY_PATH = DATA_DIR / "calendar.json"
HEALTH_PATH = DATA_DIR / "health.json"
SEED_PATH = DATA_DIR / "seed" / "calendar.latest.seed.json"

# ── Ingestion en thread de fond (non bloquant) ──────────────────────────────
_lock = threading.Lock()
_running = False
_result: dict = {}


def _kick_ingestion():
    """Lance l'ingestion en arrière-plan si le fichier canonical est absent ou périmé."""
    global _running
    if _running:
        return
    if CANONICAL_PATH.exists():
        age = time.time() - CANONICAL_PATH.stat().st_mtime
        if age < pc.MIN_FETCH_SPACING_S:
            return

    with _lock:
        if _running:
            return
        _running = True

    def _worker():
        global _running
        try:
            session = pc.build_session()
            payload = pc.run_once(DATA_DIR, session)
            with _lock:
                _result["payload"] = payload
                _result["done"] = True
                _result["ts"] = time.time()
        except Exception as e:
            with _lock:
                _result["error"] = str(e)
                _result["done"] = True
        finally:
            _running = False

    threading.Thread(target=_worker, daemon=True, name="pipeline-fetch").start()


# ── Chargement JSON (cache invalidé par mtime/size) ─────────────────────────
def _load_json(path: Path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _file_stats(path: Path):
    try:
        s = path.stat()
        return s.st_mtime_ns, s.st_size
    except OSError:
        return 0, 0


# Cache invalidé automatiquement par le mtime/size du fichier :
# quand le thread de fond écrit un nouveau fichier, les stats changent → cache invalidé.
@st.cache_data(show_spinner=False)
def load_canonical(_mt: int = 0, _sz: int = 0):
    """Charge le JSON canonique — fallback seed si data/ absent (cold-start)."""
    data = _load_json(CANONICAL_PATH)
    if data is not None:
        return data, False
    if SEED_PATH.exists():
        seed = _load_json(SEED_PATH)
        if seed is not None:
            return seed, True
    return None, False


@st.cache_data(show_spinner=False)
def load_legacy(_mt: int = 0, _sz: int = 0):
    """Charge le JSON legacy — reconstruit depuis le seed si data/ absent."""
    data = _load_json(LEGACY_PATH)
    if data is not None:
        return data, False
    seed_data = _load_json(SEED_PATH)
    if seed_data and "events" in seed_data:
        try:
            from calendar_core import CalendarPayload, to_legacy_payload
            payload = CalendarPayload.model_validate(seed_data)
            now = datetime.now(UTC)
            return to_legacy_payload(payload, now), True
        except Exception:
            pass
    return None, False


@st.cache_data(show_spinner=False)
def load_health(_mt: int = 0, _sz: int = 0):
    return _load_json(HEALTH_PATH), False


def _c_stats():
    return _file_stats(CANONICAL_PATH)


def _l_stats():
    return _file_stats(LEGACY_PATH)


def _h_stats():
    return _file_stats(HEALTH_PATH)


# ── CSS minimal ─────────────────────────────────────────────────────────────
def _apply_theme():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    :root{--bg:#0B0D12;--surface:#12151C;--surface-2:#171B24;--line:#232936;
    --text:#E7EAF0;--muted:#8B94A7;--faint:#5C6579;--red:#E5484D;--amber:#F5A524;
    --green:#3DD68C;--blue:#5B8DEF;--radius:10px;--mono:'JetBrains Mono',ui-monospace,monospace;}
    .stApp{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;}
    [data-testid="stHeader"]{background:transparent;}
    .block-container{padding:2rem 3rem;max-width:1400px;}
    h1,h2,h3,h4{font-weight:650!important;letter-spacing:-0.02em;}
    [data-testid="stSidebar"]{background:#0E1117;border-right:1px solid var(--line);}
    .stDownloadButton button{border-radius:9px!important;font-size:0.82rem!important;
        font-weight:560!important;min-height:38px!important;}
    [data-testid="stDownloadButton"] button[kind="primary"]{
        background:linear-gradient(180deg,#F2555A 0%,#D22E34 100%)!important;
        color:#fff!important;border:1px solid rgba(255,255,255,0.10)!important;}
    [data-testid="stDownloadButton"] button[kind="secondary"]{
        background:var(--surface-2)!important;border:1px solid var(--line)!important;
        color:var(--muted)!important;}
    [data-testid="stDataFrame"]{border:1px solid var(--line);border-radius:var(--radius);}
    [data-testid="stDataFrame"] *{font-size:0.8rem!important;}
    .stJson{background:var(--surface)!important;border:1px solid var(--line);
        border-radius:var(--radius);padding:10px 12px;font-size:0.75rem!important;}
    code,pre{font-family:var(--mono)!important;font-size:0.75rem!important;}
    .stAlert{border-radius:var(--radius);border:1px solid var(--line);}
    hr{border-color:var(--line);}
    ::-webkit-scrollbar{width:8px;height:8px;}
    ::-webkit-scrollbar-thumb{background:#262C39;border-radius:4px;}
    </style>
    """, unsafe_allow_html=True)


# ── Rendu ────────────────────────────────────────────────────────────────────
def render_status_bar():
    canonical, is_seed = load_canonical(*_c_stats())
    if not canonical:
        st.error("🚨 Aucun artefact disponible. Ingestion en cours…")
        return

    q = canonical.get("quality", {})
    src = canonical.get("source", {})
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Événements", len(canonical.get("events", [])))
    with col2:
        score = q.get("data_quality_score", 0)
        st.metric("Qualité", q.get("status", "?"), f"{score:.2f}" if score else None)
    with col3:
        st.metric("Hash", (canonical.get("content_hash", "") or "").split(":")[-1][:12])

    if is_seed:
        st.info("🌱 Données semence (cold-start) — ingestion fraîche en cours…", icon="🌱")
    elif _running:
        st.info("🔄 Ingestion en cours…", icon="⏳")
    else:
        fetched = src.get("fetched_at_utc", "?")
        st.caption(f"Dernière actualisation: {fetched}  |  DATA_DIR: `{DATA_DIR.name}/`")


def render_events_table():
    canonical, _is_seed = load_canonical(*_c_stats())
    if not canonical:
        st.info("Chargement des données…")
        return

    st.subheader("📰 Événements économiques")

    rows = []
    for e in canonical.get("events", []):
        tc = e.get("time_context", {}) or {}
        f = e.get("forecast", {}) or {}
        p = e.get("previous", {}) or {}
        a = e.get("actual", {}) or {}
        sd = e.get("scheduled_at_display", "")
        rows.append({
            "Date": e.get("date_display", ""),
            "Heure": sd.split("+")[0].split("T")[-1][:5] if "T" in sd else "",
            "TZ": e.get("display_timezone", ""),
            "Devise": e.get("currency", ""),
            "Impact": (e.get("impact", "")).upper(),
            "Événement": e.get("name", ""),
            "Forecast": f.get("raw") or "—",
            "Previous": p.get("raw") or "—",
            "Actual": a.get("raw") or "—",
            "Proximity": tc.get("time_proximity", ""),
            "Countdown": tc.get("hours_until_display", ""),
        })

    st.dataframe(
        rows,
        column_config={
            "Date": st.column_config.TextColumn(width="small"),
            "Heure": st.column_config.TextColumn(width="small"),
            "TZ": st.column_config.TextColumn(width="small"),
            "Devise": st.column_config.TextColumn(width="small"),
            "Impact": st.column_config.TextColumn(width="small"),
            "Événement": st.column_config.TextColumn(width="large"),
            "Forecast": st.column_config.TextColumn(width="small"),
            "Previous": st.column_config.TextColumn(width="small"),
            "Actual": st.column_config.TextColumn(width="small"),
            "Proximity": st.column_config.TextColumn(width="small"),
            "Countdown": st.column_config.TextColumn(width="small"),
        },
        hide_index=True,
    )


def render_summary_by_day():
    legacy, _ = load_legacy(*_l_stats())
    if not legacy:
        st.info("Données non disponibles.")
        return

    st.subheader("📅 Résumé par jour")
    summary = legacy.get("summary_by_day", {})
    for day, events in sorted(summary.items()):
        with st.expander(f"**{day}** — {len(events)} événement(s)"):
            for ev in events:
                st.markdown(f"・{ev}")


def render_exports():
    st.subheader("📦 Export pipeline (télécharger le JSON riche)")

    col1, col2, col3 = st.columns(3)
    canonical, _ = load_canonical(*_c_stats())
    legacy, _ = load_legacy(*_l_stats())
    health, _ = load_health(*_h_stats())

    # calendar.json (legacy v1 — format consommé par le merge)
    if legacy:
        legacy_bytes = json.dumps(legacy, indent=2, ensure_ascii=False).encode("utf-8")
        with col1:
            st.download_button(
                "calendar.json (legacy v1)",
                data=legacy_bytes,
                file_name="calendar.json",
                mime="application/json",
                width="stretch",
                type="primary",
            )
            st.caption(f"{len(legacy_bytes) / 1024:.1f} KiB · merge format")
    else:
        with col1:
            st.button("calendar.json", disabled=True, width="stretch")

    # calendar.latest.json (canonique v2 — rich)
    if canonical:
        canon_bytes = json.dumps(canonical, indent=2, ensure_ascii=False).encode("utf-8")
        with col2:
            st.download_button(
                "calendar.latest.json (v2)",
                data=canon_bytes,
                file_name="calendar.latest.json",
                mime="application/json",
                width="stretch",
                type="primary",
            )
            st.caption(f"{len(canon_bytes) / 1024:.1f} KiB · canonical schema")
    else:
        with col2:
            st.button("calendar.latest.json", disabled=True, width="stretch")

    # health.json
    with col3:
        health_bytes = json.dumps(health or {}, indent=2, ensure_ascii=False).encode("utf-8")
        st.download_button(
            "health.json",
            data=health_bytes,
            file_name="health.json",
            mime="application/json",
            width="stretch",
            type="secondary",
        )
        st.caption(f"{len(health_bytes) / 1024:.1f} KiB · supervision")


def render_diagnostics():
    canonical, _ = load_canonical(*_c_stats())
    health, _ = load_health(*_h_stats())
    st.json({
        "data_dir": str(DATA_DIR),
        "canonical_exists": CANONICAL_PATH.exists(),
        "seed_file": str(SEED_PATH),
        "seed_exists": SEED_PATH.exists(),
        "fetch_running": _running,
        "canonical_schema_version": canonical.get("schema_version") if canonical else None,
        "canonical_event_count": len(canonical.get("events", [])) if canonical else 0,
        "health": health,
        "tz": pc.tz_environment(),
        "source_urls": list(pc.SOURCE_URLS),
        "min_fetch_spacing_s": pc.MIN_FETCH_SPACING_S,
    })


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    st.set_page_config(
        page_title="BLUESTAR Pipeline Calendar",
        page_icon="🔷",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _apply_theme()

    # Kick ingestion (non-bloquant) au démarrage
    _kick_ingestion()

    # Sidebar
    with st.sidebar:
        st.markdown("### 🔷 BLUESTAR Calendar")
        st.caption("News économiques · Forex Factory / Fair Economy")
        st.divider()
        if st.button("🔄 Raffraîchir", width="stretch"):
            _kick_ingestion()
            st.rerun()
        st.caption(f"DATA_DIR: `{DATA_DIR.name}/`")

    # En-tête
    st.title("🔷 BLUESTAR Pipeline Calendar")
    st.caption("News économiques en temps réel — données depuis le flux public Forex Factory (Fair Economy)")

    render_status_bar()

    tab_events, tab_summary, tab_exports, tab_diag = st.tabs(
        ["🎯 Événements", "📅 Résumé", "📦 Export JSON", "🔧 Diagnostics"]
    )
    with tab_events:
        render_events_table()
    with tab_summary:
        render_summary_by_day()
    with tab_exports:
        render_exports()
    with tab_diag:
        render_diagnostics()

    # Footer
    st.divider()
    canonical, _ = load_canonical(*_c_stats())
    if canonical:
        src_age = 0
        try:
            fetched = canonical.get("source", {}).get("fetched_at_utc", "")
            if fetched:
                src_age = int((datetime.now(UTC) - datetime.fromisoformat(
                    fetched.replace("Z", "+00:00")
                )).total_seconds())
        except Exception:
            pass
        st.caption(
            f"Dernière actualisation : {canonical.get('generated_at_utc', '?')}  |  "
            f"Source age : {src_age}s  |  "
            f"Événements : {len(canonical.get('events', []))}  |  "
            f"Ingestion : {'en cours' if _running else 'au repos'}"
        )
    else:
        st.caption(f"Ingestion : {'en cours' if _running else 'au repos'}")


if __name__ == "__main__":
    main()

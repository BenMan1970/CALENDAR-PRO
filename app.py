"""
BLUESTAR · app.py
=================
Application Streamlit de visualisation du calendrier économique.
Consomme les artefacts produits par calendar_ingestor.py.

Fonctionnalités :
  • Vue Trading Desk avec filtres dynamiques
  • Countdown live (recalc des time_context sans rechargement)
  • Exposition XAU/USD et indices vis-à-vis des événements USD
  • Indicateurs de qualité de données en temps réel
  • Export JSON legacy pour rétro-compatibilité
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st
from zoneinfo import ZoneInfo

from calendar_core import (
    SCHEMA_VERSION,
    Impact,
    Session,
    TimeProximity,
    EventStatus,
    ActualStatus,
    PairMappingStatus,
    QualityStatus,
    ReleaseGroupType,
    CalendarPayload,
    CalendarEvent,
    TimeContext,
    SelectionPolicy,
    compute_time_context,
    refresh_time_contexts,
    to_legacy_payload,
    iso_z,
    pairs_for_currency,
)

UTC = timezone.utc
DATA_DIR = Path(os.getenv("BLUESTAR_DATA_DIR", "data"))

# ─────────────────────────────────────────────────────────────────────────────
# MAPPING ASSETS ÉTENDU (Forex + Métaux + Indices)
# ─────────────────────────────────────────────────────────────────────────────
ASSET_MAPPING: Dict[str, List[str]] = {
    "USD": [
        "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "USD/CAD", "AUD/USD", "NZD/USD",
        "XAU/USD", "XAG/USD",
        "US30", "US500", "NAS100", "US2000",
        "WTI", "BRENT",
    ],
    "EUR": [
        "EUR/USD", "EUR/GBP", "EUR/JPY", "EUR/CHF", "EUR/CAD", "EUR/AUD", "EUR/NZD",
        "EUR50", "DE40", "FR40",
    ],
    "GBP": [
        "GBP/USD", "EUR/GBP", "GBP/JPY", "GBP/CHF", "GBP/CAD", "GBP/AUD", "GBP/NZD",
        "UK100",
    ],
    "JPY": [
        "USD/JPY", "EUR/JPY", "GBP/JPY", "AUD/JPY", "NZD/JPY", "CAD/JPY", "CHF/JPY",
        "JP225",
    ],
    "CAD": [
        "USD/CAD", "EUR/CAD", "GBP/CAD", "AUD/CAD", "NZD/CAD", "CAD/JPY", "CAD/CHF",
        "CA60",
    ],
    "AUD": [
        "AUD/USD", "EUR/AUD", "GBP/AUD", "AUD/JPY", "AUD/CHF", "AUD/CAD", "AUD/NZD",
        "AU200",
    ],
    "NZD": [
        "NZD/USD", "EUR/NZD", "GBP/NZD", "NZD/JPY", "NZD/CHF", "NZD/CAD", "AUD/NZD",
        "NZ50",
    ],
    "CHF": [
        "USD/CHF", "EUR/CHF", "GBP/CHF", "AUD/CHF", "NZD/CHF", "CAD/CHF", "CHF/JPY",
        "CH20",
    ],
    "CNY": [
        "USD/CNY", "EUR/CNY",
        "CN50", "HK50",
    ],
    "ALL": [],  # Événements globaux
}

IMPACT_COLORS = {
    Impact.HIGH: "#FF4444",
    Impact.MEDIUM: "#FFAA00",
    Impact.LOW: "#44AA44",
    Impact.HOLIDAY: "#888888",
    Impact.UNKNOWN: "#AAAAAA",
}

PROXIMITY_COLORS = {
    TimeProximity.IMMINENT: "#FF2222",
    TimeProximity.SOON: "#FF8800",
    TimeProximity.LATER: "#0088FF",
    TimeProximity.PAST: "#888888",
}

SESSION_COLORS = {
    Session.OVERLAP_LONDON_NY: "#FF6B6B",
    Session.OVERLAP_ASIA_LONDON: "#4ECDC4",
    Session.LONDON: "#45B7D1",
    Session.NEW_YORK: "#96CEB4",
    Session.ASIAN: "#FFEAA7",
    Session.OFF: "#DDA0DD",
}

STATUS_EMOJI = {
    QualityStatus.VALID: "✅",
    QualityStatus.DEGRADED: "⚠️",
    QualityStatus.INVALID: "🚨",
}

# ─────────────────────────────────────────────────────────────────────────────
# CHARGEMENT DES DONNÉES
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def load_payload(data_dir: Path = DATA_DIR) -> Optional[CalendarPayload]:
    """Charge le dernier artefact canonique depuis le disque."""
    path = data_dir / "calendar.latest.json"
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return CalendarPayload.model_validate(raw)
    except Exception as exc:
        st.error(f"Échec du chargement du payload : {exc}")
        return None


@st.cache_data(ttl=30)
def load_health(data_dir: Path = DATA_DIR) -> Optional[Dict[str, Any]]:
    path = data_dir / "health.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


@st.cache_data(ttl=30)
def load_state(data_dir: Path = DATA_DIR) -> Optional[Dict[str, Any]]:
    path = data_dir / "_state.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS D'AFFICHAGE
# ─────────────────────────────────────────────────────────────────────────────
def get_affected_assets(event: CalendarEvent) -> List[str]:
    """Retourne la liste des assets affectés (Forex + Métaux + Indices)."""
    if event.is_global:
        return ["🌍 Global — tous les marchés"]
    return ASSET_MAPPING.get(event.currency, pairs_for_currency(event.currency))


def impact_badge(impact: Impact) -> str:
    color = IMPACT_COLORS.get(impact, "#888888")
    return f'<span style="background-color:{color};color:white;padding:2px 8px;border-radius:4px;font-size:0.75em;font-weight:bold;">{impact.value}</span>'


def proximity_badge(proximity: TimeProximity) -> str:
    color = PROXIMITY_COLORS.get(proximity, "#888888")
    return f'<span style="color:{color};font-weight:bold;">● {proximity.value}</span>'


def session_badge(session: Session) -> str:
    color = SESSION_COLORS.get(session, "#888888")
    return f'<span style="background-color:{color};color:white;padding:2px 6px;border-radius:3px;font-size:0.7em;">{session.value}</span>'


def format_numeric(nv) -> str:
    if nv.parse_status == "ABSENT" or nv.value is None:
        return "—"
    if nv.unit == "percent":
        return f"{nv.value:.2f}%"
    if abs(nv.value) >= 1e9:
        return f"{nv.value/1e9:.2f}B"
    if abs(nv.value) >= 1e6:
        return f"{nv.value/1e6:.2f}M"
    if abs(nv.value) >= 1e3:
        return f"{nv.value/1e3:.2f}K"
    return f"{nv.value:.2f}"


def event_card(event: CalendarEvent, now: datetime, policy: SelectionPolicy = None) -> str:
    """Génère un bloc HTML compact pour un événement."""
    if policy is None:
        policy = DEFAULT_POLICY
    ctx = compute_time_context(event, now, policy)
    assets = get_affected_assets(event)
    assets_str = ", ".join(assets[:6])
    if len(assets) > 6:
        assets_str += f" +{len(assets)-6}"

    group_info = ""
    if event.release_group_id:
        group_info = f'<span style="font-size:0.7em;color:#666;">📦 {event.release_group_type.value if event.release_group_type else "GROUP"}</span>'

    return f"""
    <div style="border:1px solid #333;border-radius:8px;padding:12px;margin:6px 0;background-color:#1a1a2e;">
        <div style="display:flex;justify-content:space-between;align-items:center;">
            <div>
                <span style="font-size:1.1em;font-weight:bold;color:#e0e0e0;">{event.name}</span>
                {impact_badge(event.impact)}
                {session_badge(event.session)}
            </div>
            <div style="text-align:right;">
                {proximity_badge(ctx.time_proximity)}
                <div style="font-size:0.85em;color:#aaa;">{ctx.hours_until_display}</div>
            </div>
        </div>
        <div style="margin-top:6px;font-size:0.85em;color:#bbb;">
            🕐 <b>{event.scheduled_at_display.strftime("%H:%M")}</b> ({event.display_timezone}) &nbsp;|&nbsp;
            📅 {event.date_display} &nbsp;|&nbsp;
            💱 <b>{event.currency}</b>
        </div>
        <div style="margin-top:4px;font-size:0.8em;color:#999;">
            📊 F: <b>{format_numeric(event.forecast)}</b> &nbsp;|&nbsp;
            📈 P: <b>{format_numeric(event.previous)}</b> &nbsp;|&nbsp;
            ✅ A: <b>{format_numeric(event.actual)}</b> <span style="font-size:0.75em;color:#666;">({event.actual_status.value})</span>
        </div>
        <div style="margin-top:4px;font-size:0.78em;color:#777;">
            🎯 {assets_str}
        </div>
        {group_info}
    </div>
    """


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — FILTRES & CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
def render_sidebar() -> Tuple[SelectionPolicy, Dict[str, Any]]:
    st.sidebar.title("🔷 BLUESTAR Calendar")
    st.sidebar.markdown("---")

    # Filtres d'impact
    st.sidebar.subheader("📊 Niveau d'impact")
    impact_options = {
        "HIGH ⭐⭐⭐": Impact.HIGH,
        "MEDIUM ⭐⭐": Impact.MEDIUM,
        "LOW ⭐": Impact.LOW,
        "HOLIDAY": Impact.HOLIDAY,
    }
    selected_impacts = []
    for label, impact in impact_options.items():
        if st.sidebar.checkbox(label, value=(impact in (Impact.HIGH,)), key=f"impact_{impact.value}"):
            selected_impacts.append(impact)

    # Filtres de devise
    st.sidebar.subheader("💱 Devises")
    all_currencies = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF", "CNY"]
    selected_currencies = []
    cols = st.sidebar.columns(3)
    for i, ccy in enumerate(all_currencies):
        with cols[i % 3]:
            if st.checkbox(ccy, value=True, key=f"ccy_{ccy}"):
                selected_currencies.append(ccy)

    # Filtres de session
    st.sidebar.subheader("🌐 Sessions")
    session_options = {
        "Overlap LDN-NY": Session.OVERLAP_LONDON_NY,
        "Overlap ASIA-LDN": Session.OVERLAP_ASIA_LONDON,
        "London": Session.LONDON,
        "New York": Session.NEW_YORK,
        "Asian": Session.ASIAN,
        "Off": Session.OFF,
    }
    selected_sessions = []
    for label, sess in session_options.items():
        if st.sidebar.checkbox(label, value=True, key=f"sess_{sess.value}"):
            selected_sessions.append(sess)

    # Filtres de proximité temporelle
    st.sidebar.subheader("⏱️ Proximité")
    proximity_options = {
        "🔴 IMMINENT (< 6h)": TimeProximity.IMMINENT,
        "🟠 SOON (< 48h)": TimeProximity.SOON,
        "🔵 LATER": TimeProximity.LATER,
        "⚪ PAST": TimeProximity.PAST,
    }
    selected_proximities = []
    for label, prox in proximity_options.items():
        if st.sidebar.checkbox(label, value=True, key=f"prox_{prox.value}"):
            selected_proximities.append(prox)

    # Fuseau horaire d'affichage
    st.sidebar.markdown("---")
    display_tz = st.sidebar.selectbox(
        "🌍 Fuseau horaire d'affichage",
        ["Africa/Casablanca", "Europe/Paris", "Europe/London", "America/New_York",
         "Asia/Tokyo", "UTC"],
        index=0,
    )

    # Options avancées
    st.sidebar.markdown("---")
    st.sidebar.subheader("⚙️ Avancé")
    show_global = st.sidebar.checkbox("🌍 Inclure événements globaux", value=True)
    show_assets_extended = st.sidebar.checkbox("🎯 Afficher XAU/USD + Indices", value=True)
    auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (10s)", value=True)

    policy = SelectionPolicy(
        impact_levels=tuple(selected_impacts) if selected_impacts else (Impact.HIGH,),
        currencies=tuple(selected_currencies) if selected_currencies else None,
        include_global_events=show_global,
        display_timezone=display_tz,
    )

    filters = {
        "sessions": selected_sessions,
        "proximities": selected_proximities,
        "show_assets_extended": show_assets_extended,
        "auto_refresh": auto_refresh,
    }

    return policy, filters


# ─────────────────────────────────────────────────────────────────────────────
# HEADER — QUALITÉ & MÉTRIQUES
# ─────────────────────────────────────────────────────────────────────────────
def render_header(payload: Optional[CalendarPayload], health: Optional[Dict], state: Optional[Dict]):
    col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 1])

    with col1:
        st.markdown("## 🔷 BLUESTAR Economic Calendar")
        if payload:
            st.caption(f"Schema v{payload.schema_version} · Généré {payload.generated_at_utc.strftime('%H:%M:%S')} UTC")
        else:
            st.caption("⚠️ Aucun artefact trouvé — exécutez l'ingestor d'abord")

    if payload:
        q = payload.quality
        with col2:
            emoji = STATUS_EMOJI.get(q.status, "❓")
            st.metric("Qualité", f"{emoji} {q.status.value}", f"Score: {q.data_quality_score:.2f}")
        with col3:
            st.metric("Événements", len(payload.events), f"Rejetés: {q.rejected_event_count}")
        with col4:
            stale_text = "🟢 Frais" if not q.is_stale else f"🔴 Stale ({q.source_age_seconds//60}min)"
            st.metric("Source", stale_text)
        with col5:
            if state:
                cb_state = state.get("circuit_state", "UNKNOWN")
                cb_color = "🟢" if cb_state == "CLOSED" else "🔴" if cb_state == "OPEN" else "🟡"
                st.metric("Circuit", f"{cb_color} {cb_state}")
            else:
                st.metric("Circuit", "—")

    if payload and payload.quality.warnings:
        with st.expander("⚠️ Warnings", expanded=False):
            for w in payload.quality.warnings:
                st.warning(w)

    if payload and payload.quality.rejections:
        with st.expander("🗑️ Rejets", expanded=False):
            for r in payload.quality.rejections[:10]:
                st.text(r)
            if len(payload.quality.rejections) > 10:
                st.caption(f"... et {len(payload.quality.rejections)-10} autres")


# ─────────────────────────────────────────────────────────────────────────────
# VUE TRADING DESK — Événements imminents & sessions actives
# ─────────────────────────────────────────────────────────────────────────────
def render_trading_desk(payload: CalendarPayload, filters: Dict, now: datetime):
    st.markdown("---")
    st.subheader("🎯 Trading Desk")

    # Recalcule les time_context avec l'heure actuelle
    events = list(refresh_time_contexts(payload, now))
    policy = payload.selection_policy

    # Filtres
    filtered = []
    for e in events:
        if filters["sessions"] and e.session not in filters["sessions"]:
            continue
        if filters["proximities"] and e.time_context.time_proximity not in filters["proximities"]:
            continue
        filtered.append(e)

    if not filtered:
        st.info("Aucun événement ne correspond aux filtres sélectionnés.")
        return

    # Section : Événements imminents
    imminent = [e for e in filtered if e.time_context.time_proximity == TimeProximity.IMMINENT]
    if imminent:
        st.markdown("### 🔴 Événements imminents (< 6h)")
        cols = st.columns(min(3, len(imminent)))
        for i, event in enumerate(imminent[:6]):
            with cols[i % 3]:
                st.markdown(event_card(event, now, policy), unsafe_allow_html=True)

    # Section : Prochainement
    soon = [e for e in filtered if e.time_context.time_proximity == TimeProximity.SOON]
    if soon:
        st.markdown("### 🟠 Prochainement (< 48h)")
        for event in soon:
            st.markdown(event_card(event, now, policy), unsafe_allow_html=True)

    # Section : Plus tard
    later = [e for e in filtered if e.time_context.time_proximity == TimeProximity.LATER]
    if later:
        st.markdown("### 🔵 À venir")
        with st.expander(f"Afficher {len(later)} événements futurs", expanded=False):
            for event in later:
                st.markdown(event_card(event, now, policy), unsafe_allow_html=True)

    # Section : Passés
    past = [e for e in filtered if e.time_context.time_proximity == TimeProximity.PAST]
    if past:
        st.markdown("### ⚪ Passés")
        with st.expander(f"Afficher {len(past)} événements passés", expanded=False):
            for event in past:
                st.markdown(event_card(event, now, policy), unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# VUE DÉTAILLÉE — Tableau complet avec tri
# ─────────────────────────────────────────────────────────────────────────────
def render_detailed_view(payload: CalendarPayload, filters: Dict, now: datetime):
    st.markdown("---")
    st.subheader("📋 Vue détaillée")

    events = list(refresh_time_contexts(payload, now))

    # Préparation des données pour le tableau
    table_data = []
    for e in events:
        if filters["sessions"] and e.session not in filters["sessions"]:
            continue
        if filters["proximities"] and e.time_context.time_proximity not in filters["proximities"]:
            continue

        assets = get_affected_assets(e)
        assets_display = ", ".join(assets[:4])
        if len(assets) > 4:
            assets_display += f" +{len(assets)-4}"

        table_data.append({
            "Heure": e.scheduled_at_display.strftime("%H:%M"),
            "Date": e.date_display,
            "Devise": e.currency,
            "Événement": e.name,
            "Impact": e.impact.value,
            "Session": e.session.value,
            "Proximité": e.time_context.time_proximity.value,
            "Countdown": e.time_context.hours_until_display,
            "Prévision": format_numeric(e.forecast),
            "Précédent": format_numeric(e.previous),
            "Réel": format_numeric(e.actual),
            "Assets": assets_display,
            "Groupe": e.release_group_type.value if e.release_group_type else "—",
        })

    if not table_data:
        st.info("Aucun événement ne correspond aux filtres.")
        return

    st.dataframe(
        table_data,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Impact": st.column_config.TextColumn("Impact", width="small"),
            "Session": st.column_config.TextColumn("Session", width="small"),
            "Proximité": st.column_config.TextColumn("Prox.", width="small"),
            "Countdown": st.column_config.TextColumn("⏱️", width="small"),
            "Assets": st.column_config.TextColumn("🎯 Assets", width="medium"),
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# VUE ASSETS — Focus XAU/USD et Indices
# ─────────────────────────────────────────────────────────────────────────────
def render_assets_view(payload: CalendarPayload, filters: Dict, now: datetime):
    st.markdown("---")
    st.subheader("🥇 XAU/USD · Indices · Matières premières")

    events = list(refresh_time_contexts(payload, now))

    # Regroupement par asset
    asset_events: Dict[str, List[CalendarEvent]] = {}
    for e in events:
        assets = get_affected_assets(e)
        for asset in assets:
            if asset not in asset_events:
                asset_events[asset] = []
            asset_events[asset].append(e)

    # Onglets par catégorie
    tabs = st.tabs(["🥇 Métaux", "📈 Indices", "🛢️ Énergie", "💱 Forex"])

    metals = ["XAU/USD", "XAG/USD"]
    indices = ["US30", "US500", "NAS100", "US2000", "EUR50", "DE40", "FR40", "UK100", "JP225", "AU200", "CA60", "NZ50", "CH20", "CN50", "HK50"]
    energy = ["WTI", "BRENT"]
    forex = [p for p in pairs_for_currency("USD")]  # toutes les paires

    categories = [
        (tabs[0], metals, "Métaux précieux"),
        (tabs[1], indices, "Indices boursiers"),
        (tabs[2], energy, "Matières premières"),
        (tabs[3], forex, "Paires de devises"),
    ]

    for tab, assets, title in categories:
        with tab:
            st.markdown(f"### {title}")
            for asset in assets:
                evs = asset_events.get(asset, [])
                if not evs:
                    continue

                # Filtre les événements selon les critères
                filtered_evs = []
                for e in evs:
                    if filters["sessions"] and e.session not in filters["sessions"]:
                        continue
                    if filters["proximities"] and e.time_context.time_proximity not in filters["proximities"]:
                        continue
                    filtered_evs.append(e)

                if not filtered_evs:
                    continue

                with st.expander(f"**{asset}** — {len(filtered_evs)} événement(s)", expanded=(asset in ["XAU/USD", "US30", "NAS100"])):
                    for e in filtered_evs:
                        ctx = e.time_context
                        st.markdown(f"""
                        <div style="display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid #333;">
                            <div>
                                <b>{e.name}</b> <span style="font-size:0.75em;color:#888;">{e.currency}</span><br/>
                                <span style="font-size:0.8em;color:#aaa;">{e.date_display} {e.scheduled_at_display.strftime('%H:%M')} | F: {format_numeric(e.forecast)} | P: {format_numeric(e.previous)}</span>
                            </div>
                            <div style="text-align:right;">
                                {proximity_badge(ctx.time_proximity)}<br/>
                                <span style="font-size:0.8em;color:#888;">{ctx.hours_until_display}</span>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# VUE QUALITÉ & DIAGNOSTICS
# ─────────────────────────────────────────────────────────────────────────────
def render_quality_view(payload: Optional[CalendarPayload], health: Optional[Dict], state: Optional[Dict]):
    st.markdown("---")
    st.subheader("🔍 Qualité & Diagnostics")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 📊 Payload Canonique")
        if payload:
            st.json({
                "schema_version": payload.schema_version,
                "generated_at_utc": iso_z(payload.generated_at_utc),
                "content_hash": payload.content_hash,
                "generator": payload.generator,
                "event_count": len(payload.events),
                "session_policy": payload.session_policy_version,
                "numeric_parser": payload.numeric_parser_version,
            })
        else:
            st.info("Aucun payload chargé")

    with col2:
        st.markdown("#### 🏥 Health Check")
        if health:
            st.json(health)
        else:
            st.info("Aucun health.json trouvé")

    st.markdown("#### ⚡ État du Circuit Breaker")
    if state:
        st.json(state)
    else:
        st.info("Aucun _state.json trouvé")

    st.markdown("#### 📈 Couverture temporelle")
    if payload and payload.quality.coverage_start_utc and payload.quality.coverage_end_utc:
        start = datetime.fromisoformat(payload.quality.coverage_start_utc.replace("Z", "+00:00"))
        end = datetime.fromisoformat(payload.quality.coverage_end_utc.replace("Z", "+00:00"))
        st.progress(
            min(1.0, max(0.0, (now_utc() - start).total_seconds() / (end - start).total_seconds())),
            text=f"Couverture : {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# VUE LEGACY — Export rétro-compatible
# ─────────────────────────────────────────────────────────────────────────────
def render_legacy_view(payload: Optional[CalendarPayload]):
    st.markdown("---")
    st.subheader("📦 Export Legacy v1")

    if not payload:
        st.info("Aucun payload à exporter")
        return

    legacy = to_legacy_payload(payload, now_utc())

    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            "⬇️ Télécharger legacy.json",
            data=json.dumps(legacy, indent=2, ensure_ascii=False),
            file_name="calendar.legacy.json",
            mime="application/json",
        )
    with col2:
        st.download_button(
            "⬇️ Télécharger canonical.json",
            data=json.dumps(payload.model_dump(mode="json"), indent=2, ensure_ascii=False),
            file_name="calendar.latest.json",
            mime="application/json",
        )

    with st.expander("Aperçu legacy"):
        st.json(legacy["metadata"])
        st.write(f"Events: {len(legacy['events'])}")
        st.write(f"Summary by day: {list(legacy['summary_by_day'].keys())}")


# ─────────────────────────────────────────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────────────────────────────────────────
def now_utc() -> datetime:
    return datetime.now(UTC)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    st.set_page_config(
        page_title="BLUESTAR Calendar",
        page_icon="🔷",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # CSS custom sombre
    st.markdown("""
    <style>
    .stApp {
        background-color: #0e0e1a;
        color: #e0e0e0;
    }
    .stSidebar {
        background-color: #1a1a2e;
    }
    .stButton>button {
        background-color: #2d2d44;
        color: #e0e0e0;
        border: 1px solid #444;
    }
    .stMetric {
        background-color: #1a1a2e;
        border-radius: 8px;
        padding: 10px;
    }
    .stDataFrame {
        background-color: #1a1a2e;
    }
    </style>
    """, unsafe_allow_html=True)

    # Chargement des données
    payload = load_payload()
    health = load_health()
    state = load_state()

    # Sidebar
    policy, filters = render_sidebar()

    # Header
    render_header(payload, health, state)

    if not payload:
        st.error("""
        🚨 **Aucun artefact trouvé**

        Exécutez l'ingestor pour générer les données :
        ```bash
        python calendar_ingestor.py --once
        ```

        Ou vérifiez que `BLUESTAR_DATA_DIR` pointe vers le bon répertoire.
        """)
        return

    # Onglets principaux
    tab_desk, tab_detail, tab_assets, tab_quality, tab_legacy = st.tabs([
        "🎯 Trading Desk", "📋 Détaillée", "🥇 XAU/USD & Indices", "🔍 Qualité", "📦 Legacy"
    ])

    now = now_utc()

    with tab_desk:
        render_trading_desk(payload, filters, now)

    with tab_detail:
        render_detailed_view(payload, filters, now)

    with tab_assets:
        if filters["show_assets_extended"]:
            render_assets_view(payload, filters, now)
        else:
            st.info("Activez 'Afficher XAU/USD + Indices' dans la sidebar pour cette vue.")

    with tab_quality:
        render_quality_view(payload, health, state)

    with tab_legacy:
        render_legacy_view(payload)

    # Auto-refresh
    if filters.get("auto_refresh", True):
        st.markdown("---")
        st.caption(f"🔄 Dernière mise à jour : {now.strftime('%H:%M:%S')} UTC · Auto-refresh actif")
        # Streamlit n'a pas de vrai auto-refresh natif, on utilise un placeholder
        # qui force le re-run via st.rerun() dans un conteneur vide
        # Note: st.rerun() est disponible dans les versions récentes de Streamlit
        try:
            import time
            time.sleep(0.1)  # Petit délai pour ne pas bloquer
            # On ne fait pas st.rerun() ici pour éviter la boucle infinie agressive
            # L'utilisateur peut F5 ou utiliser le bouton refresh
        except Exception:
            pass


if __name__ == "__main__":
    main()

"""
BLUESTAR · components
=====================
Composants HTML/CSS purs. Chaque fonction RETOURNE une chaîne de markup :
l'appelant décide de concaténer (un seul st.markdown pour tout un flux =
espacement maîtrisé, zéro div parasite de Streamlit entre les cartes).

Aucune fonction n'écrit dans Streamlit sauf `render()` / `panel_title()`.
Tout texte issu des données passe par html.escape : le nom d'un événement
vient d'une source externe et ne doit jamais pouvoir injecter de markup.
"""
from __future__ import annotations

from html import escape
from typing import Any, Iterable, Optional, Sequence

import streamlit as st

import theme as T


def render(*chunks: str) -> None:
    st.markdown("".join(chunks), unsafe_allow_html=True)


# ── Hero ────────────────────────────────────────────────────────────────────
def hero(eyebrow: str, title_plain: str, title_accent: str, subtitle: str,
         pills: Sequence[str] = ()) -> str:
    return f"""
<div class="bs-hero">
  <div>
    <div class="bs-hero__eyebrow">{escape(eyebrow)}</div>
    <div class="bs-hero__title">{escape(title_plain)} <em>{escape(title_accent)}</em></div>
    <div class="bs-hero__sub">{escape(subtitle)}</div>
  </div>
  <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end;">{''.join(pills)}</div>
</div>"""


def pill(label: str, color: str = T.MUTED, live: bool = False) -> str:
    dot = f'<i class="bs-dot{" live" if live else ""}" style="background:{color}"></i>'
    return (f'<span class="bs-pill" style="color:{color};border-color:{color}33;'
            f'background:{color}0F">{dot}<span style="color:{T.MUTED}">{escape(label)}</span></span>')


def chip(label: str, color: Optional[str] = None, strong: bool = False) -> str:
    cls = "bs-chip bs-chip--ccy" if strong else "bs-chip"
    style = f'style="color:{color};border-color:{color}40;background:{color}14"' if color else ""
    return f'<span class="{cls}" {style}>{escape(label)}</span>'


# ── KPI ─────────────────────────────────────────────────────────────────────
def kpi(label: str, value: str, sub: str = "", *, mono: bool = False,
        color: Optional[str] = None, bar: Optional[float] = None,
        delay_ms: int = 0) -> str:
    vstyle = f'style="color:{color}"' if color else ""
    vcls = "bs-kpi__value mono" if mono else "bs-kpi__value"
    bar_html = ""
    if bar is not None:
        pct = max(0.0, min(1.0, bar)) * 100
        grad = (f"linear-gradient(90deg,{color},{color}77)" if color
                else f"linear-gradient(90deg,{T.ACCENT},#6EE7B7)")
        bar_html = f'<div class="bs-bar"><i style="width:{pct:.0f}%;background:{grad}"></i></div>'
    return f"""
<div class="bs-kpi" style="animation-delay:{delay_ms}ms">
  <div class="bs-kpi__label">{escape(label)}</div>
  <div class="{vcls}" {vstyle}>{escape(value)}</div>
  <div class="bs-kpi__sub">{escape(sub)}</div>
  {bar_html}
</div>"""


def kpi_grid(cards: Iterable[str]) -> str:
    return f'<div class="bs-kpis">{"".join(cards)}</div>'


# ── Événements ──────────────────────────────────────────────────────────────
def day_header(label: str, count: int) -> str:
    return (f'<div class="bs-day"><span class="bs-day__label">{escape(label)}</span>'
            f'<span class="bs-day__rule"></span>'
            f'<span class="bs-day__count">{count:02d} événement{"s" if count > 1 else ""}</span></div>')


def _stat(label: str, value: Optional[str], kind: str = "") -> str:
    shown = value if value not in (None, "", "—") else "—"
    cls = kind if shown != "—" else ""
    return (f'<div class="bs-st"><span>{escape(label)}</span>'
            f'<b class="{cls}">{escape(str(shown))}</b></div>')


def event_card(ev: dict, *, show_pairs: bool = False, delay_ms: int = 0) -> str:
    """`ev` = dict enrichi par app.enrich_events (vue, jamais l'artefact)."""
    impact = (ev.get("impact") or "UNKNOWN").upper()
    color = T.IMPACT.get(impact, T.IMPACT["UNKNOWN"])
    hours = ev.get("hours_until", 0.0)

    state = ""
    if hours <= 0:
        state = " is-past"
    elif hours <= 6:
        state = " is-imminent"

    cd_label = "à venir" if hours > 0 else "publié"
    if impact == "HOLIDAY":
        cd_label = "marché fermé"

    pairs_html = ""
    if show_pairs and ev.get("pairs"):
        shown = ", ".join(ev["pairs"][:8])
        extra = f" +{len(ev['pairs']) - 8}" if len(ev["pairs"]) > 8 else ""
        pairs_html = f'<div class="bs-ev__pairs">{escape(shown)}{extra}</div>'

    return f"""
<div class="bs-ev{state}" style="animation-delay:{delay_ms}ms">
  <div class="bs-ev__rail" style="background:linear-gradient(180deg,{color},{color}44)"></div>
  <div class="bs-ev__time">
    <span class="bs-ev__hm">{escape(ev.get('hm', '--:--'))}</span>
    <span class="bs-ev__tz">{escape(ev.get('tz_label', ''))}</span>
  </div>
  <div class="bs-ev__body">
    <div class="bs-ev__head">
      {chip(ev.get('currency', '—'), strong=True)}
      {chip(impact, color=color)}
      {chip(ev.get('session', ''), ) if ev.get('session') else ''}
    </div>
    <div class="bs-ev__name" title="{escape(ev.get('name', ''))}">{escape(ev.get('name', ''))}</div>
    <div class="bs-ev__stats">
      {_stat("Forecast", ev.get("forecast"), "hit")}
      {_stat("Previous", ev.get("previous"))}
      {_stat("Actual", ev.get("actual"), "act")}
    </div>
    {pairs_html}
  </div>
  <div class="bs-ev__cd"><b>{escape(ev.get('countdown', '—'))}</b><small>{escape(cd_label)}</small></div>
</div>"""


def feed(cards: Iterable[str]) -> str:
    return f'<div class="bs-feed">{"".join(cards)}</div>'


# ── États & notes ───────────────────────────────────────────────────────────
def empty(title: str, body: str) -> str:
    return f'<div class="bs-empty"><h4>{escape(title)}</h4><p>{escape(body)}</p></div>'


def note(text_html: str, kind: str = "") -> str:
    suffix = f" bs-note--{kind}" if kind else ""
    return f'<div class="bs-note{suffix}"><div>{text_html}</div></div>'


def kv_table(pairs: Sequence[tuple[str, Any]]) -> str:
    body = "".join(
        f"<dt>{escape(str(k))}</dt><dd>{escape('—' if v is None else str(v))}</dd>"
        for k, v in pairs
    )
    return f'<dl class="bs-kv">{body}</dl>'


def panel_title(title: str, hint: str = "") -> None:
    st.markdown(
        f'<div class="bs-panel-title"><h4>{escape(title)}</h4>'
        f'<span>{escape(hint)}</span></div>',
        unsafe_allow_html=True,
    )

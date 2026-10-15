"""
BLUESTAR · ui — design system « Midnight Luxe »
================================================
Un seul module pour tout le rendu de l'app : tokens, feuille de styles,
composants HTML et charts Plotly.

  • UN point de vérité pour les couleurs : les constantes Python alimentent
    le CSS ET les charts (aucune couleur écrite deux fois) ;
  • les composants RETOURNENT une chaîne de markup — l'appelant décide de
    concaténer (un seul st.markdown pour tout un flux = espacement maîtrisé,
    zéro div parasite de Streamlit entre les cartes) ; seuls render() et
    panel_title() écrivent directement ;
  • tout texte issu des données passe par html.escape : le nom d'un événement
    vient d'une source externe et ne doit jamais pouvoir injecter de markup ;
  • charts « frameless » : papier et tracé transparents, aucune grille
    verticale, barre d'outils masquée. Dégradation propre si plotly est
    absent (AVAILABLE=False → l'app affiche un état vide maison, jamais de
    stack trace).
"""
from __future__ import annotations

from collections import Counter, OrderedDict
from html import escape
from typing import Any, Dict, Iterable, Optional, Sequence

import streamlit as st

# ── Tokens ───────────────────────────────────────────────────────────────────
BG = "#030712"
BG_ELEV = "#070C16"
SURFACE = "rgba(255,255,255,0.024)"
SURFACE_HOVER = "rgba(255,255,255,0.045)"
LINE = "rgba(148,163,184,0.11)"
LINE_STRONG = "rgba(148,163,184,0.20)"

TEXT = "#F8FAFC"
MUTED = "#94A3B8"
FAINT = "#64748B"
GHOST = "#475569"

ACCENT = "#22D3EE"          # accent unique, électrique
ACCENT_DIM = "rgba(34,211,238,0.14)"
ACCENT_GLOW = "rgba(34,211,238,0.28)"

# Couleurs sémantiques (information, pas décoration)
IMPACT = {
    "HIGH": "#FB7185",
    "MEDIUM": "#FBBF24",
    "LOW": "#38BDF8",
    "HOLIDAY": "#94A3B8",
    "UNKNOWN": "#64748B",
}
OK = "#34D399"
WARN = "#FBBF24"
BAD = "#FB7185"

FONT = "'Inter', -apple-system, BlinkMacSystemFont, 'SF Pro Display', system-ui, sans-serif"
MONO = "'JetBrains Mono', ui-monospace, 'SF Mono', Menlo, monospace"

QUALITY_COLOR = {"VALID": OK, "DEGRADED": WARN, "INVALID": BAD, "UNAVAILABLE": GHOST}


def _css() -> str:
    return f"""
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

:root {{
  --bg:{BG}; --bg-elev:{BG_ELEV}; --surface:{SURFACE}; --surface-h:{SURFACE_HOVER};
  --line:{LINE}; --line-strong:{LINE_STRONG};
  --text:{TEXT}; --muted:{MUTED}; --faint:{FAINT}; --ghost:{GHOST};
  --accent:{ACCENT}; --accent-dim:{ACCENT_DIM}; --accent-glow:{ACCENT_GLOW};
  --high:{IMPACT['HIGH']}; --medium:{IMPACT['MEDIUM']}; --low:{IMPACT['LOW']};
  --ok:{OK}; --warn:{WARN}; --bad:{BAD};
  --r-sm:8px; --r:12px; --r-lg:16px; --r-xl:20px;
  --font:{FONT}; --mono:{MONO};
  --ease:cubic-bezier(.22,.61,.36,1);
}}

/* ── Fond : dégradés radiaux très faibles = profondeur sans bruit ───────── */
.stApp {{
  background:
    radial-gradient(1100px 620px at 12% -8%, rgba(34,211,238,0.055), transparent 60%),
    radial-gradient(900px 520px at 92% 4%, rgba(52,211,153,0.035), transparent 62%),
    var(--bg);
  color: var(--text);
  font-family: var(--font);
  -webkit-font-smoothing: antialiased;
}}
html, body, [class*="css"] {{ font-family: var(--font); }}

/* ── Frameless : on efface le chrome natif ─────────────────────────────── */
[data-testid="stHeader"] {{ background: transparent; height: 0; }}
[data-testid="stToolbar"] {{ right: 12px; top: 6px; }}
[data-testid="stDecoration"], #MainMenu, footer {{ display: none !important; }}
[data-testid="stStatusWidget"] {{ display: none; }}
.block-container {{ padding: 1.1rem 2.6rem 4rem; max-width: 1520px; }}
[data-testid="stVerticalBlock"] {{ gap: 0.85rem; }}
hr, [data-testid="stDivider"] hr {{
  border: 0; height: 1px; background: linear-gradient(90deg, var(--line), transparent);
}}

h1, h2, h3, h4 {{
  font-weight: 700 !important; letter-spacing: -0.028em !important;
  color: var(--text) !important;
}}
p, span, label, li {{ color: var(--text); }}

/* ── Sidebar ───────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {{
  background: linear-gradient(180deg, #060A13 0%, #04070E 100%);
  border-right: 1px solid var(--line);
}}
[data-testid="stSidebar"] .block-container {{ padding-top: 1.4rem; }}
[data-testid="stSidebarUserContent"] {{ padding-top: 0.6rem; }}
[data-testid="stSidebar"] label p {{
  font-size: 0.70rem !important; letter-spacing: 0.10em; text-transform: uppercase;
  color: var(--faint) !important; font-weight: 600 !important;
}}

/* ── Onglets : pills, zéro bordure native ──────────────────────────────── */
[data-baseweb="tab-list"] {{
  gap: 4px !important; border-bottom: none !important; background: transparent !important;
  padding: 4px; border: 1px solid var(--line); border-radius: 14px;
  width: fit-content; margin-bottom: 0.5rem;
}}
[data-baseweb="tab-list"] button[data-baseweb="tab"] {{
  background: transparent !important; border: none !important; border-radius: 10px !important;
  padding: 7px 16px !important; color: var(--faint) !important;
  font-size: 0.80rem !important; font-weight: 600 !important; letter-spacing: -0.01em;
  transition: all .22s var(--ease);
}}
[data-baseweb="tab-list"] button[data-baseweb="tab"]:hover {{
  color: var(--text) !important; background: var(--surface) !important;
}}
[data-baseweb="tab-list"] button[aria-selected="true"] {{
  background: linear-gradient(180deg, rgba(34,211,238,0.16), rgba(34,211,238,0.06)) !important;
  color: var(--text) !important;
  box-shadow: inset 0 0 0 1px var(--accent-glow);
}}
[data-baseweb="tab-highlight"], [data-baseweb="tab-border"] {{ display: none !important; }}

/* ── Panels (st.container(border=True)) ────────────────────────────────── */
[data-testid="stVerticalBlockBorderWrapper"] {{
  background: linear-gradient(180deg, rgba(255,255,255,0.028), rgba(255,255,255,0.008));
  border: 1px solid var(--line) !important; border-radius: var(--r-lg) !important;
  padding: 1.0rem 1.15rem !important;
  backdrop-filter: blur(6px);
}}

/* ── Cartes KPI ────────────────────────────────────────────────────────── */
.bs-kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; }}
.bs-kpi {{
  position: relative; overflow: hidden;
  background: linear-gradient(180deg, rgba(255,255,255,0.032), rgba(255,255,255,0.006));
  border: 1px solid var(--line); border-radius: var(--r-lg); padding: 15px 17px 16px;
  transition: transform .28s var(--ease), border-color .28s var(--ease);
  animation: bsUp .5s var(--ease) both;
}}
.bs-kpi:hover {{ transform: translateY(-3px); border-color: var(--line-strong); }}
.bs-kpi::after {{
  content: ""; position: absolute; inset: 0 0 auto 0; height: 1px;
  background: linear-gradient(90deg, transparent, var(--accent-glow), transparent);
  opacity: .8;
}}
.bs-kpi__label {{
  font-size: 0.665rem; letter-spacing: 0.13em; text-transform: uppercase;
  color: var(--faint); font-weight: 600;
}}
.bs-kpi__value {{
  font-size: 1.72rem; font-weight: 750; letter-spacing: -0.045em;
  margin-top: 6px; line-height: 1.05; color: var(--text);
}}
.bs-kpi__value.mono {{ font-family: var(--mono); font-size: 1.28rem; letter-spacing: -0.02em; }}
.bs-kpi__sub {{ font-size: 0.735rem; color: var(--muted); margin-top: 5px; }}
.bs-bar {{ height: 3px; border-radius: 99px; background: rgba(148,163,184,0.14); margin-top: 11px; overflow: hidden; }}
.bs-bar > i {{ display: block; height: 100%; border-radius: 99px; background: linear-gradient(90deg, var(--accent), #6EE7B7); }}

/* ── En-tête / hero ───────────────────────────────────────────────────── */
.bs-hero {{ display: flex; align-items: flex-end; justify-content: space-between; gap: 18px; margin: 4px 0 18px; }}
.bs-hero__eyebrow {{
  font-size: 0.66rem; letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--accent); font-weight: 700; margin-bottom: 6px;
}}
.bs-hero__title {{ font-size: 2.02rem; font-weight: 780; letter-spacing: -0.048em; line-height: 1.04; }}
.bs-hero__title em {{
  font-style: normal;
  background: linear-gradient(94deg, var(--accent), #A7F3D0 62%);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}}
.bs-hero__sub {{ color: var(--muted); font-size: 0.83rem; margin-top: 7px; max-width: 62ch; }}

/* ── Pills / chips ────────────────────────────────────────────────────── */
.bs-pill {{
  display: inline-flex; align-items: center; gap: 7px;
  padding: 5px 11px; border-radius: 99px; font-size: 0.705rem; font-weight: 600;
  border: 1px solid var(--line); background: var(--surface); color: var(--muted);
  white-space: nowrap;
}}
.bs-dot {{ width: 6px; height: 6px; border-radius: 99px; background: currentColor; flex: 0 0 auto; }}
.bs-dot.live {{ box-shadow: 0 0 0 0 currentColor; animation: bsPulse 2.1s infinite; }}
.bs-chip {{
  display: inline-flex; align-items: center; gap: 6px; padding: 3px 9px;
  border-radius: 7px; font-size: 0.67rem; font-weight: 700; letter-spacing: 0.05em;
  font-family: var(--mono); border: 1px solid var(--line); color: var(--muted);
  background: rgba(255,255,255,0.03);
}}
.bs-chip--ccy {{ color: var(--text); border-color: var(--line-strong); }}

/* ── Carte événement ─────────────────────────────────────────────────── */
.bs-day {{ display: flex; align-items: center; gap: 12px; margin: 22px 2px 11px; }}
.bs-day__label {{ font-size: 0.72rem; font-weight: 700; letter-spacing: 0.14em; text-transform: uppercase; color: var(--muted); }}
.bs-day__rule {{ flex: 1; height: 1px; background: linear-gradient(90deg, var(--line), transparent); }}
.bs-day__count {{ font-size: 0.68rem; color: var(--ghost); font-family: var(--mono); }}

.bs-feed {{ display: flex; flex-direction: column; gap: 9px; }}
.bs-ev {{
  position: relative; display: grid; grid-template-columns: 3px 84px 1fr auto; gap: 14px;
  align-items: center; padding: 13px 16px 13px 0;
  background: linear-gradient(180deg, rgba(255,255,255,0.026), rgba(255,255,255,0.006));
  border: 1px solid var(--line); border-radius: var(--r); overflow: hidden;
  transition: transform .26s var(--ease), border-color .26s var(--ease), background .26s var(--ease);
  animation: bsUp .42s var(--ease) both;
}}
.bs-ev:hover {{ transform: translateY(-2px); border-color: var(--line-strong); background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.012)); }}
.bs-ev__rail {{ height: 100%; min-height: 58px; border-radius: 0 3px 3px 0; opacity: .85; }}
.bs-ev__time {{ padding-left: 2px; }}
.bs-ev__hm {{ font-family: var(--mono); font-size: 0.98rem; font-weight: 600; letter-spacing: -0.02em; color: var(--text); }}
.bs-ev__tz {{ display: block; font-size: 0.605rem; color: var(--ghost); margin-top: 2px; letter-spacing: 0.02em; }}
.bs-ev__body {{ min-width: 0; }}
.bs-ev__head {{ display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }}
.bs-ev__name {{
  font-size: 0.915rem; font-weight: 600; letter-spacing: -0.018em; margin-top: 6px;
  color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}}
.bs-ev__stats {{ display: flex; gap: 20px; margin-top: 8px; flex-wrap: wrap; }}
.bs-st {{ display: flex; flex-direction: column; gap: 2px; }}
.bs-st span {{ font-size: 0.605rem; letter-spacing: 0.10em; text-transform: uppercase; color: var(--ghost); font-weight: 600; }}
.bs-st b {{ font-family: var(--mono); font-size: 0.79rem; font-weight: 500; color: var(--muted); }}
.bs-st b.hit {{ color: var(--text); }}
.bs-st b.act {{ color: var(--ok); font-weight: 600; }}
.bs-ev__pairs {{ margin-top: 9px; font-family: var(--mono); font-size: 0.645rem; color: var(--ghost); letter-spacing: 0.02em; }}
.bs-ev__cd {{ text-align: right; padding-left: 10px; min-width: 104px; }}
.bs-ev__cd b {{ display: block; font-family: var(--mono); font-size: 0.875rem; font-weight: 600; letter-spacing: -0.02em; color: var(--muted); }}
.bs-ev__cd small {{ font-size: 0.605rem; letter-spacing: 0.10em; text-transform: uppercase; color: var(--ghost); }}
.bs-ev.is-imminent {{ border-color: var(--accent-glow); }}
.bs-ev.is-imminent .bs-ev__cd b {{ color: var(--accent); text-shadow: 0 0 22px var(--accent-glow); }}
.bs-ev.is-past {{ opacity: .52; }}
.bs-ev.is-past:hover {{ opacity: .82; }}

/* ── Vide / alertes maison ───────────────────────────────────────────── */
.bs-empty {{
  border: 1px dashed var(--line-strong); border-radius: var(--r-lg);
  padding: 46px 22px; text-align: center; background: rgba(255,255,255,0.012);
}}
.bs-empty h4 {{ font-size: 0.97rem; margin: 0 0 6px; }}
.bs-empty p {{ color: var(--faint); font-size: 0.8rem; margin: 0; }}
.bs-note {{
  display: flex; gap: 11px; align-items: flex-start; padding: 12px 14px;
  border-radius: var(--r); border: 1px solid var(--line);
  background: rgba(255,255,255,0.02); font-size: 0.795rem; color: var(--muted);
}}
.bs-note b {{ color: var(--text); font-weight: 600; }}
.bs-note--warn {{ border-color: rgba(251,191,36,0.30); background: rgba(251,191,36,0.055); }}
.bs-note--bad {{ border-color: rgba(251,113,133,0.32); background: rgba(251,113,133,0.06); }}
.bs-note--ok {{ border-color: rgba(52,211,153,0.28); background: rgba(52,211,153,0.05); }}

.bs-panel-title {{ display: flex; align-items: baseline; justify-content: space-between; margin-bottom: 8px; }}
.bs-panel-title h4 {{ font-size: 0.83rem !important; font-weight: 650 !important; margin: 0 !important; letter-spacing: -0.01em; }}
.bs-panel-title span {{ font-size: 0.66rem; color: var(--ghost); letter-spacing: 0.08em; text-transform: uppercase; }}

.bs-kv {{ display: grid; grid-template-columns: 180px 1fr; gap: 7px 16px; font-size: 0.775rem; }}
.bs-kv dt {{ color: var(--faint); }}
.bs-kv dd {{ margin: 0; font-family: var(--mono); color: var(--text); font-size: 0.735rem; word-break: break-all; }}

/* ── Widgets Streamlit réhabillés ────────────────────────────────────── */
.stButton button, .stDownloadButton button {{
  border-radius: 11px !important; font-family: var(--font) !important;
  font-size: 0.79rem !important; font-weight: 600 !important; min-height: 40px !important;
  letter-spacing: -0.01em !important; transition: all .22s var(--ease) !important;
}}
.stButton button[kind="secondary"], .stDownloadButton button[kind="secondary"] {{
  background: var(--surface) !important; border: 1px solid var(--line) !important; color: var(--muted) !important;
}}
.stButton button[kind="secondary"]:hover, .stDownloadButton button[kind="secondary"]:hover {{
  background: var(--surface-h) !important; border-color: var(--line-strong) !important;
  color: var(--text) !important; transform: translateY(-1px);
}}
.stButton button[kind="primary"], .stDownloadButton button[kind="primary"] {{
  background: linear-gradient(180deg, rgba(34,211,238,0.22), rgba(34,211,238,0.08)) !important;
  border: 1px solid var(--accent-glow) !important; color: var(--text) !important;
}}
.stButton button[kind="primary"]:hover, .stDownloadButton button[kind="primary"]:hover {{
  transform: translateY(-1px); box-shadow: 0 8px 26px -14px var(--accent-glow) !important;
}}
[data-testid="stBaseButton-secondary"]:disabled {{ opacity: .38 !important; }}

[data-baseweb="select"] > div, [data-baseweb="input"] > div, .stTextInput input {{
  background: rgba(255,255,255,0.028) !important; border: 1px solid var(--line) !important;
  border-radius: 10px !important; color: var(--text) !important; font-size: 0.80rem !important;
}}
[data-baseweb="tag"] {{
  background: var(--accent-dim) !important; border: 1px solid var(--accent-glow) !important;
  color: var(--text) !important; border-radius: 7px !important; font-size: 0.70rem !important;
}}
[data-testid="stSliderTickBar"] {{ background: transparent !important; }}
[data-testid="stToggle"] div[data-baseweb="checkbox"] div {{ border-radius: 99px; }}

[data-testid="stDataFrame"] {{ border: 1px solid var(--line); border-radius: var(--r); overflow: hidden; }}
[data-testid="stDataFrame"] * {{ font-size: 0.755rem !important; }}
.stJson, [data-testid="stJson"] {{
  background: rgba(255,255,255,0.018) !important; border: 1px solid var(--line);
  border-radius: var(--r); padding: 10px 12px;
}}
code, pre, .stJson {{ font-family: var(--mono) !important; font-size: 0.72rem !important; }}
[data-testid="stExpander"] details {{
  background: rgba(255,255,255,0.016); border: 1px solid var(--line) !important;
  border-radius: var(--r) !important;
}}
[data-testid="stExpander"] summary {{ font-size: 0.80rem; font-weight: 600; }}
[data-testid="stCaptionContainer"], [data-testid="stCaptionContainer"] p {{
  color: var(--ghost) !important; font-size: 0.715rem !important;
}}
[data-testid="stAlert"] {{ border-radius: var(--r) !important; border: 1px solid var(--line) !important; }}
[data-testid="stSpinner"] i {{ border-top-color: var(--accent) !important; }}

::-webkit-scrollbar {{ width: 9px; height: 9px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{ background: rgba(148,163,184,0.16); border-radius: 99px; }}
::-webkit-scrollbar-thumb:hover {{ background: rgba(148,163,184,0.30); }}
::selection {{ background: var(--accent-glow); color: #041016; }}

@keyframes bsUp {{ from {{ opacity: 0; transform: translateY(7px); }} to {{ opacity: 1; transform: none; }} }}
@keyframes bsPulse {{
  0% {{ box-shadow: 0 0 0 0 currentColor; opacity: 1; }}
  70% {{ box-shadow: 0 0 0 7px transparent; opacity: .75; }}
  100% {{ box-shadow: 0 0 0 0 transparent; opacity: 1; }}
}}
@media (max-width: 1100px) {{
  .block-container {{ padding: 1rem 1.1rem 3rem; }}
  .bs-ev {{ grid-template-columns: 3px 68px 1fr; }}
  .bs-ev__cd {{ display: none; }}
  .bs-hero {{ flex-direction: column; align-items: flex-start; }}
}}
@media (prefers-reduced-motion: reduce) {{
  * {{ animation: none !important; transition: none !important; }}
}}
"""


def inject() -> None:
    """Injecte le design system. Idempotent (une fois par run de script)."""
    st.markdown(f"<style>{_css()}</style>", unsafe_allow_html=True)


# ── Composants HTML ──────────────────────────────────────────────────────────
def render(*chunks: str) -> None:
    st.markdown("".join(chunks), unsafe_allow_html=True)


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


def pill(label: str, color: str = MUTED, live: bool = False) -> str:
    dot = f'<i class="bs-dot{" live" if live else ""}" style="background:{color}"></i>'
    return (f'<span class="bs-pill" style="color:{color};border-color:{color}33;'
            f'background:{color}0F">{dot}<span style="color:{MUTED}">{escape(label)}</span></span>')


def chip(label: str, color: Optional[str] = None, strong: bool = False) -> str:
    cls = "bs-chip bs-chip--ccy" if strong else "bs-chip"
    style = f'style="color:{color};border-color:{color}40;background:{color}14"' if color else ""
    return f'<span class="{cls}" {style}>{escape(label)}</span>'


def kpi(label: str, value: str, sub: str = "", *, mono: bool = False,
        color: Optional[str] = None, bar: Optional[float] = None,
        delay_ms: int = 0) -> str:
    vstyle = f'style="color:{color}"' if color else ""
    vcls = "bs-kpi__value mono" if mono else "bs-kpi__value"
    bar_html = ""
    if bar is not None:
        pct = max(0.0, min(1.0, bar)) * 100
        grad = (f"linear-gradient(90deg,{color},{color}77)" if color
                else f"linear-gradient(90deg,{ACCENT},#6EE7B7)")
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
    color = IMPACT.get(impact, IMPACT["UNKNOWN"])
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


# ── Charts Plotly ────────────────────────────────────────────────────────────
try:
    import plotly.graph_objects as go
    AVAILABLE = True
except Exception:                                            # noqa: BLE001
    go = None                                                # type: ignore
    AVAILABLE = False

PLOTLY_CONFIG = {"displayModeBar": False, "staticPlot": False, "displaylogo": False,
                 "scrollZoom": False, "doubleClick": False}

_IMPACT_ORDER = ("HIGH", "MEDIUM", "LOW", "HOLIDAY", "UNKNOWN")


def _layout(height: int, *, ygrid: bool = True, xgrid: bool = False) -> dict:
    axis = dict(showline=False, zeroline=False, ticks="",
                tickfont=dict(family="Inter, sans-serif", size=10, color=GHOST))
    return dict(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=4, r=4, t=6, b=4),
        font=dict(family="Inter, sans-serif", size=11, color=MUTED),
        showlegend=False,
        bargap=0.42,
        hoverlabel=dict(bgcolor="#0A1120", bordercolor="rgba(148,163,184,0.22)",
                        font=dict(family="Inter, sans-serif", size=11, color=TEXT)),
        xaxis={**axis, "showgrid": xgrid, "gridcolor": "rgba(148,163,184,0.07)"},
        yaxis={**axis, "showgrid": ygrid, "gridcolor": "rgba(148,163,184,0.07)",
               "griddash": "dot"},
        dragmode=False,
    )


def density_by_day(events: Sequence[dict], height: int = 210):
    """Barres empilées : volume d'événements par jour, segmenté par impact."""
    if not AVAILABLE or not events:
        return None
    days: "OrderedDict[str, Counter]" = OrderedDict()
    for e in sorted(events, key=lambda x: x["dt_utc"]):
        days.setdefault(e["date_display"], Counter())[(e["impact"] or "UNKNOWN").upper()] += 1

    labels = list(days)
    short = [l[5:] if len(l) >= 10 else l for l in labels]      # MM-DD
    fig = go.Figure()
    for imp in _IMPACT_ORDER:
        values = [days[l].get(imp, 0) for l in labels]
        if not any(values):
            continue
        fig.add_bar(
            x=short, y=values, name=imp.title(),
            marker=dict(color=IMPACT[imp], line=dict(width=0)),
            hovertemplate=f"<b>%{{x}}</b><br>{imp.title()} · %{{y}}<extra></extra>",
        )
    fig.update_layout(**_layout(height), barmode="stack")
    fig.update_yaxes(rangemode="tozero")
    return fig


def impact_donut(events: Sequence[dict], height: int = 210):
    """Anneau : répartition d'impact. Centre = total (pas de légende bavarde)."""
    if not AVAILABLE or not events:
        return None
    counts = Counter((e["impact"] or "UNKNOWN").upper() for e in events)
    keys = [k for k in _IMPACT_ORDER if counts.get(k)]
    fig = go.Figure(go.Pie(
        labels=[k.title() for k in keys],
        values=[counts[k] for k in keys],
        hole=0.74, sort=False, direction="clockwise",
        marker=dict(colors=[IMPACT[k] for k in keys],
                    line=dict(color="rgba(3,7,18,0.9)", width=2)),
        textinfo="none",
        hovertemplate="<b>%{label}</b><br>%{value} · %{percent}<extra></extra>",
    ))
    total = sum(counts.values())
    fig.update_layout(
        **_layout(height, ygrid=False),
        annotations=[dict(text=f"<b>{total}</b>", x=0.5, y=0.54, showarrow=False,
                          font=dict(size=24, color=TEXT, family="Inter, sans-serif")),
                     dict(text="ÉVÉNEMENTS", x=0.5, y=0.38, showarrow=False,
                          font=dict(size=9, color=GHOST, family="Inter, sans-serif"))],
    )
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    return fig


def currency_exposure(events: Sequence[dict], height: int = 210, top: int = 9):
    """Barres horizontales : charge de publication par devise."""
    if not AVAILABLE or not events:
        return None
    counts = Counter(e.get("currency") or "—" for e in events)
    items = counts.most_common(top)[::-1]
    labels = [k for k, _ in items]
    values = [v for _, v in items]
    vmax = max(values) if values else 1
    colors = [f"rgba(34,211,238,{0.30 + 0.60 * (v / vmax):.2f})" for v in values]
    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker=dict(color=colors, line=dict(color="rgba(34,211,238,0.45)", width=1)),
        text=values, textposition="outside",
        textfont=dict(family="JetBrains Mono, monospace", size=10, color=MUTED),
        hovertemplate="<b>%{y}</b> · %{x} publication(s)<extra></extra>",
    ))
    fig.update_layout(**_layout(height, ygrid=False, xgrid=True))
    fig.update_xaxes(visible=False, range=[0, vmax * 1.18])
    fig.update_yaxes(tickfont=dict(family="JetBrains Mono, monospace", size=11, color=MUTED))
    return fig

"""
BLUESTAR · theme
================
Système de design « Midnight Luxe » : tokens + feuille de style injectée.

Un SEUL point de vérité pour les couleurs : les mêmes constantes Python
alimentent le CSS et les charts Plotly (aucune couleur écrite deux fois).

Principes :
  • fond très sombre (#030712), textes adoucis, hairlines semi-transparentes ;
  • UNE couleur d'accent électrique (cyan #22D3EE) ; les couleurs d'impact
    sont sémantiques (elles portent de l'information, pas de la décoration) ;
  • « frameless » : bordures natives de Streamlit neutralisées (onglets,
    séparateurs, header) ;
  • aucune ombre lourde : élévation par la lumière (bordure + gradient), pas
    par le drop-shadow.
"""
from __future__ import annotations

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


def html(markup: str) -> None:
    """Rendu d'un bloc HTML maison."""
    st.markdown(markup, unsafe_allow_html=True)

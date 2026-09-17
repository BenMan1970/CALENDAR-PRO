"""
BLUESTAR · charts
=================
Plotly configuré pour DISPARAÎTRE dans l'interface : papier et tracé
transparents, aucune grille verticale, barre d'outils masquée, palette
strictement celle de theme.py.

Dégradation propre : si plotly n'est pas installé, `AVAILABLE` est False et
l'app affiche un état vide maison (jamais de stack trace).
"""
from __future__ import annotations

from collections import Counter, OrderedDict
from typing import Dict, List, Sequence

import theme as T

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
                tickfont=dict(family="Inter, sans-serif", size=10, color=T.GHOST))
    return dict(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=4, r=4, t=6, b=4),
        font=dict(family="Inter, sans-serif", size=11, color=T.MUTED),
        showlegend=False,
        bargap=0.42,
        hoverlabel=dict(bgcolor="#0A1120", bordercolor="rgba(148,163,184,0.22)",
                        font=dict(family="Inter, sans-serif", size=11, color=T.TEXT)),
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
            marker=dict(color=T.IMPACT[imp], line=dict(width=0)),
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
        marker=dict(colors=[T.IMPACT[k] for k in keys],
                    line=dict(color="rgba(3,7,18,0.9)", width=2)),
        textinfo="none",
        hovertemplate="<b>%{label}</b><br>%{value} · %{percent}<extra></extra>",
    ))
    total = sum(counts.values())
    fig.update_layout(
        **_layout(height, ygrid=False),
        annotations=[dict(text=f"<b>{total}</b>", x=0.5, y=0.54, showarrow=False,
                          font=dict(size=24, color=T.TEXT, family="Inter, sans-serif")),
                     dict(text="ÉVÉNEMENTS", x=0.5, y=0.38, showarrow=False,
                          font=dict(size=9, color=T.GHOST, family="Inter, sans-serif"))],
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
        textfont=dict(family="JetBrains Mono, monospace", size=10, color=T.MUTED),
        hovertemplate="<b>%{y}</b> · %{x} publication(s)<extra></extra>",
    ))
    fig.update_layout(**_layout(height, ygrid=False, xgrid=True), bargap=0.32)
    fig.update_xaxes(visible=False, range=[0, vmax * 1.18])
    fig.update_yaxes(tickfont=dict(family="JetBrains Mono, monospace", size=11, color=T.MUTED))
    return fig


def quality_sparkline(scores: List[float], height: int = 90):
    """Courbe de score qualité (historique en session). Aire dégradée, sans axes."""
    if not AVAILABLE or len(scores) < 2:
        return None
    fig = go.Figure(go.Scatter(
        y=scores, x=list(range(len(scores))), mode="lines",
        line=dict(color=T.ACCENT, width=2, shape="spline", smoothing=0.6),
        fill="tozeroy", fillcolor="rgba(34,211,238,0.10)",
        hovertemplate="score %{y:.3f}<extra></extra>",
    ))
    fig.update_layout(**_layout(height, ygrid=False))
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False, range=[0, 1.05])
    return fig

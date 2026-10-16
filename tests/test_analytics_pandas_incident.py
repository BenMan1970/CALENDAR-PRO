"""INCIDENT-ANALYTICS — AttributeError "module 'pandas' has no attribute
'Series'" sur l'onglet Analytique, en production (Streamlit Cloud).

Trace d'origine :
    app.py:1428 main -> render_analytics -> ui.py:577 density_by_day
    -> fig.add_bar(..., hovertemplate=...)
    -> _plotly_utils/basevalidators.py:199
       or (pd and isinstance(v, (pd.Series, pd.Index)))
    AttributeError: module 'pandas' has no attribute 'Series'

CAUSE RACINE (reproduite, pas deduite)
--------------------------------------
``_plotly_utils.optional_imports.get_module(name, should_load=False)`` fait
``sys.modules.get(name)``. Si ``pandas`` est present dans ``sys.modules``
mais PAS encore initialise (import circulaire streamlit -> pandas), l'objet
renvoye est TRUTHY mais sans attributs. ``basevalidators`` execute alors
``pd and isinstance(v, (pd.Series, pd.Index))`` sur un module sans
``Series`` -> AttributeError.

Le declencheur est ``hovertemplate`` : une CHAINE. Comme ``array_ok`` est
vrai pour les proprietes de type string, le validateur appelle ``is_array``,
donc ``is_homogeneous_array``, donc le crash. Tous nos graphiques passent un
``hovertemplate`` — d'ou l'onglet mort alors que le reste de l'app vit.

POINT CRITIQUE : le crash ne vient PAS de notre code. Il frappe dans
``streamlit/elements/lib/streamlit_plotly_theme.py``, DES l'import de
Streamlit (mesure). Un ``try/except`` autour de nos builders serait donc
inutile si le garde n'etait pas place AVANT ``import streamlit``. C'est
precisement ce que ces tests verrouillent.

PARADE
------
Forcer l'initialisation COMPLETE de pandas tout en haut de app.py et ui.py.
Un simple ``import pandas`` ne suffit PAS (Python renvoie l'entree tronquee
telle quelle, mesure) : il faut ``importlib.reload``.
"""
from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

# Etat exact reproduit sur Streamlit Cloud : entree presente, attributs absents.
SPOOF = 'import sys, types; sys.modules["pandas"] = types.ModuleType("pandas")'


def _run(code: str) -> subprocess.CompletedProcess:
    """Execute dans un interpreteur NEUF : l'ordre d'import est le sujet."""
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=str(ROOT), capture_output=True, text=True, timeout=120)


# --------------------------------------------------------------------------
# 1. Le defaut lui-meme, reproduit sans notre code
# --------------------------------------------------------------------------

def test_the_plotly_trap_is_real():
    """Sans parade, plotly leve bien l'AttributeError. C'est la reference.

    Si ce test cesse d'echouer, plotly a corrige son validateur : la parade
    pourra etre retiree, mais pas avant verification.
    """
    r = _run(f"""
        {SPOOF}
        import plotly.graph_objects as go
        f = go.Figure()
        f.add_bar(x=["a"], y=[1], hovertemplate="<b>%{{x}}</b>")
        print("NO_CRASH")
    """)
    assert "AttributeError" in r.stderr, (
        "le piege plotly a disparu : verifier avant de retirer la parade")
    assert "Series" in r.stderr


def test_a_plain_import_pandas_does_not_fix_it():
    """Le piege dans le piege : `import pandas` NE repare PAS le module.

    Python renvoie l'entree deja presente dans sys.modules sans la reevaluer.
    Ma premiere version du correctif faisait exactement cette erreur et ne
    marchait pas — d'ou ce test.
    """
    r = _run(f"""
        {SPOOF}
        import pandas
        print("HasSeries", hasattr(pandas, "Series"))
    """)
    assert "HasSeries False" in r.stdout


def test_importlib_reload_does_fix_it():
    """La parade retenue, verifiee isolement."""
    r = _run(f"""
        {SPOOF}
        import importlib, pandas
        importlib.reload(pandas)
        print("HasSeries", hasattr(pandas, "Series"))
    """)
    assert "HasSeries True" in r.stdout


# --------------------------------------------------------------------------
# 2. Notre application doit tenir debout malgre le piege
# --------------------------------------------------------------------------

def test_app_imports_with_a_truncated_pandas():
    """LE test de non-regression de l'incident.

    Avant correctif : AttributeError pendant `import streamlit`, donc avant
    toute ligne de notre code. Apres : l'app s'importe.
    """
    r = _run(f"""
        {SPOOF}
        import app
        print("APP_OK")
    """)
    assert "APP_OK" in r.stdout, f"import d'app.py en echec :\n{r.stderr}"
    assert "AttributeError" not in r.stderr


def test_ui_imports_with_a_truncated_pandas():
    """ui.py est aussi importable seul (il porte sa propre parade)."""
    r = _run(f"""
        {SPOOF}
        import importlib, pandas
        importlib.reload(pandas)          # streamlit en a besoin pour demarrer
        import ui
        print("UI_OK", ui.AVAILABLE)
    """)
    assert "UI_OK True" in r.stdout, f"import de ui.py en echec :\n{r.stderr}"


def test_all_three_charts_build_with_a_truncated_pandas():
    """Les trois graphiques de l'onglet Analytique, dans l'ordre reel."""
    r = _run(f"""
        {SPOOF}
        import app, ui
        from datetime import datetime, timezone
        evs = [{{"dt_utc": datetime(2026,10,15,12,0,tzinfo=timezone.utc),
                "date_display": "2026-10-15", "impact": "HIGH",
                "currency": "USD", "hours_until": 1.0}}]
        for name, fn in (("density", ui.density_by_day),
                         ("donut", ui.impact_donut),
                         ("currency", ui.currency_exposure)):
            assert fn(evs) is not None, name
            print("BUILT", name)
    """)
    for name in ("density", "donut", "currency"):
        assert f"BUILT {name}" in r.stdout, f"{name} non construit :\n{r.stderr}"


# --------------------------------------------------------------------------
# 3. Les gardes sont a la BONNE PLACE (le point le plus subtil)
# --------------------------------------------------------------------------

def test_guard_precedes_the_streamlit_import_in_app():
    """Mesure : le crash frappe DANS l'import de streamlit.

    Un garde place apres ne servirait donc a rien. On verifie la POSITION,
    pas seulement la presence.
    """
    src = (ROOT / "app.py").read_text(encoding="utf-8")
    guard = src.find("import pandas as _pd")
    stmt = src.find("\nimport streamlit as st")
    assert guard > 0, "garde pandas absent de app.py"
    assert stmt > 0, "import streamlit introuvable"
    assert guard < stmt, (
        "le garde pandas doit preceder `import streamlit`, sinon le crash "
        "se produit avant qu'il ne s'execute")


def test_guard_reloads_a_truncated_module():
    """Le garde doit utiliser reload, pas un simple import."""
    for name in ("app.py", "ui.py"):
        src = (ROOT / name).read_text(encoding="utf-8")
        assert "hasattr(_pd, \"Series\")" in src, f"{name} : detection absente"
        assert "_importlib.reload(_pd)" in src, f"{name} : reload absent"


# --------------------------------------------------------------------------
# 4. Defense en profondeur : un graphique ne tue jamais la page
# --------------------------------------------------------------------------

def test_safe_chart_returns_none_instead_of_raising():
    import ui
    def boom(_events):
        raise AttributeError("module 'pandas' has no attribute 'Series'")
    assert ui._safe_chart(boom, []) is None


def test_safe_chart_records_the_error_for_diagnostics():
    """Degrader en silence rendrait le prochain incident invisible."""
    import ui
    ui._CHART_ERRORS.clear()
    def boom(_events):
        raise RuntimeError("panne simulee")
    ui._safe_chart(boom, [])
    assert any("panne simulee" in e for e in ui._CHART_ERRORS)


def test_safe_chart_passes_through_success():
    import ui
    assert ui._safe_chart(lambda e: "figure" if e else None, [1]) == "figure"


def test_analytics_uses_safe_chart():
    src = (ROOT / "app.py").read_text(encoding="utf-8")
    i = src.find("def render_analytics")
    assert i > 0
    bloc = src[i:i + 4000]
    for fn in ("density_by_day", "impact_donut", "currency_exposure"):
        assert f"U._safe_chart(U.{fn}" in bloc, f"{fn} non protege"
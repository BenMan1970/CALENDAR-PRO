"""AUDIT-S2 — robustesse des builders de graphiques Plotly.

Defaut trouve en FUZZANT ``ui.density_by_day`` (et non en le relisant) :
``date_display=None`` levait ``TypeError: object of type 'NoneType' has no
len()`` a la ligne du raccourcissement ``MM-DD``, ce qui aurait fait planter
la page entiere au lieu de degrader proprement.

Atteignabilite : sur le chemin normal c'est INATTEIGNABLE, car
``app.enrich_events`` produit toujours une chaine via ``strftime`` et ignore
les evenements dont la date est illisible. Le defaut est donc une FRAGILITE
LATENTE, pas une panne active — ce module le verrouille pour qu'il le reste.

Un test ne remplace pas un fuzzing : ces trois builders sont publics, ils
doivent encaisser des lignes imparfaites sans faire tomber le rendu.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

import ui as U

UTC = timezone.utc
pytestmark = pytest.mark.skipif(not U.AVAILABLE, reason="plotly absent")


def ev(**kw):
    d = {"dt_utc": datetime(2026, 10, 15, 12, 0, tzinfo=UTC),
         "date_display": "2026-10-15", "impact": "HIGH",
         "currency": "USD", "hours_until": 1.0}
    d.update(kw)
    return d


BUILDERS = (U.density_by_day, U.impact_donut, U.currency_exposure)


@pytest.mark.parametrize("builder", BUILDERS, ids=lambda b: b.__name__)
@pytest.mark.parametrize("events", [
    [],                                  # vide -> None, jamais d'exception
    [ev()],
    [ev(impact=None)],
    [ev(impact="ZZZ")],                  # impact hors referentiel
    [ev(currency=None)],
    [ev(currency="<img src=x onerror=alert(1)>")],
    [ev() for _ in range(200)],
], ids=["vide", "un", "impact-null", "impact-inconnu", "devise-null",
        "devise-xss", "volume"])
def test_builders_never_raise(builder, events):
    result = builder(events)
    if not events:
        assert result is None, "une liste vide doit rendre None, pas une figure"
    else:
        assert result is not None


def test_density_survives_missing_date_display():
    """LE defaut fuzzze : c'etait un TypeError, pas un rendu degrade."""
    fig = U.density_by_day([ev(date_display=None)])
    assert fig is not None
    # La cle est derivee de dt_utc : le libelle reste exploitable.
    assert "10-15" in list(fig.data[0].x)


def test_density_survives_empty_date_display():
    assert U.density_by_day([ev(date_display="")]) is not None


def test_density_survives_no_usable_date_at_all():
    """Ni date_display ni dt_utc : on degrade, on ne plante pas."""
    assert U.density_by_day([ev(date_display=None, dt_utc=None)]) is not None


def test_density_keeps_impact_stacking_order():
    """L'ordre d'empilement doit rester HIGH -> MEDIUM -> LOW -> HOLIDAY."""
    fig = U.density_by_day([ev(impact=i) for i in
                            ("HOLIDAY", "LOW", "MEDIUM", "HIGH")])
    names = [t.name for t in fig.data]
    assert names == ["High", "Medium", "Low", "Holiday"]


def test_donut_centre_shows_the_total():
    fig = U.impact_donut([ev(), ev(impact="LOW")])
    assert fig.layout.annotations[0].text == "<b>2</b>"
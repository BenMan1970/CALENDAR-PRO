"""REGRESSION — le selecteur de fuseau ne changeait RIEN.

Defaut trouve pendant la verification du besoin « je voyage » :

  le widget ``st.selectbox`` du fuseau n'avait pas de ``key``. Sans cle
  stable, Streamlit ne rattache pas le widget a son etat d'un rerun a
  l'autre : il repartait de ``index=`` a chaque passage et le choix de
  l'utilisateur etait ramene a Casablanca. Le selecteur SEMBLAIT marcher
  (on pouvait cliquer, la valeur s'affichait une fraction de seconde) mais
  l'affichage ne changeait jamais.

Instrumentation d'origine : deux appels consecutifs au selectbox, tous deux
avec ``index=0`` et sans ``key`` -> la selection etait perdue.
"""
from __future__ import annotations

import inspect
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

import app as A

UTC = timezone.utc
ROOT = Path(__file__).resolve().parent.parent


def test_timezone_selector_has_a_stable_key():
    """Sans `key`, le widget ne conserve pas la selection entre les reruns."""
    src = (ROOT / "app.py").read_text(encoding="utf-8")
    i = src.find('"Fuseau d\'affichage"')
    assert i > 0, "selecteur de fuseau introuvable"
    bloc = src[i:i + 400]
    assert "key=" in bloc, (
        "le selectbox du fuseau doit avoir une key stable, sinon la selection "
        "est silencieusement revertee")


def test_enrich_events_honours_the_chosen_timezone():
    """Le choix utilisateur doit reellement deplacer l'heure affichee."""
    from tests._make_golden import build
    doc = build()[0].model_dump(mode="json")
    now = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)

    cas = A.enrich_events(doc, now, display_tz="Africa/Casablanca")[0]
    sgp = A.enrich_events(doc, now, display_tz="Asia/Singapore")[0]

    assert cas["hm"] != sgp["hm"], "le fuseau choisi n'a aucun effet"
    assert sgp["tz_label"] == "Singapore"
    # 10:00Z -> 18:00 a Singapour (UTC+8)
    assert cas["hm"] == "10:00"
    assert sgp["hm"] == "18:00"


def test_changing_timezone_never_moves_the_instant():
    """Invariant : le fuseau est une VUE, jamais une decision."""
    from tests._make_golden import build
    doc = build()[0].model_dump(mode="json")
    now = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
    a = A.enrich_events(doc, now, display_tz="Africa/Casablanca")
    b = A.enrich_events(doc, now, display_tz="Asia/Tokyo")
    for x, y in zip(a, b):
        assert x["dt_utc"] == y["dt_utc"]
        assert x["hours_until"] == y["hours_until"]
        assert x["occurrence_id"] == y["occurrence_id"]
    # mais la vue differe bien
    assert [x["hm"] for x in a] != [y["hm"] for y in b]


def test_day_of_week_follows_the_travel_timezone():
    """Un evenement de fin de journee peut changer de JOUR en voyageant."""
    from tests._make_golden import build
    doc = build()[0].model_dump(mode="json")
    # 23:00Z le 15/10 -> le 16/10 a Tokyo
    now = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
    events = A.enrich_events(doc, now, display_tz="Africa/Casablanca")
    tokyo = A.enrich_events(doc, now, display_tz="Asia/Tokyo")
    for x, y in zip(events, tokyo):
        hx = int(x["hm"].split(":")[0])
        hy = int(y["hm"].split(":")[0])
        if hx + 9 >= 24:      # Casablanca->Tokyo = +9h en octobre
            assert y["date_display"] > x["date_display"], \
                "le passage de minuit doit changer la date affichee"


# ── Le selecteur complet, teste bout-en-bout ────────────────────────────
@pytest.fixture
def app_ready(tmp_path, monkeypatch):
    import pipeline_calendar as pc
    from tests._make_golden import build
    data = tmp_path / "data"
    (data / "seed").mkdir(parents=True)
    payload, legacy = build()
    pc.atomic_write_json(data / "calendar.latest.json", payload.model_dump(mode="json"))
    pc.atomic_write_json(data / "calendar.json", legacy)
    pc.atomic_write_json(data / "calendar.legacy.json", legacy)
    pc.atomic_write_json(data / "health.json", {"status": "VALID"})
    monkeypatch.setattr(A, "DATA_DIR", data)
    monkeypatch.setattr(A, "CANONICAL_PATH", data / "calendar.latest.json")
    monkeypatch.setattr(A, "LEGACY_PATH", data / "calendar.json")
    monkeypatch.setattr(A, "HEALTH_PATH", data / "health.json")
    monkeypatch.setattr(A, "SEED_PATH", data / "seed" / "calendar.latest.seed.json")
    monkeypatch.setenv("BLUESTAR_DISABLE_INGEST", "1")
    A.load_canonical.clear()
    A.load_legacy.clear()
    return data


def test_app_selection_survives_a_rerun(app_ready):
    """LE test du bug : on choisit un fuseau, on rerun, il doit rester."""
    at = pytest.importorskip("streamlit.testing.v1").AppTest.from_file(
        str(ROOT / "app.py"), default_timeout=120)
    at.run()
    assert not at.exception, [str(e) for e in at.exception]

    sel = lambda: [x for x in at.sidebar.selectbox if "Fuseau" in x.label][0]
    assert sel().value.startswith("Africa/Casablanca")

    sg = [o for o in sel().options if o.startswith("Asia/Singapore")][0]
    sel().select(sg).run()
    assert sel().value == sg, "la selection n'a pas ete conservee"

    at.run()
    assert sel().value == sg, "la selection est perdue au rerun suivant"


def test_app_travel_timezone_changes_rendered_hours(app_ready):
    at = pytest.importorskip("streamlit.testing.v1").AppTest.from_file(
        str(ROOT / "app.py"), default_timeout=120)
    at.run()
    sel = lambda: [x for x in at.sidebar.selectbox if "Fuseau" in x.label][0]
    before = " ".join(str(getattr(m, "value", "")) for m in at.markdown)

    sg = [o for o in sel().options if o.startswith("Asia/Singapore")][0]
    sel().select(sg).run()
    after = " ".join(str(getattr(m, "value", "")) for m in at.markdown)
    assert before != after, "changer de fuseau n'a rien change au rendu"


def test_travel_suggestion_only_reads_the_browser(app_ready):
    """La suggestion vient du navigateur, jamais du serveur."""
    at = pytest.importorskip("streamlit.testing.v1").AppTest.from_file(
        str(ROOT / "app.py"), default_timeout=120)
    at.run()
    assert not at.exception, [str(e) for e in at.exception]
    # le bouton de suggestion n'apparait que si l'info navigateur existe
    labels = [b.label for b in at.sidebar.button]
    for l in labels:
        assert "Utiliser le fuseau" not in l or "UTC" in l
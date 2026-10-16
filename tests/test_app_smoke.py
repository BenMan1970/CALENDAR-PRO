"""Smoke test Streamlit : l'app se charge-t-elle sans exception sur la fixture ?

On utilise ``streamlit.testing.v1.AppTest``. L'app est pointee sur un data/
isole (artefact canonique + legacy issus de la fixture) et l'ingestion est
desactivee : aucun acces reseau.

Si AppTest n'est pas disponible dans la version installee, le test est
explicitement SKIPPE (jamais faussement vert).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests._make_golden import build

apptest = pytest.importorskip("streamlit.testing.v1",
                              reason="AppTest requis (streamlit >= 1.28)")


@pytest.fixture
def app_env(tmp_path, monkeypatch):
    """Prépare un data/ complet et pointe app.py dessus."""
    import pipeline_calendar as pc
    data = tmp_path / "data"
    (data / "seed").mkdir(parents=True)
    payload, legacy = build()
    pc.atomic_write_json(data / "calendar.latest.json", payload.model_dump(mode="json"))
    pc.atomic_write_json(data / "calendar.json", legacy)
    pc.atomic_write_json(data / "calendar.legacy.json", legacy)
    pc.atomic_write_json(data / "health.json", {
        "schema_version": "health-pipeline-1.2",
        "core_schema_version": pc.SCHEMA_VERSION,
        "status": "VALID", "mode": "live",
        "checked_at_utc": "2026-10-15T12:00:00Z",
        "event_count": len(payload.events), "data_quality_score": 1.0,
        "content_hash": payload.content_hash,
        "warnings": list(payload.quality.warnings),
        "feeds_status": {"thisweek": "ok"},
        "last_success_utc": "2026-10-15T12:00:00Z",
        "last_success_content_hash": payload.content_hash,
        "last_success_event_count": len(payload.events),
        "artifact_present": True,
        "served_artifact_age_s": 42,
    })

    import app as A
    monkeypatch.setattr(A, "DATA_DIR", data)
    monkeypatch.setattr(A, "CANONICAL_PATH", data / "calendar.latest.json")
    monkeypatch.setattr(A, "LEGACY_PATH", data / "calendar.json")
    monkeypatch.setattr(A, "HEALTH_PATH", data / "health.json")
    monkeypatch.setattr(A, "SEED_PATH", data / "seed" / "calendar.latest.seed.json")
    # Mode lecteur : l'app ne fetche JAMAIS, et aucun thread de fond ne demarre.
    monkeypatch.setenv("BLUESTAR_DISABLE_INGEST", "1")
    # Les caches Streamlit gardent des references au data_dir precedent.
    A.load_canonical.clear()
    A.load_legacy.clear()
    return data


def test_app_loads_without_exception(app_env):
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(str(Path(__file__).resolve().parent.parent / "app.py"),
                           default_timeout=60)
    at.run()
    assert not at.exception, [str(e) for e in at.exception]


def test_app_renders_the_expected_tabs(app_env):
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(str(Path(__file__).resolve().parent.parent / "app.py"),
                           default_timeout=60)
    at.run()
    assert not at.exception, [str(e) for e in at.exception]
    labels = [t.label for t in at.tabs] if hasattr(at, "tabs") else []
    joined = " ".join(labels)
    for expected in ("Flux", "Analytique", "Résumé", "Export", "Diagnostics"):
        assert expected in joined, f"onglet {expected} absent de {labels}"


def test_app_exposes_a_timezone_selector(app_env):
    """[2.3] Le fuseau d'affichage est un choix de VUE, exposé dans la sidebar."""
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(str(Path(__file__).resolve().parent.parent / "app.py"),
                           default_timeout=60)
    at.run()
    assert not at.exception, [str(e) for e in at.exception]
    labels = [s.label for s in at.sidebar.selectbox]
    assert any("Fuseau" in l for l in labels), labels


def test_app_never_renders_active_markup_from_event_name(app_env):
    """[2.6] Preuve bout-en-bout : un nom hostile ne produit pas de markup actif."""
    import re
    import pipeline_calendar as pc
    XSS = "<img src=x onerror=alert(1)>"
    doc = json.loads((app_env / "calendar.latest.json").read_text(encoding="utf-8"))
    for ev in doc["events"]:
        ev["name"] = XSS
    pc.atomic_write_json(app_env / "calendar.latest.json", doc)

    import app as A
    A.load_canonical.clear()

    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(str(Path(__file__).resolve().parent.parent / "app.py"),
                           default_timeout=60)
    at.run()
    assert not at.exception, [str(e) for e in at.exception]

    blob = " ".join(
        str(getattr(m, "value", "")) for m in list(at.markdown) + list(at.warning)
    )
    active = re.search(r"<\s*img\b[^>]*onerror", blob, re.I)
    assert not active, f"MARKUP ACTIF RENDU: {active.group(0) if active else ''}"
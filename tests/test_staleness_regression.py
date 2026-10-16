"""REGRESSION CRITIQUE — fetched_at_utc ne doit JAMAIS etre le Last-Modified.

Defaut observe en production le 2026-10-15 :
  le CDN annoncait Last-Modified = 15:42:18Z (heure de PUBLICATION du fichier
  hebdomadaire) alors que notre fetch avait lieu a 16:20:13Z. Mon correctif 2.4
  ecrasait fetched_at_utc par cette valeur -> source_age_seconds valait 2275 s
  a la naissance de l'artefact -> is_stale=True -> SOURCE_AGE_EXCEEDS_900S ->
  score 1.00 - 0.35 = 0.65 -> verdict DEGRADED a CHAQUE cycle, alors que le
  fetch venait de reussir (HTTP 200, 25 evenements).

Ces tests verrouillent les deux horloges separement.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import pipeline_calendar as pc
from tests.fixture import load_fixture
from tests.helpers import CountingSession, UTC


def _session_with_last_modified(lm: str) -> CountingSession:
    s = CountingSession()
    orig = s.get

    def get(url, **kw):
        r = orig(url, **kw)
        r.headers = dict(r.headers)
        r.headers["Last-Modified"] = lm
        return r

    s.get = get
    return s


def test_fresh_fetch_is_never_degraded_by_an_old_last_modified(data_dir):
    """LE BUG DE PRODUCTION : un Last-Modified vieux de 38 min ne doit PAS
    rendre l'artefact perime des sa naissance."""
    now = datetime.now(UTC)
    lm = (now - timedelta(minutes=38)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    payload = pc.run_once(data_dir, session=_session_with_last_modified(lm))

    assert payload is not None
    assert payload.quality.status.value == "VALID", (
        f"artefact neuf declare {payload.quality.status.value} "
        f"(warnings={list(payload.quality.warnings)}, "
        f"score={payload.quality.data_quality_score})")
    assert payload.quality.is_stale is False
    assert payload.quality.source_age_seconds <= 5, \
        "un fetch qui vient de reussir a un age quasi nul"
    assert not any(w.startswith("SOURCE_AGE_EXCEEDS") for w in payload.quality.warnings)
    assert payload.quality.data_quality_score == 1.0


def test_fetched_at_is_our_clock_not_the_publishers(data_dir):
    """Les deux horloges doivent rester DISTINCTES."""
    now = datetime.now(UTC)
    lm_dt = now - timedelta(minutes=38)
    lm = lm_dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
    payload = pc.run_once(data_dir, session=_session_with_last_modified(lm))

    # notre horloge : quasi maintenant
    age = (datetime.now(UTC) - payload.source.fetched_at_utc).total_seconds()
    assert 0 <= age < 30, f"fetched_at_utc={payload.source.fetched_at_utc} (age {age}s)"

    # l'horodatage editeur : conserve tel quel, separement
    assert payload.source.last_modified == lm

    # et surtout : fetched_at_utc N'EST PAS last_modified
    from email.utils import parsedate_to_datetime
    assert payload.source.fetched_at_utc != parsedate_to_datetime(lm).astimezone(UTC)


def test_a_week_old_last_modified_still_yields_valid(data_dir):
    """Le cas extreme du flux hebdomadaire : 6 jours d'ecart."""
    now = datetime.now(UTC)
    lm = (now - timedelta(days=6)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    payload = pc.run_once(data_dir, session=_session_with_last_modified(lm))
    assert payload.quality.status.value == "VALID"
    assert payload.quality.source_age_seconds <= 5


def test_genuinely_stale_artifact_is_still_detected():
    """Le detecteur de peremption ne doit pas avoir ete desactive.

    On construit un SourceInfo dont fetched_at_utc a TROIS HEURES : c'est le
    seul cas ou is_stale doit valoir True.
    """
    from calendar_core import SourceInfo, build_payload, SCHEMA_VERSION
    rows = load_fixture()
    now = datetime.now(UTC)
    src = SourceInfo(
        provider="test", url="https://example.invalid/thisweek.json",
        fetched_at_utc=now - timedelta(hours=3),
        http_status=200, payload_bytes=1, payload_sha256="sha256:x",
    )
    payload = build_payload(rows, source=src, now_utc=now)
    assert payload.quality.is_stale is True
    assert payload.quality.source_age_seconds > 900
    assert any(w.startswith("SOURCE_AGE_EXCEEDS") for w in payload.quality.warnings)
    assert payload.quality.data_quality_score < 1.0


def test_future_last_modified_does_not_shift_our_clock(data_dir):
    now = datetime.now(UTC)
    lm = (now + timedelta(days=365)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    payload = pc.run_once(data_dir, session=_session_with_last_modified(lm))
    assert payload.quality.source_age_seconds <= 5
    assert payload.quality.is_stale is False
    assert payload.source.fetched_at_utc <= datetime.now(UTC)


def test_live_cycle_health_reports_valid(data_dir):
    payload = pc.run_once(data_dir, session=CountingSession())
    h = json.loads((data_dir / "health.json").read_text(encoding="utf-8"))
    assert h["status"] == "VALID", h
    assert payload.quality.status.value == "VALID"


def test_app_renders_valid_on_a_fresh_cycle(tmp_path, monkeypatch):
    """Bout-en-bout : l'app ne doit PAS afficher DEGRADED sur un cycle frais."""
    import os
    import app as A
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

    at = pytest.importorskip("streamlit.testing.v1").AppTest.from_file(
        str(Path(__file__).resolve().parent.parent / "app.py"), default_timeout=60)
    at.run()
    assert not at.exception, [str(e) for e in at.exception]

    blob = " ".join(str(getattr(m, "value", "")) for m in at.markdown)
    assert "DEGRADED" not in blob, "l'app a rendu DEGRADED sur un artefact VALID"
    assert "VALID" in blob
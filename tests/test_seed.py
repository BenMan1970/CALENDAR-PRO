"""POINT 2.8 — emit_seed / setup.py en cooldown.

AVANT : si ``run_once`` renvoyait ``None`` (cooldown 429, artefact récent,
mode lecteur), le seed n'était PAS écrit et ``setup.py`` répondait « vérifiez
votre connexion » — alors que ``data/calendar.latest.json`` existait et était
parfaitement valide.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

import pipeline_calendar as pc
from tests._make_golden import build, ROOT

UTC = timezone.utc


@pytest.fixture
def seeded_dir(data_dir):
    """data/ contenant un artefact canonique VALIDE (comme en production)."""
    payload, _ = build()
    pc.atomic_write_json(data_dir / "calendar.latest.json",
                         payload.model_dump(mode="json"))
    return data_dir


def test_seed_written_from_existing_artifact_during_cooldown(seeded_dir, session,
                                                             monkeypatch):
    """Le cas exact : cooldown actif -> run_once renvoie None, MAIS le seed
    doit quand même être écrit depuis l'artefact valide."""
    monkeypatch.setattr(pc, "run_once", lambda *a, **k: None)
    seed = pc.emit_seed(seeded_dir, session)
    assert seed is not None, "le seed doit être écrit depuis l'artefact existant"
    assert seed.exists()
    data = json.loads(seed.read_text(encoding="utf-8"))
    assert data["events"], "le seed doit contenir des evenements"
    assert data["content_hash"] == build()[0].content_hash


def test_emit_seed_does_not_fetch_when_cooldown(seeded_dir, session, monkeypatch):
    """Aucune requete ne doit etre emise pendant un cooldown."""
    calls = {"n": 0}
    real = pc.run_once

    def counting(*a, **k):
        calls["n"] += 1
        return None

    monkeypatch.setattr(pc, "run_once", counting)
    pc.emit_seed(seeded_dir, session)
    assert calls["n"] == 1


def test_seed_from_existing_rejects_invalid_status(seeded_dir):
    """Un artefact INVALID ne doit JAMAIS produire un seed."""
    art = json.loads((seeded_dir / "calendar.latest.json").read_text(encoding="utf-8"))
    art["quality"]["status"] = "INVALID"
    pc.atomic_write_json(seeded_dir / "calendar.latest.json", art)
    path, why = pc.seed_from_existing(seeded_dir)
    assert path is None
    assert "INVALID" in why


def test_seed_from_existing_reports_missing_artifact(tmp_path):
    d = tmp_path / "data"
    d.mkdir()
    path, why = pc.seed_from_existing(d)
    assert path is None
    assert "absent" in why


def test_seed_from_existing_reports_corrupt_artifact(seeded_dir):
    (seeded_dir / "calendar.latest.json").write_text("{not json", encoding="utf-8")
    path, why = pc.seed_from_existing(seeded_dir)
    assert path is None
    assert "illisible" in why, why
    assert "absent" not in why, "un JSON corrompu ne doit pas etre dit 'absent'"


def test_emit_seed_returns_none_without_any_artifact(tmp_path, monkeypatch):
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.setattr(pc, "run_once", lambda *a, **k: None)
    assert pc.emit_seed(d) is None


# ═══════════════════════════════════════════════════════════════════════════
# La VRAIE raison, jamais « verifiez votre connexion »
# ═══════════════════════════════════════════════════════════════════════════
def test_reason_reports_reader_mode(tmp_path, monkeypatch):
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.setenv("BLUESTAR_DISABLE_INGEST", "1")
    assert "mode lecteur" in pc.seed_failure_reason(d)
    monkeypatch.delenv("BLUESTAR_DISABLE_INGEST")


def test_reason_reports_cooldown(tmp_path, monkeypatch):
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.delenv("BLUESTAR_DISABLE_INGEST", raising=False)
    pc.apply_rate_limit(d, 300)
    reason = pc.seed_failure_reason(d)
    assert "cooldown 429" in reason
    assert "300" in reason


def test_reason_reports_floor(tmp_path, monkeypatch):
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.delenv("BLUESTAR_DISABLE_INGEST", raising=False)
    pc.record_attempt(d)
    assert "plancher anti-429" in pc.seed_failure_reason(d)


def test_reason_reports_last_cycle_error(tmp_path, monkeypatch):
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.delenv("BLUESTAR_DISABLE_INGEST", raising=False)
    pc.atomic_write_json(d / "health.json", {"error": "QUALITY_INVALID: x"})
    assert "QUALITY_INVALID" in pc.seed_failure_reason(d)


def test_setup_does_not_blame_the_connection(seeded_dir, tmp_path, monkeypatch):
    """Le message de setup.py ne doit plus accuser la connexion."""
    src = (ROOT / "setup.py").read_text(encoding="utf-8")
    assert "Check your internet connection" not in src
    assert "seed_failure_reason" in src
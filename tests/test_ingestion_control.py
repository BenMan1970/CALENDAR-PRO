"""POINT 2.2 b — le superviseur d'ingestion de l'UI.

Prouve, au niveau ou les boutons agissent reellement, qu'un double-clic ou
plusieurs sessions ne peuvent pas marteler la source.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest

import pipeline_calendar as pc
from tests.helpers import UTC


@pytest.fixture
def ingestion(data_dir, monkeypatch):
    """Instance d'Ingestion branchee sur un data_dir isole (pas de st.cache)."""
    import app
    monkeypatch.setattr(app, "DATA_DIR", data_dir)
    monkeypatch.setattr(app, "CANONICAL_PATH", data_dir / "calendar.latest.json")
    monkeypatch.delenv("BLUESTAR_DISABLE_INGEST", raising=False)
    return app.Ingestion()


def test_kick_disabled_when_reader_mode(ingestion, monkeypatch):
    monkeypatch.setenv("BLUESTAR_DISABLE_INGEST", "1")
    assert ingestion.kick() == "disabled"


def test_force_does_not_bypass_cooldown(ingestion, data_dir):
    """Un cooldown 429 est absolu : meme force=True ne fetche pas."""
    pc.apply_rate_limit(data_dir, 300)
    assert ingestion.kick(force=True) == "cooldown"
    assert ingestion.kick(force=False) == "cooldown"
    assert ingestion.running is False


def test_force_respects_hard_floor(ingestion, data_dir):
    """Un plancher deja consomme refuse un fetch force."""
    pc.record_attempt(data_dir)
    assert pc.fetch_floor_remaining(data_dir) > 0
    assert ingestion.kick(force=True) == "floor"
    assert ingestion.running is False


def test_repeated_forced_clicks_produce_at_most_one_fetch(ingestion, data_dir,
                                                          monkeypatch):
    """N clics forces consecutifs : la source n'est appelee qu'UNE fois."""
    monkeypatch.setattr(pc, "run_once", lambda *a, **k: None)
    states = []
    for _ in range(10):
        states.append(ingestion.kick(force=True))
        ingestion.wait_done(timeout=2.0)
    assert states.count("started") <= 1, states
    assert states[0] == "started"
    assert states[1:] == ["floor"] * 9, states


def test_concurrent_sessions_produce_one_fetch(data_dir, monkeypatch):
    """Deux instances (deux sessions Streamlit) partageant le meme data_dir :
    le plancher persiste sur DISQUE, donc il tient entre instances."""
    import app
    monkeypatch.setattr(app, "DATA_DIR", data_dir)
    monkeypatch.setattr(app, "CANONICAL_PATH", data_dir / "calendar.latest.json")
    monkeypatch.delenv("BLUESTAR_DISABLE_INGEST", raising=False)
    monkeypatch.setattr(pc, "run_once", lambda *a, **k: None)

    a, b = app.Ingestion(), app.Ingestion()
    calls = [a.kick(force=True), b.kick(force=True)]
    assert calls == ["started", "floor"], calls


def test_floor_survives_a_new_process(data_dir, monkeypatch):
    """Le plancher est sur DISQUE : un redemarrage de conteneur ne le remet pas
    a zero (sinon chaque cold start refetch)."""
    pc.record_attempt(data_dir)
    import app
    monkeypatch.setattr(app, "DATA_DIR", data_dir)
    monkeypatch.setattr(app, "CANONICAL_PATH", data_dir / "calendar.latest.json")
    monkeypatch.delenv("BLUESTAR_DISABLE_INGEST", raising=False)
    fresh_instance = app.Ingestion()          # simule un nouveau process
    assert fresh_instance.kick(force=True) == "floor"


def test_backoff_is_at_least_300s():
    """Un echec pose un backoff >= 300 s (et non 10/30/60/120 s)."""
    import app
    assert min(app._BACKOFF_S) >= 300
    assert max(app._BACKOFF_S) <= 3600


def test_deterministic_failure_is_not_replayed_immediately(ingestion, data_dir,
                                                           monkeypatch):
    """QUALITY_INVALID (deterministe) : UN appel, puis backoff.

    AVANT : _BACKOFF_S = (10, 30, 60, 120) -> rejoue toutes les 2 minutes
    indefiniment, consommant le quota pour un verdict qui ne pouvait pas
    changer.
    """
    import app
    calls = {"n": 0}

    def fake_run_once(*a, **k):
        calls["n"] += 1
        pc.atomic_write_json(data_dir / "health.json", {
            "error": "QUALITY_INVALID: ['IMPACT_VOCABULARY_ALL_REJECTED']",
            "checked_at_utc": pc.iso_z(datetime.now(UTC)),
        })
        return None

    monkeypatch.setattr(pc, "run_once", fake_run_once)
    monkeypatch.setattr(app, "_cycle_error", lambda since: "QUALITY_INVALID: x")
    ingestion.kick(force=True)
    ingestion.wait_done(timeout=5.0)
    assert calls["n"] == 1, f"REJOUÉ {calls['n']} fois"
    assert ingestion.fail_streak >= 1
    assert ingestion.next_attempt_at - time.time() > 250


def test_no_quick_retry_in_a_cycle():
    import app
    assert app._QUICK_RETRY_DELAYS_S == (), (
        "un quick retry intra-cycle multiplierait les requetes")


def test_no_error_codes_do_not_increment_streak(ingestion, data_dir, monkeypatch):
    """Un cooldown n'est PAS un echec : le streak ne doit pas monter."""
    import app
    monkeypatch.setattr(pc, "run_once", lambda *a, **k: None)
    monkeypatch.setattr(app, "_cycle_error", lambda since: "RATE_LIMIT_COOLDOWN")
    ingestion.kick(force=True)
    ingestion.wait_done(timeout=5.0)
    assert ingestion.fail_streak == 0
    assert ingestion.last_error is None


def test_shared_state_properties_are_locked(ingestion):
    """last_error / fail_streak / next_attempt_at passent par le verrou."""
    ingestion.last_error = "X: y"
    ingestion.fail_streak = 3
    ingestion.next_attempt_at = 123.0
    assert ingestion.last_error == "X: y"
    assert ingestion.fail_streak == 3
    assert ingestion.next_attempt_at == 123.0


def test_state_snapshot_is_consistent_under_concurrency(ingestion):
    """Le couple (erreur, streak) ne doit jamais etre lu a moitie mis a jour."""
    import threading
    stop = threading.Event()
    seen = []

    def writer():
        i = 0
        while not stop.is_set() and i < 2000:
            ingestion._set_failure(f"E{i}", i + 1)
            i += 1

    def reader():
        while not stop.is_set():
            err, streak = ingestion.last_error, ingestion.fail_streak
            # l'erreur Ei et le streak i+1 sont ecrits ensemble
            if err and err.startswith("E") and err[1:].isdigit():
                seen.append(int(err[1:]) + 1 == streak)

    w = threading.Thread(target=writer, daemon=True)
    r = threading.Thread(target=reader, daemon=True)
    w.start(); r.start()
    w.join(timeout=10)
    stop.set()
    r.join(timeout=5)
    assert seen, "lecteur n'a rien observe"
    assert all(seen), "etat partage lu de facon incoherente"
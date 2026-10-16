"""AUDIT-F2c — l'interaction kick() <-> run_once() sur le plancher anti-429.

REGRESSION que j'ai introduite en appliquant F2, et que ce module verrouille.

Séquence réelle de l'app :

  1. ``kick()`` vérifie le plancher SOUS VERROU (0 s, autorisé) ;
  2. ``kick()`` appelle ``pc.record_attempt()`` pour RÉSERVER la tentative ;
  3. ``kick()`` démarre le thread worker ;
  4. le worker appelle ``run_once()``.

Entre 2 et 4, ``last_attempt.json`` vient d'être écrit : le plancher vaut
donc ~120 s. Quand ``run_once`` a commencé à appliquer le plancher lui-même,
il relisait le plancher que ``kick`` venait de poser et ABANDONNAIT le cycle.
L'app ne fetchait plus jamais : panne totale, silencieuse, sans erreur.

Correctif : le worker appelle ``run_once(..., force=True)``. Le plancher est
appliqué exactement UNE fois, par ``kick()`` ; ``run_once`` n'est que
l'exécutant d'une décision déjà prise sous verrou. Les appelants directs
(CLI, cron, tests) gardent la protection par défaut ``force=False``.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

import app as A
import pipeline_calendar as pc
from tests.helpers import CountingSession


@pytest.fixture
def wired(tmp_path, monkeypatch):
    """Un data_dir branché sur app.py, sans réseau."""
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.setattr(A, "DATA_DIR", d)
    monkeypatch.setattr(A, "CANONICAL_PATH", d / "calendar.latest.json")
    monkeypatch.setattr(A, "LEGACY_PATH", d / "calendar.json")
    monkeypatch.setattr(A, "HEALTH_PATH", d / "health.json")
    monkeypatch.setattr(A, "SEED_PATH", d / "seed" / "calendar.latest.seed.json")
    monkeypatch.delenv("BLUESTAR_DISABLE_INGEST", raising=False)
    A.load_canonical.clear()
    A.load_legacy.clear()
    return d


def _run_worker(ing, monkeypatch, sess):
    """Exécute kick() + le worker en substituant la session réseau."""
    orig = pc.run_once
    seen = []

    def spy(dd, session=None, policy=None, force=False):
        seen.append(force)
        return orig(dd, session=sess, policy=policy, force=force)

    monkeypatch.setattr(pc, "run_once", spy)
    state = ing.kick(force=True)
    for _ in range(200):
        if not ing.running:
            break
        time.sleep(0.05)
    return state, seen


def test_kick_then_worker_actually_fetches(wired, monkeypatch):
    """LE test de la regression : sans force=True, 0 GET emis."""
    ing = A.Ingestion()
    sess = CountingSession()
    state, seen = _run_worker(ing, monkeypatch, sess)

    assert state == "started"
    assert seen == [True], f"le worker doit forcer, vu {seen}"
    assert sess.n_calls == 1, (
        "l'app n'a emis aucun GET : le plancher pose par kick() a bloque "
        "son propre cycle")


def test_kick_does_not_report_a_false_failure(wired, monkeypatch):
    """Un cycle normal ne doit laisser ni erreur ni fail_streak."""
    ing = A.Ingestion()
    _run_worker(ing, monkeypatch, CountingSession())
    assert ing.fail_streak == 0
    assert ing.last_error is None


def test_second_kick_is_blocked_by_the_floor(wired, monkeypatch):
    """Le plancher doit quand meme empecher DEUX fetchs rapproches."""
    ing = A.Ingestion()
    sess = CountingSession()
    _run_worker(ing, monkeypatch, sess)
    assert sess.n_calls == 1

    # deuxieme relance immediate : kick() doit refuser
    state = ing.kick(force=True)
    assert state in ("floor", "fresh"), f"etat inattendu {state!r}"
    assert sess.n_calls == 1, "un second GET a ete emis malgre le plancher"


def test_cli_direct_run_once_still_protected(tmp_path):
    """Les appelants SANS kick (CLI, cron) doivent rester proteges."""
    d = tmp_path / "data"
    d.mkdir()
    sess = CountingSession()
    pc.run_once(d, session=sess)
    pc.run_once(d, session=sess)
    assert sess.n_calls == 1, "le CLI a martele la source"


def test_floor_code_is_not_treated_as_a_failure():
    """FETCH_FLOOR_ACTIVE est un fonctionnement normal, pas une panne."""
    assert "FETCH_FLOOR_ACTIVE" in A._NO_ERROR_CODES
    assert "RATE_LIMIT_COOLDOWN" in A._NO_ERROR_CODES
    assert "INGEST_DISABLED" in A._NO_ERROR_CODES
    # mais un vrai echec n'y est PAS
    assert "HTTP_ERROR" not in A._NO_ERROR_CODES
    assert "QUALITY_INVALID" not in A._NO_ERROR_CODES


def test_health_reports_floor_mode_explicitly(wired):
    """Un cycle bloque doit etre lisible dans health.json."""
    pc.record_attempt(wired)
    pc.run_once(wired, session=CountingSession())
    h = json.loads((wired / "health.json").read_text(encoding="utf-8"))
    assert h["error"] == "FETCH_FLOOR_ACTIVE"
    assert h["mode"] == "floor"
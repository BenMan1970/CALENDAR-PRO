"""AUDIT-F2 — le plancher anti-429 doit être appliqué PAR L'INGESTEUR.

Défaut prouvé : ``run_once`` n'appelait jamais ``fetch_floor_remaining``. Le
plancher n'existait que dans ``app.py`` (bouton de relance) et ``setup.py``.
Or ``run_once`` est le SEUL composant qui parle à la source. Cinq appels
consécutifs émettaient donc cinq GET en quelques millisecondes :

    apres cycle 1 : 1 GET
    apres cycle 2 : 2 GET
    ...
    apres cycle 5 : 5 GET          <- plancher de 120 s ignoré

C'était la cause racine STRUCTURELLE des 429 : ``--loop --interval 30`` la
déclenchait directement, sans qu'aucun garde-fou ne s'y oppose.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import pipeline_calendar as pc
from tests.helpers import CountingSession


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    return d


def test_repeated_cycles_do_not_hammer_the_source(data_dir):
    """LE test du bug : 5 appels rapprochés = 1 seul GET."""
    sess = CountingSession()
    for _ in range(5):
        pc.run_once(data_dir, session=sess)
    assert sess.n_calls == 1, (
        f"{sess.n_calls} GET emis alors que le plancher de "
        f"{pc.FETCH_FLOOR_S}s n'etait pas ecoule")


def test_floor_is_persisted_across_processes(data_dir):
    """Le plancher doit tenir entre processus (last_attempt.json)."""
    pc.record_attempt(data_dir)
    assert pc.fetch_floor_remaining(data_dir) > 0
    # un NOUVEAU cycle, sans etat memoire partage, doit quand meme etre bloque
    sess = CountingSession()
    pc.run_once(data_dir, session=sess)
    assert sess.n_calls == 0, "le plancher n'a pas survecu au redemarrage"


def test_force_bypasses_the_floor_explicitly(data_dir):
    """L'exploitant peut outrepasser, mais seulement explicitement."""
    pc.record_attempt(data_dir)
    sess = CountingSession()
    pc.run_once(data_dir, session=sess, force=True)
    assert sess.n_calls == 1, "force=True doit passer le plancher"


def test_floor_writes_an_explicit_health_reason(data_dir):
    """Un cycle bloqué doit DIRE pourquoi, pas rester muet."""
    import json
    pc.record_attempt(data_dir)
    pc.run_once(data_dir, session=CountingSession())
    health = json.loads((data_dir / "health.json").read_text(encoding="utf-8"))
    assert health["error"] == "FETCH_FLOOR_ACTIVE"
    assert health["mode"] == "floor"


def test_floor_does_not_destroy_the_served_artifact(data_dir):
    """Un cycle bloqué ne doit JAMAIS toucher l'artefact existant."""
    import json
    sentinel = {"generated_at_utc": "2026-10-15T00:00:00Z",
                "content_hash": "sha256:sentinel", "events": []}
    (data_dir / "calendar.latest.json").write_text(
        json.dumps(sentinel), encoding="utf-8")
    before = (data_dir / "calendar.latest.json").read_bytes()

    pc.record_attempt(data_dir)
    pc.run_once(data_dir, session=CountingSession())

    assert (data_dir / "calendar.latest.json").read_bytes() == before


def test_budget_is_bounded_across_a_whole_loop_iteration(data_dir):
    """Simule la boucle du CLI : l'ecart entre GET doit tenir le plancher."""
    sess = CountingSession()
    # 20 iterations serrees, comme `--loop --interval 5`
    for _ in range(20):
        pc.run_once(data_dir, session=sess)
    assert sess.n_calls == 1, f"{sess.n_calls} GET sur 20 iterations"


def test_cli_exposes_force_flag():
    """L'echappatoire doit etre documentee dans l'usage."""
    import subprocess, sys
    out = subprocess.run(
        [sys.executable, "pipeline_calendar.py", "--help"],
        capture_output=True, text=True, cwd=Path(pc.__file__).resolve().parent)
    assert "--force" in out.stdout
    assert "--clear-cooldown" in out.stdout
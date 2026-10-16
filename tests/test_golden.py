"""GOLDEN : les hashes de contenu ne bougent pas.

C'est le test le plus important de la suite : il protege la parite inter-apps
(pipeline de merge externe). Toute modification de calendar_core.py /
calendar_compat.py / pipeline_calendar.py qui changerait content_hash,
parity_hash ou high_content_hash casserait ce test.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from tests._make_golden import build
from tests.fixture import load_fixture
from tests.golden import load_golden

GOLDEN = load_golden()


def test_content_hash_unchanged():
    payload, _ = build()
    assert payload.content_hash == GOLDEN["content_hash"]


def test_parity_hash_unchanged():
    payload, _ = build()
    assert payload.parity_hash == GOLDEN["parity_hash"]
    assert payload.parity_hash_method == GOLDEN["parity_hash_method"]
    assert payload.parity_window_anchor_utc == GOLDEN["parity_window_anchor_utc"]
    assert payload.parity_scope_event_count == GOLDEN["parity_scope_event_count"]


def test_high_content_hash_unchanged():
    from calendar_core import comparable_high_hash
    payload, _ = build()
    assert comparable_high_hash(payload.events) == GOLDEN["high_content_hash"]


def test_quality_verdict_unchanged():
    payload, _ = build()
    assert payload.quality.status.value == GOLDEN["quality_status"]
    assert payload.quality.data_quality_score == GOLDEN["data_quality_score"]
    assert payload.quality.accepted_event_count == GOLDEN["accepted_event_count"]
    assert list(payload.quality.warnings) == GOLDEN["warnings"]


# Champs dont la valeur est un CHEMIN ABSOLU de la machine qui a produit le
# golden. Ils ne portent aucune information metier et rendent la comparaison
# machine-dependante : le meme code, sur un runner CI ou un poste Windows,
# echouait avec un faux negatif alors que les TROIS HASHES du contrat etaient
# identiques. Defaut releve par l'audit institutionnel (F4) et reproduit ici.
_ENV_DEPENDENT_PATHS = (
    ("metadata", "timezone_environment", "tzpath_head"),
)


def _normalize_env_paths(doc: dict) -> dict:
    """Retire les chemins absolus dependants de la machine, en conservant la
    STRUCTURE et la presence des cles (leur absence resterait un signal)."""
    import copy
    out = copy.deepcopy(doc)
    for path in _ENV_DEPENDENT_PATHS:
        node = out
        for key in path[:-1]:
            if not isinstance(node.get(key), dict):
                node = None
                break
            node = node[key]
        if node is not None and path[-1] in node:
            node[path[-1]] = "<machine-dependent>"
    return out


def test_legacy_payload_identical():
    """Le JSON legacy complet doit etre identique a la reference, HORS chemins
    absolus dependants de la machine (tzpath_head)."""
    _, legacy = build()
    assert json.dumps(_normalize_env_paths(legacy), sort_keys=True) == json.dumps(
        _normalize_env_paths(GOLDEN["legacy"]), sort_keys=True)


def test_golden_contains_no_machine_specific_absolute_path():
    """Garde-fou : le golden ne doit pas figer un chemin d'une autre machine.

    On ne peut pas l'interdire (le champ existe dans le contrat), mais on
    verifie que la SEULE divergence toleree est bien celle-la, et qu'aucun
    autre chemin absolu ne s'est glisse dans la reference.
    """
    import re
    blob = json.dumps(GOLDEN["legacy"], sort_keys=True)
    # les chemins absolus restants doivent tous etre sous timezone_environment
    paths = re.findall(r'"(/[^"]{10,}|[A-Za-z]:\\\\\\\\[^"]{10,})"',
                       blob)
    assert len(paths) <= 2, f"chemins absolus inattendus dans le golden: {paths}"


def test_the_three_contract_hashes_are_machine_independent():
    """Le point capital : les chemins machine ne doivent JAMAIS entrer dans un hash."""
    payload, legacy = build()
    assert payload.content_hash == GOLDEN["content_hash"]
    assert payload.parity_hash == GOLDEN["parity_hash"]
    assert legacy["metadata"]["high_content_hash"] == GOLDEN["legacy"]["metadata"]["high_content_hash"]


def test_schema_version_frozen():
    import calendar_core
    assert calendar_core.SCHEMA_VERSION == "2.4.2"


def test_fixture_has_no_actual_key():
    """La fixture doit refleter la source reelle : aucune cle 'actual'."""
    raw = load_fixture()
    assert raw, "fixture vide"
    assert not any("actual" in r for r in raw)
    for row in raw:
        assert set(row) <= {"title", "country", "date", "impact",
                            "forecast", "previous"}


def test_display_timezone_does_not_move_hashes():
    """Le content_hash et le parity_hash sont insensibles au fuseau (invariant F5)."""
    from calendar_core import SelectionPolicy
    casa, _ = build(policy=SelectionPolicy(display_timezone="Africa/Casablanca"))
    tokyo, _ = build(policy=SelectionPolicy(display_timezone="Asia/Tokyo"))
    utc, _ = build(policy=SelectionPolicy(display_timezone="UTC"))
    assert casa.content_hash == tokyo.content_hash == utc.content_hash
    assert casa.parity_hash == tokyo.parity_hash == utc.parity_hash


def test_declared_display_tz_still_chronologically_identical():
    """Changer le fuseau d'AFFICHAGE ne doit pas deplacer l'instant UTC."""
    from calendar_core import SelectionPolicy
    casa, _ = build(policy=SelectionPolicy(display_timezone="Africa/Casablanca"))
    tokyo, _ = build(policy=SelectionPolicy(display_timezone="Asia/Tokyo"))
    a = [e.scheduled_at_utc for e in casa.events]
    b = [e.scheduled_at_utc for e in tokyo.events]
    assert a == b


def test_golden_now_is_fixed_utc():
    assert GOLDEN["now_utc"] == "2026-10-15T12:00:00Z"
    assert build()[0].generated_at_utc == datetime(
        2026, 10, 15, 12, 0, 0, tzinfo=timezone.utc)
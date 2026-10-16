"""INVARIANTS DU PROJET — a ne jamais casser.

Chacun de ces tests verrouille une regle de la doctrine du projet. Ils ne
testent pas une fonctionnalite : ils empechent une regression de conception.
"""
from __future__ import annotations

import inspect
import json
import re
from pathlib import Path

import pytest

import app as A
import calendar_core
import pipeline_calendar as pc

ROOT = Path(__file__).resolve().parent.parent


# ═══════════════════════════════════════════════════════════════════════════
# 1. Aucun widget de l'UI n'alimente SelectionPolicy
# ═══════════════════════════════════════════════════════════════════════════
def test_app_never_constructs_a_selection_policy():
    """Les filtres de page sont des filtres de VUE : ils ne doivent jamais
    devenir une politique machine (le JSON est partage avec le merge)."""
    src = (ROOT / "app.py").read_text(encoding="utf-8")
    # on ignore la ligne de doctrine dans le docstring de module
    code = "\n".join(
        line for line in src.splitlines()
        if not line.lstrip().startswith(("•", "#", '"', "'"))
    )
    assert "SelectionPolicy(" not in code, (
        "app.py ne doit jamais instancier SelectionPolicy")


def test_apply_filters_is_pure_and_does_not_mutate_the_payload():
    import app as A
    from datetime import datetime, timezone
    payload = {"events": [{
        "occurrence_id": "a" * 32, "name": "X", "currency": "USD",
        "impact": "HIGH", "session": "LONDON",
        "scheduled_at_utc": "2026-10-15T14:00:00Z",
        "scheduled_at_display": "x", "display_timezone": "UTC",
        "date_display": "2026-10-15", "day_of_week": "THURSDAY",
        "forecast": {"raw": "1"}, "previous": {"raw": "0"},
        "actual": {"raw": None}, "actual_status": "UNSUPPORTED_BY_SOURCE",
        "pairs_with_currency_exposure": [],
    }]}
    before = json.dumps(payload, sort_keys=True)
    now = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
    events = A.enrich_events(payload, now, display_tz="UTC")
    f = A.Filters(currencies={"EUR"}, impacts={"HIGH"}, query="",
                  horizon_h=168, upcoming_only=False, show_pairs=False)
    A.apply_filters(events, f, now)
    assert json.dumps(payload, sort_keys=True) == before, "payload mute par un filtre de vue"


def test_exports_serve_raw_disk_bytes():
    """Les boutons d'export servent les OCTETS DU DISQUE, pas une re-serialisation."""
    src = (ROOT / "app.py").read_text(encoding="utf-8")
    assert "def disk_bytes" in src
    assert "read_bytes()" in src, "disk_bytes doit lire les octets bruts"
    # le download_button doit recevoir ces octets
    assert re.search(r"data=disk_bytes\(", src) or "data = disk_bytes(" in src


def test_disk_bytes_is_exactly_the_file_content():
    import app as A
    p = ROOT / "data" / "calendar.json"
    if not p.exists():
        pytest.skip("artefact absent")
    assert A.disk_bytes(p, None) == p.read_bytes()


# ═══════════════════════════════════════════════════════════════════════════
# 2. Le backend reste en UTC ; le fuseau est un choix de VUE
# ═══════════════════════════════════════════════════════════════════════════
def test_backend_writes_utc_only():
    """Les artefacts ne doivent contenir que des instants UTC en *_utc."""
    payload, _ = __import__("tests._make_golden", fromlist=["x"]).build()
    doc = payload.model_dump(mode="json")
    for ev in doc["events"]:
        assert ev["scheduled_at_utc"].endswith("Z")
    assert doc["generated_at_utc"].endswith("Z")


def test_display_timezone_is_not_used_to_compute_content_hash():
    """Invariant de parite : le fuseau d'affichage ne doit pas entrer dans le hash."""
    from calendar_core import SelectionPolicy
    from tests._make_golden import build
    a, _ = build(policy=SelectionPolicy(display_timezone="UTC"))
    b, _ = build(policy=SelectionPolicy(display_timezone="Asia/Tokyo"))
    assert a.content_hash == b.content_hash


# ═══════════════════════════════════════════════════════════════════════════
# 3. Un artefact existant n'est jamais ecrase par un cycle en echec
# ═══════════════════════════════════════════════════════════════════════════
def test_artifact_survives_cooldown_and_failure(data_dir):
    from tests.helpers import CountingSession
    good = pc.run_once(data_dir, session=CountingSession())
    assert good is not None
    snapshot = (data_dir / "calendar.latest.json").read_bytes()

    # echec reseau
    pc.run_once(data_dir, session=CountingSession(plan={"status": 503}))
    assert (data_dir / "calendar.latest.json").read_bytes() == snapshot
    # cooldown
    pc.apply_rate_limit(data_dir, 300)
    pc.run_once(data_dir, session=CountingSession())
    assert (data_dir / "calendar.latest.json").read_bytes() == snapshot


# ═══════════════════════════════════════════════════════════════════════════
# 4. Schema et hashes figes
# ═══════════════════════════════════════════════════════════════════════════
def test_schema_version_is_2_4_2():
    assert calendar_core.SCHEMA_VERSION == "2.4.2"


def test_hash_methods_are_unchanged():
    payload, _ = __import__("tests._make_golden", fromlist=["x"]).build()
    assert payload.content_hash.startswith("sha256:")
    assert payload.parity_hash.startswith("sha256:")
    assert payload.parity_hash_method


# ═══════════════════════════════════════════════════════════════════════════
# 5. Le code d'overlay actuals reste NON branche (decision a valider)
# ═══════════════════════════════════════════════════════════════════════════
def test_actuals_overlay_is_not_wired():
    """Le scraping de la page FF ne doit pas etre branche sans accord explicite.
    On verifie qu'aucun module applicatif ne l'appelle."""
    for name in ("app.py", "pipeline_calendar.py", "trading_export.py", "setup.py"):
        src = (ROOT / name).read_text(encoding="utf-8")
        for fn in ("parse_ff_embedded_calendar", "build_actuals_index",
                   "find_overlay_actual"):
            # autorise les mentions en commentaire, interdit l'appel
            calls = re.findall(rf"(?<![\w.]){fn}\s*\(", src)
            assert not calls, f"{name} appelle {fn} — overlay non valide"


def test_actuals_overlay_functions_still_exist():
    """Ne pas les brancher ne veut pas dire les supprimer (code conserve)."""
    from calendar_core import (build_actuals_index, find_overlay_actual,
                               parse_ff_embedded_calendar)
    assert callable(parse_ff_embedded_calendar)
    assert callable(build_actuals_index)
    assert callable(find_overlay_actual)


# ═══════════════════════════════════════════════════════════════════════════
# 5bis. [AUDIT-H2] CODE MORT SUPPLEMENTAIRE — signale, NON supprime
# ═══════════════════════════════════════════════════════════════════════════
def test_rebase_metadata_is_dead_code():
    """``rebase_metadata`` n'a AUCUN appelant : ni module applicatif, ni test,
    ni artefact produit. Ses champs de sortie (artifact_age_seconds,
    feed_horizon_h_now…) n'apparaissent dans aucun metadata de data/.

    Constate par audit, CONSERVE conformement a la doctrine (« ne pas
    supprimer du code mort sans le signaler »). Ce test verrouille le
    constat : si quelqu'un la branche, il devra le faire sciemment.
    """
    import ast
    from calendar_core import rebase_metadata
    assert callable(rebase_metadata)

    # aucun appel dans les modules applicatifs
    for name in ("app.py", "pipeline_calendar.py", "trading_export.py", "setup.py"):
        src = (ROOT / name).read_text(encoding="utf-8")
        assert not re.findall(r"(?<![\w.])rebase_metadata\s*\(", src), \
            f"{name} appelle desormais rebase_metadata"

    # aucun appel dans les tests non plus (sinon ce n'est plus du code mort)
    for p in (ROOT / "tests").glob("test_*.py"):
        if p.name == "test_invariants.py":
            continue
        src = p.read_text(encoding="utf-8")
        assert not re.findall(r"(?<![\w.])rebase_metadata\s*\(", src), \
            f"{p.name} appelle desormais rebase_metadata"


def test_rebase_metadata_output_is_absent_from_artifacts():
    """Confirme que ses champs ne sont produits nulle part."""
    import json
    leg = json.loads((ROOT / "data" / "calendar.json").read_text(encoding="utf-8"))
    meta = leg.get("metadata", {})
    for key in ("artifact_age_seconds", "feed_horizon_h_now",
                "data_coverage_horizon_h_now"):
        assert key not in meta, f"{key} est desormais produit : le code est branche"


# ═══════════════════════════════════════════════════════════════════════════
# 6. INFO_WARNINGS : exclu du bandeau, conserve dans le JSON
# ═══════════════════════════════════════════════════════════════════════════
def test_info_warning_is_excluded_from_alert_count():
    assert "SOURCE_DOES_NOT_PROVIDE_ACTUAL" in A.INFO_WARNINGS
    alerts, infos = A.split_warnings(["SOURCE_DOES_NOT_PROVIDE_ACTUAL"])
    assert alerts == []
    assert infos == ["SOURCE_DOES_NOT_PROVIDE_ACTUAL"]


def test_real_warnings_still_count_as_alerts():
    alerts, infos = A.split_warnings(
        ["SOURCE_DOES_NOT_PROVIDE_ACTUAL", "IMPACT_VOCABULARY_ALL_REJECTED"])
    assert alerts == ["IMPACT_VOCABULARY_ALL_REJECTED"]
    assert infos == ["SOURCE_DOES_NOT_PROVIDE_ACTUAL"]


def test_info_warning_is_kept_in_the_canonical_json():
    """On ne le retire PAS du JSON : le contrat canonique est inchange."""
    from tests._make_golden import build
    payload, _ = build()
    assert "SOURCE_DOES_NOT_PROVIDE_ACTUAL" in list(payload.quality.warnings)


def test_split_warnings_handles_empty_and_none():
    assert A.split_warnings(None) == ([], [])
    assert A.split_warnings([]) == ([], [])


def test_split_warnings_matches_prefixed_variants():
    alerts, infos = A.split_warnings(["SOURCE_DOES_NOT_PROVIDE_ACTUAL: detail"])
    assert infos and not alerts
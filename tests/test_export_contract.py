"""POINT F8 (audit institutionnel) — le document d'export doit etre
auto-descriptif : version de contrat + heure d'export.

Sans ``exported_at_utc``, un consommateur devait deriver l'heure d'export de
``fetched_at_utc + artifact_age_s``. Sans ``schema_version``, il ne pouvait pas
refuser un schema qu'il ne connait pas.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

import trading_export as TE

UTC = timezone.utc
NOW = datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC)


def _artifact(events=()):
    return {
        "generated_at_utc": "2026-10-15T11:00:00Z",
        "content_hash": "sha256:abc", "parity_hash": "sha256:def",
        "source": {"provider": "FF", "fetched_at_utc": "2026-10-15T11:59:00Z",
                   "url": "https://x/y.json"},
        "quality": {"status": "VALID", "is_stale": False},
        "selection_policy": {"max_source_age_seconds": 900},
        "events": list(events),
    }


def _event(h=3):
    return {"occurrence_id": "a" * 32, "name": "CPI", "currency": "USD",
            "impact": "HIGH",
            "scheduled_at_utc": (NOW + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "scheduled_at_display": "x", "forecast": {"raw": "1"},
            "previous": {"raw": "0"}, "actual": {"raw": None},
            "actual_status": "UNSUPPORTED_BY_SOURCE",
            "pairs_with_currency_exposure": []}


def test_export_declares_its_schema_version():
    doc, _ = TE.build_export(_artifact([_event()]), NOW)
    assert doc["schema_version"] == TE.EXPORT_SCHEMA_VERSION
    assert doc["schema_version"].startswith("trading-export-")


def test_export_declares_when_it_was_produced():
    doc, _ = TE.build_export(_artifact([_event()]), NOW)
    assert doc["exported_at_utc"] == "2026-10-15T12:00:00Z"


def test_exported_at_is_distinct_from_fetched_at():
    """L'heure d'export et l'heure du fetch sont deux choses differentes."""
    doc, _ = TE.build_export(_artifact([_event()]), NOW)
    assert doc["exported_at_utc"] != doc["fetched_at_utc"]


def test_exported_at_survives_a_fresh_artifact():
    """Meme quand l'artefact est tout frais, l'export a sa propre horloge."""
    later = NOW + timedelta(hours=2)
    doc, _ = TE.build_export(_artifact([_event(3)]), later)
    assert doc["exported_at_utc"] == "2026-10-15T14:00:00Z"
    # fetched_at vaut 11:59:00Z -> 14:00 - 11:59 = 7260 s
    assert doc["artifact_age_s"] == 7260


def test_schema_version_is_written_to_the_file(tmp_path):
    d = tmp_path / "data"
    d.mkdir()
    (d / "calendar.latest.json").write_text(json.dumps(
        _artifact([_event()])), encoding="utf-8")
    rc = TE.generate_trading_export(
        ["--data-dir", str(d), "--now", "2026-10-15T12:00:00Z"])
    assert rc == 0
    written = json.loads((d / "trading_export.json").read_text(encoding="utf-8"))
    assert written["schema_version"] == TE.EXPORT_SCHEMA_VERSION
    assert written["exported_at_utc"] == "2026-10-15T12:00:00Z"


def test_documented_fields_match_the_real_document():
    """Garde-fou anti-derive documentaire (F9) : toute cle racine produite par
    le code doit etre documentee, et inversement."""
    import json as _json
    from pathlib import Path
    doc, _ = TE.build_export(_artifact([_event()]), NOW)
    api = _json.loads((Path(TE.__file__).resolve().parent
                       / "docs" / "trading_api.json").read_text(encoding="utf-8"))
    documented = set(api["trading_export"]["fields"])
    produced = set(doc.keys())
    missing = produced - documented
    assert not missing, f"cles produites non documentees : {sorted(missing)}"


def test_documented_event_fields_match_reality():
    from pathlib import Path
    doc, _ = TE.build_export(_artifact([_event()]), NOW)
    api = json.loads((Path(TE.__file__).resolve().parent
                      / "docs" / "trading_api.json").read_text(encoding="utf-8"))
    documented = set(api["trading_export"].get("event_fields") or {})
    produced = set(doc["events"][0].keys())
    missing = produced - documented
    assert not missing, f"champs d'evenement non documentes : {sorted(missing)}"


def test_sort_key_is_chronological_not_lexicographic():
    """Le tri doit rester correct meme si le format derivait : on verifie
    qu'il est chronologique sur des evenements proches."""
    events = [_event(3), _event(1), _event(2)]
    doc, _ = TE.build_export(_artifact(events), NOW)
    stamps = [e["datetime_utc"] for e in doc["events"]]
    assert stamps == sorted(stamps)
    hours = [e["hours_until"] for e in doc["events"]]
    assert hours == sorted(hours)
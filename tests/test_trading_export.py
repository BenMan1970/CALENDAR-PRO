"""POINT 2.7 — trading_export.py : durcissement.

Quatre defauts, tous reproduits ici avant correction :
  a) filtrage sur ``time_context.is_upcoming``, valeur FIGEE a l'ingestion :
     faux des que l'artefact vieillit, ce qui est permanent pendant un cooldown ;
  b) ``event.get("time_context", {})`` renvoie ``None`` si la cle existe avec
     la valeur null -> AttributeError ; idem forecast/previous/actual ;
  c) aucun age, aucun statut qualite : risque pour un usage trading ;
  d) chemin ``data/...`` relatif au repertoire courant.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import trading_export as TE

UTC = timezone.utc
NOW = datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC)


def _artifact(events, *, fetched=None, quality="VALID", stale=False):
    return {
        "schema_version": "2.4.2",
        "generated_at_utc": "2026-10-15T11:00:00Z",
        "content_hash": "sha256:abc",
        "parity_hash": "sha256:def",
        "source": {
            "provider": "Forex Factory / Fair Economy weekly feed",
            "url": "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
            "fetched_at_utc": (fetched or NOW).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "quality": {"status": quality, "is_stale": stale},
        "selection_policy": {"max_source_age_seconds": 900},
        "events": events,
    }


def _event(scheduled: datetime, **over):
    ev = {
        "occurrence_id": "a" * 32, "name": "CPI y/y", "currency": "USD",
        "impact": "HIGH", "scheduled_at_utc": scheduled.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scheduled_at_display": scheduled.isoformat(),
        "actual_status": "UNSUPPORTED_BY_SOURCE",
        "forecast": {"raw": "0.2%"}, "previous": {"raw": "0.1%"},
        "actual": {"raw": None},
        "pairs_with_currency_exposure": ["EUR/USD"],
    }
    ev.update(over)
    return ev


# ═══════════════════════════════════════════════════════════════════════════
# a) is_upcoming recalcule, pas lu
# ═══════════════════════════════════════════════════════════════════════════
def test_future_event_is_included():
    doc, n = TE.build_export(
        _artifact([_event(NOW + timedelta(hours=3))]), NOW)
    assert n == 1
    assert doc["events"][0]["hours_until"] == pytest.approx(3.0)


def test_past_event_is_excluded_even_if_artifact_says_upcoming():
    """Le coeur du bug : l'artefact pretend « is_upcoming: true » pour un
    evenement deja passe."""
    ev = _event(NOW - timedelta(hours=5),
                time_context={"is_upcoming": True, "hours_until": 99.0})
    doc, n = TE.build_export(_artifact([ev]), NOW)
    assert n == 0, "un evenement passe ne doit pas etre exporte"


def test_event_becoming_past_is_dropped_as_time_passes():
    """Le meme artefact, lu a deux instants differents, doit donner deux
    resultats : c'est la preuve que le filtre n'est plus fige."""
    ev = _event(NOW + timedelta(hours=1), time_context={"is_upcoming": True})
    art = _artifact([ev])
    assert TE.build_export(art, NOW)[1] == 1
    assert TE.build_export(art, NOW + timedelta(hours=2))[1] == 0


# ═══════════════════════════════════════════════════════════════════════════
# b) acces defensif aux sous-objets nuls
# ═══════════════════════════════════════════════════════════════════════════
def test_null_time_context_does_not_raise():
    """event.get("time_context", {}) renvoie None si la cle vaut null."""
    ev = _event(NOW + timedelta(hours=2), time_context=None)
    doc, n = TE.build_export(_artifact([ev]), NOW)
    assert n == 1


def test_null_forecast_previous_actual_do_not_raise():
    ev = _event(NOW + timedelta(hours=2), forecast=None, previous=None, actual=None)
    doc, n = TE.build_export(_artifact([ev]), NOW)
    assert n == 1
    got = doc["events"][0]
    assert got["forecast"] is None and got["previous"] is None and got["actual"] is None


def test_null_source_and_quality_do_not_raise():
    art = _artifact([_event(NOW + timedelta(hours=2))])
    art["source"] = None
    art["quality"] = None
    doc, n = TE.build_export(art, NOW)
    assert n == 1
    assert doc["quality_status"] is None


def test_events_not_a_list_does_not_raise():
    art = _artifact([])
    art["events"] = None
    doc, n = TE.build_export(art, NOW)
    assert n == 0 and doc["total_events"] == 0


def test_non_dict_events_are_skipped():
    art = _artifact([None, "x", _event(NOW + timedelta(hours=1))])
    doc, n = TE.build_export(art, NOW)
    assert n == 1


def test_missing_scheduled_at_is_skipped():
    ev = _event(NOW + timedelta(hours=1))
    ev["scheduled_at_utc"] = None
    art = _artifact([ev])
    assert TE.build_export(art, NOW)[1] == 0


# ═══════════════════════════════════════════════════════════════════════════
# c) fraicheur, qualite, et refus d'un artefact perime
# ═══════════════════════════════════════════════════════════════════════════
def test_export_exposes_artifact_metadata():
    art = _artifact([_event(NOW + timedelta(hours=1))], fetched=NOW - timedelta(seconds=120))
    doc, _ = TE.build_export(art, NOW)
    assert doc["artifact_age_s"] == 120
    assert doc["quality_status"] == "VALID"
    assert doc["is_stale"] is False
    assert doc["fetched_at_utc"]
    assert doc["generated_at_utc"]
    assert doc["content_hash"] == "sha256:abc"
    assert doc["parity_hash"] == "sha256:def"


def test_is_stale_is_recomputed_not_trusted():
    """quality.is_stale est fige a l'ingestion : on le recalcule."""
    art = _artifact([_event(NOW + timedelta(hours=1))],
                    fetched=NOW - timedelta(seconds=5000), stale=False)
    doc, _ = TE.build_export(art, NOW)
    assert doc["artifact_age_s"] == 5000
    assert doc["is_stale"] is True


def test_cli_refuses_a_stale_artifact(tmp_path):
    d = tmp_path / "data"
    d.mkdir()
    (d / "calendar.latest.json").write_text(json.dumps(
        _artifact([_event(NOW + timedelta(hours=1))],
                  fetched=NOW - timedelta(seconds=99999))), encoding="utf-8")
    rc = TE.generate_trading_export(
        ["--data-dir", str(d), "--max-age-s", "3600", "--now",
         NOW.strftime("%Y-%m-%dT%H:%M:%SZ")])
    assert rc != 0, "un artefact perime doit etre refuse"
    assert not (d / "trading_export.json").exists(), "aucun fichier ne doit etre ecrit"


def test_cli_accepts_a_fresh_artifact(tmp_path):
    d = tmp_path / "data"
    d.mkdir()
    (d / "calendar.latest.json").write_text(json.dumps(
        _artifact([_event(NOW + timedelta(hours=1))],
                  fetched=NOW - timedelta(seconds=60))), encoding="utf-8")
    rc = TE.generate_trading_export(
        ["--data-dir", str(d), "--now", NOW.strftime("%Y-%m-%dT%H:%M:%SZ")])
    assert rc == 0
    doc = json.loads((d / "trading_export.json").read_text(encoding="utf-8"))
    assert doc["upcoming_events"] == 1
    assert doc["artifact_age_s"] == 60


def test_cli_refuses_invalid_quality(tmp_path):
    d = tmp_path / "data"
    d.mkdir()
    (d / "calendar.latest.json").write_text(json.dumps(
        _artifact([], quality="INVALID", fetched=NOW)), encoding="utf-8")
    rc = TE.generate_trading_export(
        ["--data-dir", str(d), "--now", NOW.strftime("%Y-%m-%dT%H:%M:%SZ")])
    assert rc != 0


def test_cli_allow_stale_permits_export(tmp_path):
    d = tmp_path / "data"
    d.mkdir()
    (d / "calendar.latest.json").write_text(json.dumps(
        _artifact([_event(NOW + timedelta(hours=1))],
                  fetched=NOW - timedelta(seconds=99999))), encoding="utf-8")
    rc = TE.generate_trading_export(
        ["--data-dir", str(d), "--allow-stale", "--now",
         NOW.strftime("%Y-%m-%dT%H:%M:%SZ")])
    assert rc == 0
    assert (d / "trading_export.json").exists()


# ═══════════════════════════════════════════════════════════════════════════
# d) chemins resolus depuis __file__
# ═══════════════════════════════════════════════════════════════════════════
def test_default_data_dir_is_absolute_and_file_relative():
    assert TE.DEFAULT_DATA_DIR.is_absolute()
    assert TE.DEFAULT_DATA_DIR == Path(TE.__file__).resolve().parent / "data"


def test_missing_canonical_file_returns_error(tmp_path, capsys):
    rc = TE.generate_trading_export(["--data-dir", str(tmp_path / "nope")])
    assert rc != 0
    assert "ERROR" in capsys.readouterr().err


def test_export_is_atomic(tmp_path):
    """Aucun fichier partiel ne doit rester apres un export."""
    d = tmp_path / "data"
    d.mkdir()
    (d / "calendar.latest.json").write_text(json.dumps(
        _artifact([_event(NOW + timedelta(hours=1))], fetched=NOW)), encoding="utf-8")
    TE.generate_trading_export(
        ["--data-dir", str(d), "--now", NOW.strftime("%Y-%m-%dT%H:%M:%SZ")])
    assert not list(d.glob("*.tmp"))


# ═══════════════════════════════════════════════════════════════════════════
# pairs reste un champ de CONTEXTE
# ═══════════════════════════════════════════════════════════════════════════
def test_pairs_is_declared_as_context_not_signal():
    doc, _ = TE.build_export(_artifact([_event(NOW + timedelta(hours=1))]), NOW)
    got = doc["events"][0]
    assert got["pairs_with_currency_exposure"] == ["EUR/USD"]
    assert got["pairs_semantics"] == "context_only_not_a_signal"
    assert "signal" not in json.dumps(
        {k: v for k, v in got.items() if k != "pairs_semantics"}).lower()
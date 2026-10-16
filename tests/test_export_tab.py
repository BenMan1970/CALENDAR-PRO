"""AUDIT-F3 — tous les artefacts PUBLIÉS doivent être téléchargeables.

Défaut trouvé : ``run_once`` publie CINQ artefacts, mais l'onglet Export n'en
exposait que trois. ``calendar.committee.json`` (la vue de parité, celle qui
prouve que deux apps voient le même calendrier) et ``trading_export.json``
(le contrat du desk) n'étaient accessibles qu'en allant les chercher sur le
disque du serveur.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

import app as A
import pipeline_calendar as pc


def test_every_published_artifact_is_wired_in_the_export_tab():
    """Le source doit exposer les 5 artefacts reellement publies."""
    src = (Path(A.__file__)).read_text(encoding="utf-8")
    i = src.find("specs = (")
    assert i > 0
    bloc = src[i:i + 900]
    for name in ("calendar.json", "calendar.latest.json",
                 "calendar.committee.json", "trading_export.json",
                 "health.json"):
        assert f'"{name}"' in bloc, f"{name} n'est pas telechargeable"


def test_export_paths_point_into_the_data_dir():
    assert A.COMMITTEE_PATH.parent == A.DATA_DIR
    assert A.EXPORT_PATH.parent == A.DATA_DIR
    assert A.COMMITTEE_PATH.name == "calendar.committee.json"
    assert A.EXPORT_PATH.name == "trading_export.json"


def test_loader_reads_the_five_inputs(tmp_path, monkeypatch):
    monkeypatch.setattr(A, "COMMITTEE_PATH", tmp_path / "calendar.committee.json")
    monkeypatch.setattr(A, "EXPORT_PATH", tmp_path / "trading_export.json")
    monkeypatch.setattr(A, "HEALTH_PATH", tmp_path / "health.json")
    pc.atomic_write_json(A.COMMITTEE_PATH, {"committee_schema": "c1"})
    pc.atomic_write_json(A.EXPORT_PATH, {"schema_version": "trading-export-1.0.0"})
    pc.atomic_write_json(A.HEALTH_PATH, {"status": "VALID"})

    com, exp, hp = A._load_export_tab_inputs()
    assert com["committee_schema"] == "c1"
    assert exp["schema_version"] == "trading-export-1.0.0"
    assert hp["status"] == "VALID"


def test_loader_is_resilient_to_missing_or_corrupt_files(tmp_path, monkeypatch):
    monkeypatch.setattr(A, "COMMITTEE_PATH", tmp_path / "absent.json")
    monkeypatch.setattr(A, "EXPORT_PATH", tmp_path / "corrupt.json")
    monkeypatch.setattr(A, "HEALTH_PATH", tmp_path / "health.json")
    (tmp_path / "corrupt.json").write_text("{ pas du json", encoding="utf-8")

    com, exp, hp = A._load_export_tab_inputs()
    assert com is None and exp is None and hp is None


def test_disk_bytes_serves_exact_file_bytes(tmp_path):
    """L'invariant du projet : l'export sert le disque, pas une vue."""
    p = tmp_path / "a.json"
    pc.atomic_write_json(p, {"k": "valeur accentuée", "n": 1})
    assert A.disk_bytes(p) == p.read_bytes()


def test_disk_bytes_never_returns_empty_on_a_missing_file(tmp_path):
    """Un fichier absent doit produire un repli lisible, pas 0 octet."""
    out = A.disk_bytes(tmp_path / "absent.json", {"fallback": True})
    assert json.loads(out.decode("utf-8")) == {"fallback": True}
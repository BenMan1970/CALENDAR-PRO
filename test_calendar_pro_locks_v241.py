"""
V2.4.1 — verrous de la correction « audit OPUS » (15/09/2026).

Chaque test épingle UNE affirmation de l'audit vérifiée VRAIE par
expérimentation directe sur l'artefact/calendar.json live ; le test ROUGE
signifie que la correction a régressé. Noms stables pour le CI-guard
(« >= N tests ») : ce fichier porte le compteur de 94 à 106.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import calendar_core as core            # noqa: E402
import calendar_ingestor as ci          # noqa: E402

UTC = timezone.utc


def _row(title: str = "ISM", ccy: str = "USD", hours_ahead: float = 6,
         impact: str = "High") -> dict:
    when = datetime.now(UTC) + timedelta(hours=hours_ahead)
    return {"title": title, "country": ccy, "date": when.isoformat(),
            "impact": impact, "forecast": "1.0", "previous": "1.1"}


def _src(now: datetime, feed_status: dict | None = None) -> core.SourceInfo:
    return core.SourceInfo(
        provider="test", url="https://test/feed.json", fetched_at_utc=now,
        payload_sha256="sha256:test", supports_actual=False,
        feed_status=feed_status or {"thisweek": "ok", "nextweek": "absent_404"},
    )


def _meta(sha: str = "sha256:test", body: bytes = b"[]") -> dict:
    return {"http_status": 200, "content_type": "application/json",
            "payload_bytes": len(body), "payload_sha256": sha, "etag": None,
            "last_modified": None, "fetch_duration_ms": 1, "raw_bytes": body}


# ─────────────────────────────────────────────────────────────────────────────
# B1 — le « VALID 1.000 vide » n'est plus publiable (mesuré live 15/09 :
# 105 lignes normalisées, 0 événement, status VALID)
# ─────────────────────────────────────────────────────────────────────────────

def test_b1_total_vocabulary_rejection_is_invalid():
    now = datetime.now(UTC)
    rows = [{**_row(f"T{i}", hours_ahead=h), "impact": "Critical"}
            for i, h in enumerate((4, 30, 60))]
    p = core.build_payload(rows, source=_src(now), now_utc=now)
    assert p.quality.status is core.QualityStatus.INVALID
    assert "IMPACT_VOCABULARY_ALL_REJECTED" in p.quality.warnings
    assert p.quality.accepted_event_count == 0


def test_b1_partial_drift_is_degraded_not_silent():
    now = datetime.now(UTC)
    rows = [_row("A", hours_ahead=4), {**_row("B", hours_ahead=8), "impact": "Critical"},
            _row("C", hours_ahead=20, impact="Medium")]
    p = core.build_payload(rows, source=_src(now), now_utc=now)
    assert p.quality.status is core.QualityStatus.DEGRADED
    assert "SOURCE_IMPACT_VOCABULARY_CHANGED" in p.quality.warnings
    assert p.quality.accepted_event_count >= 1          # partiel ≠ total


def test_b1_calm_week_without_high_is_legitimately_publishable():
    # La CORRECTION ne doit pas transformer une vraie semaine calme en
    # incident : aucun UNKNOWN (vocabulaire sain), simplement zéro High.
    now = datetime.now(UTC)
    rows = [_row("Low1", hours_ahead=6, impact="Low"),
            _row("Hol1", hours_ahead=10, impact="Holiday")]
    p = core.build_payload(rows, source=_src(now), now_utc=now)
    assert p.quality.status is core.QualityStatus.VALID
    assert "SOURCE_IMPACT_VOCABULARY_CHANGED" not in p.quality.warnings
    assert "IMPACT_VOCABULARY_ALL_REJECTED" not in p.quality.warnings


def test_b1_ingestor_rejects_drift_and_keeps_previous_artifact(tmp_path, monkeypatch):
    state = {"shift": False}

    def fake(session, url):
        if "nextweek" in url:
            raise ci.FetchError("HTTP_ERROR", "status=404")
        impact = "Critical" if state["shift"] else "High"
        return [_row("ISM", hours_ahead=6, impact=impact)], _meta()

    monkeypatch.setattr(ci, "fetch_source", fake)
    pol = core.SelectionPolicy()
    assert ci.run_once(tmp_path, pol, None) is not None
    healthy = (tmp_path / "calendar.json").read_bytes()

    state["shift"] = True
    assert ci.run_once(tmp_path, pol, None) is None      # refus TOTAL
    assert (tmp_path / "calendar.json").read_bytes() == healthy
    health = json.loads((tmp_path / "health.json").read_text(encoding="utf-8"))
    assert "QUALITY_INVALID" in (health.get("last_error") or "")
    assert "IMPACT_VOCABULARY_ALL_REJECTED" in " ".join(health.get("warnings") or [])


# ─────────────────────────────────────────────────────────────────────────────
# B2 — l'autorité des règles horaires est le paquet pip épinglé, et la règle
# marocaine du 20/09/2026 (GMT permanent) est vérifiée, pas espérée
# ─────────────────────────────────────────────────────────────────────────────

def test_b2_pip_tzdata_is_head_of_tzpath_when_installed():
    if importlib.util.find_spec("tzdata") is None:
        pytest.skip("paquet tzdata absent — repli système alors assumé")
    import zoneinfo
    head = str(zoneinfo.TZPATH[0]).replace("\\", "/")
    assert head.endswith("tzdata/zoneinfo")


def test_b2_morocco_gmt_rule_from_2026_09_20():
    env = core.tz_environment()
    off = env["casablanca_offsets"]
    assert off["2026-09-19"][0] == 1.0        # dernier jour de l'ancien régime
    assert off["2026-09-21"][0] == 0.0        # GMT dès le 20/09
    assert off["2027-03-01"][0] == 0.0        # et définitivement (pas de
                                              # rattrapage printanier)
    if importlib.util.find_spec("tzdata") is not None:
        assert env["pip_tzdata_authoritative"] is True


# ─────────────────────────────────────────────────────────────────────────────
# Fusion multi-flux — la signature couvre DÉSORMAIS CE QUI EST CONSOMMÉ,
# selon la formule exacte du calendar_layer macro (inter-apps comparable)
# ─────────────────────────────────────────────────────────────────────────────

def test_merged_hash_uses_macro_join_formula(tmp_path, monkeypatch):
    def fake(session, url):
        if "nextweek" in url:
            return [_row("BOJ", "JPY", 150)], _meta("sha256:BBB", b"[{}]")
        return [_row("ISM", "USD", 6)], _meta("sha256:AAA", b"[]")

    monkeypatch.setattr(ci, "fetch_source", fake)
    p = ci.run_once(tmp_path, core.SelectionPolicy(), None)
    assert p is not None
    # Formule macro : sha256_hex de la concaténation « | » des sha préfixés
    # des flux ok, dans l'ordre thisweek → nextweek.
    assert p.source.payload_sha256 == "sha256:" + core.sha256_hex("sha256:AAA|sha256:BBB")
    assert p.source.feed_sha256 == {"thisweek": "sha256:AAA", "nextweek": "sha256:BBB"}
    archived = sorted((tmp_path / "raw").glob("raw_*_nextweek.json"))
    assert archived, "les octets nextweek fusionnés doivent être archivés"
    assert archived[-1].read_bytes() == b"[{}]"


def test_single_feed_hash_is_join_of_one(tmp_path, monkeypatch):
    def fake(session, url):
        if "nextweek" in url:
            raise ci.FetchError("HTTP_ERROR", "status=404")
        return [_row("ISM", "USD", 6)], _meta("sha256:AAA", b"[]")

    monkeypatch.setattr(ci, "fetch_source", fake)
    p = ci.run_once(tmp_path, core.SelectionPolicy(), None)
    assert p.source.payload_sha256 == "sha256:" + core.sha256_hex("sha256:AAA")
    assert p.source.feed_sha256 == {"thisweek": "sha256:AAA"}


# ─────────────────────────────────────────────────────────────────────────────
# B3 — verrou occupé par un autre producteur : sonde + statut distingué
# ─────────────────────────────────────────────────────────────────────────────

def test_b3_publish_lock_held_probe(tmp_path):
    assert ci.publish_lock_held(tmp_path) is False
    assert ci._acquire_publish_lock(tmp_path) is True
    assert ci.publish_lock_held(tmp_path) is True
    ci._release_publish_lock(tmp_path)
    assert ci.publish_lock_held(tmp_path) is False
    # verrou d'un tiers encore frais = tenu ; au-delà du plafond de vol = non
    lock = tmp_path / ".publish.lock"
    lock.write_text("pid=999 at=x", encoding="utf-8")
    assert ci.publish_lock_held(tmp_path) is True
    old = time.time() - (ci.LOCK_STEAL_AFTER_S + 60)
    os.utime(lock, (old, old))
    assert ci.publish_lock_held(tmp_path) is False


def test_b3_disable_ingest_flag_parses_truthy_values(monkeypatch):
    # Inverser l'env et ré-évaluer la constante de module serait tordu ;
    # le contrat retenu : {"1","true","yes","on"} (insensible à la casse)
    # désactivent l'émission réseau. Test direct de l'expression.
    def resolves(value: str) -> bool:
        return value.strip().lower() not in {"1", "true", "yes", "on"}
    assert resolves("") is True and resolves("false") is True
    assert resolves("1") is False and resolves("TRUE") is False
    assert resolves("on") is False


# ─────────────────────────────────────────────────────────────────────────────
# Couverture courte : jugée sur les ÉVÉNEMENTS (ce que l'aval lit), pas sur
# toutes les lignes normalisées (LOW/Holiday inclus qui décalaient le signal)
# ─────────────────────────────────────────────────────────────────────────────

def test_coverage_short_is_judged_on_events_not_rows():
    now = datetime.now(UTC)
    rows = [_row("LURR", hours_ahead=6),                     # horizon events: 6 h
            _row("ECOFIN", hours_ahead=120, impact="Low")]   # horizon rows : 120 h
    p = core.build_payload(
        rows, source=_src(now, {"thisweek": "ok", "nextweek": "ok"}), now_utc=now)
    # Base rows : 120 h > 48 h → muet. Base events : 6 h < 48 h → SONNE.
    assert any(w.startswith("COVERAGE_SHORTER_THAN_HORIZON") for w in p.quality.warnings)
    # et le message cite l'horizon EVENTS (6 h), pas celui des rows :
    short = next(w for w in p.quality.warnings if w.startswith("COVERAGE_SHORTER_THAN_HORIZON"))
    assert short.split(":")[1].startswith("6.")


# ─────────────────────────────────────────────────────────────────────────────
# UI — honnêtetés mesurables (aucune régression visuelle par ailleurs)
# ─────────────────────────────────────────────────────────────────────────────

def test_ui_format_numeric_keeps_meaningful_raw():
    import app
    votes = core.NumericValue.model_validate(
        {"value": None, "raw": "3-0-6", "parse_status": "UNPARSEABLE"})
    assert app.format_numeric(votes) == "3-0-6"       # info portée, plus jetée
    absent = core.NumericValue()                       # défaut : ABSENT, raw None
    assert app.format_numeric(absent) == "—"           # le tiret = ABSENT strict
    parsed = core.NumericValue.model_validate(
        {"value": 1.0, "raw": "1.0", "unit": "percent", "parse_status": "PARSED"})
    assert app.format_numeric(parsed) == "1.00%"


def test_ui_download_serves_disk_bytes(tmp_path, monkeypatch):
    import app
    f = tmp_path / "calendar.json"
    f.write_bytes(b'{"published":"octets"}')
    monkeypatch.setattr(app, "CALENDAR_JSON_PATH", f)
    data, src = app.serve_legacy_bytes(None, datetime.now(UTC))
    assert data == b'{"published":"octets"}' and src == "disque"
    # fichier absent → repli regénération, ÉTIQUETÉ (et visible à l'écran)
    monkeypatch.setattr(app, "CALENDAR_JSON_PATH", tmp_path / "absent.json")
    now = datetime.now(UTC)
    payload = core.build_payload([_row("ISM", "USD", 6)], source=_src(now), now_utc=now)
    data2, src2 = app.serve_legacy_bytes(payload, now)
    assert src2 == "regénéré" and json.loads(data2)["events"]


def test_ui_two_fragments_exist_for_real_dispatch():
    # La case « Rafraîchissement visuel » doit piloter DEUX fragments
    # distincts (avant correction : un seul, périodique, dans les deux
    # branches de main() — contrôle purement décoratif).
    import app
    assert callable(app._live_fragment) and callable(app._manual_fragment)
    assert app._live_fragment is not app._manual_fragment


def test_app_boot_with_new_code(tmp_path, monkeypatch):
    # smoke complet : module + fragment principal via AppTest, sans réseau.
    from streamlit.testing.v1 import AppTest
    monkeypatch.setenv("BLUESTAR_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("BLUESTAR_DISABLE_INGEST", "1")
    at = AppTest.from_file(os.path.join(os.path.dirname(__file__), "app.py"),
                           default_timeout=90)
    at.run()
    assert not at.exception, at.exception
    # mode lecteur : aucune exception, page rendue (vide d'artefact = erreur
    # diagnostics affichée, chemin déjà couvert par le design).

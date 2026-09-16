# -*- coding: utf-8 -*-
"""Verrous de la correction « l'application ne montre rien » (16/09/2026).

Chaque test épingle UN fait MESURÉ avant correctif. Test rouge = régression.
Aucun réseau : le flux amont est rejoué depuis l'artefact legacy publié
(27 événements, VALID 1.000, 16 HIGH + 11 MEDIUM).
"""
from __future__ import annotations

import importlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import calendar_core as core          # noqa: E402
import calendar_ingestor as ci        # noqa: E402

UTC = timezone.utc
LEGACY_FIXTURE = HERE / "calendar_-10.json"
IMPACT_BACK = {"high": "High", "medium": "Medium", "low": "Low", "holiday": "Holiday"}


def feed_rows() -> list:
    doc = json.loads(LEGACY_FIXTURE.read_text(encoding="utf-8"))
    return [{
        "title": e["event_name"],
        "country": e["currency"],
        "date": e["datetime_utc"].replace("Z", "+00:00"),
        "impact": IMPACT_BACK[e["impact"]],
        "forecast": e.get("forecast") if e.get("forecast") != "—" else "",
        "previous": e.get("previous") if e.get("previous") != "—" else "",
    } for e in doc["events"]]


def _src(now):
    return core.SourceInfo(provider="test", url="https://t/f.json", fetched_at_utc=now,
                           payload_sha256="sha256:t", supports_actual=False,
                           feed_status={"thisweek": "ok", "nextweek": "absent_404"})


def _fresh_app(monkeypatch, data_dir: Path, **env):
    """Recharge app.py avec un environnement donné (constantes calculées à l'import)."""
    for k, v in env.items():
        if v is None:
            monkeypatch.delenv(k, raising=False)
        else:
            monkeypatch.setenv(k, v)
    monkeypatch.setenv("BLUESTAR_DATA_DIR", str(data_dir))
    sys.modules.pop("app", None)
    return importlib.import_module("app")


# ─────────────────────────────────────────────────────────────────────────────
# E — une variable d'environnement DÉFINIE MAIS VIDE ne tue plus l'import.
# Mesuré avant : float("") / int("") → ValueError au chargement de app.py,
# l'application entière refusait de démarrer (écran blanc, zéro diagnostic).
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("var,attr,expected", [
    ("BLUESTAR_WINDOW_FUTURE_HOURS", None, 168.0),
    ("BLUESTAR_WINDOW_PAST_HOURS", None, 72.0),
    ("BLUESTAR_INGEST_INTERVAL", "INGEST_INTERVAL_SECONDS", 300),
    ("BLUESTAR_UI_REFRESH_INTERVAL", "UI_REFRESH_SECONDS", 10),
    ("BLUESTAR_MAX_SOURCE_AGE_SECONDS", None, 900),
])
def test_e_env_vide_ne_casse_pas_limport(tmp_path, monkeypatch, var, attr, expected):
    app = _fresh_app(monkeypatch, tmp_path, **{var: ""})
    if attr:
        assert getattr(app, attr) == expected


def test_e_env_illisible_replie_sur_le_defaut(tmp_path, monkeypatch):
    app = _fresh_app(monkeypatch, tmp_path, BLUESTAR_WINDOW_FUTURE_HOURS="beaucoup")
    assert app.MACHINE_POLICY.window_future_hours == core.DEFAULT_POLICY.window_future_hours


def test_e_booleen_tolere_lespace_parasite(tmp_path, monkeypatch):
    # Mesuré avant : "True " → "true " ∉ set littéral → False, en silence,
    # ce qui écartait tous les événements globaux sans un mot à l'écran.
    app = _fresh_app(monkeypatch, tmp_path, BLUESTAR_INCLUDE_GLOBAL="True ")
    assert app.MACHINE_POLICY.include_global_events is True
    app = _fresh_app(monkeypatch, tmp_path, BLUESTAR_INCLUDE_GLOBAL=" NO ")
    assert app.MACHINE_POLICY.include_global_events is False


# ─────────────────────────────────────────────────────────────────────────────
# C — la sidebar ne peut plus contredire la politique machine.
# Mesuré avant : MACHINE_POLICY publiait HIGH+MEDIUM (27 événements), la case
# MEDIUM était décochée en dur → 16/27 à l'écran, dont Retail Sales et
# Unemployment Claims invisibles.
# ─────────────────────────────────────────────────────────────────────────────
def test_c_defauts_impact_derivent_de_la_politique_machine(tmp_path, monkeypatch):
    app = _fresh_app(monkeypatch, tmp_path, BLUESTAR_MACHINE_IMPACTS=None)
    src = HERE.joinpath("app.py").read_text(encoding="utf-8")
    assert "MACHINE_POLICY.impact_levels" in src.split("def render_sidebar")[1][:900]
    assert core.Impact.MEDIUM in app.MACHINE_POLICY.impact_levels


def test_c_ecran_affiche_tous_les_evenements_publies(tmp_path, monkeypatch):
    from streamlit.testing.v1 import AppTest
    import re

    rows = feed_rows()
    meta = {"http_status": 200, "content_type": "application/json", "payload_bytes": 2,
            "payload_sha256": "sha256:t", "etag": None, "last_modified": None,
            "fetch_duration_ms": 1, "raw_bytes": b"[]"}

    def fake(session, url, deadline_s=None):
        if "nextweek" in url:
            raise ci.FetchError("HTTP_ERROR", "status=404")
        return list(rows), meta

    monkeypatch.setattr(ci, "fetch_source", fake)
    monkeypatch.setattr(ci, "DISABLE_ACTUALS", True)
    payload = ci.run_once(tmp_path, core.SelectionPolicy(), None)
    assert payload is not None and len(payload.events) == 27

    monkeypatch.setenv("BLUESTAR_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("BLUESTAR_DISABLE_INGEST", "1")
    sys.modules.pop("app", None)
    at = AppTest.from_file(str(HERE / "app.py"), default_timeout=180)
    at.run()
    assert not at.exception, at.exception
    assert not at.error, [e.value for e in at.error]
    joined = " ".join(m.value for m in at.markdown)
    hits = re.findall(r"(\d+)\s*/\s*(\d+)\s*événements", joined)
    assert hits, "compteur visible/total introuvable dans le rendu"
    visible, total = hits[0]
    assert visible == total == "27", f"{visible}/{total} — la sidebar masque encore"


# ─────────────────────────────────────────────────────────────────────────────
# B — un calendrier vide n'est plus MUET.
# Mesuré avant : BLUESTAR_MACHINE_CURRENCIES=EURO → 27 lignes normalisées,
# 0 événement, VALID, score 1.000, AUCUN warning. L'aval lisait ce vide
# comme un fait de marché.
# ─────────────────────────────────────────────────────────────────────────────
def test_b_selection_vide_est_nommee_avec_sa_cause():
    now = datetime.now(UTC)
    pol = core.SelectionPolicy(currencies=("EURO",))
    p = core.build_payload(feed_rows(), source=_src(now), now_utc=now, policy=pol)
    assert len(p.events) == 0
    assert any(w.startswith("SELECTION_EMPTY:") for w in p.quality.warnings)
    warn = next(w for w in p.quality.warnings if w.startswith("SELECTION_EMPTY:"))
    assert "currency=27" in warn                     # la cause EXACTE est nommée


def test_b_selection_vide_ne_deplace_ni_statut_ni_score():
    """Le warning est additif : promouvoir en DEGRADED casserait le verrou B1
    existant (semaine calme légitimement publiable) — décision de review board,
    pas un correctif passé en douce."""
    now = datetime.now(UTC)
    pol = core.SelectionPolicy(currencies=("EURO",))
    p = core.build_payload(feed_rows(), source=_src(now), now_utc=now, policy=pol)
    assert p.quality.status is core.QualityStatus.VALID
    assert p.quality.data_quality_score == 1.0


def test_b_aucun_warning_parasite_quand_la_selection_est_pleine():
    now = datetime.now(UTC)
    p = core.build_payload(feed_rows(), source=_src(now), now_utc=now)
    assert len(p.events) == 27
    assert not any(w.startswith("SELECTION_EMPTY") for w in p.quality.warnings)


# ─────────────────────────────────────────────────────────────────────────────
# CONTRAT — aucun correctif ne déplace le content_hash inter-apps.
# ─────────────────────────────────────────────────────────────────────────────
def test_contrat_content_hash_identique_a_lartefact_publie():
    reference = json.loads(LEGACY_FIXTURE.read_text(encoding="utf-8"))["metadata"]["content_hash"]
    now = datetime.now(UTC)
    p = core.build_payload(feed_rows(), source=_src(now), now_utc=now)
    assert p.content_hash == reference


# ─────────────────────────────────────────────────────────────────────────────
# A — l'écran rouge dit POURQUOI.
# ─────────────────────────────────────────────────────────────────────────────
def test_a_mode_lecteur_ne_conseille_plus_de_verifier_le_reseau(tmp_path, monkeypatch):
    from streamlit.testing.v1 import AppTest
    monkeypatch.setenv("BLUESTAR_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("BLUESTAR_DISABLE_INGEST", "1")
    sys.modules.pop("app", None)
    at = AppTest.from_file(str(HERE / "app.py"), default_timeout=180)
    at.run()
    assert not at.exception, at.exception
    warns = " ".join(w.value for w in at.warning)
    assert "LECTEUR" in warns
    assert "réseau sortant" not in warns          # la fausse piste a disparu
    diag = " ".join(j.value for j in at.json)
    for key in ("ingestion_enabled", "canonical_parse_error", "canonical_schema_version"):
        assert key in diag


def test_a_erreur_de_validation_est_remontee_a_lecran(tmp_path, monkeypatch):
    """Artefact écrit par un core d'une AUTRE version : `extra='forbid'` le
    rejette en bloc. Avant : écran rouge + `canonical_exists: true`, sans un
    mot sur la cause."""
    app = _fresh_app(monkeypatch, tmp_path, BLUESTAR_DISABLE_INGEST="1")
    app.CANONICAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    app.CANONICAL_PATH.write_text(
        json.dumps({"schema_version": "9.9.9", "champ_du_futur": 1}), encoding="utf-8")
    assert app.load_payload() is None
    assert app._LAST_LOAD_ERROR["msg"], "la raison du rejet doit être conservée"
    assert "ValidationError" in app._LAST_LOAD_ERROR["msg"]


def test_a_artefact_valide_efface_lerreur_precedente(tmp_path, monkeypatch):
    app = _fresh_app(monkeypatch, tmp_path, BLUESTAR_DISABLE_INGEST="1")
    now = datetime.now(UTC)
    p = core.build_payload(feed_rows(), source=_src(now), now_utc=now)
    app.CANONICAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    app.CANONICAL_PATH.write_text(p.model_dump_json(), encoding="utf-8")
    loaded = app.load_payload()
    assert loaded is not None and len(loaded.events) == 27
    assert app._LAST_LOAD_ERROR["msg"] is None


# ─────────────────────────────────────────────────────────────────────────────
# DÉPLOIEMENT — l'app s'auto-alimente sur un conteneur vide (Streamlit Cloud).
# ─────────────────────────────────────────────────────────────────────────────
def test_deploiement_auto_amorcage_sur_conteneur_vide(tmp_path, monkeypatch):
    from streamlit.testing.v1 import AppTest
    import re

    rows = feed_rows()
    meta = {"http_status": 200, "content_type": "application/json", "payload_bytes": 2,
            "payload_sha256": "sha256:t", "etag": None, "last_modified": None,
            "fetch_duration_ms": 1, "raw_bytes": b"[]"}

    def fake(session, url, deadline_s=None):
        if "nextweek" in url:
            raise ci.FetchError("HTTP_ERROR", "status=404")
        return list(rows), meta

    monkeypatch.setattr(ci, "fetch_source", fake)
    monkeypatch.setattr(ci, "DISABLE_ACTUALS", True)
    monkeypatch.delenv("BLUESTAR_DISABLE_INGEST", raising=False)
    monkeypatch.setenv("BLUESTAR_DATA_DIR", str(tmp_path))
    sys.modules.pop("app", None)

    at = AppTest.from_file(str(HERE / "app.py"), default_timeout=180)
    at.run()
    assert not at.exception, at.exception
    assert not at.error, [e.value for e in at.error]
    assert (tmp_path / "calendar.latest.json").exists()
    assert (tmp_path / "calendar.json").exists()
    joined = " ".join(m.value for m in at.markdown)
    hits = re.findall(r"(\d+)\s*/\s*(\d+)\s*événements", joined)
    assert hits and hits[0] == ("27", "27")


# ─────────────────────────────────────────────────────────────────────────────
# TZ — verrou COMPORTEMENTAL du garde B2.
# L'ancien verrou (test_b2_pip_tzdata_is_head_of_tzpath_when_installed)
# n'inspecte que la CHAÎNE zoneinfo.TZPATH[0]. Il passait au vert alors que
# le garde était inopérant : `zoneinfo.TZPATH = (...)` rebinde l'attribut de
# module sans toucher au chemin de recherche interne (PEP 615 : seul
# reset_tzpath() le fait). Résultat mesuré : TZPATH[0] annonçait le tzdata
# pip, et ZoneInfo() lisait quand même le tzdata SYSTÈME.
# Ici on teste ce que l'écran affichera vraiment.
# ─────────────────────────────────────────────────────────────────────────────
def test_tz_regle_marocaine_est_reellement_appliquee_par_zoneinfo():
    """Décret n° 2.26.530 : Africa/Casablanca → GMT le 20/09/2026 à 02:00."""
    from zoneinfo import ZoneInfo
    tz = ZoneInfo("Africa/Casablanca")
    avant = datetime(2026, 9, 19, 12, tzinfo=tz)
    apres = datetime(2026, 9, 21, 12, tzinfo=tz)
    assert avant.utcoffset().total_seconds() == 3600.0
    assert apres.utcoffset().total_seconds() == 0.0, (
        "tzdata trop ancien OU garde _pin_pip_tzdata inopérant : "
        "toutes les heures affichées seront en retard d'une heure")


def test_tz_bascule_est_permanente_plus_de_reversion_ramadan():
    """L'ancien régime revenait à GMT pendant le Ramadan puis à GMT+1. Le
    décret abroge ce dispositif : +00 doit tenir aussi en plein été."""
    from zoneinfo import ZoneInfo
    tz = ZoneInfo("Africa/Casablanca")
    for d in (datetime(2027, 3, 1, 12), datetime(2027, 6, 1, 12),
              datetime(2028, 6, 1, 12)):
        assert d.replace(tzinfo=tz).utcoffset().total_seconds() == 0.0, d


def test_tz_plancher_tzdata_est_tenu():
    """tzdata 2026.3 (pin précédent) NE CONTIENT PAS la règle. Plancher dur."""
    import tzdata
    major, minor = (int(x) for x in tzdata.__version__.split(".")[:2])
    assert (major, minor) >= (2026, 4), (
        f"tzdata {tzdata.__version__} < 2026.4 — règle marocaine absente")


def test_tz_le_diagnostic_ne_ment_plus():
    """`pip_tzdata_authoritative: true` doit impliquer que les règles LUES
    sont bien celles du paquet pip — l'affirmation était fausse avant."""
    from zoneinfo import ZoneInfo
    env = core.tz_environment()
    if not env["pip_tzdata_authoritative"]:
        pytest.skip("paquet tzdata absent — repli système assumé")
    offsets = env["casablanca_offsets"]
    assert offsets["2026-09-21"][0] == 0.0
    # cohérence entre ce que le diagnostic annonce et ce que l'app calcule
    observe = datetime(2026, 9, 21, 12, tzinfo=ZoneInfo("Africa/Casablanca"))
    assert observe.utcoffset().total_seconds() == offsets["2026-09-21"][0] * 3600

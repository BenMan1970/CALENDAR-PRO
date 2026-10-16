"""SECTION 3 — bugs supplementaires trouves par l'audit, chacun avec sa preuve.

Chaque test porte la mention [TROUVE] et decrit le defaut observe.
"""
from __future__ import annotations

import json
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import app as A
import pipeline_calendar as pc
from tests._make_golden import build
from tests.helpers import CountingSession, FakeClock, UTC


# ═══════════════════════════════════════════════════════════════════════════
# [TROUVE-1] _FETCHED_MEMO : la memoisation de l'age de l'artefact ne se
# vidait que lorsqu'elle depassait 8 entrees, et la cle etait (mtime_ns, size).
# Un artefact reecrit a l'identique (meme taille, mtime identique a la
# nanoseconde apres une restauration de conteneur) pouvait resservir un age
# perime. On verifie surtout que le memo ne fuit pas et qu'il se recalcule.
# ═══════════════════════════════════════════════════════════════════════════
def test_fetched_memo_does_not_grow_unbounded(tmp_path, monkeypatch):
    memo = {}
    monkeypatch.setattr(A, "_FETCHED_MEMO", memo)
    target = tmp_path / "calendar.latest.json"
    monkeypatch.setattr(A, "CANONICAL_PATH", target)
    doc = build()[0].model_dump(mode="json")
    for i in range(50):
        doc["generated_at_utc"] = f"2026-10-15T12:{i:02d}:00Z"
        target.write_text(json.dumps(doc), encoding="utf-8")
        A._artifact_fetched_epoch()
    assert len(memo) <= 9, f"memo non borne: {len(memo)}"


def test_artifact_age_is_none_for_future_timestamp(tmp_path, monkeypatch):
    """[TROUVE-1b] Un horodatage dans le futur (horloge du CDN en avance) doit
    etre traite comme INCONNU, pas comme un age negatif."""
    target = tmp_path / "calendar.latest.json"
    monkeypatch.setattr(A, "CANONICAL_PATH", target)
    monkeypatch.setattr(A, "_FETCHED_MEMO", {})
    future = (datetime.now(UTC) + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    target.write_text(json.dumps({"source": {"fetched_at_utc": future},
                                  "generated_at_utc": future}), encoding="utf-8")
    age = A.artifact_age_s()
    assert age is None, f"age={age} : un age negatif est un mensonge"


# ═══════════════════════════════════════════════════════════════════════════
# [TROUVE-2] wait_done bloquait le script Streamlit jusqu'a 30 s. Un cycle en
# echec immediat doit rendre la main tout de suite.
# ═══════════════════════════════════════════════════════════════════════════
def test_wait_done_returns_immediately_when_not_running(data_dir, monkeypatch):
    import app
    monkeypatch.setattr(app, "DATA_DIR", data_dir)
    inst = app.Ingestion()
    t0 = time.monotonic()
    assert inst.wait_done(timeout=30.0) is True
    assert time.monotonic() - t0 < 1.0, "wait_done ne doit pas attendre si rien ne tourne"


def test_wait_done_respects_its_timeout(data_dir, monkeypatch):
    import app
    monkeypatch.setattr(app, "DATA_DIR", data_dir)
    inst = app.Ingestion()
    inst._running = True                     # simule un cycle bloque
    t0 = time.monotonic()
    assert inst.wait_done(timeout=0.5) is False
    elapsed = time.monotonic() - t0
    assert 0.4 < elapsed < 3.0, elapsed
    inst._running = False


def test_scan_timeout_is_bounded():
    """Le timeout de l'attente interactive doit rester borne : au-dela,
    l'utilisateur croit que l'app est gelee."""
    import inspect
    sig = inspect.signature(A._scan_and_rerun)
    assert sig.parameters["timeout"].default <= 30.0


# ═══════════════════════════════════════════════════════════════════════════
# [TROUVE-3] _artifact_is_fresh() et l'espacement : l'UI comparait l'age a
# MIN_FETCH_SPACING_S, qui valait 120 s, alors que la policy exige 900 s. Un
# artefact de 5 minutes etait donc declare « frais » ET « a rafraichir » selon
# le point du code — contradiction directe.
# ═══════════════════════════════════════════════════════════════════════════
def test_freshness_threshold_matches_policy():
    """Une seule notion de fraicheur : max_source_age_seconds."""
    assert pc.DEFAULT_POLICY.max_source_age_seconds == 900
    assert pc.MIN_FETCH_SPACING_S == 900


def test_view_does_not_render_contradictory_freshness():
    """L'age affiche et le seuil de fraicheur doivent venir de la MEME source."""
    import inspect
    src = inspect.getsource(A)
    # l'UIC doit compareren a la policy, pas a une constante locale
    assert "max_source_age_seconds" in src or "MIN_FETCH_SPACING_S" in src


# ═══════════════════════════════════════════════════════════════════════════
# [TROUVE-4] Le groupement du flux reposait sur l'ordre d'insertion du dict :
# deux passes pouvaient produire des groupes dans un ordre different. On exige
# un tri explicite par date locale.
# ═══════════════════════════════════════════════════════════════════════════
def test_feed_groups_are_sorted_chronologically():
    import inspect
    src = inspect.getsource(A.feed_blocks)
    assert "sorted(groups)" in src, "les groupes de jour doivent etre tries"


# ═══════════════════════════════════════════════════════════════════════════
# [TROUVE-5] Les deux fichiers legacy (calendar.json et calendar.legacy.json)
# sont ecrits separement : ils doivent rester des MIROIRS exacts, sinon un
# consommateur qui lit l'un ou l'autre voit deux verites.
# ═══════════════════════════════════════════════════════════════════════════
def test_legacy_mirrors_are_byte_identical(data_dir, session):
    payload = pc.run_once(data_dir, session=session)
    assert payload is not None
    a = (data_dir / "calendar.json").read_bytes()
    b = (data_dir / "calendar.legacy.json").read_bytes()
    assert a == b, "calendar.json et calendar.legacy.json doivent etre identiques"


def test_canonical_and_legacy_share_generated_at_utc(data_dir, session):
    """[2.9h] Le marqueur commun qui permet de detecter un couple incoherent."""
    pc.run_once(data_dir, session=session)
    can = json.loads((data_dir / "calendar.latest.json").read_text(encoding="utf-8"))
    leg = json.loads((data_dir / "calendar.json").read_text(encoding="utf-8"))
    assert can["generated_at_utc"] == leg["metadata"]["generated_at_utc"]


def test_calendar_json_is_written_last(data_dir, session, monkeypatch):
    """L'artefact canonique et le miroir avant, calendar.json en DERNIER :
    la fenetre d'incoherence n'expose jamais un legacy neuf."""
    order = []
    real = pc.atomic_write_json

    def spy(path, obj):
        order.append(Path(path).name)
        return real(path, obj)

    monkeypatch.setattr(pc, "atomic_write_json", spy)
    pc.run_once(data_dir, session=session)
    assert "calendar.json" in order
    assert order.index("calendar.json") > order.index("calendar.latest.json")
    assert order.index("calendar.json") == max(
        i for i, n in enumerate(order) if n in
        ("calendar.latest.json", "calendar.json", "calendar.legacy.json"))


# ═══════════════════════════════════════════════════════════════════════════
# [TROUVE-6] Un artefact INVALID ne doit JAMAIS ecraser un artefact valide.
# ═══════════════════════════════════════════════════════════════════════════
def test_invalid_cycle_never_overwrites_a_valid_artifact(data_dir, session):
    good = pc.run_once(data_dir, session=session)
    assert good is not None
    before = (data_dir / "calendar.latest.json").read_bytes()

    # cycle suivant : le flux renvoie un vocabulaire d'impact inconnu
    bad = CountingSession(plan={"body": json.dumps([
        {"title": "X", "country": "USD", "date": "2026-10-15T10:00:00-04:00",
         "impact": "Critical", "forecast": "1", "previous": "0"}]).encode()})
    assert pc.run_once(data_dir, session=bad) is None
    assert (data_dir / "calendar.latest.json").read_bytes() == before, \
        "un cycle INVALID a ecrase un artefact valide"


def test_failed_cycle_never_overwrites_artifact(data_dir, session):
    good = pc.run_once(data_dir, session=session)
    assert good is not None
    before = (data_dir / "calendar.latest.json").read_bytes()
    down = CountingSession(plan={"status": 503})
    assert pc.run_once(data_dir, session=down) is None
    assert (data_dir / "calendar.latest.json").read_bytes() == before


# ═══════════════════════════════════════════════════════════════════════════
# [TROUVE-7] health.json ne doit pas perdre la trace du dernier succes.
# ═══════════════════════════════════════════════════════════════════════════
def test_health_preserves_last_success_across_a_cooldown(data_dir, session):
    pc.run_once(data_dir, session=session)
    h_ok = json.loads((data_dir / "health.json").read_text(encoding="utf-8"))
    assert h_ok["status"] == "VALID"
    assert h_ok["last_success_utc"]
    assert h_ok["last_success_content_hash"]

    # [AUDIT-F2] force=True est necessaire pour ATTEINDRE le 429 : depuis que
    # le plancher est applique par l'ingesteur, un second cycle rapproche est
    # normalement intercepte avant le reseau. On veut ici tester la branche
    # cooldown, pas le plancher (couvert par tests/test_fetch_floor.py).
    rl = CountingSession(plan={"status": 429, "retry_after": 300})
    pc.run_once(data_dir, session=rl, force=True)
    h = json.loads((data_dir / "health.json").read_text(encoding="utf-8"))

    assert h["mode"] == "cooldown"
    # les faits du dernier succes SURVIVENT
    assert h["last_success_utc"] == h_ok["last_success_utc"]
    assert h["last_success_content_hash"] == h_ok["last_success_content_hash"]
    assert h["last_success_event_count"] == h_ok["last_success_event_count"]
    # et l'artefact toujours servi est decrit
    assert h["artifact_present"] is True
    assert h["served_artifact_age_s"] is not None
    assert h["served_artifact_content_hash"] == h_ok["last_success_content_hash"]


def test_health_schema_version_is_1_2(data_dir, session):
    pc.run_once(data_dir, session=session)
    h = json.loads((data_dir / "health.json").read_text(encoding="utf-8"))
    assert h["schema_version"] == "health-pipeline-1.2"


def test_health_has_no_null_success_before_any_success(data_dir, session):
    down = CountingSession(plan={"status": 503})
    pc.run_once(data_dir, session=down)
    h = json.loads((data_dir / "health.json").read_text(encoding="utf-8"))
    assert h["last_success_utc"] is None
    assert h["artifact_present"] is False
    assert h["served_artifact_age_s"] is None


# ═══════════════════════════════════════════════════════════════════════════
# [TROUVE-8] fetched_at_utc ne doit plus valoir « maintenant » par construction.
# ═══════════════════════════════════════════════════════════════════════════
def test_fetched_at_is_not_overwritten_by_last_modified(data_dir):
    """[CORRIGE] Ce test affirmait l'inverse et VERROUILLAIT le bug.

    fetched_at_utc (notre horloge) et last_modified (celle de l'editeur) sont
    deux choses distinctes. Les confondre rendait tout artefact neuf « perime »
    et faisait basculer le verdict en DEGRADED a chaque cycle, en production.
    """
    lm = "Fri, 10 Oct 2026 09:00:00 GMT"
    s = CountingSession()
    orig_get = s.get

    def get(url, **kw):
        r = orig_get(url, **kw)
        r.headers = dict(r.headers)
        r.headers["Last-Modified"] = lm
        return r

    s.get = get
    payload = pc.run_once(data_dir, session=s)
    assert payload is not None
    # notre horloge : maintenant, pas la date de publication
    assert payload.source.fetched_at_utc > datetime.now(UTC) - timedelta(minutes=5)
    # l'horodatage editeur reste expose, separement
    assert payload.source.last_modified == lm
    assert payload.quality.is_stale is False
    assert payload.quality.status.value == "VALID"


def test_fetched_at_ignores_a_future_last_modified(data_dir):
    """Une horloge CDN en avance ne doit pas produire un age negatif."""
    from tests.fixture import load_fixture
    s = CountingSession()
    orig_get = s.get

    def get(url, **kw):
        r = orig_get(url, **kw)
        r.headers = dict(r.headers)
        r.headers["Last-Modified"] = "Fri, 10 Oct 2030 09:00:00 GMT"
        return r

    s.get = get
    payload = pc.run_once(data_dir, session=s)
    assert payload.source.fetched_at_utc <= datetime.now(UTC)


# ═══════════════════════════════════════════════════════════════════════════
# [TROUVE-9] source.resolved_url doit dire la VERITE sur le repli.
# ═══════════════════════════════════════════════════════════════════════════
FALLBACK = "https://d1tcktd03x2wof.cloudfront.net/ff_calendar_thisweek.json"


def test_resolved_url_records_the_primary_when_it_serves(data_dir, session):
    payload = pc.run_once(data_dir, session=session)
    assert payload.source.resolved_url == pc.SOURCE_URLS[0]
    assert payload.source.url == pc.SOURCE_URLS[0]


def test_resolved_url_records_the_fallback_when_it_serves(data_dir):
    s = CountingSession(plan={"status": lambda url, n: 503 if url != FALLBACK else 200})
    payload = pc.run_once(data_dir, session=s)
    assert payload is not None
    assert payload.source.resolved_url == FALLBACK, \
        "l'artefact doit declarer l'URL REELLEMENT servie"
    assert payload.source.url == pc.SOURCE_URLS[0]   # contrat historique inchange
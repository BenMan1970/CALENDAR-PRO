"""POINT 2.2 — Cause racine des 429 : l'app interroge la source trop souvent.

Ces tests MESURENT le nombre de requetes avec une session mockee : aucune
requete reelle n'est emise.
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

import pipeline_calendar as pc
from tests.helpers import CountingSession, FakeClock, UTC

D = "https://d1tcktd03x2wof.cloudfront.net/ff_calendar_thisweek.json"


# ═══════════════════════════════════════════════════════════════════════════
# 2.2 a — espacement coherent avec max_source_age_seconds
# ═══════════════════════════════════════════════════════════════════════════
def test_spacing_is_at_least_source_age():
    assert pc.DEFAULT_MIN_FETCH_SPACING_S == 900
    assert pc.DEFAULT_MIN_FETCH_SPACING_S == pc.DEFAULT_POLICY.max_source_age_seconds
    assert pc.MIN_FETCH_SPACING_S >= 900


def test_single_source_of_truth_for_spacing():
    """L'espacement derive de la policy : une seule valeur a maintenir."""
    assert pc.DEFAULT_MIN_FETCH_SPACING_S == int(
        pc.DEFAULT_POLICY.max_source_age_seconds)


def test_floor_and_backoff_constants_are_sane():
    assert pc.FETCH_FLOOR_S > 0
    assert pc.BACKOFF_MIN_S >= 300, "le backoff minimal doit etre de 300 s"
    assert pc.BACKOFF_MAX_S == 3600, "le backoff doit etre plafonne a 3600 s"
    assert pc.BACKOFF_MIN_S <= pc.BACKOFF_MAX_S


# ═══════════════════════════════════════════════════════════════════════════
# 2.2 d — budget de requetes par cycle
# ═══════════════════════════════════════════════════════════════════════════
def test_one_cycle_makes_at_most_two_requests(session, data_dir):
    """Primaire OK -> UNE requete. Aucun retry urllib3, aucun quick retry."""
    payload = pc.run_once(data_dir, session=session)
    assert payload is not None
    assert session.n_calls == 1, session.calls


def test_fallback_costs_at_most_two_requests(session, data_dir):
    """Primaire en echec -> secondaire : DEUX requetes, pas davantage."""
    session.plan = {"status": lambda url, n: 503 if url != D else 200}
    payload = pc.run_once(data_dir, session=session)
    assert payload is not None
    assert session.n_calls == 2, session.calls
    assert session.calls[0] != session.calls[1]


def test_both_sources_down_is_bounded(session, data_dir):
    """Les deux sources HS : 2 requetes puis arret net (pas de boucle)."""
    session.plan = {"status": 503}
    assert pc.run_once(data_dir, session=session) is None
    assert session.n_calls == 2, session.calls


def test_session_has_no_5xx_retry():
    """La couche urllib3 ne doit plus rejouer les 5xx : sinon un cycle
    multiplierait les requetes contre une source qui rate-limite."""
    s = pc.build_session()
    adapter = s.get_adapter("https://nifs.example/")
    retry = adapter.max_retries
    assert retry.total == 0, f"total={retry.total}"
    assert 500 not in (retry.status_forcelist or []), retry.status_forcelist
    assert 429 not in (retry.status_forcelist or []), retry.status_forcelist
    s.close()


# ═══════════════════════════════════════════════════════════════════════════
# 2.2 b — le 429 pose un cooldown ; rien ne fetche pendant
# ═══════════════════════════════════════════════════════════════════════════
def test_429_sets_persistent_cooldown(session, data_dir):
    session.plan = {"status": 429, "retry_after": 300}
    assert pc.run_once(data_dir, session=session) is None
    assert pc.is_rate_limited(data_dir) is True
    rl = pc.read_rate_limit(data_dir)
    assert rl and rl["cooldown_s"] >= 300


def test_zero_requests_during_cooldown(session, data_dir):
    """Le point cle : pendant un cooldown, ZERO requete, meme sur un nouveau
    cycle, une nouvelle session et un nouvel appel."""
    session.plan = {"status": 429, "retry_after": 300}
    pc.run_once(data_dir, session=session)
    assert session.n_calls == 1

    fresh = CountingSession()
    for _ in range(5):
        assert pc.run_once(data_dir, session=fresh) is None
    assert fresh.n_calls == 0, fresh.calls


def test_cooldown_expires_and_fetch_resumes(session, data_dir):
    session.plan = {"status": 429, "retry_after": 300}
    pc.run_once(data_dir, session=session)
    rl = pc.read_rate_limit(data_dir)
    until = datetime.fromisoformat(rl["blocked_until_utc"].replace("Z", "+00:00"))
    assert pc.is_rate_limited(data_dir, now=until - timedelta(seconds=1)) is True
    assert pc.is_rate_limited(data_dir, now=until + timedelta(seconds=1)) is False


def test_nextweek_429_also_sets_cooldown(session, data_dir, monkeypatch):
    """[2.9d] Un 429 sur nextweek ne doit plus etre avale : le quota est
    global a la source."""
    monkeypatch.setattr(pc, "SOURCE_URL_NEXT",
                        "https://nfs.faireconomy.media/ff_calendar_nextweek.json")
    nexturl = pc.SOURCE_URL_NEXT
    session.plan = {"status": lambda url, n: 429 if url == nexturl else 200,
                    "retry_after": 300}
    out = pc.run_once(data_dir, session=session)
    assert out is None
    assert pc.is_rate_limited(data_dir) is True


# ═══════════════════════════════════════════════════════════════════════════
# 2.2 b — plancher dur : force=True ne contourne ni le cooldown ni le plancher
# ═══════════════════════════════════════════════════════════════════════════
def test_attempt_is_persisted(data_dir):
    assert pc.read_last_attempt(data_dir) is None
    pc.record_attempt(data_dir)
    stamp = pc.read_last_attempt(data_dir)
    assert stamp is not None
    assert pc.seconds_since_last_attempt(data_dir) < 5


def test_floor_blocks_a_forced_retry(data_dir):
    """Le double-clic sur « Relancer l'ingestion » ne rappelle pas la source."""
    now = datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC)
    pc.record_attempt(data_dir, now)
    assert pc.fetch_floor_remaining(data_dir, now) == pytest.approx(pc.FETCH_FLOOR_S)
    assert pc.fetch_floor_remaining(
        data_dir, now + timedelta(seconds=30)) == pytest.approx(pc.FETCH_FLOOR_S - 30)
    assert pc.fetch_floor_remaining(
        data_dir, now + timedelta(seconds=pc.FETCH_FLOOR_S)) == 0.0
    assert pc.fetch_floor_remaining(
        data_dir, now + timedelta(seconds=9999)) == 0.0


def test_floor_applies_after_a_failure_too(data_dir):
    """Un echec horodate la tentative : le plancher doit tenir."""
    now = datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC)
    pc.record_attempt(data_dir, now)
    assert pc.fetch_floor_remaining(data_dir, now + timedelta(seconds=1)) > 100


def test_floor_is_zero_when_never_attempted(data_dir):
    assert pc.fetch_floor_remaining(data_dir) == 0.0
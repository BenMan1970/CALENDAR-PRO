"""Suite offline de calendar_ingestor (audit calendrier 2026-09-11).

Avant cette vague : ZÉRO test sur tout l'I/O critique (fetch, fusion nextweek,
circuit breaker, last-known-good, écriture atomique, publish). Le réseau n'est
jamais touché : `fetch_source` est monkeypatché, `run_once` reçoit un tmp_path.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

import calendar_ingestor as ci
from calendar_core import QualityStatus, SelectionPolicy

UTC = timezone.utc


def _row(title: str, ccy: str, hours_ahead: float, impact: str = "High") -> dict:
    when = datetime.now(UTC) + timedelta(hours=hours_ahead)
    return {"title": title, "country": ccy, "date": when.isoformat(),
            "impact": impact, "forecast": "1.0", "previous": "1.1"}


def _meta() -> dict:
    return {"http_status": 200, "content_type": "application/json",
            "payload_bytes": 2, "payload_sha256": "sha256:test", "etag": None,
            "last_modified": None, "fetch_duration_ms": 1, "raw_bytes": b"[]"}


def _installer(monkeypatch, primary=None, next_fn=None):
    """Contrôle fin des deux flux ; renvoie la liste des URLs appelées."""
    calls: list[str] = []

    def fake(session, url):
        calls.append(url)
        if "nextweek" in url:
            if next_fn is None:
                raise ci.FetchError("HTTP_ERROR", "status=404")
            return next_fn(session, url)
        if callable(primary):
            return primary(session, url)
        if isinstance(primary, ci.FetchError):
            raise primary
        return (primary if primary is not None else [_row("ISM", "USD", 6)]), _meta()

    monkeypatch.setattr(ci, "fetch_source", fake)
    return calls


# ── Fusion thisweek + nextweek ───────────────────────────────────────────────
def test_nextweek_is_merged_when_available(tmp_path, monkeypatch):
    calls = _installer(monkeypatch,
                       primary=[_row("ISM", "USD", 6)],
                       next_fn=lambda s, u: ([_row("BOJ Rate Decision", "JPY", 120)], _meta()))
    payload = ci.run_once(tmp_path, SelectionPolicy(), None)
    assert payload is not None
    assert {"thisweek", "nextweek"} == set(calls_url_kinds(calls))
    names = {e.name for e in payload.events}
    assert {"ISM", "BOJ Rate Decision"} <= names          # fusion réelle
    assert payload.source.feed_status == {"thisweek": "ok", "nextweek": "ok"}
    assert payload.quality.status is QualityStatus.VALID


def calls_url_kinds(calls):
    return {"nextweek" if "nextweek" in u else "thisweek" for u in calls}


def test_nextweek_404_is_normal_not_an_incident(tmp_path, monkeypatch):
    """Le 404 nextweek en milieu de semaine est la condition NORMALE de la
    source : pas de warning permanent, pas d'échec du circuit breaker."""
    calls = _installer(monkeypatch, primary=[_row("ISM", "USD", 6)])
    payload = ci.run_once(tmp_path, SelectionPolicy(), None)
    assert payload is not None
    assert payload.source.feed_status["nextweek"] == "absent_404"
    assert not any("NEXTWEEK" in w for w in payload.quality.warnings)  # pas de bruit
    health = json.loads((tmp_path / "health.json").read_text(encoding="utf-8"))
    assert health["last_error"] is None                    # breaker intact


def test_nextweek_hard_error_recorded_but_breaker_intact(tmp_path, monkeypatch):
    calls = _installer(monkeypatch,
                       primary=[_row("ISM", "USD", 6)],
                       next_fn=lambda s, u: (_ for _ in ()).throw(
                           ci.FetchError("NETWORK_TIMEOUT", "read timed out")))
    payload = ci.run_once(tmp_path, SelectionPolicy(), None)
    assert payload is not None
    assert payload.source.feed_status["nextweek"] == "error:NETWORK_TIMEOUT"
    health = json.loads((tmp_path / "health.json").read_text(encoding="utf-8"))
    assert health["last_error"] is None                    # bonus muet ≠ panne primaire


# ── Last-known-good : recyclage borné ───────────────────────────────────────
def test_lkg_served_on_primary_failure_then_expired_is_refused(tmp_path, monkeypatch):
    # 1) run sain → publie + alimente le cache
    _installer(monkeypatch, primary=[_row("ISM", "USD", 6)])
    assert ci.run_once(tmp_path, SelectionPolicy(), None) is not None
    legacy_path = tmp_path / "calendar.json"
    first_bytes = legacy_path.read_bytes()

    # 2) primaire en panne → LKG resservi, DEGRADED mais publiable
    _installer(monkeypatch, primary=ci.FetchError("HTTP_ERROR", "status=503"))
    payload = ci.run_once(tmp_path, SelectionPolicy(), None)
    assert payload is not None and payload.source.from_last_known_good
    assert payload.quality.status is QualityStatus.DEGRADED
    served = json.loads(legacy_path.read_text(encoding="utf-8"))
    assert served["metadata"]["serving_mode"] == "last_known_good"

    # 3) on rajeunit facticement l'horodatage du cache à 49 h → plafond dur
    cache_path = tmp_path / "raw" / "last_known_good.json"
    cached = json.loads(cache_path.read_text(encoding="utf-8"))
    cached["fetched_at_utc"] = ci.iso_z(datetime.now(UTC) - timedelta(hours=49))
    cache_path.write_text(json.dumps(cached), encoding="utf-8")

    _installer(monkeypatch, primary=ci.FetchError("HTTP_ERROR", "status=503"))
    assert ci.run_once(tmp_path, SelectionPolicy(), None) is None
    assert legacy_path.read_bytes() == first_bytes or \
        json.loads(legacy_path.read_text(encoding="utf-8"))["metadata"]["serving_mode"] == "last_known_good"
    # L'artefact sur disque n'a PAS été rajeuni par un ressassement expiré :
    # son generated_at_utc reste celui du dernier publish légitime.
    on_disk = json.loads(legacy_path.read_text(encoding="utf-8"))
    assert on_disk["metadata"]["generated_at_utc"] == served["metadata"]["generated_at_utc"]


def test_no_data_no_lkg_returns_none(tmp_path, monkeypatch):
    _installer(monkeypatch, primary=ci.FetchError("NETWORK_ERROR", "dns down"))
    assert ci.run_once(tmp_path, SelectionPolicy(), None) is None
    assert not (tmp_path / "calendar.json").exists()


# ── Contrat legacy publié ────────────────────────────────────────────────────
def test_published_legacy_carries_honest_coverage(tmp_path, monkeypatch):
    _installer(monkeypatch,
               primary=[_row("ISM", "USD", 6), _row("ECB Decision", "EUR", 20)],
               next_fn=lambda s, u: ([_row("BOJ Rate Decision", "JPY", 150)], _meta()))
    ci.run_once(tmp_path, SelectionPolicy(), None)
    doc = json.loads((tmp_path / "calendar.json").read_text(encoding="utf-8"))
    meta, events = doc["metadata"], doc["events"]
    assert meta["schema_version"].startswith("legacy-1.2.0")
    # La couverture revendiquée est exactement celle du contenu : devises
    # observées ∪ sans-publication — jamais le plancher « 8 desk » hérité du null.
    fa_ccys = set(meta["filters_applied"]["currencies"])
    ev_ccys = {e["currency"] for e in events} - {"ALL"}
    assert ev_ccys <= fa_ccys
    assert fa_ccys == set(meta["currencies_covered"]) | set(meta["currencies_no_data_in_source"])
    assert meta["data_coverage_end_utc"] == max(e["datetime_utc"] for e in events)
    assert meta["ui_filters_applied"] is None
    assert meta["feeds_status"]["nextweek"] == "ok"


def test_atomic_writes_leave_no_temp_files(tmp_path, monkeypatch):
    _installer(monkeypatch, primary=[_row("ISM", "USD", 6)])
    ci.run_once(tmp_path, SelectionPolicy(), None)
    strays = [p for p in tmp_path.rglob(".*.tmp")]
    assert strays == []
    lock = tmp_path / ".publish.lock"
    assert not lock.exists()          # verrou libéré même en chemin de succès


# ── Hygiène H1-H4 (audit calendrier 2026-09-11, seconde vague) ──────────────
def test_h2_relative_data_dir_anchors_to_install_dir():
    import os
    from pathlib import Path as P
    anchored = ci._anchor_data_dir("data")
    assert anchored.is_absolute()
    assert str(anchored).startswith(str(P(ci.__file__).resolve().parent))
    explicit = ci._anchor_data_dir(os.path.join("C:\\", "srv", "bluestar"))
    assert str(explicit).replace("\\", "/").endswith("srv/bluestar")


def test_h1_raw_rotation_bounded(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    for i in range(25):
        (raw / f"raw_2026010{i:02d}T00000{i % 10}Z.json").write_text("[]", encoding="utf-8")
    lkg = raw / "last_known_good.json"
    lkg.write_text("{}", encoding="utf-8")
    removed = ci._rotate_raw(raw, keep=10)
    assert removed == 15
    assert lkg.exists()                                   # le cache n'est jamais une roture
    assert len(list(raw.glob("raw_*.json"))) == 10
    assert ci._rotate_raw(raw, keep=0) == 0               # 0 = purge désactivée


def test_h3_second_producer_is_shut_out(tmp_path):
    """Le verrou inter-processus doit rendre deux `run_once` concurrents
    sériels : le second échoue proprement (None, sans effet de bord)."""
    assert ci._acquire_publish_lock(tmp_path) is True
    assert ci._acquire_publish_lock(tmp_path) is False    # occupé, frais
    ci._release_publish_lock(tmp_path)
    assert ci._acquire_publish_lock(tmp_path) is True     # re-acquis après release
    ci._release_publish_lock(tmp_path)


def test_h3_stale_lock_is_stolen(tmp_path, monkeypatch):
    import os, time
    lock = tmp_path / ".publish.lock"
    lock.write_text("pid=999999 at=old", encoding="utf-8")
    old = time.time() - (ci.LOCK_STEAL_AFTER_S + 120)
    os.utime(lock, (old, old))
    assert ci._acquire_publish_lock(tmp_path) is True     # vol du verrou périmé
    ci._release_publish_lock(tmp_path)


def test_h3_release_only_ours(tmp_path):
    lock = tmp_path / ".publish.lock"
    lock.write_text("pid=424242 at=other", encoding="utf-8")   # verrou d'un TIERS
    ci._release_publish_lock(tmp_path)
    assert lock.exists()                              # on ne le supprime pas


def test_h3_run_once_skips_when_locked(tmp_path, monkeypatch):
    _installer(monkeypatch, primary=[_row("ISM", "USD", 6)])
    assert ci._acquire_publish_lock(tmp_path) is True
    assert ci.run_once(tmp_path, SelectionPolicy(), None) is None
    assert not (tmp_path / "calendar.json").exists()  # aucun effet de bord
    ci._release_publish_lock(tmp_path)
    assert ci.run_once(tmp_path, SelectionPolicy(), None) is not None


def test_h4_payload_hash_is_of_real_bytes():
    """Deux volets : (a) pour tout corps UTF-8 valide, l'ancien et le nouveau
    chemin donnent la MÊME valeur (aucune dérive de hash entre versions) ;
    (b) le code source utilise bien le hash des octets, plus le décodage
    destructif errors='replace' (verrou textuel du correctif H4)."""
    import hashlib
    from calendar_core import sha256_hex
    good = b'[{"title":"OK","impact":"High"}]'
    assert sha256_hex(good.decode("utf-8")) == hashlib.sha256(good).hexdigest()
    src = open(ci.__file__, encoding="utf-8").read()
    assert 'hashlib.sha256(body).hexdigest()' in src
    assert 'errors="replace"' not in src.split("payload_sha256")[1][:200]


def test_h7_dripping_connection_is_cut_by_deadline():
    """H7 : une connexion qui dégouline (timeout par opération jamais atteint,
    total infini) doit être coupée par le deadline global du fetch."""
    import time

    class _DripResponse:
        status_code = 200
        headers = {"Content-Type": "application/json"}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def iter_content(self, chunk_size=65536):
            while True:                     # 1 octet par « chunk », sans fin
                yield b" "

    class _DripSession:
        def get(self, url, timeout=None, stream=False):
            return _DripResponse()

    t0 = time.monotonic()
    with pytest.raises(ci.FetchError) as ei:
        ci.fetch_source(_DripSession(), "https://drip.invalid/feed.json", deadline_s=1.0)
    elapsed = time.monotonic() - t0
    assert "FETCH_DEADLINE_EXCEEDED" in str(ei.value)
    assert elapsed < 10.0                   # coupé sec, pas laissé courir

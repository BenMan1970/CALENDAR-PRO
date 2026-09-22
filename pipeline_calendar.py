#!/usr/bin/env python3
"""
BLUESTAR Pipeline Calendar — ingesteur
======================================
Fetche les news économiques depuis Fair Economy (Forex Factory) et produit
les artefacts JSON consommés par le pipeline de merge.

FIX 429 (cause racine de l'écran vide sur Streamlit Cloud) :
  - 429 retiré de ``status_forcelist`` → urllib3 ne réessaie PAS un quota
  - ``respect_retry_after_header=False`` → pas de sommeil de 247 s dans le thread
  - ``RateLimited`` est un événement de flux, pas une panne → breaker INTACT
  - cooldown persisté sur disque (rate_limit.json) entre deux tentatives

CORRECTIF v1.1 (2026-09-17) — LE COOLDOWN 429 N'ÉTAIT JAMAIS HONORÉ :
  ``apply_rate_limit`` écrivait la clé ``rate_limited_until_utc`` tandis que
  ``is_rate_limited`` lisait ``blocked_until_utc``. Le garde retournait donc
  toujours False et l'app retapait la source à chaque cycle, rallumant le
  quota qu'on croyait éteint. Les deux fonctions partagent maintenant une
  constante unique (``_RL_KEY``), l'ancienne clé restant écrite en alias
  pour tout lecteur externe. Ajouts : ``read_rate_limit`` (état exposé à
  l'UI), mode lecteur ``BLUESTAR_DISABLE_INGEST`` (B3), session HTTP
  jetable par cycle (B4), archivage des octets bruts dans ``raw/``.

Sources :
  primaire   : https://nfs.faireconomy.media/ff_calendar_thisweek.json
  secondaire : https://d1tcktd03x2wof.cloudfront.net/ff_calendar_thisweek.json
  nextweek   : https://nfs.faireconomy.media/ff_calendar_nextweek.json (404 normal)

Usage :
  python pipeline_calendar.py --once
  python pipeline_calendar.py --loop --interval 300
  python pipeline_calendar.py --once --data-dir /srv/data
  python pipeline_calendar.py --seed
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import threading
import time
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Le moteur de normalisation est réutilisé TEL QUEL : le format canonique et
# legacy est le contrat du pipeline de merge (parité de content_hash).
from calendar_core import (
    SCHEMA_VERSION,
    CalendarPayload,
    to_committee_view,
    DEFAULT_POLICY,
    QualityStatus,
    SelectionPolicy,
    SourceInfo,
    build_payload,
    to_legacy_payload,
    iso_z,
    sha256_hex,
    tz_environment,
)

UTC = timezone.utc
LOG = logging.getLogger("pipeline_calendar")

__all__ = [
    "SCHEMA_VERSION", "SOURCE_URLS", "MIN_FETCH_SPACING_S", "FetchError",
    "RateLimited", "build_session", "fetch_source", "run_once", "emit_seed",
    "is_rate_limited", "read_rate_limit", "apply_rate_limit", "clear_rate_limit",
    "ingest_disabled", "tz_environment", "main", "to_committee_view",
]

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
SOURCE_URLS: Tuple[str, ...] = (
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
    "https://d1tcktd03x2wof.cloudfront.net/ff_calendar_thisweek.json",
)
# [COMPAT-C10] nextweek OPT-IN (comme le macro, audit v6.1 [M2]) : mesuré ici
# même — health.json vendredi 18/09 14:32Z ET calendar.json dimanche 20/09
# 22:44Z portent « nextweek: absent_404 ». Un GET mort toutes les 300 s (~288/j)
# contre une source qui rate-limite est un risque sans contrepartie.
# Réactivation : BLUESTAR_SOURCE_URL_NEXT=https://nfs.faireconomy.media/ff_calendar_nextweek.json
SOURCE_URL_NEXT = os.getenv("BLUESTAR_SOURCE_URL_NEXT", "") or None

SOURCE_PROVIDER = "Forex Factory / Fair Economy weekly public feed"
USER_AGENT = os.getenv(
    "BLUESTAR_USER_AGENT",
    "BluestarPipelineCalendar/1.1 (+https://github.com/bluestar/calendar-pipeline)",
)


def _env_float(name: str, default: float) -> float:
    """Vide ou illisible == défaut (pas de crash à l'import sur Cloud)."""
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        LOG.warning("%s=%r illisible — repli sur %s", name, raw, default)
        return default


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        LOG.warning("%s=%r illisible — repli sur %s", name, raw, default)
        return default


def _env_flag(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


CONNECT_TIMEOUT = 5.0
READ_TIMEOUT = 15.0
FETCH_DEADLINE_S = _env_float("BLUESTAR_FETCH_DEADLINE_S", 90.0)
MAX_PAYLOAD_BYTES = 8 * 1024 * 1024
MIN_FETCH_SPACING_S = _env_int("BLUESTAR_MIN_FETCH_SPACING_S", 120)
RATE_LIMIT_COOLDOWN_S = _env_int("BLUESTAR_RATE_LIMIT_COOLDOWN_S", 600)
RATE_LIMIT_MAX_COOLDOWN_S = 3600
ARCHIVE_RAW = _env_flag("BLUESTAR_ARCHIVE_RAW")

# [B3] mode lecteur : l'UI sert les artefacts sur disque sans jamais toucher
# au réseau (utile derrière un ingesteur externe type cron/GitHub Action).
_DISABLE_INGEST_ENV = "BLUESTAR_DISABLE_INGEST"


def ingest_disabled() -> bool:
    return _env_flag(_DISABLE_INGEST_ENV)


# ─────────────────────────────────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────────────
class FetchError(RuntimeError):
    """Erreur réseau ou HTTP (hors 429)."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


class RateLimited(RuntimeError):
    """429 honoré sans dormir, sans retry, sans casser le breaker."""

    def __init__(self, retry_after: Optional[float] = None):
        super().__init__(f"RATE_LIMITED (retry_after={retry_after})")
        self.retry_after = retry_after


# ─────────────────────────────────────────────────────────────────────────────
# SESSION HTTP
# ─────────────────────────────────────────────────────────────────────────────
def build_session() -> requests.Session:
    """
    Session avec gestion correcte du taux de requête :
      • ``429`` hors de ``status_forcelist`` : un quota n'est pas une panne ;
      • ``respect_retry_after_header=False`` : aucun sommeil imposé au thread ;
      • retries (3) réservés aux erreurs transitives (5xx, 408, timeout).
    """
    session = requests.Session()
    retry = Retry(
        total=3, connect=3, read=3,
        backoff_factor=1.5, backoff_jitter=0.4,
        status_forcelist=[408, 500, 502, 503, 504],   # 429 EXCLU
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=False,             # [KEY FIX]
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=4)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain;q=0.8",
        "Accept-Encoding": "gzip, deflate",
    })
    return session


def parse_retry_after(value: Optional[str], now: datetime) -> Optional[float]:
    """RFC 9110 : delta-seconds OU HTTP-date. None si illisible."""
    if not value:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        pass
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S GMT"):
        try:
            dt = datetime.strptime(value, fmt)
            return max(0.0, (dt.replace(tzinfo=UTC) - now).total_seconds())
        except (ValueError, TypeError):
            continue
    return None


# ─────────────────────────────────────────────────────────────────────────────
# FETCH AVEC PLAFOND DE TEMPS
# ─────────────────────────────────────────────────────────────────────────────
def _fetch_blocking(session: requests.Session, url: str,
                    deadline_s: float) -> Tuple[Any, Dict[str, Any]]:
    started = time.monotonic()
    try:
        response = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), stream=True)
    except requests.Timeout as exc:
        raise FetchError("NETWORK_TIMEOUT", str(exc)) from exc
    except requests.RequestException as exc:
        raise FetchError("NETWORK_ERROR", str(exc)) from exc

    with response:
        status = response.status_code
        if status == 429:
            ra = parse_retry_after(response.headers.get("Retry-After"), datetime.now(UTC))
            raise RateLimited(ra if ra is not None else float(RATE_LIMIT_COOLDOWN_S))
        if status >= 400:
            raise FetchError("HTTP_ERROR", f"status={status}")

        chunks, size = [], 0
        for chunk in response.iter_content(chunk_size=65536):
            size += len(chunk)
            if size > MAX_PAYLOAD_BYTES:
                raise FetchError("PAYLOAD_TOO_LARGE", f"{size} bytes")
            if time.monotonic() - started > deadline_s:
                raise FetchError("FETCH_DEADLINE_EXCEEDED",
                                 f"{time.monotonic() - started:.0f}s > {deadline_s}s")
            chunks.append(chunk)
        body = b"".join(chunks)

        content_type = (response.headers.get("Content-Type") or "").split(";")[0].strip()
        meta = {
            "http_status": status,
            "content_type": content_type or None,
            "payload_bytes": size,
            "payload_sha256": "sha256:" + hashlib.sha256(body).hexdigest(),
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
            "fetch_duration_ms": int((time.monotonic() - started) * 1000),
            "raw_bytes": body,
        }

    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        head = body[:120].decode("utf-8", errors="replace")
        raise FetchError("INVALID_JSON", f"{exc} | head={head!r}") from exc

    if not isinstance(parsed, list):
        raise FetchError("SCHEMA_ROOT_NOT_ARRAY", f"got {type(parsed).__name__}")

    return parsed, meta


def _fetch_with_deadline(session: requests.Session, url: str,
                         deadline_s: float = FETCH_DEADLINE_S) -> Tuple[Any, Dict[str, Any]]:
    """
    Plafond dur au temps mural total. Le fetch bloquant tourne dans un thread
    daemon ; s'il dépasse le budget il est abandonné (le zombie meurt sur son
    propre timeout socket). Le cycle, lui, reste à l'heure.
    """
    outcome: Dict[str, Any] = {}

    def _worker():
        try:
            outcome["value"] = _fetch_blocking(session, url, deadline_s)
        except BaseException as exc:                          # noqa: BLE001
            outcome["error"] = exc

    th = threading.Thread(target=_worker, daemon=True, name="pipeline-fetch")
    started = time.monotonic()
    th.start()
    th.join(max(1.0, deadline_s))

    if th.is_alive():
        raise FetchError("FETCH_DEADLINE_EXCEEDED",
                         f">{deadline_s:.0f}s (worker abandonné à "
                         f"{time.monotonic() - started:.0f}s)")
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


def fetch_source(session: requests.Session) -> Tuple[List[Dict], Dict[str, Any], Dict[str, str]]:
    """
    Primaire → secondaire, puis nextweek en bonus.
    Retourne (raw_events, meta, feed_status). Un 429 remonte en RateLimited
    (sans retry, sans sommeil).
    """
    meta: Dict[str, Any] = {}
    feed_status: Dict[str, str] = {}
    feed_shas: Dict[str, str] = {}
    raw_bytes: Dict[str, bytes] = {}
    raw_list: Optional[List[Dict]] = None
    total_bytes = 0

    for i, url in enumerate(SOURCE_URLS):
        try:
            raw_list, meta = _fetch_with_deadline(session, url)
            feed_status["thisweek"] = "ok"
            feed_shas["thisweek"] = str(meta.get("payload_sha256"))
            raw_bytes["thisweek"] = meta.pop("raw_bytes", b"")
            total_bytes = int(meta.get("payload_bytes") or 0)
            break
        except RateLimited:
            raise                                             # quota = arrêt net
        except FetchError as exc:
            feed_status["thisweek"] = f"error:{exc.code}"
            LOG.warning("source #%d (%s) failed: %s", i + 1, url, exc)
            if i + 1 < len(SOURCE_URLS):
                continue
            raise

    if raw_list is None:
        raise FetchError("ALL_SOURCES_FAILED", "all primary URLs exhausted")

    if SOURCE_URL_NEXT:
        try:
            extra, extra_meta = _fetch_with_deadline(session, SOURCE_URL_NEXT)
            if isinstance(extra, list):
                raw_list = list(raw_list) + extra
                feed_status["nextweek"] = "ok"
                feed_shas["nextweek"] = str(extra_meta.get("payload_sha256"))
                raw_bytes["nextweek"] = extra_meta.pop("raw_bytes", b"")
                total_bytes += int(extra_meta.get("payload_bytes") or 0)
        except FetchError as exc:
            if exc.code == "HTTP_ERROR" and "status=404" in str(exc):
                feed_status["nextweek"] = "absent_404"
            else:
                feed_status["nextweek"] = f"error:{exc.code}"
                LOG.warning("nextweek fetch failed (bonus, non-blocking): %s", exc)
        except RateLimited:
            feed_status["nextweek"] = "rate_limited"
            LOG.warning("nextweek fetch rate-limited (bonus, non-blocking)")

    ordered = [feed_shas[k] for k in ("thisweek", "nextweek") if k in feed_shas]
    meta["payload_sha256"] = ("sha256:" + sha256_hex("|".join(ordered))
                              if ordered else "sha256:unknown")
    meta["payload_bytes"] = total_bytes
    meta["feed_status"] = dict(feed_status)
    meta["feed_sha256"] = dict(feed_shas)
    meta["raw_by_feed"] = raw_bytes
    return raw_list, meta, feed_status


# ─────────────────────────────────────────────────────────────────────────────
# ÉCRITURE ATOMIQUE
# ─────────────────────────────────────────────────────────────────────────────
def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def atomic_write_json(path: Path, obj: Any) -> None:
    atomic_write_bytes(path, json.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8"))


def read_json(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# COOLDOWN 429 (persisté sur disque)
# ─────────────────────────────────────────────────────────────────────────────
# [CORRECTIF v1.1] UNE seule clé, partagée par l'écrivain et le lecteur.
_RL_KEY = "blocked_until_utc"


def _rate_limit_path(data_dir: Path) -> Path:
    return data_dir / "rate_limit.json"


def read_rate_limit(data_dir: Path) -> Optional[Dict[str, Any]]:
    """État de cooldown exposé à l'UI (None si aucun fichier)."""
    rl = read_json(_rate_limit_path(data_dir))
    return rl if isinstance(rl, dict) else None


def is_rate_limited(data_dir: Path, now: Optional[datetime] = None) -> bool:
    """True si le cooldown 429 court encore."""
    if now is None:
        now = datetime.now(UTC)
    rl = read_rate_limit(data_dir)
    if not rl:
        return False
    # Alias historique toléré en lecture (fichiers écrits par la v1.0).
    stamp = rl.get(_RL_KEY) or rl.get("rate_limited_until_utc")
    if not stamp:
        return False
    try:
        until = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return False
    if until.tzinfo is None:
        until = until.replace(tzinfo=UTC)
    return now < until


def apply_rate_limit(data_dir: Path, retry_after: Optional[float]) -> None:
    """Persiste le cooldown 429."""
    if retry_after is None:
        retry_after = float(RATE_LIMIT_COOLDOWN_S)
    cooldown = max(float(RATE_LIMIT_COOLDOWN_S),
                   min(float(retry_after), float(RATE_LIMIT_MAX_COOLDOWN_S)))
    until = datetime.now(UTC) + timedelta(seconds=cooldown)
    atomic_write_json(_rate_limit_path(data_dir), {
        _RL_KEY: iso_z(until),
        "rate_limited_until_utc": iso_z(until),   # alias rétro-compatible
        "retry_after": retry_after,
        "cooldown_s": cooldown,
        "applied_at_utc": iso_z(datetime.now(UTC)),
    })
    LOG.warning("rate-limited — cooldown %ds until %s", int(cooldown), iso_z(until))


def clear_rate_limit(data_dir: Path) -> None:
    try:
        _rate_limit_path(data_dir).unlink()
    except OSError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# CYCLE D'INGESTION
# ─────────────────────────────────────────────────────────────────────────────
def write_health(data_dir: Path, now: datetime, payload: Optional[CalendarPayload],
                 error: Optional[str], *, rate_limited: bool = False,
                 mode: str = "live") -> None:
    status = payload.quality.status.value if payload else "UNAVAILABLE"
    atomic_write_json(data_dir / "health.json", {
        "schema_version": "health-pipeline-1.1",
        "core_schema_version": SCHEMA_VERSION,
        "status": status,
        "mode": mode,
        "checked_at_utc": iso_z(now),
        "error": error,
        "rate_limited": rate_limited,
        "rate_limit": read_rate_limit(data_dir) if rate_limited else None,
        "event_count": len(payload.events) if payload else 0,
        "data_quality_score": payload.quality.data_quality_score if payload else 0.0,
        "content_hash": payload.content_hash if payload else None,
        # [COMPAT-C14] Le hash comparable au macro. C'est LUI qu'on regarde
        # pour répondre à « les deux apps voient-elles le même calendrier ? ».
        "parity_hash": payload.parity_hash if payload else None,
        "parity_window_anchor_utc": (payload.parity_window_anchor_utc
                                     if payload else None),
        "warnings": list(payload.quality.warnings) if payload else [],
        "feeds_status": dict(payload.source.feed_status) if payload else {},
        "tz": tz_environment(),
    })


def _archive_raw(data_dir: Path, raw_by_feed: Dict[str, bytes], now: datetime) -> None:
    """Archive les octets bruts par flux (traçabilité de la FUSION)."""
    if not ARCHIVE_RAW or not raw_by_feed:
        return
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    for feed, blob in raw_by_feed.items():
        if blob:
            atomic_write_bytes(data_dir / "raw" / f"{feed}.{stamp}.json", blob)


def run_once(data_dir: Path, session: Optional[requests.Session] = None,
             policy: Optional[SelectionPolicy] = None) -> Optional[CalendarPayload]:
    """
    Cycle unique : fetch → normalize → écriture des artefacts.

      • ``calendar.latest.json`` — canonique v2 (riche)
      • ``calendar.json``        — legacy v1 (consommé par le merge)
      • ``calendar.legacy.json`` — copie de secours
      • ``health.json``          — supervision

    Un 429 pose un cooldown persisté, ne casse pas le breaker et n'écrase
    JAMAIS l'artefact précédent (le LKG sur disque continue d'être servi).

    ``session=None`` → session HTTP jetable créée et fermée dans le cycle [B4].
    """
    policy = policy or DEFAULT_POLICY
    now = datetime.now(UTC)
    data_dir.mkdir(parents=True, exist_ok=True)

    if ingest_disabled():
        LOG.info("ingestion disabled (%s) — reader mode", _DISABLE_INGEST_ENV)
        write_health(data_dir, now, None, "INGEST_DISABLED", mode="reader")
        return None

    if is_rate_limited(data_dir, now):
        LOG.warning("rate-limit cooldown active — serving existing artifact")
        write_health(data_dir, now, None, "RATE_LIMIT_COOLDOWN",
                     rate_limited=True, mode="cooldown")
        return None

    owns_session = session is None
    session = session or build_session()

    raw_list: Optional[List[Dict]] = None
    meta: Dict[str, Any] = {}
    feed_status: Dict[str, str] = {}
    error: Optional[str] = None

    try:
        with closing(session) if owns_session else _nullcontext():
            raw_list, meta, feed_status = fetch_source(session)
    except RateLimited as exc:
        apply_rate_limit(data_dir, exc.retry_after)
        write_health(data_dir, now, None, str(exc), rate_limited=True, mode="cooldown")
        LOG.warning("rate-limited — previous artifact untouched")
        return None
    except FetchError as exc:
        error = str(exc)
        LOG.error("fetch failed: %s", error)

    if raw_list is None:
        write_health(data_dir, now, None, error or "NO_DATA")
        return None

    _archive_raw(data_dir, meta.pop("raw_by_feed", {}) or {}, now)

    source = SourceInfo(
        provider=SOURCE_PROVIDER,
        url=str(SOURCE_URLS[0]),
        fetched_at_utc=now,
        fetch_duration_ms=int(meta.get("fetch_duration_ms") or 0),
        http_status=meta.get("http_status"),
        content_type=meta.get("content_type"),
        payload_bytes=int(meta.get("payload_bytes") or 0),
        payload_sha256=str(meta.get("payload_sha256") or "sha256:unknown"),
        etag=meta.get("etag"),
        last_modified=meta.get("last_modified"),
        supports_actual=any(isinstance(r, dict) and "actual" in r for r in raw_list),
        from_last_known_good=False,
        feed_status=dict(feed_status),
        feed_sha256=dict(meta.get("feed_sha256") or {}),
    )

    try:
        payload = build_payload(raw_list, source=source, now_utc=now, policy=policy)
    except Exception as exc:                                  # noqa: BLE001
        LOG.exception("normalization failed")
        write_health(data_dir, now, None, f"NORMALIZATION_FAILED: {exc}")
        return None

    if payload.quality.status is QualityStatus.INVALID:
        error = f"QUALITY_INVALID: {list(payload.quality.warnings)}"
        LOG.error("payload rejected (INVALID) — previous artifact untouched")
        write_health(data_dir, now, payload, error)
        return None

    canonical = payload.model_dump(mode="json")
    legacy = to_legacy_payload(payload, now)

    atomic_write_json(data_dir / "calendar.latest.json", canonical)
    atomic_write_json(data_dir / "calendar.json", legacy)
    atomic_write_json(data_dir / "calendar.legacy.json", legacy)
    # [COMPAT-C15] Artefact consommé par l'app committee. Écrit APRÈS les
    # artefacts historiques : un échec ici ne doit jamais empêcher la
    # publication du calendrier lui-même.
    try:
        atomic_write_json(data_dir / "calendar.committee.json",
                          to_committee_view(payload, now, legacy))
    except Exception:                                         # noqa: BLE001
        LOG.exception("committee view not written (non-blocking)")
    write_health(data_dir, now, payload, None)
    clear_rate_limit(data_dir)

    LOG.info("published %d events | status=%s | score=%.2f | hash=%s | parity=%s",
             len(payload.events), payload.quality.status.value,
             payload.quality.data_quality_score,
             (payload.content_hash or "").split(":")[-1][:12],
             (payload.parity_hash or "").split(":")[-1][:12] or "n/a")
    return payload


class _nullcontext:
    def __enter__(self):
        return None

    def __exit__(self, *exc):
        return False


def emit_seed(data_dir: Path, session: Optional[requests.Session] = None) -> Optional[Path]:
    """Produit seed/calendar.latest.seed.json pour le cold-start Cloud."""
    seed_dir = data_dir / "seed"
    seed_dir.mkdir(parents=True, exist_ok=True)
    payload = run_once(data_dir, session)
    if payload is None:
        LOG.error("seed not written (no valid payload)")
        return None
    seed_path = seed_dir / "calendar.latest.seed.json"
    atomic_write_json(seed_path, payload.model_dump(mode="json"))
    LOG.info("seed written to %s", seed_path)
    return seed_path


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="BLUESTAR Pipeline Calendar — economic news ingestor")
    parser.add_argument("--once", action="store_true", help="un seul cycle puis sortie")
    parser.add_argument("--loop", action="store_true", help="boucle continue")
    parser.add_argument("--interval", type=int, default=300, help="secondes entre cycles")
    parser.add_argument("--data-dir", type=Path, default=None, help="répertoire de sortie")
    parser.add_argument("--seed", action="store_true", help="génère le seed de cold-start")
    parser.add_argument("--clear-cooldown", action="store_true",
                        help="supprime rate_limit.json puis sort")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    LOG.info("tz environment: %s", json.dumps(tz_environment(), default=str))

    data_dir = args.data_dir or Path(__file__).resolve().parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    if args.clear_cooldown:
        clear_rate_limit(data_dir)
        LOG.info("cooldown cleared")
        return 0

    if args.seed:
        return 0 if emit_seed(data_dir) else 1

    if args.loop:
        import signal

        def _signal(signum, _frame):
            LOG.info("signal %s — shutting down", signum)
            raise SystemExit(0)

        signal.signal(signal.SIGINT, _signal)
        signal.signal(signal.SIGTERM, _signal)

        LOG.info("entering loop mode, interval=%ds", args.interval)
        while True:
            try:
                run_once(data_dir)
            except Exception:                                 # noqa: BLE001
                LOG.exception("unhandled error in cycle")
            time.sleep(max(5, args.interval))

    result = run_once(data_dir)
    if result is not None:
        return 0
    # [B3] exit 3 = « rien publié mais rien de cassé » (cooldown/mode lecteur),
    # distinct de exit 2 (échec réel) pour ne pas alerter un orchestrateur.
    if ingest_disabled() or is_rate_limited(data_dir):
        return 3
    return 2


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
BLUESTAR Pipeline Calendar — version simplifiée
=============================================
Fetche les news économiques depuis Fair Economy (Forex Factory) et produit
un .json riche consommé par le pipeline de merge.

FIX 429 (cause racine de l'écran vide sur Streamlit Cloud) :
  - 429 retiré de ``status_forcelist`` → urllib3 ne réessaie PAS un quota
  - ``respect_retry_after_header=False`` → pas de sommeil de 247s dans le thread
  - ``RateLimited`` est un événement de flux, pas une panne → breaker INTACT
  - Cooldown minimal persé sur disque (rate_limit.json) entre deux tentatives

Sources :
  primaire  : https://nfs.faireconomy.media/ff_calendar_thisweek.json
  secondaire: https://d1tcktd03x2wof.cloudfront.net/ff_calendar_thisweek.json
  nextweek  : https://nfs.faireconomy.media/ff_calendar_nextweek.json  (bonus, 404 normal)

Usage :
  python pipeline_calendar.py --once
  python pipeline_calendar.py --loop --interval 300
  python pipeline_calendar.py --once --data-dir /srv/data
  python pipeline_calendar.py --once --output /tmp/calendar.json
  python pipeline_calendar.py --seed   # écrit seed/ pour le cold-start Streamlit
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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Le moteur de normalisation est réutilisé tel quel (calendar_core.py) :
# le format JSON canonique et legacy est IMPACTÉ par le pipeline de merge.
from calendar_core import (
    SCHEMA_VERSION,
    CalendarPayload,
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

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
SOURCE_URLS: Tuple[str, ...] = (
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
    "https://d1tcktd03x2wof.cloudfront.net/ff_calendar_thisweek.json",
)
SOURCE_URL_NEXT = os.getenv(
    "BLUESTAR_SOURCE_URL_NEXT",
    "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
)

SOURCE_PROVIDER = "Forex Factory / Fair Economy weekly public feed"
USER_AGENT = os.getenv(
    "BLUESTAR_USER_AGENT",
    "BluestarPipelineCalendar/1.0 (+https://github.com/bluestar/calendar-pipeline)",
)

def _env_float(name: str, default: float) -> float:
    """Vide ou illisible == défaut (pas de crash à l'import sur Streamlit Cloud)."""
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


CONNECT_TIMEOUT = 5.0
READ_TIMEOUT = 15.0
FETCH_DEADLINE_S = _env_float("BLUESTAR_FETCH_DEADLINE_S", 90.0)
MAX_PAYLOAD_BYTES = 8 * 1024 * 1024
MIN_FETCH_SPACING_S = _env_int("BLUESTAR_MIN_FETCH_SPACING_S", 120)
RATE_LIMIT_COOLDOWN_S = 600   # 10 min après un 429
RATE_LIMIT_MAX_COOLDOWN_S = 3600  # plafond 1h


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
# SESSION HTTP (fix 429)
# ─────────────────────────────────────────────────────────────────────────────
def build_session() -> requests.Session:
    """
    [FIX 429] Session avec gestion correcte du taux de requête :

    • ``429`` retiré de ``status_forcelist`` : un quota n'est PAS une panne réseau.
    • ``respect_retry_after_header=False`` : urllib3 ne dort PLUS la durée du
      Retry-After (247s mesuré) dans le thread de fetch.
    • Retries (3) pour erreurs transitives uniquement (5xx, 408, timeout).
    """
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.5,
        backoff_jitter=0.4,
        status_forcelist=[408, 500, 502, 503, 504],  # 429 EXCLU
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=False,  # [KEY FIX]
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
    """RFC 9110 : delta-seconds OU HTTP-date. Retourne None si illisible."""
    if not value:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        pass
    try:
        dt = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %Z")
        return max(0.0, (dt.replace(tzinfo=UTC) - now).total_seconds())
    except (ValueError, TypeError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# FETCH AVEC PLAFOND DE TEMPS (H7)
# ─────────────────────────────────────────────────────────────────────────────
def _fetch_blocking(session: requests.Session, url: str, deadline_s: float) -> Tuple[Any, Dict[str, Any]]:
    started = time.monotonic()
    try:
        response = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), stream=True)
    except requests.Timeout as exc:
        raise FetchError("NETWORK_TIMEOUT", str(exc)) from exc
    except requests.RequestException as exc:
        raise FetchError("NETWORK_ERROR", str(exc)) from exc

    with response:
        status = response.status_code

        # [FIX 429] Détection manuelle du 429
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
    [H7] Plafond dur au temps mural total. Le fetch bloquant est exécuté dans
    un thread daemon ; s'il dépasse le budget, le thread est abandonné (zombie
    meurt à l'EOF ou au timeout interne). Le cycle, lui, est à l'heure.
    """
    outcome: Dict[str, Any] = {}

    def _worker():
        try:
            outcome["value"] = _fetch_blocking(session, url, deadline_s)
        except BaseException as exc:  # noqa: BLE001
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
    Tente les URLs primaire → secondaire, puis nextweek en bonus.
    Retourne (raw_events, meta, feed_status).

    Le 429 est propage via RateLimited (pas de retry, pas de sommeil).
    """
    meta: Dict[str, Any] = {}
    feed_status: Dict[str, str] = {}
    feed_shas: Dict[str, str] = {}
    raw_list: Optional[List[Dict]] = None
    total_bytes = 0

    # --- Primaire ---
    for i, url in enumerate(SOURCE_URLS):
        try:
            raw_list, meta = _fetch_with_deadline(session, url)
            feed_status["thisweek"] = "ok"
            feed_shas["thisweek"] = str(meta.get("payload_sha256"))
            total_bytes = int(meta.get("payload_bytes") or 0)
            break
        except RateLimited:
            raise  # le quota est un arrêt immédiat
        except FetchError as exc:
            feed_status["thisweek"] = f"error:{exc.code}"
            LOG.warning("source #%d (%s) failed: %s", i + 1, url, exc)
            if i + 1 < len(SOURCE_URLS):
                continue
            raise

    if raw_list is None:
        raise FetchError("ALL_SOURCES_FAILED", "all primary URLs exhausted")

    # --- Next week (bonus, jamais bloquant) ---
    if SOURCE_URL_NEXT:
        try:
            extra, extra_meta = _fetch_with_deadline(session, SOURCE_URL_NEXT)
            if isinstance(extra, list):
                raw_list = list(raw_list) + extra
                feed_status["nextweek"] = "ok"
                feed_shas["nextweek"] = str(extra_meta.get("payload_sha256"))
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

    ordered_shas = [feed_shas[k] for k in ("thisweek", "nextweek") if k in feed_shas]
    meta["payload_sha256"] = "sha256:" + sha256_hex("|".join(ordered_shas)) if ordered_shas else "sha256:unknown"
    meta["payload_bytes"] = total_bytes
    meta["feed_status"] = dict(feed_status)
    meta["feed_sha256"] = dict(feed_shas)
    return raw_list, meta, feed_status


# ─────────────────────────────────────────────────────────────────────────────
# ÉCRITURE ATOMIQUE
# ─────────────────────────────────────────────────────────────────────────────
def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.tmp"
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def atomic_write_json(path: Path, obj: Any) -> None:
    data = json.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8")
    atomic_write_bytes(path, data)


def read_json(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# GESTION DU COOLDOWN 429 (persisté sur disque)
# ─────────────────────────────────────────────────────────────────────────────
def _rate_limit_path(data_dir: Path) -> Path:
    return data_dir / "rate_limit.json"


def is_rate_limited(data_dir: Path, now: Optional[datetime] = None) -> bool:
    """True si on est encore sous le cooldown 429."""
    if now is None:
        now = datetime.now(UTC)
    rl = read_json(_rate_limit_path(data_dir))
    if not rl:
        return False
    blocked_until = rl.get("blocked_until_utc")
    if not blocked_until:
        return False
    try:
        until = datetime.fromisoformat(blocked_until.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return False
    return now < until


def apply_rate_limit(data_dir: Path, retry_after: Optional[float]) -> None:
    """Persiste le cooldown 429 sur disque."""
    if retry_after is None:
        retry_after = float(RATE_LIMIT_COOLDOWN_S)
    cooldown = max(RATE_LIMIT_COOLDOWN_S, min(float(retry_after), RATE_LIMIT_MAX_COOLDOWN_S))
    until = datetime.now(UTC) + timedelta(seconds=cooldown)
    atomic_write_json(_rate_limit_path(data_dir), {
        "rate_limited_until_utc": iso_z(until),
        "retry_after": retry_after,
        "cooldown_s": cooldown,
    })
    LOG.warning("rate-limited — cooldown %ds until %s", int(cooldown), iso_z(until))


# ─────────────────────────────────────────────────────────────────────────────
# CYCLE D'INGESTION
# ─────────────────────────────────────────────────────────────────────────────
def write_health(data_dir: Path, state: Dict[str, Any], now: datetime,
                 payload: Optional[CalendarPayload], error: Optional[str],
                 rate_limited: bool = False) -> None:
    status = payload.quality.status.value if payload else "UNAVAILABLE"
    atomic_write_json(data_dir / "health.json", {
        "schema_version": "health-pipeline-1.0",
        "core_schema_version": SCHEMA_VERSION,
        "status": status,
        "checked_at_utc": iso_z(now),
        "error": error,
        "rate_limited": rate_limited,
        "event_count": len(payload.events) if payload else 0,
        "data_quality_score": payload.quality.data_quality_score if payload else 0.0,
        "content_hash": payload.content_hash if payload else None,
        "warnings": list(payload.quality.warnings) if payload else [],
        "feeds_status": dict(payload.source.feed_status) if payload else {},
        "tz": tz_environment(),
    })


def run_once(data_dir: Path, session: requests.Session,
             policy: Optional[SelectionPolicy] = None) -> Optional[CalendarPayload]:
    """
    Cycle unique : fetch → normalize → write 3 fichiers.

    • ``calendar.latest.json``  — format canonique v2 (rich)
    • ``calendar.json``         — format legacy v1 (consommé par le merge)
    • ``health.json``           — supervision

    [FIX 429] Un 429 active un cooldown persé, ne casse pas le breaker,
    ne rentre pas en échec. Le LKG (dernier fichier sur disque) reste servi.
    """
    if policy is None:
        policy = DEFAULT_POLICY
    now = datetime.now(UTC)
    data_dir.mkdir(parents=True, exist_ok=True)

    # Cooldown 429 ?
    if is_rate_limited(data_dir):
        LOG.warning("rate-limit cooldown active — serving existing file")
        write_health(data_dir, {}, now, None, "RATE_LIMIT_COOLDOWN", rate_limited=True)
        return None

    raw_list: Optional[List[Dict]] = None
    meta: Dict[str, Any] = {}
    feed_status: Dict[str, str] = {}
    error: Optional[str] = None
    rate_limited = False

    try:
        raw_list, meta, feed_status = fetch_source(session)
    except RateLimited as exc:
        rate_limited = True
        error = str(exc)
        apply_rate_limit(data_dir, exc.retry_after)
        write_health(data_dir, {}, now, None, error, rate_limited=True)
        # Le fichier précédent reste sur disque — pas d'écrasement
        LOG.warning("rate-limited — previous artifact untouched")
        return None
    except FetchError as exc:
        error = str(exc)
        LOG.error("fetch failed: %s", error)

    if raw_list is None:
        state = {"consecutive_failures": 1}
        write_health(data_dir, state, now, None, error)
        return None

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
    except Exception as exc:  # noqa: BLE001
        error = f"NORMALIZATION_FAILED: {exc}"
        LOG.exception("normalization failed")
        write_health(data_dir, {}, now, None, error)
        return None

    if payload.quality.status is QualityStatus.INVALID:
        error = f"QUALITY_INVALID: {list(payload.quality.warnings)}"
        LOG.error("payload rejected (INVALID)")
        write_health(data_dir, {}, now, payload, error)
        return None

    # Écriture atomique des 3 artefacts
    canonical = payload.model_dump(mode="json")
    legacy = to_legacy_payload(payload, now)

    atomic_write_json(data_dir / "calendar.latest.json", canonical)
    atomic_write_json(data_dir / "calendar.json", legacy)
    atomic_write_json(data_dir / "calendar.legacy.json", legacy)  # backup identique

    write_health(data_dir, {}, now, payload, None)

    LOG.info(
        "published %d events | status=%s | score=%.2f | hash=%s",
        len(payload.events),
        payload.quality.status.value,
        payload.quality.data_quality_score,
        (payload.content_hash or "").split(":")[-1][:12],
    )
    return payload


def emit_seed(data_dir: Path, session: Optional[requests.Session] = None) -> Optional[Path]:
    """Produit seed/calendar.latest.seed.json pour le cold-start Streamlit Cloud."""
    if session is None:
        session = build_session()
    seed_dir = data_dir / "seed"
    seed_dir.mkdir(parents=True, exist_ok=True)
    payload = run_once(data_dir, session)
    if payload is None:
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
        description="BLUESTAR Pipeline Calendar — economic news ingestor"
    )
    parser.add_argument("--once", action="store_true", help="un seul cycle puis sortie")
    parser.add_argument("--loop", action="store_true", help="boucle continue")
    parser.add_argument("--interval", type=int, default=300, help="secondes entre cycles")
    parser.add_argument("--data-dir", type=Path, default=None, help="répertoire de sortie")
    parser.add_argument("--seed", action="store_true", help="génère le seed pour cold-start")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    # Logging JSON structuré, UTC
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )

    data_dir = args.data_dir or Path(__file__).resolve().parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    if args.seed:
        path = emit_seed(data_dir)
        return 0 if path else 1

    policy = DEFAULT_POLICY
    session = build_session()

    if args.loop:
        import signal

        def _signal(signum, frame):
            LOG.info("signal %s — shutting down", signum)
            raise SystemExit(0)

        signal.signal(signal.SIGINT, _signal)
        signal.signal(signal.SIGTERM, _signal)

        LOG.info("entering loop mode, interval=%ds", args.interval)
        while True:
            try:
                run_once(data_dir, session, policy)
            except Exception:  # noqa: BLE001
                LOG.exception("unhandled error in cycle")
            time.sleep(max(5, args.interval))
        return 0

    # --once (défaut)
    result = run_once(data_dir, session, policy)
    return 0 if result else 2


if __name__ == "__main__":
    sys.exit(main())

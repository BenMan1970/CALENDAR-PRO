"""
BLUESTAR · calendar_ingestor
============================
Producteur autonome de l'artefact canonique. Ne dépend PAS de Streamlit.
À exécuter en cron / systemd timer / Airflow, ou en boucle interne.

    python calendar_ingestor.py --once
    python calendar_ingestor.py --loop --interval 300
    python calendar_ingestor.py --once --data-dir /srv/bluestar/data

Sorties (écriture atomique) :
    data/calendar.latest.json    artefact canonique v2 (SCHEMA_VERSION, cf. calendar_core)
    data/calendar.legacy.json    forme v1 corrigée, pont de migration
    data/calendar.json           alias identique à calendar.legacy.json (nom attendu par l'app merge)
    data/health.json             contrat de supervision minimal
    data/_state.json             circuit breaker + last-known-good
    data/raw/raw_<ts>.json       payload brut horodaté
    data/history/<ts>_<hash>.json historique versionné
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from calendar_core import (
    SCHEMA_VERSION,
    CalendarPayload,
    QualityStatus,
    SelectionPolicy,
    SourceInfo,
    build_payload,
    iso_z,
    sha256_hex,
    to_legacy_payload,
)

UTC = timezone.utc
LOG = logging.getLogger("bluestar.ingestor")

SOURCE_URL = os.getenv(
    "BLUESTAR_SOURCE_URL", "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
)
# HARNESS-CAL (audit calendrier 2026-09-11, F-1) : le flux « this week » ne
# peut pas satisfaire un horizon de 168 h dès jeudi (données qui s'arrêtent à
# dimanche). Le flux « next week » est publié par Fair Economy en fin de
# semaine : un 404 en milieu de semaine est la condition NORMALE de la source,
# ni un incident, ni un signal à crier à chaque cycle. On le fetch en BONUS
# fusionné ; son échec ne compte PAS dans le circuit breaker (qui protège le
# flux primaire, lui). BLUESTAR_SOURCE_URL_NEXT="" désactive le bonus.
SOURCE_URL_NEXT = os.getenv(
    "BLUESTAR_SOURCE_URL_NEXT",
    SOURCE_URL.replace("thisweek", "nextweek") if "thisweek" in SOURCE_URL else "",
)
SOURCE_PROVIDER = "Forex Factory / Fair Economy weekly public feed"
USER_AGENT = os.getenv("BLUESTAR_USER_AGENT", "BluestarCalendarIngestor/2.0 (+ops@bluestar)")


def _anchor_data_dir(value: str) -> Path:
    """H2 (audit 2026-09-11) : « data » relatif était résolu contre le CWD —
    cron (cwd=/) et Streamlit (cwd=app) produisaient silencieusement DEUX jeux
    d'artefacts parallèles. Un chemin par défaut relatif s'ancre désormais sur
    le dossier d'installation ; un `--data-dir` explicite reste relatif au cwd
    (choix humain, intentionnel)."""
    p = Path(value)
    return p if p.is_absolute() else Path(__file__).resolve().parent / p


DATA_DIR = _anchor_data_dir(os.getenv("BLUESTAR_DATA_DIR", "data"))
RAW_KEEP = int(os.getenv("BLUESTAR_RAW_KEEP", "200"))   # H1 : rotation raw/
LOCK_STEAL_AFTER_S = int(os.getenv("BLUESTAR_LOCK_STEAL_AFTER", "600"))

CONNECT_TIMEOUT = 5.0
READ_TIMEOUT = 15.0
# H7 (le « deadline global » de la doctrine GPS, transplanté) : les timeouts
# par opération (5 s/15 s) ne bornent PAS le total — une connexion qui
# dégouline quelques octets toutes les <15 s peut faire durer un fetch
# indéfiniment, et un producteur qui boucle toutes les 5 min avec un cycle
# gelé est un producteur mort sans health.json. Chaque fetch est désormais
# borné au temps TOTAL, contrôlé entre les chunks.
FETCH_DEADLINE_S = float(os.getenv("BLUESTAR_FETCH_DEADLINE_S", "90"))
MAX_PAYLOAD_BYTES = 8 * 1024 * 1024
CB_FAILURE_THRESHOLD = 3
CB_RESET_SECONDS = 300


# ─────────────────────────────────────────────────────────────────────────────
# I/O ATOMIQUE
# ─────────────────────────────────────────────────────────────────────────────
def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.tmp"
    try:
        with open(tmp, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        if hasattr(os, "O_DIRECTORY"):
            try:
                fd = os.open(str(path.parent), os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            except OSError:
                pass
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def atomic_write_json(path: Path, obj: Any) -> bytes:
    data = json.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8")
    atomic_write_bytes(path, data)
    return data


def read_json(path: Path) -> Optional[Any]:
    try:
        return read_json_text(path)
    except (OSError, json.JSONDecodeError):
        return None


def read_json_text(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


# ─────────────────────────────────────────────────────────────────────────────
# VERROU INTER-PROCESSUS DU PRODUCTEUR (H3, audit 2026-09-11)
# cron et Streamlit peuvent courir en parallèle sur le même DATA_DIR ; le
# verrou RuntimeControl de l'app est process-local et ne protège rien entre
# processus. Verrou par création exclusive (sans dépendance), volé s'il est
# périmé (producteur crashé). Placé dans run_once : TOUT producteur qui passe
# par run_once est couvert, y compris l'app Streamlit qui l'importe.
# ─────────────────────────────────────────────────────────────────────────────
class ProducerLockBusy(RuntimeError):
    pass


def _lock_path(data_dir: Path) -> Path:
    return data_dir / ".publish.lock"


def _acquire_publish_lock(data_dir: Path) -> bool:
    lp = _lock_path(data_dir)
    lp.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(2):                                  # 2 essais : rattrapage périmé
        try:
            fd = os.open(str(lp), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                age = time.time() - lp.stat().st_mtime
            except OSError:
                continue                                # libéré entre-temps
            if age > LOCK_STEAL_AFTER_S:
                LOG.warning("verrou périmé (%.0fs > %ds) — volé", age, LOCK_STEAL_AFTER_S)
                try:
                    lp.unlink()
                except OSError:
                    pass
                continue
            return False
        else:
            with os.fdopen(fd, "wb") as fh:
                fh.write(f"pid={os.getpid()} at={iso_z(datetime.now(UTC))}".encode("utf-8"))
            return True
    return False


def _release_publish_lock(data_dir: Path) -> None:
    lp = _lock_path(data_dir)
    try:
        # Ne supprimer QUE notre verrou (un autre l'aurait peut-être volé).
        content = lp.read_text(encoding="utf-8")
        if f"pid={os.getpid()}" in content:
            lp.unlink()
    except OSError:
        pass


def _rotate_raw(raw_dir: Path, keep: int) -> int:
    """H1 : raw/raw_<ts>.json s'accumulait SANS plafond (~12 Mo/jour en cycle
    300 s, jamais purgé). On garde les `keep` plus récents (0 = désactivé) ;
    last_known_good.json n'est jamais concerné (nom hors motif)."""
    if keep <= 0:
        return 0
    files = sorted(raw_dir.glob("raw_*.json"))
    removed = 0
    for old in files[:-keep]:
        try:
            old.unlink()
            removed += 1
        except OSError:
            pass
    return removed


# ─────────────────────────────────────────────────────────────────────────────
# ÉTAT PERSISTANT (circuit breaker + last-known-good)
# ─────────────────────────────────────────────────────────────────────────────
class IngestorState:
    def __init__(self, path: Path):
        self.path = path
        raw = read_json(path) or {}
        self.consecutive_failures: int = int(raw.get("consecutive_failures", 0))
        self.circuit_state: str = raw.get("circuit_state", "CLOSED")
        self.opened_at: Optional[str] = raw.get("opened_at")
        self.last_successful_fetch_utc: Optional[str] = raw.get("last_successful_fetch_utc")
        self.last_publish_utc: Optional[str] = raw.get("last_publish_utc")
        self.last_content_hash: Optional[str] = raw.get("last_content_hash")
        self.last_error: Optional[str] = raw.get("last_error")
        self.etag: Optional[str] = raw.get("etag")
        self.last_modified: Optional[str] = raw.get("last_modified")

    def save(self) -> None:
        atomic_write_json(self.path, {
            "consecutive_failures": self.consecutive_failures,
            "circuit_state": self.circuit_state,
            "opened_at": self.opened_at,
            "last_successful_fetch_utc": self.last_successful_fetch_utc,
            "last_publish_utc": self.last_publish_utc,
            "last_content_hash": self.last_content_hash,
            "last_error": self.last_error,
            "etag": self.etag,
            "last_modified": self.last_modified,
        })

    def allow_request(self, now: datetime) -> bool:
        if self.circuit_state != "OPEN":
            return True
        if not self.opened_at:
            self.circuit_state = "HALF_OPEN"
            return True
        opened = datetime.fromisoformat(self.opened_at.replace("Z", "+00:00"))
        if (now - opened).total_seconds() >= CB_RESET_SECONDS:
            self.circuit_state = "HALF_OPEN"
            LOG.warning("circuit breaker -> HALF_OPEN")
            return True
        return False

    def record_success(self, now: datetime) -> None:
        self.consecutive_failures = 0
        self.circuit_state = "CLOSED"
        self.opened_at = None
        self.last_error = None
        self.last_successful_fetch_utc = iso_z(now)

    def record_failure(self, now: datetime, error: str) -> None:
        self.consecutive_failures += 1
        self.last_error = error
        if self.consecutive_failures >= CB_FAILURE_THRESHOLD:
            if self.circuit_state != "OPEN":
                LOG.error("circuit breaker -> OPEN after %d failures", self.consecutive_failures)
            self.circuit_state = "OPEN"
            self.opened_at = iso_z(now)


# ─────────────────────────────────────────────────────────────────────────────
# FETCH RÉSILIENT
# ─────────────────────────────────────────────────────────────────────────────
def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1.5,
        backoff_jitter=0.4,
        status_forcelist=(408, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
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


class FetchError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def fetch_source(session: requests.Session, url: str,
                 deadline_s: float = None) -> Tuple[Any, Dict[str, Any]]:
    """H7 : plafond DUR au temps mural total, par worker daemon + join.

    Les timeouts par opération (connect 5 s / read 15 s) ne bornent pas le
    total : une connexion qui dégouline (36 octets/s mesurés en réel sur ce
    CDN sous requêtes répétées) peut tenir un fetch vivant des minutes —
    vérifié : 304 s pour un budget de 90 s, y compris en fermant la socket
    depuis un timer (close() cross-thread ne débloque pas un read Windows
    mis en tampon). Seul coupe-circuit fiable : exécuter le fetch bloquant
    dans un thread daemon et l'ABANDONNER si le budget est échu. Le thread
    zombie meurt de lui-même à l'EOF ou au premier timeout interne de socket
    (≤ quelques minutes) ; le cycle, lui, est à l'heure.
    """
    if deadline_s is None:
        deadline_s = FETCH_DEADLINE_S
    outcome: Dict[str, Any] = {}

    def _worker() -> None:
        try:
            outcome["value"] = _fetch_source_blocking(session, url, deadline_s)
        except BaseException as exc:                          # noqa: BLE001
            outcome["error"] = exc

    th = threading.Thread(target=_worker, daemon=True, name="bluestar-fetch")
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


def _fetch_source_blocking(session: requests.Session, url: str,
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
        if status >= 400:
            raise FetchError("HTTP_ERROR", f"status={status}")

        # Watchdog H7-bis : la vérification inter-chunks est AVEUGLE au drip —
        # iter_content(65536) ne rend la main qu'après avoir emmagasiné 64 Ko ;
        # une connexion qui dégouline 36 octets/s sur un payload de 11 Ko peut
        # tenir le cycle hors limite pendant des minutes (mesuré : 304 s pour
        # un budget de 90 s). Le seul coupe-circuit honnête est de FERMER la
        # socket depuis un timer quand le budget est échu : le recv bloqué lève
        # alors immédiatement.
        # Garde bon marché entre chunks (nécessaire mais non suffisant : le
        # plafond dur, lui, est le join() du wrapper fetch_source).
        chunks, size = [], 0
        for chunk in response.iter_content(chunk_size=65536):
            size += len(chunk)
            if size > MAX_PAYLOAD_BYTES:
                raise FetchError("PAYLOAD_TOO_LARGE", f"{size} bytes")
            if time.monotonic() - started > deadline_s:
                raise FetchError("FETCH_DEADLINE_EXCEEDED",
                                 f"{time.monotonic() - started:.0f}s > {deadline_s:.0f}s")
            chunks.append(chunk)
        body = b"".join(chunks)

        content_type = (response.headers.get("Content-Type") or "").split(";")[0].strip()
        meta = {
            "http_status": status,
            "content_type": content_type or None,
            "payload_bytes": size,
            # H4 : hash des OCTETS RÉCELS. L'ancien chemin hachait le décodage
            # errors="replace" — un corps invalide en UTF-8 produisait un hash
            # « destructif » qui ne correspondait plus à rien d'observable.
            # Pour tout corps UTF-8 valide (cas normal), valeur STRICTEMENT
            # identique : aucun dérivé de hash entre versions n'existe ici.
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


# ─────────────────────────────────────────────────────────────────────────────
# CYCLE D'INGESTION
# ─────────────────────────────────────────────────────────────────────────────
def write_health(data_dir: Path, state: IngestorState, now: datetime,
                 payload: Optional[CalendarPayload], error: Optional[str]) -> None:
    if payload is None:
        status = "UNAVAILABLE"
    else:
        status = payload.quality.status.value

    atomic_write_json(data_dir / "health.json", {
        "schema_version": "health-1.0.0",
        "core_schema_version": SCHEMA_VERSION,
        "status": status,
        "checked_at_utc": iso_z(now),
        "circuit_breaker_state": state.circuit_state,
        "consecutive_failures": state.consecutive_failures,
        "last_successful_fetch_utc": state.last_successful_fetch_utc,
        "last_publish_utc": state.last_publish_utc,
        "content_hash": state.last_content_hash,
        "source_age_seconds": payload.quality.source_age_seconds if payload else None,
        "is_stale": payload.quality.is_stale if payload else True,
        "stale_data_alert": bool(payload.quality.is_stale) if payload else True,
        "using_last_known_good": bool(payload.source.from_last_known_good) if payload else False,
        "event_count": len(payload.events) if payload else 0,
        "data_quality_score": payload.quality.data_quality_score if payload else 0.0,
        "warnings": list(payload.quality.warnings) if payload else [],
        "last_error": error or state.last_error,
    })


def run_once(data_dir: Path, policy: SelectionPolicy, session: requests.Session,
             keep_history: int = 200) -> Optional[CalendarPayload]:
    """Cycle unique sous verrou inter-processus (H3). Si l'autre producteur
    (cron ou Streamlit) tient déjà le verrou, le cycle est Sauté sans effet
    de bord — pas d'échec compté, pas d'écriture : le prochain tick reprendra."""
    if not _acquire_publish_lock(data_dir):
        LOG.warning("un autre producteur détient le verrou d'édition — cycle sauté")
        return None
    try:
        return _run_once_impl(data_dir, policy, session, keep_history)
    finally:
        _release_publish_lock(data_dir)


def _run_once_impl(data_dir: Path, policy: SelectionPolicy, session: requests.Session,
                   keep_history: int = 200) -> Optional[CalendarPayload]:
    now = datetime.now(UTC)
    state = IngestorState(data_dir / "_state.json")
    lkg_path = data_dir / "raw" / "last_known_good.json"

    raw_list: Any = None
    meta: Dict[str, Any] = {}
    feed_status: Dict[str, str] = {}
    from_lkg = False
    error: Optional[str] = None

    if not state.allow_request(now):
        error = "CIRCUIT_OPEN"
        feed_status["thisweek"] = "circuit_open"
        LOG.warning("circuit breaker OPEN - skipping remote fetch")
    else:
        try:
            raw_list, meta = fetch_source(session, SOURCE_URL)
            feed_status["thisweek"] = "ok"
            state.record_success(now)
            state.etag = meta.get("etag")
            state.last_modified = meta.get("last_modified")
            if SOURCE_URL_NEXT:
                try:
                    extra_rows, extra_meta = fetch_source(session, SOURCE_URL_NEXT)
                    if isinstance(extra_rows, list):
                        raw_list = list(raw_list) + extra_rows
                        feed_status["nextweek"] = "ok"
                    else:
                        feed_status["nextweek"] = "schema_root_not_array"
                except FetchError as exc_next:
                    code_next = getattr(exc_next, "code", "ERR")
                    if code_next == "HTTP_ERROR" and "status=404" in str(exc_next):
                        feed_status["nextweek"] = "absent_404"
                        LOG.info("nextweek not published yet (normal mid-week)")
                    else:
                        feed_status["nextweek"] = f"error:{code_next}"
                        LOG.warning("nextweek fetch failed (bonus feed, breaker intact): %s", exc_next)
            raw_bytes = meta.pop("raw_bytes")
            meta["feed_status"] = dict(feed_status)
            atomic_write_bytes(data_dir / "raw" / f"raw_{now.strftime('%Y%m%dT%H%M%SZ')}.json",
                               raw_bytes)
            # Le cache est écrit APRÈS la fusion : un resservi LKG rejoue exactly
            # le même contenu (events + statuts de flux) que l'artefact publié.
            atomic_write_bytes(lkg_path, json.dumps({
                "fetched_at_utc": iso_z(now),
                "meta": {k: v for k, v in meta.items()},
                "payload": raw_list,
            }, ensure_ascii=False).encode("utf-8"))
            _rotate_raw(data_dir / "raw", RAW_KEEP)
        except FetchError as exc:
            error = str(exc)
            feed_status.setdefault("thisweek", f"error:{getattr(exc, 'code', 'ERR')}")
            state.record_failure(now, error)
            LOG.error("fetch failed: %s", error)
        except OSError as exc:
            # H3 : verrou volé entre le stat et l'O_EXCL par un tiers, ou disque
            # plein sur le cache — un cycle doit mourir proprement, pas tuer la
            # boucle. Compté comme échec (le breaker protège le réseau, pas le
            # disque, mais le health.json doit refléter la panne).
            error = f"LOCAL_IO_ERROR: {exc}"
            state.record_failure(now, error)
            LOG.exception("local I/O failure during fetch/cache")

    if raw_list is None:
        cached = read_json(lkg_path)
        if cached and isinstance(cached.get("payload"), list):
            raw_list = cached["payload"]
            meta = dict(cached.get("meta") or {})
            meta.pop("raw_bytes", None)
            fs = dict(meta.get("feed_status") or {})
            fs["served_from"] = "last_known_good"
            meta["feed_status"] = fs
            fetched_at = datetime.fromisoformat(
                str(cached.get("fetched_at_utc", iso_z(now))).replace("Z", "+00:00"))
            from_lkg = True
            LOG.warning("serving last-known-good from %s", iso_z(fetched_at))
        else:
            state.save()
            write_health(data_dir, state, now, None, error)
            LOG.critical("no data available and no last-known-good on disk")
            return None
    else:
        fetched_at = now

    source = SourceInfo(
        provider=SOURCE_PROVIDER,
        url=SOURCE_URL,
        fetched_at_utc=fetched_at,
        fetch_duration_ms=int(meta.get("fetch_duration_ms") or 0),
        http_status=meta.get("http_status"),
        content_type=meta.get("content_type"),
        payload_bytes=int(meta.get("payload_bytes") or 0),
        payload_sha256=str(meta.get("payload_sha256") or "sha256:unknown"),
        etag=meta.get("etag"),
        last_modified=meta.get("last_modified"),
        supports_actual=any(isinstance(r, dict) and "actual" in r for r in raw_list),
        from_last_known_good=from_lkg,
        feed_status=dict(meta.get("feed_status") or {}),
    )

    try:
        payload = build_payload(raw_list, source=source, now_utc=now, policy=policy)
    except Exception as exc:                                   # noqa: BLE001
        error = f"NORMALIZATION_FAILED: {exc}"
        state.record_failure(now, error)
        state.save()
        write_health(data_dir, state, now, None, error)
        LOG.exception("normalization failed - previous artifact left untouched")
        return None

    if payload.quality.status is QualityStatus.INVALID:
        error = f"QUALITY_INVALID: {list(payload.quality.warnings)}"
        state.save()
        write_health(data_dir, state, now, payload, error)
        LOG.error("payload rejected (INVALID) - previous artifact left untouched")
        return None

    canonical = payload.model_dump(mode="json")
    legacy = to_legacy_payload(payload, now)
    atomic_write_json(data_dir / "calendar.latest.json", canonical)
    atomic_write_json(data_dir / "calendar.legacy.json", legacy)
    atomic_write_json(data_dir / "calendar.json", legacy)  # alias : nom attendu par l'app merge

    short_hash = (payload.content_hash or "sha256:unknown").split(":")[-1][:12]
    atomic_write_json(
        data_dir / "history" / f"{now.strftime('%Y%m%dT%H%M%SZ')}_{short_hash}.json", canonical
    )

    history_dir = data_dir / "history"
    files = sorted(history_dir.glob("*.json"))
    for old in files[:-keep_history]:
        try:
            old.unlink()
        except OSError:
            pass

    changed = payload.content_hash != state.last_content_hash
    state.last_content_hash = payload.content_hash
    state.last_publish_utc = iso_z(now)
    state.save()
    write_health(data_dir, state, now, payload, error)

    LOG.info(
        "published %d events | status=%s | score=%.2f | content_%s | hash=%s",
        len(payload.events), payload.quality.status.value,
        payload.quality.data_quality_score,
        "CHANGED" if changed else "unchanged", short_hash,
    )
    return payload


_STOP = False


def _handle_signal(signum, _frame):
    global _STOP
    _STOP = True
    LOG.info("signal %s received - shutting down after current cycle", signum)


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="BLUESTAR calendar ingestor")
    parser.add_argument("--once", action="store_true", help="un seul cycle puis sortie")
    parser.add_argument("--loop", action="store_true", help="boucle continue")
    parser.add_argument("--interval", type=int, default=300, help="secondes entre cycles")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--display-tz", default=os.getenv("BLUESTAR_DISPLAY_TZ",
                                                          "Africa/Casablanca"))
    parser.add_argument("--impact", nargs="*", default=["HIGH", "MEDIUM"],
                        help="niveaux retenus (HIGH MEDIUM LOW HOLIDAY)")
    parser.add_argument("--log-level", default=os.getenv("BLUESTAR_LOG_LEVEL", "INFO"))
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s",'
               '"msg":"%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )

    policy = SelectionPolicy(
        impact_levels=tuple(i.upper() for i in args.impact),
        display_timezone=args.display_tz,
    )
    session = build_session()
    args.data_dir.mkdir(parents=True, exist_ok=True)

    if not args.loop or args.once:
        return 0 if run_once(args.data_dir, policy, session) else 2

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    while not _STOP:
        cycle_start = time.monotonic()
        try:
            run_once(args.data_dir, policy, session)
        except Exception:                                       # noqa: BLE001
            LOG.exception("unhandled error in ingest cycle")
        sleep_for = max(5.0, args.interval - (time.monotonic() - cycle_start))
        slept = 0.0
        while slept < sleep_for and not _STOP:
            time.sleep(min(1.0, sleep_for - slept))
            slept += 1.0
    return 0


if __name__ == "__main__":
    sys.exit(main())

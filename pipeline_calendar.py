#!/usr/bin/env python3
"""
BLUESTAR Pipeline Calendar — ingesteur
=====================================
Fetche les news économiques depuis Fair Economy (Forex Factory) et produit
les artefacts JSON consommés par le pipeline de merge.

Sources :
  primaire   : https://nfs.faireconomy.media/ff_calendar_thisweek.json
  secondaire : https://d1tcktd03x2wof.cloudfront.net/ff_calendar_thisweek.json
  nextweek   : https://nfs.faireconomy.media/ff_calendar_nextweek.json (404 normal)

Politique 429 (cause racine de l'écran vide sur Streamlit Cloud) :
  - 429 retiré de ``status_forcelist`` → urllib3 ne réessaie PAS un quota ;
  - ``respect_retry_after_header=False`` → pas de sommeil de 247 s dans le
    thread ;
  - ``RateLimited`` est un événement de flux, pas une panne : breaker INTACT,
    cooldown persisté sur disque (rate_limit.json) entre deux tentatives.

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
from contextlib import closing, nullcontext
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
# [COMPAT-C10] nextweek OPT-IN : la source a régulièrement renvoyé
# « absent_404 » — un GET mort toutes les 300 s contre une source qui
# rate-limite est un risque sans contrepartie.
# Activer : BLUESTAR_SOURCE_URL_NEXT=https://nfs.faireconomy.media/ff_calendar_nextweek.json
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

# ─────────────────────────────────────────────────────────────────────────────
# CADENCE DE FETCH — une SEULE source de vérité
# ─────────────────────────────────────────────────────────────────────────────
# [2.2a] L'espacement minimal entre deux TENTATIVES est l'invariant central qui
# protège du 429. Il valait 120 s alors que le flux est HEBDOMADAIRE et que
# ``DEFAULT_POLICY.max_source_age_seconds`` vaut 900 : l'artefact était déclaré
# « à rafraîchir » au bout de 2 min, et comme l'ouverture de page, le fragment
# live_strip (20 s) et le veilleur (3 s) appellent tous ``kick()``, un simple
# onglet laissé ouvert refetchait la source toutes les ~2 minutes.
#
# RELATION À max_source_age_seconds — les deux valeurs DOIVENT rester égales :
#   • ``max_source_age_seconds`` dit « au-delà, l'artefact est périmé » ;
#   • ``MIN_FETCH_SPACING_S`` dit « en deçà, ne pas rappeler la source ».
# Si l'espacement était plus COURT que la fraîcheur exigée, l'app se
# contredirait : elle réclamerait un artefact frais tout en sachant qu'elle
# vient d'interroger la source. S'il était plus LONG, on servirait un artefact
# jugé périmé sans même essayer de le rafraîchir. D'où une valeur unique
# DÉRIVÉE de la policy (jamais saisie deux fois), surchargeable par
# BLUESTAR_MIN_FETCH_SPACING_S — mais jamais en dessous du plancher dur.
DEFAULT_MIN_FETCH_SPACING_S = int(DEFAULT_POLICY.max_source_age_seconds)   # 900

# [2.2b] PLANCHER DUR : même ``force=True`` ne peut pas rappeler la source avant
# ce délai depuis la dernière TENTATIVE (pas seulement le dernier succès). Un
# double-clic, deux sessions simultanées ou un rafraîchissement compulsif ne
# peuvent donc pas marteler la source. Persisté sur disque (last_attempt.json)
# pour tenir entre sessions, threads et redémarrages de conteneur.
DEFAULT_FETCH_FLOOR_S = 120

# [2.2c] Backoff après échec : minimum 300 s, plafond 3600 s. Un échec
# DÉTERMINISTE (QUALITY_INVALID, NORMALIZATION_FAILED) ne doit pas être rejoué
# à l'identique toutes les 2 minutes indéfiniment.
DEFAULT_BACKOFF_MIN_S = 300
BACKOFF_MAX_S = 3600

MIN_FETCH_SPACING_S = _env_int("BLUESTAR_MIN_FETCH_SPACING_S",
                               DEFAULT_MIN_FETCH_SPACING_S)
FETCH_FLOOR_S = _env_int("BLUESTAR_FETCH_FLOOR_S", DEFAULT_FETCH_FLOOR_S)
BACKOFF_MIN_S = _env_int("BLUESTAR_BACKOFF_MIN_S", DEFAULT_BACKOFF_MIN_S)
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
      • AUCUN retry urllib3 par défaut.

    [2.2d] Le flux est un fichier statique servi par un CDN : un 5xx est déjà
    rejoué par la cascade primaire -> secondaire du cycle, et une seconde
    couche de retries ne faisait que multiplier les requêtes contre une source
    qui rate-limite. Avec ``total=3`` et ``status_forcelist`` sur les 5xx, un
    cycle pouvait émettre jusqu'à 2 URLs × (1 + 3) = 8 requêtes, avant même le
    « quick retry » de l'UI. On borne donc à UNE requête par URL et par cycle
    (``total=0``), ce qui rend le budget vérifiable : au plus 2 requêtes.

    Surchargeable par BLUESTAR_HTTP_RETRIES pour l'exploitation.
    """
    session = requests.Session()
    retries = max(0, _env_int("BLUESTAR_HTTP_RETRIES", 0))
    retry = Retry(
        total=retries, connect=retries, read=retries,
        backoff_factor=0.5, backoff_jitter=0.2,
        status_forcelist=(),                          # 5xx/408/429 : AUCUN retry
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
        # [AUDIT-F1] ``except BaseException`` capturait aussi KeyboardInterrupt
        # et SystemExit, qui etaient ensuite RE-LEVES dans le thread principal
        # (``raise outcome["error"]``). Un signal recu pendant le fetch tuait
        # donc le cycle hors de tout ``except FetchError``/``RateLimited`` :
        # run_once ne rattrape que ces deux classes, l'exception remontait
        # jusqu'au CLI. On capture ``Exception`` (les erreurs reseau et de
        # parsing), et on laisse les signaux suivre leur cours normal.
        try:
            outcome["value"] = _fetch_blocking(session, url, deadline_s)
        except Exception as exc:                              # noqa: BLE001
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
        error = outcome["error"]
        # [AUDIT-F1b] Une exception non prevue ne doit pas remonter brute :
        # run_once ne rattrape que FetchError/RateLimited, donc elle ferait
        # echouer le cycle SANS que l'artefact existant soit preserve ni que
        # health.json soit ecrit. On l'enveloppe pour que le cycle se termine
        # proprement (artefact intact, raison tracee).
        if not isinstance(error, (FetchError, RateLimited)):
            raise FetchError("UNEXPECTED_FETCH_ERROR",
                             f"{type(error).__name__}: {error}") from error
        raise error
    if "value" not in outcome:
        # [AUDIT-F1c] Ni resultat ni erreur : le worker a ete interrompu par un
        # signal (KeyboardInterrupt/SystemExit), qui n'est plus capture depuis
        # F1. Sans ce garde-fou, ``outcome["value"]`` levait un KeyError nu,
        # absurde a diagnostiquer et hors du contrat FetchError.
        raise FetchError("FETCH_INTERRUPTED",
                         "worker interrompu avant de rendre un resultat")
    return outcome["value"]


def fetch_source(session: requests.Session) -> Tuple[List[Dict], Dict[str, Any], Dict[str, str]]:
    """
    Primaire → secondaire, puis nextweek en bonus.
    Retourne (raw_events, meta, feed_status). Un 429 remonte en RateLimited
    (sans retry, sans sommeil).

    [2.2d] Au plus UNE requête par URL et par cycle : la cascade primaire ->
    secondaire est la seule forme de reprise, il n'y a plus de couche urllib3.
    Le budget d'un cycle est donc borné à 2 requêtes (primaire puis
    secondaire), ou 3 si ``BLUESTAR_SOURCE_URL_NEXT`` est activé.

    [2.9c] ``meta['resolved_url']`` porte l'URL RÉELLEMENT servie : le repli
    CloudFront ne doit plus être maquillé en source primaire.

    [2.9d] Un 429 sur n'importe quel flux pose le cooldown : sur ``nextweek``,
    il remonte donc en ``RateLimited`` au lieu d'être avalé comme un « bonus ».
    Un quota est global à la source, pas par endpoint.
    """
    meta: Dict[str, Any] = {}
    feed_status: Dict[str, str] = {}
    feed_shas: Dict[str, str] = {}
    raw_bytes: Dict[str, bytes] = {}
    raw_list: Optional[List[Dict]] = None
    total_bytes = 0
    resolved_url: Optional[str] = None

    for i, url in enumerate(SOURCE_URLS):
        try:
            raw_list, meta = _fetch_with_deadline(session, url)
            feed_status["thisweek"] = "ok"
            feed_shas["thisweek"] = str(meta.get("payload_sha256"))
            raw_bytes["thisweek"] = meta.pop("raw_bytes", b"")
            total_bytes = int(meta.get("payload_bytes") or 0)
            resolved_url = url                    # [2.9c] URL réellement servie
            if i > 0:
                LOG.warning("primary source unavailable — served by fallback #%d (%s)",
                            i + 1, url)
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
            # [2.9d] Le quota est global à la source : on ne l'avale plus.
            feed_status["nextweek"] = "rate_limited"
            LOG.warning("nextweek rate-limited — cooldown applies to the whole source")
            raise

    ordered = [feed_shas[k] for k in ("thisweek", "nextweek") if k in feed_shas]
    meta["payload_sha256"] = ("sha256:" + sha256_hex("|".join(ordered))
                              if ordered else "sha256:unknown")
    meta["payload_bytes"] = total_bytes
    meta["feed_status"] = dict(feed_status)
    meta["feed_sha256"] = dict(feed_shas)
    meta["raw_by_feed"] = raw_bytes
    meta["resolved_url"] = resolved_url or str(SOURCE_URLS[0])
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
# Une seule clé, partagée par l'écrivain et le lecteur (v1.1).
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
# DERNIÈRE TENTATIVE (persistée) — plancher de cadence entre sessions
# ─────────────────────────────────────────────────────────────────────────────
# [2.2b] Le plancher dur doit tenir au-delà du processus : l'UI Streamlit
# réinstancie son superviseur, Streamlit Cloud redémarre le conteneur, et deux
# sessions naviguent en parallèle. Un simple attribut en mémoire ne suffit donc
# pas — la date de la dernière TENTATIVE (succès OU échec) est écrite sur
# disque, dans le même répertoire que rate_limit.json.
_LAST_ATTEMPT_KEY = "last_attempt_utc"


def _last_attempt_path(data_dir: Path) -> Path:
    return data_dir / "last_attempt.json"


def read_last_attempt(data_dir: Path) -> Optional[datetime]:
    """Instant de la dernière tentative de fetch, None si inconnu. ``None``
    signifie « jamais tenté » -> le plancher ne s'applique pas."""
    rec = read_json(_last_attempt_path(data_dir))
    if not isinstance(rec, dict):
        return None
    stamp = rec.get(_LAST_ATTEMPT_KEY)
    if not stamp:
        return None
    try:
        dt = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def record_attempt(data_dir: Path, now: Optional[datetime] = None) -> None:
    """Horodate une TENTATIVE de fetch (avant même de savoir si elle réussit).

    C'est ce qui rend le plancher dur réellement contraignant : un échec répété
    ne peut pas être rejoué en boucle tant que le plancher n'est pas écoulé.
    """
    now = now or datetime.now(UTC)
    try:
        atomic_write_json(_last_attempt_path(data_dir), {
            _LAST_ATTEMPT_KEY: iso_z(now),
            "applied_at_utc": iso_z(datetime.now(UTC)),
        })
    except OSError:                                           # pragma: no cover
        LOG.warning("last_attempt.json non écrit (disque en lecture seule ?)")


def seconds_since_last_attempt(data_dir: Path,
                               now: Optional[datetime] = None) -> Optional[float]:
    """Secondes écoulées depuis la dernière tentative, None si jamais tenté."""
    last = read_last_attempt(data_dir)
    if last is None:
        return None
    now = now or datetime.now(UTC)
    return (now - last).total_seconds()


def fetch_floor_remaining(data_dir: Path, now: Optional[datetime] = None,
                          floor_s: Optional[float] = None) -> float:
    """Secondes à attendre avant qu'une tentative FORCÉE soit permise.

    0.0 = autorisé. C'est la garde que ``force=True`` NE PEUT PAS contourner :
    un double-clic sur « Relancer l'ingestion » ne rappelle pas la source.
    """
    floor = float(FETCH_FLOOR_S if floor_s is None else floor_s)
    elapsed = seconds_since_last_attempt(data_dir, now)
    if elapsed is None:
        return 0.0
    return max(0.0, floor - elapsed)


# ─────────────────────────────────────────────────────────────────────────────
# CYCLE D'INGESTION
# ─────────────────────────────────────────────────────────────────────────────
def _served_artifact_facts(data_dir: Path) -> Dict[str, Any]:
    """Ce que le disque sert RÉELLEMENT, indépendamment du cycle en cours.

    [2.4] Pendant un cooldown ou un échec, ``write_health`` écrasait
    health.json avec ``status UNAVAILABLE``, ``event_count 0`` et un hash nul —
    ce qui contredisait l'artefact toujours servi et faisait perdre la date du
    dernier succès. On relit donc l'état précédent pour le CONSERVER, et on
    décrit l'artefact présent sur disque.
    """
    prev = read_json(data_dir / "health.json")
    prev = prev if isinstance(prev, dict) else {}

    canonical = read_json(data_dir / "calendar.latest.json")
    canonical = canonical if isinstance(canonical, dict) else None

    out: Dict[str, Any] = {
        "last_success_utc": prev.get("last_success_utc"),
        "last_success_content_hash": prev.get("last_success_content_hash"),
        "last_success_event_count": prev.get("last_success_event_count"),
        "last_success_parity_hash": prev.get("last_success_parity_hash"),
        "artifact_present": canonical is not None,
        "artifact_generated_at_utc": None,
        "artifact_fetched_at_utc": None,
        "served_artifact_age_s": None,
        "served_artifact_event_count": None,
        "served_artifact_content_hash": None,
    }
    if canonical is None:
        return out

    src = canonical.get("source") if isinstance(canonical.get("source"), dict) else {}
    gen_raw = canonical.get("generated_at_utc")
    fetch_raw = src.get("fetched_at_utc") or gen_raw
    out["artifact_generated_at_utc"] = gen_raw
    out["artifact_fetched_at_utc"] = fetch_raw
    out["served_artifact_content_hash"] = canonical.get("content_hash")
    events = canonical.get("events")
    out["served_artifact_event_count"] = len(events) if isinstance(events, list) else None

    stamp = _parse_iso_utc(fetch_raw)
    if stamp is not None:
        # L'âge est NÉGATIF si l'horodatage est dans le futur (horloge du CDN en
        # avance) : on le borne à 0, un âge négatif serait un mensonge.
        out["served_artifact_age_s"] = max(0, int((datetime.now(UTC) - stamp).total_seconds()))
    return out


def _parse_iso_utc(raw: Any) -> Optional[datetime]:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def write_health(data_dir: Path, now: datetime, payload: Optional[CalendarPayload],
                 error: Optional[str], *, rate_limited: bool = False,
                 mode: str = "live") -> None:
    """Écrit health.json — ADDITIF, sans jamais effacer l'historique du succès.

    Version 1.2 : ajoute last_success_* et served_artifact_* (2.4). Un cycle en
    cooldown/échec RELIT l'ancien health et ne remet pas ces champs à zéro :
    l'UI peut ainsi afficher l'âge RÉEL de l'artefact servi.
    """
    facts = _served_artifact_facts(data_dir)
    # Horodatage du dernier SUCCÈS : mis à jour uniquement quand ce cycle a
    # réellement publié (payload VALID/DEGRADED), sinon hérité du health
    # précédent.
    if payload is not None:
        facts["last_success_utc"] = iso_z(now)
        facts["last_success_content_hash"] = payload.content_hash
        facts["last_success_event_count"] = len(payload.events)
        facts["last_success_parity_hash"] = payload.parity_hash

    status = payload.quality.status.value if payload else "UNAVAILABLE"
    atomic_write_json(data_dir / "health.json", {
        "schema_version": "health-pipeline-1.2",
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
        # [COMPAT-C14] Hash comparable au macro : « les deux apps voient-elles
        # le même calendrier ? »
        "parity_hash": payload.parity_hash if payload else None,
        "parity_window_anchor_utc": (payload.parity_window_anchor_utc
                                     if payload else None),
        "warnings": list(payload.quality.warnings) if payload else [],
        "feeds_status": dict(payload.source.feed_status) if payload else {},
        "tz": tz_environment(),
        # ── [2.4] champs ADDITIFS : ce qui est servi, et depuis quand ────────
        **facts,
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
             policy: Optional[SelectionPolicy] = None,
             force: bool = False) -> Optional[CalendarPayload]:
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

    # [AUDIT-F2] PLANCHER ANTI-429 APPLIQUÉ PAR L'INGESTEUR LUI-MÊME.
    #
    # Le plancher n'existait que dans app.py (bouton « Relancer l'ingestion »)
    # et setup.py. ``run_once`` — le seul composant qui parle réellement à la
    # source — n'en tenait aucun compte : cinq appels consécutifs émettaient
    # cinq GET en quelques millisecondes. C'était la cause racine STRUCTURELLE
    # des 429, et ``--loop --interval 30`` la déclenchait directement.
    #
    # Le plancher est PERSISTÉ (last_attempt.json) : il tient donc entre
    # processus, threads et redémarrages de conteneur. ``force=True`` autorise
    # l'exploitant à l'outrepasser explicitement, jamais par accident.
    if not force:
        remaining = fetch_floor_remaining(data_dir, now)
        if remaining > 0:
            LOG.info("fetch floor active — %.0fs remaining, artifact served as-is",
                     remaining)
            write_health(data_dir, now, None, "FETCH_FLOOR_ACTIVE", mode="floor")
            return None

    owns_session = session is None
    session = session or build_session()

    raw_list: Optional[List[Dict]] = None
    meta: Dict[str, Any] = {}
    feed_status: Dict[str, str] = {}
    error: Optional[str] = None

    # [2.2b] La tentative est horodatée AVANT le fetch, sur disque : c'est ce
    # qui rend le plancher dur contraignant même après un échec, un redémarrage
    # de conteneur ou depuis une autre session.
    record_attempt(data_dir, now)

    try:
        with closing(session) if owns_session else nullcontext():
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

    # [2.4] fetched_at_utc est pris À LA RÉCEPTION DE LA RÉPONSE, pas avant le
    # fetch. Avec ``now`` (capturé en entrée de run_once), l'âge de la source
    # valait 0 par construction : ``source_age_seconds`` = 0, ``is_stale``
    # toujours False, ``serving_mode`` toujours "live" — l'artefact ne pouvait
    # jamais se déclarer périmé.
    #
    # [2.4-CORRECTIF] NE JAMAIS remplacer fetched_at_utc par le Last-Modified du
    # CDN. Ce sont DEUX HORLOGES DIFFÉRENTES :
    #   * fetched_at_utc    = quand NOUS avons récupéré la charge utile ;
    #   * last_modified     = quand l'ÉDITEUR a régénéré le fichier.
    # Pour un flux HEBDOMADAIRE, le second peut précéder le premier de plusieurs
    # jours. Les confondre faisait calculer ``source_age_seconds`` sur l'âge du
    # FICHIER : à chaque cycle l'artefact naissait « périmé » (age > 900 s),
    # d'où ``is_stale=True``, ``SOURCE_AGE_EXCEEDS_900S``, score 1.00 − 0.35 =
    # 0.65 et un verdict DEGRADED systématique — alors que le fetch venait de
    # réussir. L'horodatage éditeur reste exposé tel quel dans
    # ``source.last_modified`` ; il n'entre PAS dans le calcul de l'âge.
    received_at = datetime.now(UTC)
    fetched_at = received_at

    source = SourceInfo(
        provider=SOURCE_PROVIDER,
        url=str(SOURCE_URLS[0]),
        resolved_url=str(meta.get("resolved_url") or SOURCE_URLS[0]),   # [2.9c]
        fetched_at_utc=fetched_at,                                      # [2.4]
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
        served_from_cache=False,
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

    # [2.9h] ORDRE D'ÉCRITURE — les trois artefacts ne sont pas écrits dans une
    # transaction : un téléchargement qui tomberait ENTRE deux écritures
    # servirait un couple incohérent. On écrit donc l'artefact canonique
    # D'ABORD et le legacy EN DERNIER, si bien que la fenêtre d'incohérence
    # n'expose jamais un legacy neuf face à un canonique périmé.
    # Les deux fichiers portent le MÊME ``generated_at_utc`` (celui du payload)
    # : c'est le marqueur qui permet à un consommateur aval — et à l'onglet
    # Export de l'app — de DÉTECTER l'incohérence au lieu de la subir.
    # ``calendar.legacy.json`` est un miroir octet pour octet de
    # ``calendar.json`` : il est écrit en dernier et n'a pas de consommateur
    # distinct.
    atomic_write_json(data_dir / "calendar.latest.json", canonical)
    atomic_write_json(data_dir / "calendar.legacy.json", legacy)
    atomic_write_json(data_dir / "calendar.json", legacy)
    # [COMPAT-C15] Vue committee : écrite APRÈS les artefacts historiques,
    # un échec ici ne bloque jamais la publication du calendrier.
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


def seed_from_existing(data_dir: Path) -> Tuple[Optional[Path], Optional[str]]:
    """Écrit le seed depuis l'artefact canonique DÉJÀ présent sur disque.

    [2.8] ``run_once`` renvoie ``None`` pour trois raisons parfaitement
    normales — cooldown 429, artefact encore récent, mode lecteur — et le seed
    n'était alors jamais écrit, ``setup.py`` répondant « vérifiez votre
    connexion » alors que le calendrier existait sur le disque. On réutilise
    donc l'artefact local s'il est VALIDE (``CalendarPayload.model_validate``,
    statut non INVALID).

    Retourne (chemin, raison_d_echec).
    """
    canonical_path = data_dir / "calendar.latest.json"
    if not canonical_path.exists():
        return None, f"aucun artefact canonical ({canonical_path.name} absent)"
    canonical = read_json(canonical_path)
    if not isinstance(canonical, dict):
        # read_json renvoie None aussi bien pour « absent » que pour « illisible » :
        # les confondre produirait un diagnostic trompeur, exactement le defaut
        # que le point 2.8 corrige.
        return None, (f"artefact canonical illisible ou non-objet JSON "
                      f"({canonical_path.name})")

    # Un schéma plus ancien reste exploitable : on laisse pydantic appliquer ses
    # défauts sur les champs additifs, mais on refuse un payload incohérent.
    try:
        payload = CalendarPayload.model_validate(canonical)
    except Exception as exc:                                  # noqa: BLE001
        return None, f"artefact canonical invalide ({type(exc).__name__}: {exc})"

    status = payload.quality.status
    if status == QualityStatus.INVALID:
        return None, ("artefact canonical INVALID — un seed ne doit jamais "
                      "propager des données rejetées")

    seed_dir = data_dir / "seed"
    seed_dir.mkdir(parents=True, exist_ok=True)
    seed_path = seed_dir / "calendar.latest.seed.json"
    atomic_write_json(seed_path, payload.model_dump(mode="json"))
    LOG.info("seed written from existing artifact to %s", seed_path)
    return seed_path, None


def seed_failure_reason(data_dir: Path) -> str:
    """Pourquoi ``run_once`` n'a rien publié — la VRAIE raison, pas
    « vérifiez votre connexion »."""
    if ingest_disabled():
        return ("mode lecteur actif (BLUESTAR_DISABLE_INGEST=1) : l'ingestion "
                "est déléguée à un ingesteur externe, aucun fetch n'est tenté")
    if is_rate_limited(data_dir):
        rl = read_rate_limit(data_dir) or {}
        return (f"cooldown 429 actif jusqu'à {rl.get('blocked_until_utc')} "
                f"(retry_after={rl.get('retry_after')}s)")
    remaining = fetch_floor_remaining(data_dir)
    if remaining > 0:
        return (f"plancher anti-429 en cours ({remaining:.0f}s restantes) : la "
                "source vient d'être interrogée")
    health = read_json(data_dir / "health.json")
    if isinstance(health, dict) and health.get("error"):
        return f"dernier cycle en échec : {health['error']}"
    return "aucun payload valide produit par le cycle"


def emit_seed(data_dir: Path, session: Optional[requests.Session] = None) -> Optional[Path]:
    """Produit seed/calendar.latest.seed.json pour le cold-start Cloud.

    [2.8] Si le cycle n'a rien publié (cooldown, artefact récent, mode
    lecteur), on retombe sur l'artefact canonique local plutôt que d'échouer :
    un seed doit exister dès qu'un calendrier valide est disponible.
    """
    seed_dir = data_dir / "seed"
    seed_dir.mkdir(parents=True, exist_ok=True)
    seed_path = seed_dir / "calendar.latest.seed.json"

    payload = run_once(data_dir, session)
    if payload is not None:
        atomic_write_json(seed_path, payload.model_dump(mode="json"))
        LOG.info("seed written to %s", seed_path)
        return seed_path

    reason = seed_failure_reason(data_dir)
    LOG.warning("aucun cycle publié (%s) — repli sur l'artefact existant", reason)
    fallback, why = seed_from_existing(data_dir)
    if fallback is not None:
        return fallback

    LOG.error("seed not written: %s ; %s", reason, why)
    return None


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
    parser.add_argument("--force", action="store_true",
                        help="outrepasse le plancher anti-429 (usage explicite)")
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

        LOG.info("entering loop mode, interval=%ds (floor=%ds)",
                 args.interval, FETCH_FLOOR_S)
        while True:
            try:
                run_once(data_dir, force=args.force)
            except Exception:                                 # noqa: BLE001
                LOG.exception("unhandled error in cycle")
            time.sleep(max(5, args.interval))

    result = run_once(data_dir, force=args.force)
    if result is not None:
        return 0
    # [B3] exit 3 = « rien publié mais rien de cassé » (cooldown/mode lecteur),
    # distinct de exit 2 (échec réel) pour ne pas alerter un orchestrateur.
    if ingest_disabled() or is_rate_limited(data_dir):
        return 3
    return 2


if __name__ == "__main__":
    sys.exit(main())

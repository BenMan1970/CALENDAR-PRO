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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple  # [OPUS-F] List: annotation

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
    parse_ff_embedded_calendar,
    sha256_hex,
    to_legacy_payload,
    tz_environment,
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

# ─────────────────────────────────────────────────────────────────────────────
# OVERLAY « ACTUALS » — SECONDE PORTE PUBLIQUE DE FOREX FACTORY (v2.5.0).
# Le flux JSON hebdo ne publie pas les actuals (mesuré 0/105 clés le 15/09) ;
# la page publique du calendrier du site FF, elle, les embarque (objet serveur
# window.calendarComponentStates) avec le MÊME dateline epoch que le flux.
# Ce fichier est un ADDITIF DE VUE : calendar.json, le content_hash inter-apps
# et l'ENGINE restent strictement inchangés. Source unique = FF : on ne
# réinvente pas la roue, on prend la donnée là où la maison la publie.
# ─────────────────────────────────────────────────────────────────────────────
FF_CALENDAR_PAGE = os.getenv("BLUESTAR_FF_CALENDAR_PAGE",
                             "https://www.forexfactory.com/calendar")
# Le site sert ses pages selon l'UA (mur anti-bot occasionnel sur UA custom) ;
# cet habillage navigateur est spécifique à la page publique, le flux JSON
# garde USER_AGENT d'origine (comportement prouvé inchangé).
FF_PAGE_UA = os.getenv(
    "BLUESTAR_FF_PAGE_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
ACTUALS_REFRESH_S = int(os.getenv("BLUESTAR_ACTUALS_REFRESH", "900"))
ACTUALS_FETCH_DEADLINE_S = float(os.getenv("BLUESTAR_ACTUALS_DEADLINE_S", "45"))
ACTUALS_OVERVIEW_NAME = "actuals_overlay.json"
# v2.5.2 (audit OPUS R6) : la collecte actuals est désormais OPT-IN.
# forexfactory.com/calendar renvoie 403 + challenge Cloudflare « managé »
# depuis une IP datacenter (mesuré le 16/09/2026). Sept GET pour sept refus,
# exécutés dans le verrou de publication, retardait le seul travail qui
# compte. BLUESTAR_ENABLE_ACTUALS=1 réactive (utile depuis IP résidentielle).
ENABLE_ACTUALS = (os.getenv("BLUESTAR_ENABLE_ACTUALS", "") or "").strip().lower() in {
    "1", "true", "yes", "on"}
# v2.5.2 (audit OPUS R6) : une fois le challenge Cloudflare observé, on ne
# réessaie plus pendant ce plafond (1 h par défaut) — insister amplifierait
# le signalement anti-bot. Persisté via _state.json.actuals_blocked_until.
ACTUALS_BLOCKED_S = int(os.getenv("BLUESTAR_ACTUALS_BLOCKED_S", "3600"))
# v2.5.2 (audit OPUS R3) : espacement minimal entre fetchs distants. L'egress
# de Streamlit Community Cloud est partagé sur 18 adresses GCP documentées :
# votre quota n'est pas le vôtre. Plafond dur — deux fetchs distants de moins
# que cette valeur dorment. 0 = désactivé.
MIN_FETCH_SPACING_S = max(0, int(os.getenv("BLUESTAR_MIN_FETCH_SPACING", "0")))
# v2.5.2 (audit OPUS R2) : bornes du cooldown de quota. Un Retry-After observé
# est honoré, mais borné — un serveur demandant 6 h ne doit pas geler le
# producteur pour la journée.
RATE_LIMIT_MIN_S = 60
RATE_LIMIT_MAX_S = 3600
RATE_LIMIT_FALLBACK_S = 300  # Retry-After absent ou illisible


def anchor_data_dir(value: str) -> Path:
    """H2 (audit 2026-09-11) : « data » relatif était résolu contre le CWD —
    cron (cwd=/) et Streamlit (cwd=app) produisaient silencieusement DEUX jeux
    d'artefacts parallèles. Un chemin par défaut relatif s'ancre désormais sur
    le dossier d'installation ; un `--data-dir` explicite reste relatif au cwd
    (choix humain, intentionnel).

    v2.5.2 (audit OPUS A7) : la fonction perd son underscore privé — app.py
    l'importait, couplage par détail d'implémentation. Le nom public est
    désormais le contrat."""
    p = Path(value)
    return p if p.is_absolute() else Path(__file__).resolve().parent / p


# v2.5.2 (audit OPUS A7) : alias privé pour rétro-compatibilité. Le nom
# public `anchor_data_dir` est le contrat ; l'alias évite de casser un éventuel
# import tiers existant.
_anchor_data_dir = anchor_data_dir


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


def publish_lock_held(data_dir: Path) -> bool:
    """[B3 audit OPUS] Sonde LECTURE SEULE : le verrou d'édition est-il tenu
    par un producteur vivant (_mtime sous le plafond de vol) ? Permet à l'UI
    et au CLI de distinguer « un autre producteur works » (rien d'anormal)
    d'un échec d'ingestion — les deux retournaient None jusque-là."""
    lp = _lock_path(data_dir)
    try:
        return (time.time() - lp.stat().st_mtime) < LOCK_STEAL_AFTER_S
    except OSError:
        return False


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
        # v2.5.2 (audit OPUS R2) : cooldown de quota persistant. Un reboot ne
        # doit pas faire repartir le producteur marteler la source après un 429.
        self.rate_limited_until: Optional[str] = raw.get("rate_limited_until")
        self.rate_limit_hits: int = int(raw.get("rate_limit_hits", 0))
        # v2.5.2 (audit OPUS R3) : espacement minimal entre fetchs distants.
        # last_fetch_attempt_utc sert de mémoire pour ce plafond.
        self.last_fetch_attempt_utc: Optional[str] = raw.get("last_fetch_attempt_utc")
        # v2.5.2 (audit OPUS R6) : challenge Cloudflare managé observé.
        self.actuals_blocked_until: Optional[str] = raw.get("actuals_blocked_until")

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
            # v2.5.2 : la mémoire de quota traverse les redémarrages.
            "rate_limited_until": self.rate_limited_until,
            "rate_limit_hits": self.rate_limit_hits,
            "last_fetch_attempt_utc": self.last_fetch_attempt_utc,
            "actuals_blocked_until": self.actuals_blocked_until,
        })

    def allow_request(self, now: datetime) -> bool:
        # v2.5.2 (audit OPUS R2) : le cooldown de quota court AVANT le
        # breaker — un 429 n'ouvre jamais le breaker, mais il gèle le cycle
        # pour la durée observée (bornée par RATE_LIMIT_MAX_S).
        if self.rate_limit_remaining(now) > 0:
            return False
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
        # v2.5.2 (audit OPUS R2) : un quota ne doit JAMAIS ouvrir le breaker.
        # Si l'appelant a levé RateLimited, il doit passer par
        # record_rate_limit à la place — pas ici. Ce garde défensif empêche
        # un bug futur de contournement silencieux.
        if error.startswith("RATE_LIMITED"):
            return
        self.consecutive_failures += 1
        self.last_error = error
        if self.consecutive_failures >= CB_FAILURE_THRESHOLD:
            if self.circuit_state != "OPEN":
                LOG.error("circuit breaker -> OPEN after %d failures", self.consecutive_failures)
            self.circuit_state = "OPEN"
            self.opened_at = iso_z(now)

    # ─────────────────────────────────────────────────────────────────────────
    # v2.5.2 (audit OPUS R2) — cooldown de quota borné des deux côtés.
    # ─────────────────────────────────────────────────────────────────────────
    def record_rate_limit(self, now: datetime,
                          retry_after_s: Optional[float]) -> float:
        """Retourne le cooldown effectivement appliqué (borné)."""
        if retry_after_s is None or retry_after_s < 0:
            applied = RATE_LIMIT_FALLBACK_S
        else:
            applied = max(RATE_LIMIT_MIN_S, min(RATE_LIMIT_MAX_S, int(retry_after_s)))
        self.rate_limited_until = iso_z(now + timedelta(seconds=applied))
        self.rate_limit_hits += 1
        self.last_error = f"RATE_LIMITED:Retry-After={retry_after_s}s→cooldown={applied}s"
        return applied

    def rate_limit_remaining(self, now: datetime) -> int:
        if not self.rate_limited_until:
            return 0
        until = datetime.fromisoformat(self.rate_limited_until.replace("Z", "+00:00"))
        rem = int((until - now).total_seconds())
        return max(0, rem)

    # ─────────────────────────────────────────────────────────────────────────
    # v2.5.2 (audit OPUS R3) — espacement minimal entre fetchs.
    # ─────────────────────────────────────────────────────────────────────────
    def note_fetch_attempt(self, now: datetime) -> None:
        self.last_fetch_attempt_utc = iso_z(now)

    def fetch_spacing_remaining(self, now: datetime) -> int:
        if MIN_FETCH_SPACING_S <= 0 or not self.last_fetch_attempt_utc:
            return 0
        last = datetime.fromisoformat(self.last_fetch_attempt_utc.replace("Z", "+00:00"))
        elapsed = (now - last).total_seconds()
        # v2.5.2 (audit OPUS R3) : une horloge qui recule (VM restaurée, NTP
        # brutal) produit un « last » dans le futur. Traité comme maintenant,
        # jamais comme un gel indéfini.
        if elapsed < 0:
            return 0
        return max(0, MIN_FETCH_SPACING_S - int(elapsed))

    # ─────────────────────────────────────────────────────────────────────────
    # v2.5.2 (audit OPUS R6) — challenge anti-bot.
    # ─────────────────────────────────────────────────────────────────────────
    def actuals_blocked_remaining(self, now: datetime) -> int:
        if not self.actuals_blocked_until:
            return 0
        until = datetime.fromisoformat(self.actuals_blocked_until.replace("Z", "+00:00"))
        rem = int((until - now).total_seconds())
        return max(0, rem)


# ─────────────────────────────────────────────────────────────────────────────
# FETCH RÉSILIENT
# ─────────────────────────────────────────────────────────────────────────────
def build_session() -> requests.Session:
    session = requests.Session()
    # v2.5.2 (audit OPUS R1) — 429 RETIRE du status_forcelist et
    # respect_retry_after_header=False. Cause racine de la panne : urllib3
    # Retry.sleep() honore Retry-After sans que backoff_max ne plafonne cette
    # durée. Avec total=4 et Retry-After: 247 (mesuré sur FF depuis une IP
    # datacenter), le worker dormait ~988 s — soit ~17 minutes de zombie
    # tenant une requests.Session non thread-safe. On ne délègue PAS le 429 à
    # urllib3 ; on le traite nous-mêmes via RateLimited (R1) + cooldown borné
    # (R2).
    retry = Retry(
        total=4,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1.5,
        backoff_jitter=0.4,
        status_forcelist=(408, 500, 502, 503, 504),  # 429 retire
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=False,  # R1 — ne pas dormir Retry-After
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


# v2.5.2 (audit OPUS R1) — un quota n'est pas une panne. Exception dédiée,
# remontée à run_once qui déclenche le cooldown borné (R2) SANS toucher au
# breaker.
class RateLimited(FetchError):
    """Le serveur a répondu 429 (ou 503 avec Retry-After). Le cooldown est
    honour mais borné par RATE_LIMIT_MAX_S. La Session est fermée avant levée
    pour ne pas garder une connexion sur un hôte qui vient de nous demander
    de ralentir."""

    def __init__(self, retry_after_s: Optional[float], message: str = ""):
        super().__init__("RATE_LIMITED",
                         message or f"Retry-After={retry_after_s}s")
        self.retry_after = retry_after_s


# v2.5.2 (audit OPUS R1) — parse Retry-After selon RFC 9110. Accepte
# delta-seconds (« 120 ») OU HTTP-date (« Wed, 16 Sep 2026 12:05:00 GMT »).
def parse_retry_after(value: Optional[str],
                      now: datetime) -> Optional[float]:
    if not value:
        return None
    v = value.strip()
    if not v:
        return None
    # delta-seconds
    try:
        return float(v)
    except ValueError:
        pass
    # HTTP-date — formats RFC 7231 / 9110
    for fmt in (
        "%a, %d %b %Y %H:%M:%S GMT",
        "%A, %d-%b-%y %H:%M:%S GMT",
        "%a %b %d %H:%M:%S %Y",
    ):
        try:
            then = datetime.strptime(v, fmt).replace(tzinfo=UTC)
            return max(0.0, (then - now).total_seconds())
        except ValueError:
            continue
    return None


# v2.5.2 (audit OPUS R6) — détection du challenge Cloudflare « managé ».
# Retourne True si la page ressemble au challenge — un body avec timeLabel
# seul ne suffit pas, il faut le marqueur _cf_chl_opt ou le titre « Just a
# moment ». Sans cela, on publierait un overlay trompeur.
_BOT_CHALLENGE_MARKERS = (
    "just a moment", "_cf_chl_opt", "cf-browser-verification",
    "cf_chl_managed", "managed challenge",
)


def looks_like_bot_challenge(html: str) -> bool:
    if not html:
        return False
    head = html[:4096].lower()
    return any(m in head for m in _BOT_CHALLENGE_MARKERS)


# v2.5.2 (audit OPUS R4) — registre des fetchs en vol, par URL. Empêche
# l'accumulation de zombies : un second appel pour la même URL pendant que
# le premier worker dort dans Retry.sleep() lève FETCH_BUSY, AUCUNE
# requête réseau ajoutée.
_INFLIGHT: Dict[str, threading.Thread] = {}
_INFLIGHT_LOCK = threading.Lock()


def fetch_source(session: requests.Session, url: str,
                 deadline_s: float = None,
                 headers: Optional[Dict[str, str]] = None) -> Tuple[Any, Dict[str, Any]]:
    """H7 : plafond DUR au temps mural total, par worker daemon + join.

    v2.5.2 (audit OPUS R4) : garde FETCH_BUSY. Un fetch précédent peut
    survivre au-delà du deadline (worker daemon encore vivant dans
    urllib3.Retry.sleep()). Sans ce garde, le cycle suivant ajoutait un
    NOUVEAU worker pour la même URL — amplification du quota, accumulation
    de zombies. Maintenant : 1 URL = au plus 1 worker en vol.

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
    with _INFLIGHT_LOCK:
        existing = _INFLIGHT.get(url)
        if existing is not None and existing.is_alive():
            raise FetchError("FETCH_BUSY",
                             f"un fetch est déjà en vol pour {url}")
    outcome: Dict[str, Any] = {}

    def _worker() -> None:
        try:
            outcome["value"] = _fetch_source_blocking(session, url, deadline_s, headers)
        except BaseException as exc:                          # noqa: BLE001
            outcome["error"] = exc
        finally:
            with _INFLIGHT_LOCK:
                # Ne retirer QUE notre propre thread — un autre pourrait
                # avoir démarré entre-temps.
                if _INFLIGHT.get(url) is th:
                    _INFLIGHT.pop(url, None)

    th = threading.Thread(target=_worker, daemon=True, name="bluestar-fetch")
    with _INFLIGHT_LOCK:
        _INFLIGHT[url] = th
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
                           deadline_s: float,
                           headers: Optional[Dict[str, str]] = None) -> Tuple[Any, Dict[str, Any]]:
    started = time.monotonic()
    try:
        response = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), stream=True,
                                headers=headers)
    except requests.Timeout as exc:
        raise FetchError("NETWORK_TIMEOUT", str(exc)) from exc
    except requests.RequestException as exc:
        raise FetchError("NETWORK_ERROR", str(exc)) from exc

    with response:
        status = response.status_code
        # v2.5.2 (audit OPUS R1) — 429 / 503 avec Retry-After : on ferme la
        # session et on lève RateLimited. urllib3 a été désactivé pour ces
        # statuts (cf. build_session) ; c'est notre responsabilité de traiter.
        # La session est fermée parce que l'hôte vient de nous demander de
        # ralentir — garder la connexion vivante serait impoli.
        if status == 429 or (status == 503 and response.headers.get("Retry-After")):
            ra = parse_retry_after(response.headers.get("Retry-After"),
                                   datetime.now(UTC))
            try:
                response.close()
            except Exception:                               # noqa: BLE001
                pass
            raise RateLimited(ra, f"status={status} Retry-After={ra}s")
        # v2.5.2 (audit OPUS R5) — 304 Not Modified : le serveur certifie la
        # fraîcheur de notre cache. On retourne un body vide + meta 304, le
        # caller (run_once) rejoue le corps depuis le LKG.
        if status == 304:
            content_type = (response.headers.get("Content-Type") or "").split(";")[0].strip()
            meta = {
                "http_status": 304,
                "content_type": content_type or None,
                "payload_bytes": 0,
                "payload_sha256": "sha256:not-modified",
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "fetch_duration_ms": int((time.monotonic() - started) * 1000),
                "raw_bytes": b"",
            }
            return [], meta
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
# OVERLAY ACTUALS — IMPLÉMENTATION (v2.5.0, vue seule ; contrat inchangé)
# ─────────────────────────────────────────────────────────────────────────────
_MONTHS_LOWER = ("jan", "feb", "mar", "apr", "may", "jun",
                 "jul", "aug", "sep", "oct", "nov", "dec")


def ff_day_url(day: datetime, page: Optional[str] = None) -> str:
    """URL publique du calendrier FF pour un jour : « …?day=sep15.2026 ».
    Format figé à dessein (sans strftime locale-dépendant : « sept. » sous
    locale française casserait silencieusement toute collecte)."""
    base = (page or FF_CALENDAR_PAGE).rstrip("/")
    return f"{base}?day={_MONTHS_LOWER[day.month - 1]}{day.day:02d}.{day.year}"


def _fetch_ff_page_blocking(session: requests.Session, url: str) -> str:
    headers = {"User-Agent": FF_PAGE_UA, "Accept": "text/html,application/xhtml+xml"}
    try:
        response = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), headers=headers)
    except requests.Timeout as exc:
        raise FetchError("NETWORK_TIMEOUT", str(exc)) from exc
    except requests.RequestException as exc:
        raise FetchError("NETWORK_ERROR", str(exc)) from exc
    with response:
        if response.status_code >= 400:
            raise FetchError("HTTP_ERROR", f"status={response.status_code}")
        body = response.content
        if len(body) > MAX_PAYLOAD_BYTES:
            raise FetchError("PAYLOAD_TOO_LARGE", f"{len(body)} bytes")
    return body.decode("utf-8", errors="replace")


def fetch_ff_page(session: requests.Session, url: str) -> str:
    """Plafond dur au temps mural (même doctrine H7 que fetch_source)."""
    outcome: Dict[str, Any] = {}

    def _worker() -> None:
        try:
            outcome["value"] = _fetch_ff_page_blocking(session, url)
        except BaseException as exc:                          # noqa: BLE001
            outcome["error"] = exc

    th = threading.Thread(target=_worker, daemon=True, name="bluestar-ffpage")
    th.start()
    th.join(max(1.0, ACTUALS_FETCH_DEADLINE_S))
    if th.is_alive():
        raise FetchError("FETCH_DEADLINE_EXCEEDED", f">{ACTUALS_FETCH_DEADLINE_S:.0f}s (ff page)")
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


def _actuals_summary_from_file(path: Path) -> Dict[str, Any]:
    obj = read_json(path)
    if not isinstance(obj, dict):
        return {"exists": False}
    entries = obj.get("entries") or []
    return {
        "exists": True,
        "fetched_at_utc": obj.get("fetched_at_utc"),
        "entries": len(entries),
        "with_actual": sum(1 for e in entries if isinstance(e, dict) and e.get("actual")),
    }


def refresh_actuals_overlay(data_dir: Path, session: requests.Session,
                            now: datetime,
                            state: Optional["IngestorState"] = None) -> Dict[str, Any]:
    """Collecte les actuals embarqués dans les pages publiques du site FF pour
    les jours écoulés de la semaine (lundi → aujourd'hui, 7 max) et écrit
    `data/actuals_overlay.json` (atomique). JAMAIS bloquant : tout échec
    network/structure conserve le fichier précédent et ne touche pas au cycle
    principal. Un fichier PARTIELlement reconstruit n'est jamais publié comme
    si tout était à jour : pages_failed est tracé dans l'artefact.

    v2.5.2 (audit OPUS R6) : la collecte est OPT-IN (BLUESTAR_ENABLE_ACTUALS=1).
    Par défaut, AUCUNE requête n'est émise vers forexfactory.com. Une fois le
    challenge Cloudflare managé observé (403 + body typique), `state` est
    configuré pour bloquer toute nouvelle tentative pendant ACTUALS_BLOCKED_S.
    """
    overlay_path = data_dir / ACTUALS_OVERVIEW_NAME
    if not ENABLE_ACTUALS:
        summary = _actuals_summary_from_file(overlay_path)
        summary["disabled"] = True
        return summary
    # v2.5.2 (audit OPUS R6) : challenge observé — on ne réessaie plus.
    if state is not None and state.actuals_blocked_remaining(now) > 0:
        summary = _actuals_summary_from_file(overlay_path)
        summary["blocked_until"] = state.actuals_blocked_until
        return summary
    try:
        age = now.timestamp() - overlay_path.stat().st_mtime
    except OSError:
        age = None
    # Gate de fraîcheur : ACTUALS_REFRESH_S <= 0 le désactive explicitement
    # (sinon un âge mesuré négatif — horloge Windows grossière — satisferait
    # « age < 0 » et gèlerait la collecte indéfiniment).
    if ACTUALS_REFRESH_S > 0 and age is not None and age < ACTUALS_REFRESH_S:
        summary = _actuals_summary_from_file(overlay_path)
        summary["skipped_fresh"] = True
        return summary

    monday = (now - timedelta(days=now.weekday())).date()
    days = [monday + timedelta(d) for d in range((now.date() - monday).days + 1)]
    collected: Dict[Any, Dict[str, Any]] = {}
    pages_ok: List[str] = []
    pages_failed: List[str] = []
    challenged = False
    for day in days:
        url = ff_day_url(day)
        # v2.5.2 (audit OPUS R6) — pré-fetch pour détection challenge même
        # si fetch_ff_page lève FetchError(HTTP_ERROR) sur un 403. On lit
        # le body directement via une fonction bas-niveau ; le cost est
        # négligeable (un seul GET par jour, de toute façon nécessaire).
        html: str = ""
        try:
            html = fetch_ff_page(session, url)
        except FetchError as exc:
            # v2.5.2 (audit OPUS R6) : 403 = probablement challenge CF.
            # On ne peut pas affirmer sans le body ; mais un 403 serveur
            # est rare pour FF sans challenge — on suppose challenge et
            # on bloque pour la durée du cooldown, sans insister sur les
            # jours suivants (7 GET pour 7 refus = signal anti-bot).
            if getattr(exc, "code", "") == "HTTP_ERROR" and "status=403" in str(exc):
                challenged = True
                if state is not None:
                    state.actuals_blocked_until = iso_z(
                        now + timedelta(seconds=ACTUALS_BLOCKED_S))
                pages_failed.append(f"{day.isoformat()}:CHALLENGE_CF_403")
                LOG.error("challenge anti-bot CF présumé (403) (%s) — blocage %ds",
                          day, ACTUALS_BLOCKED_S)
                break
            pages_failed.append(f"{day.isoformat()}:{getattr(exc, 'code', 'ERR')}")
            continue
        # v2.5.2 (audit OPUS R6) : détection retardée — le 200 peut masquer
        # un challenge. Le body suffit pour le dire.
        if looks_like_bot_challenge(html):
            challenged = True
            if state is not None:
                state.actuals_blocked_until = iso_z(
                    now + timedelta(seconds=ACTUALS_BLOCKED_S))
            pages_failed.append(f"{day.isoformat()}:CHALLENGE_CF")
            LOG.error("challenge anti-bot CF détecté dans le body (%s)", day)
            break
        events = parse_ff_embedded_calendar(html)
        if not events and '"timeLabel"' in html:
            pages_failed.append(f"{day.isoformat()}:STRUCTURE_CHANGED")
            LOG.error("structure page FF inconnue (%s) — overlay non modifié", day)
            continue
        pages_ok.append(day.isoformat())
        for e in events:
            collected[(e["name"].lower(), e["dateline"])] = e

    summary: Dict[str, Any] = {
        "exists": overlay_path.exists(),
        "pages_ok": pages_ok,
        "pages_failed": pages_failed,
        "challenged": challenged,
    }
    if challenged:
        summary["kept_previous"] = True
        summary.update(_actuals_summary_from_file(overlay_path))
        return summary
    if not pages_ok:
        summary["kept_previous"] = True
        summary.update(_actuals_summary_from_file(overlay_path))
        LOG.warning("overlay actuals : aucune page exploitable (%s) — fichier antérieur conservé",
                    ", ".join(pages_failed) or "?")
        return summary

    entries = sorted(collected.values(), key=lambda e: (e["dateline"], e["name"]))
    atomic_write_json(overlay_path, {
        "schema_version": "actuals-1.0.0",
        "source": "Forex Factory — embedded data of the public calendar pages",
        "source_pages": [ff_day_url(datetime(d.year, d.month, d.day, tzinfo=timezone.utc))
                         for d in days if d.isoformat() in pages_ok],
        "fetched_at_utc": iso_z(now),
        "join_key": "(name.lower(), dateline epoch UTC) ± 360 s — devise contrôlée",
        "pages_ok": pages_ok,
        "pages_failed": pages_failed,
        "entries": entries,
    })
    summary["entries"] = len(entries)
    summary["with_actual"] = sum(1 for e in entries if e["actual"])
    summary["fetched_at_utc"] = iso_z(now)
    summary["exists"] = True
    LOG.info("overlay actuals: %d entrées (%d avec actual) depuis %d page(s)",
             summary["entries"], summary["with_actual"], len(pages_ok))
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# CYCLE D'INGESTION
# ─────────────────────────────────────────────────────────────────────────────
def write_health(data_dir: Path, state: IngestorState, now: datetime,
                 payload: Optional[CalendarPayload], error: Optional[str],
                 actuals_info: Optional[Dict[str, Any]] = None) -> None:
    if payload is None:
        status = "UNAVAILABLE"
    else:
        status = payload.quality.status.value

    atomic_write_json(data_dir / "health.json", {
        "schema_version": "health-1.3.0",  # v2.5.2 : +rate_limit, +actuals_blocked
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
        # [audit OPUS] le health.json devient auto-suffisant pour le diagnostic
        # croisé cron-vs-app : où sont les artefacts, ce que la source a dit,
        # et quelles RÈGLES horaires sont actives (transition marocaine
        # 20/09/2026 : un offset +01 le 21/09 dans ce bloc = tzdata périmé).
        "data_dir": str(data_dir),
        "week_rollover_pending": bool(payload.quality.week_rollover_pending) if payload else None,
        "coverage": payload.coverage.model_dump(mode="json") if payload and payload.coverage else None,
        "feeds_status": dict(payload.source.feed_status) if payload else {},
        "feed_sha256": dict(payload.source.feed_sha256) if payload else {},
        "tz": tz_environment(),
        # v2.5.0 : état de l'overlay actuals (vue seule ; jamais bloquant).
        "actuals_overlay": actuals_info,
        # v2.5.2 (audit OPUS R2/R3/R6) : mémoire de quota et de challenge.
        "rate_limit": {
            "rate_limited_until": state.rate_limited_until,
            "rate_limit_remaining_s": state.rate_limit_remaining(now),
            "rate_limit_hits": state.rate_limit_hits,
        },
        "fetch_spacing": {
            "min_spacing_s": MIN_FETCH_SPACING_S,
            "remaining_s": state.fetch_spacing_remaining(now),
            "last_fetch_attempt_utc": state.last_fetch_attempt_utc,
        },
        "actuals_blocked_until": state.actuals_blocked_until,
        "actuals_blocked_remaining_s": state.actuals_blocked_remaining(now),
    })


def run_once(data_dir: Path, policy: SelectionPolicy, session: requests.Session,
             keep_history: int = 200) -> Optional[CalendarPayload]:
    """Cycle unique sous verrou inter-processus (H3). Si l'autre producteur
    (cron ou Streamlit) tient déjà le verrou, le cycle est Sauté sans effet
    de bord — pas d'échec compté, pas d'écriture : le prochain tick reprendra.

    v2.5.2 (audit OPUS R6) — l'overlay actuals tourne HORS du verrou de
    publication. Le contrat canonique est sur disque AVANT que l'overlay
    ne parte ; 7 GET vers forexfactory.com ne doivent pas retarder le seul
    travail qui compte.
    """
    if not _acquire_publish_lock(data_dir):
        LOG.warning("un autre producteur détient le verrou d'édition — cycle sauté")
        return None
    try:
        result = _run_once_impl(data_dir, policy, session, keep_history)
    finally:
        _release_publish_lock(data_dir)
    # v2.5.2 (audit OPUS R6) — overlay HORS verrou. Si _run_once_impl a
    # retourné (payload, state), on complète par l'overlay + health final.
    if result is None:
        return None
    payload, state, now = result
    try:
        actuals_info = refresh_actuals_overlay(data_dir, session, now, state)
    except Exception as exc:                               # noqa: BLE001
        LOG.exception("overlay actuals — échec inattendu, cycle principal intact")
        actuals_info = {"error": f"UNEXPECTED: {type(exc).__name__}"}
    write_health(data_dir, state, now, payload, state.last_error, actuals_info)
    return payload


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
    elif state.rate_limit_remaining(now) > 0:
        # v2.5.2 (audit OPUS R2) : quota actif. On ne tente PAS le fetch.
        error = f"RATE_LIMITED:cooldown={state.rate_limit_remaining(now)}s"
        feed_status["thisweek"] = "rate_limited"
        LOG.info("quota cooldown actif (%ds) — fetch différé",
                 state.rate_limit_remaining(now))
    elif state.fetch_spacing_remaining(now) > 0:
        # v2.5.2 (audit OPUS R3) : espacement minimal non atteint.
        # On dort le complément, pas plus — le cycle reste à l'heure.
        wait = state.fetch_spacing_remaining(now)
        LOG.info("espacement minimal : %ds restant", wait)
        time.sleep(wait)
        now = datetime.now(UTC)
        state.note_fetch_attempt(now)
    else:
        state.note_fetch_attempt(now)
        # v2.5.2 (audit OPUS R5) — conditional GET. On n'envoie If-None-Match
        # QUE si un LKG existe (sinon un 304 nous laisserait un corps vide).
        lkg_for_cond = None
        if state.etag:
            cached = read_json(lkg_path)
            if cached and isinstance(cached.get("payload"), list):
                lkg_for_cond = cached
        cond_headers = {"If-None-Match": state.etag} if lkg_for_cond else {}
        try:
            raw_list, meta = fetch_source(session, SOURCE_URL,
                                             headers=cond_headers or None)
            # v2.5.2 (audit OPUS R5) — un 304 signifie « le serveur certifie
            # la fraîcheur de votre cache ». On publie le LKG TEL QUEL, mais
            # marqué frais (from_last_known_good=False), avec fetched_at=now.
            # C'est la sémantique HTTP correcte, et l'aval ne ment plus sur la
            # fraîcheur.
            if meta.get("http_status") == 304 and lkg_for_cond is not None:
                feed_status["thisweek"] = "ok"
                feed_status["revalidated"] = "304"
                raw_list = lkg_for_cond["payload"]
                # On reprend les métadonnées du cache mais on garde le sha256
                # et le status du 304 — la signature du contenu n'a pas
                # bougé, c'est tout l'intérêt d'un conditional GET.
                meta["http_status"] = 304
                meta["fetched_at_utc"] = iso_z(now)
                state.record_success(now)
                state.etag = meta.get("etag") or state.etag
                state.last_modified = meta.get("last_modified") or state.last_modified
            else:
                feed_status["thisweek"] = "ok"
                state.record_success(now)
                # [audit OPUS] ETag/Last-Modified sont persistés à titre
                # d'observation ET désormais utilisés pour le conditional
                # GET (R5). Si un 304 survient, le corps est rejoué depuis le
                # LKG — pas de risque de INVALID_JSON.
                state.etag = meta.get("etag")
                state.last_modified = meta.get("last_modified")
            # [câblage audit OPUS] Provenance de la FUSION préservée :
            # nextweek était fusionné sans que son hash ni ses octets ne
            # soient tracés — payload_sha256 ne couvrait que thisweek alors
            # que le LKG, lui, stockait le fusionné. L'hash agrégé reprend la
            # FORMULE EXACTE du calendar_layer macro (sha256 de la
            # concaténation « | » des sha par flux ok, dans l'ordre
            # thisweek→nextweek) : deux apps, mêmes bruts, même signature.
            feed_shas: Dict[str, str] = {"thisweek": str(meta.get("payload_sha256"))}
            total_bytes = int(meta.get("payload_bytes") or 0)
            next_body: Optional[bytes] = None
            if SOURCE_URL_NEXT:
                try:
                    extra_rows, extra_meta = fetch_source(session, SOURCE_URL_NEXT)
                    if isinstance(extra_rows, list):
                        raw_list = list(raw_list) + extra_rows
                        feed_status["nextweek"] = "ok"
                        feed_shas["nextweek"] = str(extra_meta.get("payload_sha256"))
                        total_bytes += int(extra_meta.get("payload_bytes") or 0)
                        nb = extra_meta.get("raw_bytes")
                        next_body = nb if isinstance(nb, bytes) else None
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
            ordered_shas = [feed_shas[k] for k in ("thisweek", "nextweek") if k in feed_shas]
            meta["feed_sha256"] = feed_shas
            meta["payload_sha256"] = "sha256:" + sha256_hex("|".join(ordered_shas))
            meta["payload_bytes"] = total_bytes
            raw_bytes = meta.pop("raw_bytes")
            meta["feed_status"] = dict(feed_status)
            _ts = now.strftime('%Y%m%dT%H%M%SZ')
            atomic_write_bytes(data_dir / "raw" / f"raw_{_ts}_thisweek.json", raw_bytes)
            if next_body is not None:
                atomic_write_bytes(data_dir / "raw" / f"raw_{_ts}_nextweek.json", next_body)
            # Le cache est écrit APRÈS la fusion : un resservi LKG rejoue exactly
            # le même contenu (events + statuts de flux) que l'artefact publié.
            atomic_write_bytes(lkg_path, json.dumps({
                "fetched_at_utc": iso_z(now),
                "meta": {k: v for k, v in meta.items()},
                "payload": raw_list,
            }, ensure_ascii=False).encode("utf-8"))
            _rotate_raw(data_dir / "raw", RAW_KEEP)
        except RateLimited as exc:
            # v2.5.2 (audit OPUS R2) — un quota ne compte pas comme une panne.
            # Le cooldown est borné par RATE_LIMIT_MAX_S, persisté, et
            # honore Retry-After quand il est raisonnable.
            applied = state.record_rate_limit(now, exc.retry_after)
            feed_status["thisweek"] = "rate_limited"
            error = f"RATE_LIMITED:Retry-After={exc.retry_after}s→cooldown={applied}s"
            LOG.warning("source 429 — cooldown %ds (Retry-After=%s)",
                        applied, exc.retry_after)
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
        feed_sha256=dict(meta.get("feed_sha256") or {}),
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
    # v2.5.0 — overlay + health final sont désormais appelés par run_once
    # APRÈS libération du verrou de publication (audit OPUS R6).

    LOG.info(
        "published %d events | status=%s | score=%.2f | content_%s | hash=%s",
        len(payload.events), payload.quality.status.value,
        payload.quality.data_quality_score,
        "CHANGED" if changed else "unchanged", short_hash,
    )
    # v2.5.2 (audit OPUS R6) — retourne le triplet (payload, state, now)
    # pour que run_once puisse appeler l'overlay HORS du verrou.
    return (payload, state, now)


def emit_seed(data_dir: Path, seed_path: Path) -> Optional[Path]:
    """v2.5.2 (audit OPUS R7) — fige le dernier artefact canonique en une
    semence d'AFFICHAGE (lecture seule, jamais exportée sous calendar.json).

    Pré-requis : calendar.latest.json doit exister dans data_dir (sinon,
    retourne None — on ne fige pas du vide).

    Format :
        {"seed_schema": "bluestar-display-seed-1.0.0",
         "warning": "ARTEFACT D'AFFICHAGE FIGÉ — ...",
         "payload": <CalendarPayload.model_dump(mode="json")>}

    La semence est commitée dans le dépôt (typiquement `seed/`), lue par
    app.load_seed() au démarrage sur disque éphémère. Elle n'entre JAMAIS
    dans le pipeline de production — pas de last_known_good, pas de hash
    dans l'historique.
    """
    canonical = data_dir / "calendar.latest.json"
    if not canonical.exists():
        LOG.warning("emit_seed: aucun artefact canonique à figer")
        return None
    try:
        payload_dict = json.loads(canonical.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        LOG.error("emit_seed: artefact illisible: %s", exc)
        return None
    seed_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(seed_path, {
        "seed_schema": "bluestar-display-seed-1.0.0",
        "warning": ("ARTEFACT D'AFFICHAGE FIGÉ — lecture seule, jamais "
                    "exporté sous calendar.json. Le contrat de production "
                    "reste calendar.latest.json, produit par l'ingestor."),
        "payload": payload_dict,
    })
    LOG.info("emit_seed: semence écrite → %s", seed_path)
    return seed_path


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

    # [logs audit OPUS] datefmt promet un suffixe « Z » ; asctime par défaut
    # suit la locale — un cron sous UTC+1 écrivait « …:14:33Z » pour 13:33Z.
    # Convertir le formatter en gmtime rend l'horodatage conforme à sa
    # promesse, sur toute plateforme.
    logging.Formatter.converter = time.gmtime
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
    args.data_dir.mkdir(parents=True, exist_ok=True)
    # [câblage audit OPUS] le chemin RÉSOLU est journalisé : deux lanceurs
    # qui ne partagent pas le même DATA_DIR doivent pouvoir être confondus
    # en lisant les logs, pas devinés après coup.
    LOG.info("ingestor data_dir=%s | tz=%s", args.data_dir.resolve(),
             tz_environment())

    if not args.loop or args.once:
        # [B4 audit OPUS] session par cycle : un worker abandonné par le
        # plafond H7 ne peut plus marcher sur la session partagée du suivant.
        # [B3 audit OPUS] verrou occupé ≠ échec : code de sortie 3 distinct,
        # un superviseur cron ne doit pas alerter parce que Streamlit ingère.
        res = run_once(args.data_dir, policy, build_session())
        if res is not None:
            return 0
        return 3 if publish_lock_held(args.data_dir) else 2

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    while not _STOP:
        cycle_start = time.monotonic()
        try:
            run_once(args.data_dir, policy, build_session())
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

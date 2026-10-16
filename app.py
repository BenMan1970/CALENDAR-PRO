"""
BLUESTAR Pipeline Calendar — UI « Midnight Luxe »
=================================================
Orchestre pipeline_calendar.py et expose les news + le JSON, avec le design
system maison (ui.py : tokens, styles, composants et charts).

Doctrine :
  • le JSON produit est celui de calendar_core.py, INCHANGÉ (parité de
    content_hash inter-apps) ;
  • aucun widget n'alimente la SelectionPolicy : les filtres de cette page
    sont des filtres de VUE, et les exports téléchargent les OCTETS DU
    DISQUE — l'artefact consommé par le merge ne peut pas être contaminé ;
  • ingestion en thread de fond non bloquante, fallback seed au cold-start,
    cache invalidé par (mtime_ns, size) du fichier.
"""
from __future__ import annotations

# [INCIDENT-ANALYTICS] DOIT PRECEDER `import streamlit`.
#
# Reproduit : si `pandas` est present dans ``sys.modules`` mais pas encore
# initialise, ``_plotly_utils`` le recupere via ``sys.modules.get`` et fait
# ``pd.Series`` sur un objet truthy sans attributs -> AttributeError. Le crash
# ne vient donc PAS de notre code : il frappe dans
# ``streamlit/elements/lib/streamlit_plotly_theme.py``, DES l'import de
# Streamlit, avant que quoi que ce soit d'autre puisse se defendre.
#
# La seule parade possible est donc d'initialiser pandas COMPLETEMENT en tout
# premier. Un module entier ne peut plus pieger le validateur.
#
# ATTENTION, piege verifie : un simple ``import pandas`` NE SUFFIT PAS. Python
# voit l'entree deja presente dans ``sys.modules`` et la renvoie telle quelle,
# toujours cassee (mesure : ``hasattr(pandas, 'Series') == False``). Il faut
# forcer la REEXECUTION du module avec ``importlib.reload`` — c'est ce qui
# repose les attributs. Mesure apres reload : ``Series`` present.
#
# (Le meme garde existe dans ui.py, par robustesse si ui est importe seul.)
try:                                                         # noqa: SIM105
    import importlib as _importlib
    import pandas as _pd
    if not hasattr(_pd, "Series"):                            # module tronque
        _importlib.reload(_pd)
    del _pd, _importlib
except Exception:                                            # noqa: BLE001
    pass

import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape as escape_html
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import zoneinfo
from zoneinfo import ZoneInfo

import streamlit as st

import pipeline_calendar as pc
import ui as U
from calendar_core import day_name

UTC = timezone.utc

# ── Chemins ─────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parent / "data"
CANONICAL_PATH = DATA_DIR / "calendar.latest.json"
LEGACY_PATH = DATA_DIR / "calendar.json"
HEALTH_PATH = DATA_DIR / "health.json"
SEED_PATH = DATA_DIR / "seed" / "calendar.latest.seed.json"
# [AUDIT-F3] Artefacts egalement publies par l'ingesteur, exposes au
# telechargement depuis l'onglet Export.
COMMITTEE_PATH = DATA_DIR / "calendar.committee.json"
EXPORT_PATH = DATA_DIR / "trading_export.json"

IMPACT_LABELS = ("HIGH", "MEDIUM", "LOW", "HOLIDAY", "UNKNOWN")

# [2.1] Warnings qui ne sont PAS des pannes : la source ne publie pas de champ
# « actual », et c'est une propriété connue du flux hebdomadaire FF/Fair
# Economy, pas une dégradation. Ils restent dans le JSON canonique (contrat
# inchangé) mais sortent du compteur et de la couleur du bandeau d'alerte, et
# s'affichent en puce « info » dans Diagnostics. Sans cela, un bandeau jaune
# « 1 signal(aux) qualité » s'affichait EN PERMANENCE et noyait les vrais
# warnings.
INFO_WARNINGS = frozenset({
    "SOURCE_DOES_NOT_PROVIDE_ACTUAL",
})


def split_warnings(warnings: Any) -> Tuple[List[str], List[str]]:
    """(alertes, informations) — les INFO_WARNINGS ne comptent pas comme
    signaux qualité."""
    items = [str(w) for w in (warnings or [])]
    infos = [w for w in items if w.split(":", 1)[0] in INFO_WARNINGS]
    alerts = [w for w in items if w.split(":", 1)[0] not in INFO_WARNINGS]
    return alerts, infos


# ═══════════════════════════════════════════════════════════════════════════
# ÂGE RÉEL DE L'ARTEFACT (contenu, pas mtime)
# ═══════════════════════════════════════════════════════════════════════════
# Le mtime d'un fichier ne dit rien de la fraîcheur de ses DONNÉES : sur Cloud,
# le checkout git / la restauration du conteneur donne à calendar.latest.json
# un mtime « maintenant » alors que son contenu date de plusieurs jours. Avec
# un garde-fou basé sur le mtime, le premier cycle est repoussé de
# MIN_FETCH_SPACING_S et l'écran sert des données périmées pendant ce temps.
# La source de vérité est donc l'horodatage du fetch écrit DANS l'artefact.
_CLOCK_SKEW_TOLERANCE_S = 300.0
_FETCHED_MEMO: Dict[Tuple[int, int], Optional[float]] = {}


def _parse_utc_epoch(raw: Any) -> Optional[float]:
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.timestamp()


def _artifact_fetched_epoch() -> Optional[float]:
    """Epoch du dernier fetch réussi, lu dans l'artefact canonique (mémoïsé par
    (mtime_ns, size) : une relecture disque seulement quand le fichier change).
    None = inconnu (fichier absent/illisible) → traité comme « à rafraîchir »."""
    key = _stats(CANONICAL_PATH)
    if key == (0, 0):
        return None
    if key in _FETCHED_MEMO:
        return _FETCHED_MEMO[key]
    data = pc.read_json(CANONICAL_PATH)
    epoch: Optional[float] = None
    if isinstance(data, dict):
        src = data.get("source") if isinstance(data.get("source"), dict) else {}
        epoch = (_parse_utc_epoch(src.get("fetched_at_utc"))
                 or _parse_utc_epoch(data.get("generated_at_utc")))
    if len(_FETCHED_MEMO) > 8:
        _FETCHED_MEMO.clear()
    _FETCHED_MEMO[key] = epoch
    return epoch


def artifact_age_s() -> Optional[float]:
    """Âge (s) des données servies, None si inconnu. Un horodatage trop loin
    dans le futur (horloge incohérente) est traité comme inconnu."""
    epoch = _artifact_fetched_epoch()
    if epoch is None:
        return None
    age = time.time() - epoch
    return None if age < -_CLOCK_SKEW_TOLERANCE_S else max(0.0, age)


def _artifact_is_fresh() -> bool:
    age = artifact_age_s()
    return age is not None and age < pc.MIN_FETCH_SPACING_S


# ═══════════════════════════════════════════════════════════════════════════
# INGESTION DE FOND (non bloquante, une seule à la fois par process)
# ═══════════════════════════════════════════════════════════════════════════
# Codes d'échec TRANSITOIRES (réseau / HTTP / JSON tronqué par un CDN) : seuls
# ceux-là méritent un nouvel essai immédiat. QUALITY_INVALID, NORMALIZATION_*,
# RATE_LIMIT_* et INGEST_DISABLED sont déterministes : on ne les rejoue pas.
_TRANSIENT_CODES = frozenset({
    "NETWORK_ERROR", "NETWORK_TIMEOUT", "HTTP_ERROR",
    "FETCH_DEADLINE_EXCEEDED", "ALL_SOURCES_FAILED", "INVALID_JSON",
})
# [2.2d] AUCUN « quick retry » intra-cycle : le budget d'un cycle est borné à
# une requête par URL (primaire, puis secondaire), et la reprise est assurée
# par la cascade primaire -> secondaire, puis par le cycle suivant. Un retry
# immédiat après un échec ne faisait qu'ajouter des requêtes contre une source
# qui rate-limite — c'est précisément ce qui produisait les 429 observés.
_QUICK_RETRY_DELAYS_S: tuple = ()
# [2.2c] Backoff APRÈS ÉCHEC : minimum 300 s, plafond 3600 s. L'ancienne
# échelle (10/30/60/120 s) rejouait un échec DÉTERMINISTE (QUALITY_INVALID,
# NORMALIZATION_FAILED) toutes les 2 minutes indéfiniment.
_BACKOFF_S = (300.0, 600.0, 1200.0, 1800.0, 3600.0)
# [AUDIT-F2b] Codes qui ne sont PAS des pannes : le cycle s'est terminé
# normalement, il n'a simplement rien eu à publier. ``FETCH_FLOOR_ACTIVE``
# appartient à cette famille au même titre que le cooldown — sans quoi un
# plancher respecté était compté comme un échec, déclenchait un backoff
# 300 s -> 3600 s et affichait un bandeau d'erreur rouge à l'utilisateur
# alors que le système fonctionnait exactement comme prévu.
_NO_ERROR_CODES = frozenset({
    "RATE_LIMIT_COOLDOWN",
    "INGEST_DISABLED",
    "FETCH_FLOOR_ACTIVE",
})


def _cycle_error(since_epoch: float) -> Optional[str]:
    """Erreur écrite par run_once pour CE cycle (health.json), None si absente
    ou antérieure au cycle (jamais d'erreur périmée attribuée au cycle)."""
    hp = pc.read_json(HEALTH_PATH)
    if not isinstance(hp, dict) or not hp.get("error"):
        return None
    ts = _parse_utc_epoch(hp.get("checked_at_utc"))
    if ts is None or ts < since_epoch - 1.0:
        return None
    return str(hp["error"])


class Ingestion:
    """Superviseur d'ingestion : jamais plus d'un fetch concurrent, jamais de
    fetch pendant le cooldown 429, jamais de fetch en mode lecteur ; un échec
    est espacé par un backoff borné (300 s -> 3600 s).

    [thread-safety] ``last_error`` et ``fail_streak`` sont écrits par le thread
    worker et lus par le thread de script Streamlit : tout accès passe par
    ``self._lock``, sinon la lecture peut voir un état déchiré (streak à jour
    et erreur périmée, ou l'inverse)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._running = False
        self._last_error: Optional[str] = None
        self._fail_streak = 0
        self._next_attempt_at = 0.0
        self.last_finished_at: Optional[float] = None

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    # ── Etat partage, toujours lu/ecrit sous verrou ─────────────────────────
    @property
    def last_error(self) -> Optional[str]:
        with self._lock:
            return self._last_error

    @last_error.setter
    def last_error(self, value: Optional[str]) -> None:
        with self._lock:
            self._last_error = value

    @property
    def fail_streak(self) -> int:
        with self._lock:
            return self._fail_streak

    @fail_streak.setter
    def fail_streak(self, value: int) -> None:
        with self._lock:
            self._fail_streak = value

    @property
    def next_attempt_at(self) -> float:
        with self._lock:
            return self._next_attempt_at

    @next_attempt_at.setter
    def next_attempt_at(self, value: float) -> None:
        with self._lock:
            self._next_attempt_at = value

    def _set_failure(self, error: str, streak: int) -> None:
        """Ecrit (erreur, streak, prochaine tentative) en UNE section critique :
        l'UI ne peut pas observer un streak incremente avec l'ancienne erreur."""
        delay = _BACKOFF_S[min(streak, len(_BACKOFF_S)) - 1] if streak else 0.0
        with self._lock:
            self._last_error = error
            self._fail_streak = streak
            self._next_attempt_at = time.time() + delay

    def kick(self, *, force: bool = False) -> str:
        """Retourne l'état : started | running | fresh | cooldown | disabled |
        floor | backoff.

        [2.2b] ``force=True`` (boutons « Relancer l'ingestion » / « Scanner »)
        ne contourne NI le cooldown 429 NI le plancher dur depuis la dernière
        TENTATIVE. Un double-clic ou deux sessions ne peuvent donc pas
        déclencher deux fetchs rapprochés : c'est exactement ce qui produisait
        le 429 observé ~100 s après le dernier succès."""
        if pc.ingest_disabled():
            return "disabled"

        with self._lock:
            if self._running:
                return "running"
            # Tests disque à l'intérieur du verrou : sans cela, deux sessions
            # concurrentes passent la garde (cooldown + fraîcheur) en même
            # temps puis se sérialisent ; si le worker de la première s'achève
            # dans la fenêtre, la seconde relance un fetch devenu inutile.
            # 1. Un quota 429 est ABSOLU : rien ne le contourne, pas même force.
            if pc.is_rate_limited(DATA_DIR):
                return "cooldown"
            # 2. Plancher dur depuis la dernière tentative (persisté sur
            #    disque) : appliqué AVANT le test de fraîcheur, donc y compris
            #    pour un fetch forcé.
            if pc.fetch_floor_remaining(DATA_DIR) > 0:
                return "floor" if force else (
                    "fresh" if CANONICAL_PATH.exists() and _artifact_is_fresh()
                    else "backoff")
            if not force and CANONICAL_PATH.exists() and _artifact_is_fresh():
                return "fresh"
            if not force and time.time() < self._next_attempt_at:
                return "backoff"
            # [2.2b] La tentative est horodatée ICI, sous le verrou, au moment
            # exact où l'on decide de fetcher — et non dans run_once. Deux
            # raisons :
            #   • le plancher doit tenir même si le worker meurt avant d'entrer
            #     dans run_once (exception a l'import, disque plein, annulation) ;
            #   • deux instances d'Ingestion (deux sessions Streamlit, ou un
            #     redemarrage de conteneur) partagent le meme data_dir : c'est
            #     le fichier sur disque, ecrit ici, qui serialise les fetchs.
            # L'ecriture disque est faite AVANT le demarrage du thread, donc un
            # second appel concurrent voit deja le plancher.
            pc.record_attempt(DATA_DIR)
            self._running = True

        threading.Thread(target=self._worker, daemon=True, name="bs-ingest").start()
        return "started"

    def _worker(self) -> None:
        try:
            # [2.2d] UNE seule tentative par cycle : plus de « quick retry »
            # interne. Le budget de requetes est ainsi entierement determine
            # par fetch_source (au plus 2), et l'espacement est gere par le
            # plancher dur + le backoff, pas par une boucle de reprise.
            started = time.time()
            # [B4] session HTTP jetable par cycle : pas de pool partagé
            # entre threads Streamlit (sockets recyclés à froid = 429
            # fantômes).
            #
            # [AUDIT-F2c] ``force=True`` est INDISPENSABLE ici : ``kick()``
            # vient de vérifier le plancher SOUS VERROU et d'appeler
            # ``record_attempt()`` pour le réserver. Sans ce drapeau, le
            # ``fetch_floor_remaining()`` de run_once relisait le plancher que
            # kick venait d'écrire et abandonnait le cycle : l'app aurait cessé
            # de fetcher définitivement. Le plancher est bien appliqué une fois,
            # par kick() ; run_once est ici l'exécutant d'une décision déjà
            # prise. Les appelants directs (CLI, cron) n'ont pas de kick et
            # restent protégés par le défaut force=False.
            payload = pc.run_once(DATA_DIR, session=None, force=True)
            if payload is not None:
                with self._lock:
                    self._last_error = None
                    self._fail_streak = 0
                    self._next_attempt_at = 0.0
                return
            err = _cycle_error(started)
            code = (err or "").split(":", 1)[0]
            if code in _NO_ERROR_CODES or pc.ingest_disabled() \
                    or pc.is_rate_limited(DATA_DIR):
                return                        # pas un échec : rien à rejouer
            # [2.2c] Backoff >= 300 s, y compris pour un échec DÉTERMINISTE :
            # le rejouer toutes les 2 minutes ne pouvait pas changer le verdict
            # et consommait le quota de la source.
            self._set_failure(err or "NO_DATA", self.fail_streak + 1)
        except Exception as exc:                              # noqa: BLE001
            self._set_failure(f"{type(exc).__name__}: {exc}", self.fail_streak + 1)
        finally:
            self.last_finished_at = time.time()
            with self._lock:
                self._running = False

    def wait_done(self, timeout: float = 30.0) -> bool:
        """Attend la fin du cycle en cours. L'UI l'appelle juste après
        ``kick`` pour que le rerun qui suit serve des octets frais — sans elle,
        le rerun est immédiat et les boutons de téléchargement embarquent
        l'artefact d'avant le cycle."""
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                if not self._running:
                    return True
            if time.monotonic() >= deadline:
                return not self._running
            time.sleep(0.2)


# Révision de la classe Ingestion : fait partie de la CLÉ du cache_resource.
# Sans elle, un redéploiement à chaud (même process, code de _new_ingestion
# inchangé) ressort l'ANCIENNE instance — créée par l'ancienne classe, donc
# sans les attributs/méthodes ajoutés depuis (AttributeError au rendu).
# À INCRÉMENTER à chaque modification de la classe Ingestion.
_INGESTION_REV = "2026-10-01.2"


@st.cache_resource(show_spinner=False)
def _new_ingestion(rev: str) -> Ingestion:
    """Instance persistante : sans ce cache, elle est réinstanciée à chaque
    rerun complet (filtre sidebar, onglet), ce qui réinitialise ``_running``
    à False et permet à deux run_once() de tourner en parallèle — la garantie
    « jamais plus d'un fetch concurrent » de la classe ne tient plus.
    ``rev`` ne sert qu'à versionner la clé de cache (voir _INGESTION_REV)."""
    return Ingestion()


INGESTION = _new_ingestion(_INGESTION_REV)

# Messages partagés des boutons « Relancer l'ingestion » et « Scanner ».
_KICK_MSGS = {
    "started": ("Cycle lancé.", "ok"),
    "running": ("Un cycle est déjà en cours.", "warn"),
    "cooldown": ("Cooldown 429 actif — artefact précédent servi.", "warn"),
    "disabled": ("Mode lecteur (BLUESTAR_DISABLE_INGEST).", "warn"),
    "fresh": ("Artefact récent — espacement minimal respecté.", "ok"),
    "floor": ("Requête forcée ignorée : plancher anti-429 en cours. "
              "Le flux est hebdomadaire, inutile de le réinterroger.", "warn"),
    "backoff": ("Nouvelle tentative programmée — échec récent de la source.", "warn"),
}


def _scan_and_rerun(msg_key: str, *, timeout: float = 30.0) -> None:
    """Force un cycle d'ingestion, en attend la fin, puis relance la page.

    But : que les boutons de téléchargement servent les octets du cycle qui
    vient de tourner. Sans attente, le rerun est immédiat et embarque
    l'artefact précédent — sur Cloud, data/ est reverté à chaque redémarrage
    du conteneur, donc le téléchargement servirait l'artefact commité (stale)
    même après une ingestion réussie."""
    with st.spinner("Rafraîchissement en cours…"):
        state = INGESTION.kick(force=True)
        if state in ("started", "running"):
            INGESTION.wait_done(timeout=timeout)
    st.session_state[msg_key] = _KICK_MSGS.get(state, (state, "warn"))
    st.rerun()



# À l'ouverture : si l'artefact est plus vieux que l'espacement minimal de
# fetch, on attend la fin du cycle qu'on vient de lancer plutôt que de servir
# l'état stale (sur Cloud, data/ est reverté à chaque restart du conteneur).
LOAD_REFRESH_TIMEOUT_S = 12.0


def _load_refresh_due() -> bool:
    """L'artefact canonique est-il assez vieux pour justifier d'attendre le
    cycle en cours à l'ouverture de la page ?"""
    if pc.ingest_disabled() or not CANONICAL_PATH.exists():
        return False
    return not _artifact_is_fresh()

# ═══════════════════════════════════════════════════════════════════════════
# CHARGEMENT (cache invalidé par les stats fichier)
# ═══════════════════════════════════════════════════════════════════════════
def _stats(path: Path) -> Tuple[int, int]:
    try:
        s = path.stat()
        return s.st_mtime_ns, s.st_size
    except OSError:
        return 0, 0


@st.cache_data(show_spinner=False, ttl=20)
def load_canonical(mt: int = 0, sz: int = 0) -> Tuple[Optional[dict], bool]:
    """JSON canonique v2 ; fallback seed si data/ vide (cold-start Cloud)."""
    data = pc.read_json(CANONICAL_PATH)
    if data is not None:
        return data, False
    seed = pc.read_json(SEED_PATH)
    return (seed, True) if seed is not None else (None, False)


@st.cache_data(show_spinner=False, ttl=20)
def load_legacy(mt: int = 0, sz: int = 0) -> Tuple[Optional[dict], bool]:
    """JSON legacy v1 ; reconstruit depuis le seed si absent du disque."""
    data = pc.read_json(LEGACY_PATH)
    if data is not None:
        return data, False
    seed = pc.read_json(SEED_PATH)
    if seed and "events" in seed:
        try:
            from calendar_core import CalendarPayload, to_legacy_payload
            payload = CalendarPayload.model_validate(seed)
            return to_legacy_payload(payload, datetime.now(UTC)), True
        except Exception:                                     # noqa: BLE001
            return None, False
    return None, False


@st.cache_data(show_spinner=False, ttl=20)
def load_health(mt: int = 0, sz: int = 0) -> Optional[dict]:
    return pc.read_json(HEALTH_PATH)


def canonical() -> Tuple[Optional[dict], bool]:
    return load_canonical(*_stats(CANONICAL_PATH))


def legacy() -> Tuple[Optional[dict], bool]:
    return load_legacy(*_stats(LEGACY_PATH))


def health() -> Optional[dict]:
    return load_health(*_stats(HEALTH_PATH))


def _load_export_tab_inputs() -> Tuple[Optional[dict], Optional[dict], Optional[dict]]:
    """(committee, trading_export, health) lus depuis le disque.

    [AUDIT-F3] Ces artefacts sont ecrits par l'ingesteur ; on les relit a
    chaque rendu pour refleter l'etat reel du disque, sans cache (l'onglet
    Export est peu visite et la coherence prime sur le cout).
    """
    com = pc.read_json(COMMITTEE_PATH)
    exp = pc.read_json(EXPORT_PATH)
    hp = pc.read_json(HEALTH_PATH)
    return (com if isinstance(com, dict) else None,
            exp if isinstance(exp, dict) else None,
            hp if isinstance(hp, dict) else None)


def disk_bytes(path: Path, fallback: Optional[Any] = None) -> bytes:
    """Octets EXACTS du disque (audit : un export ne se re-sérialise pas)."""
    try:
        return path.read_bytes()
    except OSError:
        if fallback is None:
            return b"{}"
        return json.dumps(fallback, indent=2, ensure_ascii=False).encode("utf-8")


# ═══════════════════════════════════════════════════════════════════════════
# PROJECTION DE VUE (countdowns recalculés à l'instant du rendu)
# ═══════════════════════════════════════════════════════════════════════════
@dataclass
class Filters:
    currencies: Tuple[str, ...] = ()
    impacts: Tuple[str, ...] = ("HIGH", "MEDIUM")
    query: str = ""
    horizon_h: int = 168
    upcoming_only: bool = False
    show_pairs: bool = False
    limit: int = 400


def _fmt_delta(hours: float) -> str:
    total_min = int(round(abs(hours) * 60))
    d, rem = divmod(total_min, 1440)
    h, m = divmod(rem, 60)
    if d:
        body = f"{d}j {h:02d}h"
    elif h:
        body = f"{h}h {m:02d}m"
    else:
        body = f"{m}m"
    return body if hours > 0 else f"−{body}"


def _raw_of(node: Any) -> Any:
    """Valeur brute d'un champ numérique canonique ({"raw": …}). Tolère un
    ancien schéma où le champ est déjà un scalaire : un artefact/seed périmé ne
    doit jamais lever d'exception dans le rendu."""
    if isinstance(node, dict):
        return node.get("raw")
    return node if node not in ("", None) else None


# ═══════════════════════════════════════════════════════════════════════════
# FUSEAU D'AFFICHAGE — un choix de VUE, jamais celui du système
# ═══════════════════════════════════════════════════════════════════════════
# [2.3] Le backend reste en UTC ; le fuseau d'affichage est une décision de
# VUE. Historiquement, ``calendar_core`` résolvait le fuseau via
# ``resolve_display_tz``, qui place le fuseau SYSTÈME avant le repli : sur
# Streamlit Cloud (Etc/UTC) toutes les cartes affichaient « UTC », et le seed
# généré sur le poste du propriétaire embarquait SON fuseau — le même artefact
# s'affichait donc à des heures différentes selon sa provenance.
# Désormais l'affichage est TOUJOURS dérivé de ``dt_utc`` avec un fuseau choisi
# explicitement ici, et le JSON sur disque n'est pas modifié.
DEFAULT_VIEW_TZ = "Africa/Casablanca"

# [F1-voyage] Le propriétaire se déplace : un liste FERMÉE de 8 villes le
# laissait sans recours à Riyad, Singapour, São Paulo ou Toronto — toutes
# pourtant présentes dans tzdata (599 zones). Le sélecteur propose désormais
# une liste courte de destinations courantes ET la base IANA complète, que
# ``st.selectbox`` rend filtrable au clavier. Choisir une ville n'est donc
# jamais impossible : au pire, on tape son nom.
VIEW_TZ_COMMON = (
    "Africa/Casablanca",
    "UTC",
    "Europe/Paris",
    "Europe/London",
    "America/New_York",
    "Asia/Tokyo",
    "Asia/Dubai",
    "Australia/Sydney",
)
# Étiquette lisible : « Asia/Singapore — UTC+8 ». L'offset est calculé sur
# l'instant COURANT, donc juste en heure d'été comme en heure d'hiver.
_TZ_OFFSET_CACHE: Dict[str, str] = {}


def available_view_tzs() -> List[str]:
    """Toutes les zones IANA utilisables, triées, avec les choix courants
    d'abord. Repli sur la liste courte si tzdata est indisponible."""
    try:
        keys = sorted(zoneinfo.available_timezones())
    except Exception:                                          # noqa: BLE001
        keys = []
    if not keys:
        return list(VIEW_TZ_COMMON)
    common = [t for t in VIEW_TZ_COMMON if t in keys]
    return common + [t for t in keys if t not in set(common)]


def tz_display_label(key: str, now: Optional[datetime] = None) -> str:
    """« Asia/Singapore — UTC+8 ». Un nom seul ne dit pas l'heure qu'il est."""
    if key not in _TZ_OFFSET_CACHE:
        try:
            ref = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo(key))
            _TZ_OFFSET_CACHE[key] = _CC_OFFSET_LABEL(ref)
        except Exception:                                      # noqa: BLE001
            _TZ_OFFSET_CACHE[key] = ""
    off = _TZ_OFFSET_CACHE[key]
    return f"{key} — {off}" if off else key


def _CC_OFFSET_LABEL(dt: datetime) -> str:
    """« UTC+8 », « UTC+5:30 », « UTC-4 ». Jamais codé en dur."""
    offset = dt.utcoffset()
    if offset is None:
        return "UTC"
    minutes = int(offset.total_seconds() // 60)
    sign = "+" if minutes >= 0 else "-"
    hh, mm = divmod(abs(minutes), 60)
    return f"UTC{sign}{hh}" + (f":{mm:02d}" if mm else "")


def resolve_view_tz(explicit: Optional[str] = None) -> Tuple[str, str]:
    """(clé IANA, origine) du fuseau d'AFFICHAGE de cette page.

    Ordre : ``explicit`` (widget sidebar) > ``BLUESTAR_DISPLAY_TZ`` >
    ``DEFAULT_VIEW_TZ``. Le fuseau SYSTÈME n'est JAMAIS consulté : c'est lui
    qui faisait afficher « UTC » sur le serveur Cloud et une heure locale sur
    le poste du propriétaire, pour le même artefact.
    """
    for token, origin in ((explicit, "explicit"),
                          (os.environ.get("BLUESTAR_DISPLAY_TZ"), "env")):
        if not token:
            continue
        key = _valid_tz_key(token)
        if key:
            return key, origin
    return DEFAULT_VIEW_TZ, "fallback"


def _suggested_view_tz() -> Optional[str]:
    """Fuseau de l'APPAREIL de l'utilisateur, s'il diffère du fuseau affiché.

    [F1-voyage] Le fuseau système du SERVEUR ne doit jamais décider de
    l'affichage (c'est le bug d'origine : le conteneur Cloud affichait UTC).
    Mais celui du NAVIGATEUR de l'utilisateur, lui, est une information
    légitime : quand le propriétaire se déplace, Streamlit l'expose via
    ``st.context.timezone``. On ne l'applique PAS d'office — un changement
    d'heure silencieux serait pire qu'un défaut stable — on le PROPOSE.

    Retourne ``None`` si l'information est absente, invalide, ou identique.
    """
    try:
        raw = getattr(st.context, "timezone", None)
    except Exception:                                          # noqa: BLE001
        return None
    if not raw:
        return None
    return _valid_tz_key(str(raw))


def _valid_tz_key(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    raw = str(token).strip()
    try:
        ZoneInfo(raw)
        return raw
    except Exception:                                         # noqa: BLE001
        pass
    try:
        import calendar_compat as _cc
        return _cc.normalize_tz_token(raw)
    except Exception:                                         # noqa: BLE001
        return None


def _tz_short_label(key: str) -> str:
    return (key.split("/")[-1].replace("_", " ") if key else "UTC")


def enrich_events(payload: Optional[dict], now: datetime,
                  display_tz: Optional[str] = None) -> List[dict]:
    """Aplati le payload canonique en lignes de VUE.

    Le ``time_context`` stocké dans l'artefact est daté de l'ingestion : les
    countdowns sont TOUJOURS recalculés ici, sinon l'écran mentirait de
    plusieurs minutes.

    [2.3] ``date_display``, ``hm``, ``tz_label`` et ``day_of_week`` sont
    dérivés de ``dt_utc`` converti dans ``display_tz`` — un choix de VUE. Le
    jour vient de la table fixe de calendar_core (``day_name``), pas de
    ``strftime("%A")`` qui dépend de la locale du conteneur.
    """
    if not payload:
        return []
    tz_key, _origin = resolve_view_tz(display_tz)
    try:
        tz = ZoneInfo(tz_key)
    except Exception:                                         # noqa: BLE001
        tz_key, tz = "UTC", ZoneInfo("UTC")
    tz_label = _tz_short_label(tz_key)

    out: List[dict] = []
    for ev in (payload.get("events") or []):
        if not isinstance(ev, dict):
            continue
        raw_dt = ev.get("scheduled_at_utc") or ""
        try:
            dt_utc = datetime.fromisoformat(str(raw_dt).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=UTC)

        local = dt_utc.astimezone(tz)
        hours = (dt_utc - now).total_seconds() / 3600.0
        out.append({
            "occurrence_id": ev.get("occurrence_id"),
            "name": ev.get("name") or "",
            "currency": (ev.get("currency") or "—").upper(),
            "impact": (ev.get("impact") or "UNKNOWN").upper(),
            "session": (ev.get("session") or "").replace("_", " ").title(),
            "dt_utc": dt_utc,
            "hm": local.strftime("%H:%M"),
            "tz_label": tz_label,
            "tz_key": tz_key,
            "date_display": local.strftime("%Y-%m-%d"),
            "day_of_week": day_name(local).title(),
            "forecast": _raw_of(ev.get("forecast")),
            "previous": _raw_of(ev.get("previous")),
            "actual": _raw_of(ev.get("actual")),
            "actual_status": ev.get("actual_status"),
            "hours_until": round(hours, 4),
            "countdown": _fmt_delta(hours),
            "pairs": list(ev.get("pairs_with_currency_exposure") or []),
        })
    out.sort(key=lambda e: e["dt_utc"])
    return out


def apply_filters(events: List[dict], f: Filters, now: datetime) -> List[dict]:
    q = f.query.strip().lower()
    kept: List[dict] = []
    for e in events:
        if f.impacts and e["impact"] not in f.impacts:
            continue
        if f.currencies and e["currency"] not in f.currencies:
            continue
        if f.upcoming_only and e["hours_until"] <= 0:
            continue
        if e["hours_until"] > f.horizon_h:
            continue
        if q and q not in e["name"].lower() and q not in e["currency"].lower():
            continue
        kept.append(e)
    return kept[: f.limit]


# ═══════════════════════════════════════════════════════════════════════════
# BLOCS D'INTERFACE
# ═══════════════════════════════════════════════════════════════════════════
def serving_state(payload: Optional[dict], is_seed: bool) -> Tuple[str, str, bool]:
    """(label, couleur, pulsation) — état de service lisible en un coup d'œil."""
    if pc.ingest_disabled():
        return "MODE LECTEUR", U.MUTED, False
    if pc.is_rate_limited(DATA_DIR):
        return "COOLDOWN 429", U.WARN, False
    if INGESTION.running:
        return "INGESTION", U.ACCENT, True
    if is_seed:
        return "SEED · COLD START", U.WARN, False
    if payload:
        age = artifact_age_s()
        if age is not None and age > pc.DEFAULT_POLICY.max_source_age_seconds:
            return "PÉRIMÉ · RAFRAÎCHISSEMENT", U.WARN, False
        return "LIVE", U.OK, True
    return "INDISPONIBLE", U.BAD, False


def render_hero(payload: Optional[dict], is_seed: bool) -> None:
    label, color, live = serving_state(payload, is_seed)
    src = (payload or {}).get("source", {})
    quality = (payload or {}).get("quality", {})
    pills = [
        U.pill(label, color, live=live),
        U.pill(f"{len((payload or {}).get('events', []))} événements", U.MUTED),
        U.pill(quality.get("status", "—"),
               U.QUALITY_COLOR.get(quality.get("status", ""), U.GHOST)),
    ]
    U.render(U.hero(
        "Forex Factory · Fair Economy",
        "Pipeline",
        "Calendar",
        "Calendrier macroéconomique normalisé — fenêtre de veille glissante, "
        "horodatage UTC en backend, affichage en heure locale de place. "
        f"Source : {src.get('provider') or 'flux public hebdomadaire'}.",
        pills,
    ))


@st.fragment(run_every=20)
def next_publications(events: List[dict]) -> Tuple[Optional[dict], Optional[dict]]:
    """(prochain événement, prochain HIGH).

    [2.9b] Le KPI « Prochaine publication » prenait le premier HIGH même si un
    MEDIUM arrivait avant : il annonçait donc la mauvaise échéance. On retourne
    les deux, et la carte affiche le vrai prochain événement, avec le prochain
    HIGH en sous-titre.
    """
    upcoming = [e for e in events if e["hours_until"] > 0]
    if not upcoming:
        return None, None
    nxt = upcoming[0]                                   # deja trie par dt_utc
    nxt_high = next((e for e in upcoming if e["impact"] == "HIGH"), None)
    return nxt, nxt_high


def render_live_strip() -> None:
    """Bandeau KPI auto-rafraîchi (20 s) : relit les stats fichier à chaque
    passe, donc capte l'écriture atomique du thread de fond SANS rerun global."""
    INGESTION.kick()
    payload, is_seed = canonical()
    now = datetime.now(UTC)
    if not payload:
        U.render(U.empty("Aucun artefact disponible",
                         "L'ingestion initiale est en cours — le bandeau se "
                         "remplira automatiquement dès le premier cycle publié."))
        return

    tz_key = st.session_state.get("_view_tz") or resolve_view_tz()[0]
    events = enrich_events(payload, now, display_tz=tz_key)
    upcoming = [e for e in events if e["hours_until"] > 0]
    high = [e for e in upcoming if e["impact"] == "HIGH"]
    nxt, nxt_high = next_publications(events)

    quality = payload.get("quality", {}) or {}
    score = float(quality.get("data_quality_score") or 0.0)
    status = quality.get("status", "—")

    # [2.4] L'âge RÉEL de l'artefact servi, calculé sur son horodatage de
    # contenu (fetched_at_utc pris à la réception de la réponse) — pas sur le
    # mtime du fichier, que la restauration d'un conteneur Cloud remet à
    # « maintenant ».
    age_s = artifact_age_s()

    imminent = sum(1 for e in upcoming if e["hours_until"] <= 6)
    sub = "aucun événement à venir"
    if nxt:
        sub = f"{nxt['currency']} · {nxt['name'][:34]}"
        if nxt_high and nxt_high["occurrence_id"] != nxt["occurrence_id"]:
            sub += f" · HIGH dans {nxt_high['countdown']}"
    cards = [
        U.kpi("Prochaine publication",
              nxt["countdown"] if nxt else "—",
              sub,
              color=U.IMPACT.get(nxt["impact"], U.ACCENT) if nxt else None,
              mono=True, delay_ms=0),
        U.kpi("Fenêtre active", f"{len(upcoming)}",
              f"{imminent} dans les 6 h · {len(high)} à fort impact",
              bar=min(1.0, len(upcoming) / 40.0), delay_ms=40),
        U.kpi("Qualité artefact", str(status), f"score {score:.3f}",
              color=U.QUALITY_COLOR.get(status, U.GHOST), bar=score, delay_ms=80),
        U.kpi("Fraîcheur artefact",
              _fmt_age(age_s),
              f"hash {(payload.get('content_hash') or '').split(':')[-1][:12] or '—'}",
              mono=True,
              color=U.OK if (age_s is not None and age_s < 900) else U.WARN,
              delay_ms=120),
    ]
    U.render(U.kpi_grid(cards))

    alerts, infos = split_warnings(quality.get("warnings"))
    if is_seed:
        U.render(U.note(
            "<b>Données semence.</b> Le disque était vide au démarrage "
            "(cold-start Streamlit Cloud) : la vue s'appuie sur "
            "<code>seed/calendar.latest.seed.json</code> jusqu'au premier cycle frais.",
            "warn"))
    elif alerts:
        # [2.6] Les warnings viennent de la source : ils passent par escape.
        head = escape_html(", ".join(w.split(":")[0] for w in alerts[:4]))
        U.render(U.note(
            f"<b>{len(alerts)} signal(aux) qualité.</b> {head} — détail dans "
            "l'onglet Diagnostics.",
            "warn" if status != "INVALID" else "bad"))
    elif infos:
        # [2.1] Une information n'est pas une alerte : pas de bandeau jaune.
        head = escape_html(", ".join(w.split(":")[0] for w in infos[:3]))
        U.render(U.note(
            f"<b>Information source.</b> {head} — la source ne publie pas ces "
            "valeurs ; détail dans l'onglet Diagnostics.", ""))

    # [refresh auto] voir _sync_served_fid (rerun global après fin de cycle).
    _sync_served_fid(payload)


def _fmt_age(seconds: Optional[float]) -> str:
    """Âge lisible : 45s, 12m, 3h, 2j."""
    if seconds is None:
        return "—"
    s = int(seconds)
    if s < 90:
        return f"{s}s"
    if s < 5400:
        return f"{s // 60}m"
    if s < 172800:
        return f"{s // 3600}h"
    return f"{s // 86400}j"


def _sync_served_fid(payload: Optional[dict]) -> None:
    """Rerun global unique quand un artefact plus frais que celui servi arrive
    (cycle de fond ou scan d'un autre onglet), UNE FOIS le cycle terminé.
    Le premier passage mémorise l'identifiant servi (pas de rerun à froid)."""
    if not payload:
        return
    src = payload.get("source", {}) or {}
    fid = src.get("fetched_at_utc") or payload.get("generated_at_utc")
    if not fid:
        return
    prev = st.session_state.get("_served_fid")
    if not prev:
        st.session_state["_served_fid"] = fid
    elif prev != fid and not INGESTION.running:
        st.session_state["_served_fid"] = fid
        st.rerun()


def _needs_fast_poll() -> bool:
    """Cycle en cours, ou artefact périmé alors qu'un fetch est permis :
    on surveille à 3 s (lecture fichier seule) au lieu de 20 s."""
    if pc.ingest_disabled():
        return False
    if INGESTION.running:
        return True
    if pc.is_rate_limited(DATA_DIR):
        return False
    return not CANONICAL_PATH.exists() or not _artifact_is_fresh()


def _watcher_body() -> None:
    """Veilleur de reprise : à chaque passe il relance un cycle si le backoff
    l'autorise, explique l'état (raison de l'échec, prochaine tentative) et
    déclenche le rerun global dès que l'artefact frais est publié — sans
    attendre le tick de 20 s du bandeau."""
    INGESTION.kick()
    payload, _ = canonical()
    _sync_served_fid(payload)
    age = artifact_age_s()
    if age is not None and age < pc.MIN_FETCH_SPACING_S:
        return
    if INGESTION.running:
        U.render(U.note("<b>Rafraîchissement en cours…</b> la vue affichée "
                        "est le dernier artefact disponible.", "warn"))
    elif INGESTION.last_error:
        wait = max(0, int(INGESTION.next_attempt_at - time.time()))
        U.render(U.note(
            f"<b>Dernier cycle en échec.</b> <code>{escape_html(INGESTION.last_error[:160])}</code>"
            f" — nouvelle tentative dans {wait}s.", "bad"))


def render_watcher() -> None:
    # run_every est fixé à la déclaration : choisi à chaque rerun COMPLET.
    # Aucun timer quand tout est frais (le bandeau de 20 s suffit).
    if _needs_fast_poll():
        st.fragment(run_every=3)(_watcher_body)()


def render_sidebar(events: List[dict]) -> Filters:
    with st.sidebar:
        U.render(
            '<div style="display:flex;align-items:center;gap:10px;margin-bottom:2px;">'
            f'<div style="width:26px;height:26px;border-radius:8px;'
            f'background:linear-gradient(135deg,{U.ACCENT},#34D399);opacity:.9"></div>'
            '<div><div style="font-weight:750;letter-spacing:-.03em;font-size:.95rem">BLUESTAR</div>'
            f'<div style="font-size:.62rem;letter-spacing:.18em;color:{U.GHOST}">'
            'CALENDAR ENGINE</div></div></div>'
        )
        st.markdown("")

        ccy_pool = sorted({e["currency"] for e in events}) if events else []
        currencies = st.multiselect("Devises", ccy_pool, default=[],
                                    placeholder="Toutes les devises")
        # [2.9a] Les options d'impact sont les niveaux RÉELLEMENT présents dans
        # les données : proposer LOW/HOLIDAY/UNKNOWN alors que la policy ne
        # retient que HIGH+MEDIUM affichait des cases toujours vides. Le
        # default reste VALIDE (filtré sur les options) : c'était la leçon du
        # StreamlitAPIException — un default réclamant une option absente fait
        # planter le widget.
        present = sorted({e["impact"] for e in events if e.get("impact")}) if events else []
        impact_options = present or list(IMPACT_LABELS)
        impact_default = [i for i in ("HIGH", "MEDIUM") if i in impact_options]
        impacts = st.multiselect("Niveaux d'impact", impact_options,
                                 default=impact_default,
                                 placeholder="Tous les niveaux")
        query = st.text_input("Recherche", value="", placeholder="CPI, NFP, rate…")
        horizon = st.slider("Horizon (heures)", 6, 168, 168, step=6)
        upcoming_only = st.toggle("À venir uniquement", value=False)
        show_pairs = st.toggle("Afficher les paires exposées", value=False)

        # [2.3] Le fuseau d'affichage est un choix de VUE. Le backend reste en
        # UTC ; l'artefact sur disque n'est jamais modifié. Le défaut vient de
        # BLUESTAR_DISPLAY_TZ, sinon Africa/Casablanca — jamais du système.
        # [F1-voyage] La liste couvre la base IANA complete (599 zones) : en
        # voyage, n'importe quelle ville reste atteignable. Les destinations
        # courantes sont en tete ; ``st.selectbox`` filtre a la frappe.
        # L'étiquette porte l'offset RÉEL (« Asia/Singapore — UTC+8 »), donc
        # elle reste juste en heure d'été comme en heure d'hiver, et le nom
        # seul ne laisse pas deviner l'heure qu'il est.
        chosen = st.session_state.get("_view_tz")
        default_tz, tz_origin = resolve_view_tz(chosen)
        all_tzs = available_view_tzs()
        if default_tz not in all_tzs:
            all_tzs.insert(0, default_tz)
        labels = {tz: tz_display_label(tz) for tz in all_tzs}
        label_list = [labels[tz] for tz in all_tzs]
        label_to_tz = {labels[tz]: tz for tz in all_tzs}

        # [BUG] Sans ``key``, Streamlit ne peut pas rattacher le widget a son
        # etat d'un rerun a l'autre : le selectbox repartait de ``index=`` a
        # CHAQUE passage, et le choix de l'utilisateur etait silencieusement
        # ramene a Casablanca. Le selecteur semblait fonctionner (on pouvait
        # cliquer) mais ne changeait jamais l'affichage. La cle est desormais
        # stable et le widget est la SEULE source de verite de son etat.
        picked = st.selectbox(
            "Fuseau d'affichage",
            label_list,
            index=label_list.index(labels[default_tz]),
            key="_tz_select",
            help="Affichage uniquement — le fichier JSON reste en UTC. "
                 f"Tapez pour filtrer parmi {len(all_tzs)} fuseaux "
                 f"(origine : {tz_origin}).")
        tz_choice = label_to_tz.get(picked, default_tz)
        st.session_state["_view_tz"] = tz_choice
        # auto-détection du voyage : proposée, jamais imposée silencieusement
        suggested = _suggested_view_tz()
        if suggested and suggested != tz_choice and st.button(
                f"Utiliser le fuseau de cet appareil ({tz_display_label(suggested)})",
                key="_tz_suggest", width="stretch"):
            st.session_state["_view_tz"] = suggested
            st.rerun()

        st.markdown("")
        if st.button("Relancer l'ingestion", type="primary", width="stretch"):
            _scan_and_rerun("_kick_msg")

        if msg := st.session_state.pop("_kick_msg", None):
            U.render(U.note(f"<b>{msg[0]}</b>", msg[1]))

        U.render(
            f'<div style="margin-top:14px;font-size:.66rem;color:{U.GHOST};'
            f'font-family:{U.MONO};line-height:1.7">'
            f'DATA_DIR · {DATA_DIR.name}/<br>'
            f'SPACING · {pc.MIN_FETCH_SPACING_S}s<br>'
            f'SCHEMA · {pc.SCHEMA_VERSION}</div>'
        )

    return Filters(
        currencies=tuple(currencies),
        impacts=tuple(impacts) if impacts else tuple(IMPACT_LABELS),
        query=query,
        horizon_h=int(horizon),
        upcoming_only=upcoming_only,
        show_pairs=show_pairs,
    )


def feed_blocks(events: List[dict], f: Filters) -> List[str]:
    """Blocs HTML du flux, groupés par date LOCALE (fuseau de la vue)."""
    groups: Dict[str, List[dict]] = {}
    for e in events:
        groups.setdefault(e["date_display"], []).append(e)

    blocks: List[str] = []
    for day in sorted(groups):
        rows = groups[day]
        label = f"{rows[0]['day_of_week']} · {day}" if rows[0]["day_of_week"] else day
        cards = [U.event_card(e, show_pairs=f.show_pairs, delay_ms=min(i * 22, 260))
                 for i, e in enumerate(rows)]
        blocks.append(U.day_header(label, len(rows)) + U.feed(cards))
    return blocks


def render_feed(events: List[dict], f: Filters) -> None:
    if not events:
        U.render(U.empty(
            "Aucun événement sur ce périmètre",
            "Élargissez l'horizon ou les niveaux d'impact dans le panneau latéral. "
            "Un calendrier creux est un fait de marché, pas une panne."))
        return
    U.render(*feed_blocks(events, f))


# ═══════════════════════════════════════════════════════════════════════════
# [2.5] FLUX AUTO-RAFRAÎCHI
# ═══════════════════════════════════════════════════════════════════════════
# Seul le bandeau KPI était un fragment : les cartes, la grille et les états
# « imminent » / « publié » ne se recalculaient qu'au rerun complet, donc le KPI
# et le flux pouvaient se contredire (un événement « publié » dans le flux
# pendant que le KPI le comptait encore « dans 2 min »).
# Le rendu du flux est desormais un fragment qui recalcule ``now`` a chaque
# passe et reapplique les filtres. Les filtres sont passes en ARGUMENT (un
# fragment ne relit pas le scope appelant) ; un changement de widget provoque
# de toute facon un rerun complet, qui recree le fragment.
FEED_REFRESH_S = 30


@st.fragment(run_every=FEED_REFRESH_S)
def render_feed_fragment(filters: "Filters") -> None:
    payload, is_seed = canonical()
    now = datetime.now(UTC)
    tz_key = st.session_state.get("_view_tz") or resolve_view_tz()[0]
    all_events = enrich_events(payload, now, display_tz=tz_key)
    view = apply_filters(all_events, filters, now)
    U.render(
        f'<div style="display:flex;gap:8px;flex-wrap:wrap;margin:2px 0 4px">'
        f'{U.pill(f"{len(view)} / {len(all_events)} événements", U.MUTED)}'
        f'{U.pill(f"horizon {filters.horizon_h} h", U.MUTED)}'
        f'{U.pill("filtres de vue uniquement", U.ACCENT)}'
        f'{U.pill(tz_key, U.MUTED)}</div>'
    )
    render_feed(view, filters)


@st.fragment(run_every=FEED_REFRESH_S)
def render_grid_fragment(filters: "Filters") -> None:
    payload, _ = canonical()
    now = datetime.now(UTC)
    tz_key = st.session_state.get("_view_tz") or resolve_view_tz()[0]
    all_events = enrich_events(payload, now, display_tz=tz_key)
    view = apply_filters(all_events, filters, now)
    with st.container(border=True):
        U.panel_title("Grille dense", "vue tabulaire")
        st.dataframe(
            render_grid_rows(view),
            column_config={
                "Date": st.column_config.TextColumn(width="small"),
                "Heure": st.column_config.TextColumn(width="small"),
                "TZ": st.column_config.TextColumn(width="small"),
                "Devise": st.column_config.TextColumn(width="small"),
                "Impact": st.column_config.TextColumn(width="small"),
                "Événement": st.column_config.TextColumn(width="large"),
                "Échéance": st.column_config.TextColumn(width="small"),
            },
            hide_index=True, height=420, width="stretch",
        )


def render_grid_rows(events: List[dict]) -> List[dict]:
    """Lignes de la grille dense.

    [2.1] La colonne Actual affiche « n/d source » quand la source ne publie
    pas ce champ (statut UNSUPPORTED_BY_SOURCE) au lieu d'un tiret ambigu.
    """
    rows = []
    for e in events:
        if e.get("actual_status") == "UNSUPPORTED_BY_SOURCE":
            actual = U.ACTUAL_UNSUPPORTED_LABEL
        else:
            actual = e["actual"] or "—"
        rows.append({
            "Date": e["date_display"], "Heure": e["hm"], "TZ": e["tz_label"],
            "Devise": e["currency"], "Impact": e["impact"], "Événement": e["name"],
            "Forecast": e["forecast"] or "—", "Previous": e["previous"] or "—",
            "Actual": actual, "Session": e["session"],
            "Échéance": e["countdown"],
        })
    return rows


def render_analytics(events: List[dict]) -> None:
    if not U.AVAILABLE:
        U.render(U.note("<b>Plotly absent de l'environnement.</b> "
                        "Ajoutez <code>plotly</code> à requirements.txt pour "
                        "activer les visualisations.", "warn"))
        return
    if not events:
        U.render(U.empty("Rien à visualiser", "Aucun événement sur le périmètre courant."))
        return

    # [INCIDENT-ANALYTICS] Les constructeurs passent par U._safe_chart : un
    # echec de plotly (cf. le piege pandas reproduit dans ui.py) rend None au
    # lieu de propager un AttributeError qui tuait tout l'onglet. L'appelant
    # testait deja ``if fig:``, le contrat est donc inchange.
    c1, c2, c3 = st.columns([1.45, 1, 1])
    with c1, st.container(border=True):
        U.panel_title("Densité par jour", "volume · impact")
        fig = U._safe_chart(U.density_by_day, events)
        if fig:
            st.plotly_chart(fig, config=U.PLOTLY_CONFIG, width="stretch")
        else:
            U.render(U.empty("Graphique indisponible",
                             "Le détail est disponible dans les diagnostics."))
    with c2, st.container(border=True):
        U.panel_title("Répartition d'impact", "mix")
        fig = U._safe_chart(U.impact_donut, events)
        if fig:
            st.plotly_chart(fig, config=U.PLOTLY_CONFIG, width="stretch")
        else:
            U.render(U.empty("Graphique indisponible",
                             "Le détail est disponible dans les diagnostics."))
    with c3, st.container(border=True):
        U.panel_title("Charge par devise", "top 9")
        fig = U._safe_chart(U.currency_exposure, events)
        if fig:
            st.plotly_chart(fig, config=U.PLOTLY_CONFIG, width="stretch")
        else:
            U.render(U.empty("Graphique indisponible",
                             "Le détail est disponible dans les diagnostics."))




def render_summary() -> None:
    lg, from_seed = legacy()
    if not lg:
        U.render(U.empty("Résumé indisponible",
                         "L'artefact legacy v1 n'a pas encore été publié."))
        return
    meta = lg.get("metadata", {}) or {}
    if from_seed:
        U.render(U.note("<b>Résumé reconstruit depuis le seed</b> — "
                        "le format legacy v1 est régénéré en mémoire.", "warn"))

    U.render(U.kpi_grid([
        U.kpi("Horizon servi", f"{meta.get('feed_horizon_h') or '—'} h",
              str(meta.get("feed_horizon_state") or "—"), mono=True),
        U.kpi("Mode de service", str(meta.get("serving_mode") or "—"),
              f"flux OK {meta.get('feeds_ok', '—')}/{meta.get('feeds_total', '—')}"),
        U.kpi("Fort impact", str(meta.get("high_impact_count", "—")),
              f"{meta.get('engine_events_count', '—')} lignes moteur"),
        U.kpi("Imminents", str(meta.get("imminent_count", "—")),
              f"{meta.get('upcoming_count', '—')} à venir"),
    ]))

    if note_txt := meta.get("coverage_note"):
        # [2.6] coverage_note est derive de la source (noms de devises) : il
        # passe par escape, comme tout texte injecte en HTML.
        U.render(U.note(f"<b>Couverture.</b> {escape_html(str(note_txt))}"))

    summary = lg.get("summary_by_day", {}) or {}
    if not summary:
        U.render(U.empty("Aucun jour couvert", "Le résumé quotidien est vide."))
        return
    for day, items in sorted(summary.items()):
        # [2.6] Les lignes de summary_by_day contiennent le NOM de l'evenement,
        # qui vient de la source externe : elles doivent passer par escape,
        # sinon un nom d'evenement peut injecter du markup actif.
        with st.expander(f"{day}  ·  {len(items)} événement(s)"):
            U.render(
                '<div style="display:flex;flex-direction:column;gap:6px;">'
                + "".join(
                    f'<div style="font-size:.79rem;color:{U.MUTED};'
                    f'font-family:{U.MONO}">{escape_html(str(item))}</div>'
                    for item in items
                ) + "</div>"
            )


def render_exports() -> None:
    # [scanner] « je scan, il me donne le calendrier » : force le cycle, attend
    # sa fin, puis rerun — les boutons ci-dessous embarquent alors les octets
    # du cycle. Sans scan, ils servent l'artefact du dernier rerun complet,
    # qui sur Cloud est l'artefact commité (data/ reverté à chaque restart).
    if msg := st.session_state.pop("_scan_msg", None):
        U.render(U.note(f"<b>{msg[0]}</b>", msg[1]))
    if st.button("\u25b6  Scanner et servir le calendrier frais",
                 type="primary", width="stretch", key="scan_exports"):
        _scan_and_rerun("_scan_msg")

    can, _ = canonical()
    lg, _ = legacy()
    hp = health()
    com, exp, _ = _load_export_tab_inputs()

    U.render(U.note(
        "<b>Exports fidèles au disque.</b> Les boutons servent les octets "
        "exacts des artefacts publiés par l'ingesteur : ni les filtres de "
        "cette page ni le fuseau d'affichage ne peuvent altérer un fichier "
        "consommé en aval."))

    # [2.9h] calendar.latest.json et calendar.json sont écrits l'un après
    # l'autre : un téléchargement qui tomberait ENTRE deux écritures servirait
    # un couple incohérent. Les deux artefacts portent le même
    # generated_at_utc ; s'ils diffèrent, c'est qu'on est précisément dans
    # cette fenêtre — on le dit au lieu de laisser croire à la cohérence.
    gen_can = (can or {}).get("generated_at_utc")
    gen_leg = ((lg or {}).get("metadata") or {}).get("generated_at_utc")
    if gen_can and gen_leg and gen_can != gen_leg:
        U.render(U.note(
            "<b>Artefacts désynchronisés.</b> <code>calendar.latest.json</code> "
            f"({escape_html(str(gen_can))}) et <code>calendar.json</code> "
            f"({escape_html(str(gen_leg))}) ne portent pas le même "
            "<code>generated_at_utc</code> : un cycle d'ingestion est "
            "probablement en cours d'écriture. Attendez la fin du cycle avant "
            "de consommer ces fichiers en aval.", "warn"))
    st.markdown("")

    # [AUDIT-F3] ``calendar.committee.json`` et ``trading_export.json`` sont
    # PRODUITS par l'ingesteur mais n'étaient téléchargeables nulle part : un
    # consommateur aval (merge, desk) devait aller les chercher sur le disque
    # du serveur. On expose les cinq artefacts reellement publies.
    c1, c2, c3, c4, c5 = st.columns(5)
    specs = (
        (c1, "calendar.json", LEGACY_PATH, lg, "legacy v1 · format de merge", "primary"),
        (c2, "calendar.latest.json", CANONICAL_PATH, can, "canonique v2 · schéma riche", "primary"),
        (c3, "calendar.committee.json", COMMITTEE_PATH, com,
         "vue committee · hash de parité", "secondary"),
        (c4, "trading_export.json", EXPORT_PATH, exp,
         "export trading · recalculé", "secondary"),
        (c5, "health.json", HEALTH_PATH, hp or {}, "supervision · forensique", "secondary"),
    )
    for col, name, path, fallback, hint, kind in specs:
        with col, st.container(border=True):
            data = disk_bytes(path, fallback)
            available = bool(fallback) or path.exists()
            U.panel_title(name, hint)
            if available:
                st.download_button(
                    f"Télécharger · {len(data) / 1024:.1f} KiB",
                    data=data, file_name=name, mime="application/json",
                    type=kind, width="stretch", key=f"dl_{name}",
                )
                U.render(f'<div style="font-size:.66rem;color:{U.GHOST};'
                         f'font-family:{U.MONO};margin-top:8px">'
                         f'{"disque" if path.exists() else "reconstruit"} · '
                         f'{len(data)} octets</div>')
            else:
                st.button("Indisponible", disabled=True, width="stretch", key=f"na_{name}")

    if can:
        with st.expander("Aperçu du schéma canonique (métadonnées, hors événements)"):
            st.json({k: v for k, v in can.items() if k != "events"}, expanded=False)


def render_diagnostics() -> None:
    can, is_seed = canonical()
    hp = health() or {}
    tz = pc.tz_environment()
    rl = pc.read_rate_limit(DATA_DIR) or {}

    c1, c2 = st.columns(2)
    with c1, st.container(border=True):
        U.panel_title("Service", "état d'exécution")
        U.render(U.kv_table([
            ("data_dir", str(DATA_DIR)),
            ("canonical présent", CANONICAL_PATH.exists()),
            ("seed présent", SEED_PATH.exists()),
            ("sert le seed", is_seed),
            ("ingestion en cours", INGESTION.running),
            ("mode lecteur", pc.ingest_disabled()),
            ("dernière erreur UI", INGESTION.last_error),
            ("échecs consécutifs", INGESTION.fail_streak),
            ("cooldown 429 actif", pc.is_rate_limited(DATA_DIR)),
            ("cooldown jusqu'à", rl.get("blocked_until_utc")),
            ("espacement min", f"{pc.MIN_FETCH_SPACING_S}s"),
        ]))
    with c2, st.container(border=True):
        U.panel_title("Autorité des règles horaires", "audit B2")
        auth = bool(tz.get("pip_tzdata_authoritative"))
        U.render(U.note(
            "<b>tzdata pip prioritaire.</b> Les offsets affichés proviennent "
            "du paquet épinglé." if auth else
            "<b>tzdata système en tête de TZPATH.</b> Les offsets peuvent "
            "être périmés selon l'image d'exécution.",
            "ok" if auth else "warn"))
        st.markdown("")
        U.render(U.kv_table([
            ("tzdata (pip)", tz.get("tzdata_pip_version")),
            ("TZPATH[0]", tz.get("tzpath_head")),
            *[(f"Casablanca {k}", f"{v[0]:+.2f}h · {v[1]}")
              for k, v in (tz.get("casablanca_offsets") or {}).items()],
        ]))

    with st.container(border=True):
        U.panel_title("Flux sources", "statut par endpoint")
        feeds = ((can or {}).get("source", {}) or {}).get("feed_status", {}) or {}
        pills = [U.pill(f"{k} · {v}",
                        U.OK if str(v).startswith("ok")
                        else (U.WARN if "404" in str(v) else U.BAD))
                 for k, v in feeds.items()] or [U.pill("aucun statut publié", U.GHOST)]
        U.render(f'<div style="display:flex;gap:8px;flex-wrap:wrap">{"".join(pills)}</div>')
        st.markdown("")
        U.render(U.kv_table([(f"source #{i + 1}", u)
                             for i, u in enumerate(pc.SOURCE_URLS)]))

    warnings = ((can or {}).get("quality", {}) or {}).get("warnings") or []
    rejections = ((can or {}).get("quality", {}) or {}).get("rejections") or []
    alerts, infos = split_warnings(warnings)
    if alerts or infos or rejections:
        with st.container(border=True):
            U.panel_title(
                "Signaux qualité",
                f"{len(alerts)} alerte(s) · {len(infos)} info(s) · "
                f"{len(rejections)} rejet(s)")
            # [2.6] Les warnings viennent de la source : ils passent par escape.
            # [2.1] Les INFO_WARNINGS sortent du compteur d'alertes et sont
            # presentes comme des informations, pas comme des signaux.
            U.render(*[U.note(f"<code>{escape_html(w)}</code>",
                              "bad" if "INVALID" in w or "EXPIRED" in w else "warn")
                       + '<div style="height:6px"></div>' for w in alerts])
            U.render(*[U.note(f"<code>{escape_html(w)}</code>")
                       + '<div style="height:6px"></div>' for w in infos])
            if rejections:
                with st.expander(f"Lignes rejetées ({len(rejections)})"):
                    st.code("\n".join(str(r) for r in rejections), language="text")

    with st.container(border=True):
        U.panel_title("health.json", "dernier cycle")
        st.json(hp, expanded=False)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
def main() -> None:
    st.set_page_config(
        page_title="BLUESTAR · Pipeline Calendar",
        page_icon="◆",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={"about": "BLUESTAR Pipeline Calendar — calendrier "
                             "macroéconomique normalisé (Forex Factory / Fair Economy)."},
    )
    U.inject()

    # [refresh à l'ouverture] « j'ouvre le lien, c'est frais ». kick()
    # lance le cycle en arrière-plan ; si l'artefact est stale (typiquement
    # après un restart du conteneur Cloud, qui revert data/ à l'état
    # commité), on en attend la fin avant de rendre la page — les
    # téléchargements de l'onglet Export servent alors l'artefact frais
    # dès le premier rendu. Borné : un cycle trop lent sert l'état
    # précédent et le bandeau ci-dessous rafraîchit seul.
    _state = INGESTION.kick()
    if _state in ("started", "running") and _load_refresh_due():
        with st.spinner("Rafraîchissement du calendrier…"):
            INGESTION.wait_done(timeout=LOAD_REFRESH_TIMEOUT_S)

    payload, is_seed = canonical()
    now = datetime.now(UTC)

    # Le fuseau d'affichage est resolu AVANT la sidebar pour que la liste des
    # devises et des impacts soit construite dans le bon referentiel horaire.
    tz_for_sidebar = st.session_state.get("_view_tz") or resolve_view_tz()[0]
    all_events = enrich_events(payload, now, display_tz=tz_for_sidebar)

    filters = render_sidebar(all_events)
    tz_key = st.session_state.get("_view_tz") or resolve_view_tz()[0]
    # La sidebar a pu changer le fuseau : on reconstruit la vue.
    all_events = enrich_events(payload, now, display_tz=tz_key)

    render_hero(payload, is_seed)
    render_live_strip()
    render_watcher()
    st.markdown("")

    view = apply_filters(all_events, filters, now)

    tab_feed, tab_analytics, tab_summary, tab_export, tab_diag = st.tabs(
        ["Flux", "Analytique", "Résumé", "Export", "Diagnostics"]
    )
    with tab_feed:
        # [2.5] Fragment auto-rafraîchi : recalcule ``now`` et réapplique les
        # filtres, donc les états « imminent » / « publié » ne peuvent plus
        # contredire le bandeau KPI.
        render_feed_fragment(filters)
    with tab_analytics:
        render_analytics(view)
        # [2.5] La grille est rafraîchie par le meme mecanisme que le flux.
        render_grid_fragment(filters)
    with tab_summary:
        render_summary()
    with tab_export:
        render_exports()
    with tab_diag:
        render_diagnostics()

    gen = (payload or {}).get("generated_at_utc", "—")
    U.render(
        f'<div style="margin-top:34px;padding-top:14px;'
        f'border-top:1px solid {U.LINE};display:flex;justify-content:space-between;'
        f'flex-wrap:wrap;gap:10px;font-size:.68rem;color:{U.GHOST};font-family:{U.MONO}">'
        f'<span>BLUESTAR CALENDAR · core {pc.SCHEMA_VERSION}</span>'
        f'<span>généré {gen}</span>'
        f'<span>{"ingestion en cours" if INGESTION.running else "au repos"}</span></div>'
    )


if __name__ == "__main__":
    main()

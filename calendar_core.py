"""
BLUESTAR · calendar_core
========================
Logique métier pure. Aucun import Streamlit, aucun I/O réseau, aucun accès disque.
Toutes les fonctions sont déterministes : le temps est TOUJOURS injecté.

=============================================================================
v2.4.1 (15/09/2026) — CORRECTION DE L'AUDIT OPUS (CALENDAR PRO A CORRIGER/
audit OPUS-CALENDAR.md), chaque point d'abord REVÉRIFIÉ empiriquement sur le
flux live figé (105 lignes) — voir le matrix de verdicts dans le rapport :
  [B1] invariant de rejet : lignes normalisées + zéro événement + warning de
       vocabulaire = artefact de parsing, plus jamais « VALID 1.000 vide »
       (mesuré : avant, 105 rows/0 event/VALID ; après, INVALID explicite,
       l'ingestor refuse et l'artefact précédent survit). Dérive partielle →
       DEGRADED (−0.2). Semaine calme légitime → inchangée, publiable.
       MIROIR EXACT côté macro (calendar_layer v6.4, bascule actée).
  [B2] autorité tzdata : le paquet pip épinglé est placé EN TÊTE de TZPATH
       avant tout ZoneInfo() — sur une image au tzdata système antérieur,
       l'app aurait affiché de fausses heures marocaines dès le 20/09/2026
       (GMT permanent, règle encodée dans tzdata 2026.3, vérifiée ici).
       ``tz_environment()`` le prouve à froid (health.json + logs + UI).
  [COV] COVERAGE_SHORTER_THAN_HORIZON jugé sur les ÉVÉNEMENTS (ce que l'aval
       lit en data_coverage_end_utc), non sur toutes les rows : ~24 h d'angle
       mort en moins ; repli rows si events vide (semaine calme).
  [FUS] payload_sha256 = formule d'agrégation macro (jonction « | » des sha
       par flux ok) : la signature couvre enfin la FUSION consommée ;
       feed_sha256 + octets des deux flux archivés dans raw/.
Verrouillés par test_calendar_pro_locks_v241.py (15 tests, total 109) ; le
content_hash économique est resté IDENTIQUE avant/après (reconcile 19:19Z :
P1 sha256:73b8daa2…, 0 divergence) — seule la réaction à un flux empoisonné
change. Côté app/ingestor : B3 (verrou≠échec, mode lecteur
BLUESTAR_DISABLE_INGEST, exit 3), B4 (session HTTP jetable par cycle),
exports = octets du disque, format_numeric affiche le brut non numérique,
fragment auto-refresh enfin agissant, logs UTC, health.json forensique.

=============================================================================
v2.4.0 (15/09/2026) — PORT DES CORRECTIFS calendar_layer v6/v6.1/v6.2 (app
macro) vers l'app TA. Objectif : les deux modules de normalisation produisent
des LIGNES ET UNE PROJECTION ÉCONOMIQUES IDENTIQUES sur le même flux brut
(reconvergence par ``occurrence_id`` + ``content_hash`` commun, contrat
ENGINE.V10 « events_engine », ``extra=ignore`` — see RUNBOOK).
Chaque port est nommé par son tag macro d'origine :

  [F2]  fuseau d'affichage plus codé en dur : ``BLUESTAR_DISPLAY_TZ`` override,
        défaut inchangé « Africa/Casablanca » (zéro changement visible ici ;
        le HACHAGE n'y est plus sensible grâce à [F5]).
  [F3]  placeholder « — » dans les lignes legacy (forecast/previous/actual),
        aligné sur le contrat models/ENGINE._parse_ff_value qui traite déjà
        « — » comme absent (l.1318). Les ``*_value`` numériques restent None.
  [F5]  ``content_hash`` = projection économique explicite 10 champs
        (``economic_projection_v2``, méthode IDENTIQUE au macro) : insensible
        au fuseau d'affichage, à l'ordre de fusion, au locale, aux champs de
        vue. Avant ce port, changer DEFAULT_DISPLAY_TZ ou l'ordre des flux
        changeait le hash — l'objectif « les deux apps voient le même
        calendrier » était invérifiable. C'est la dette [F5] annoncée par
        l'en-tête du calendar_layer macro (« à porter dans calendar_core.py »).
  [F6]  parseur numérique : « 1,234 » = 1 234 et non 1.234 (facteur 1000 sur
        NFP/retail sans suffixe) ; statut APPROXIMATE pour < > ~ ;
        placeholders enrichis (« n.a. », « tentative »).
  [F7]  ``day_of_week`` via table fixe (plus strftime("%A"), dépendant de la
        locale du conteneur) ; ``_LEGACY_SESSION.get`` (plus KeyError si un
        membre rejoint ``Session``) ; payload surdimensionné TRONQUÉ+averti
        plutôt que ValueError → LKG périmé servi à la place d'un flux frais.
  [M3]  ``forecast_status``/``previous_status`` exportés dans les lignes
        (additif, ENGINE ignore les clés inconnues) + préfixes ≈ ≤ ≥ ± dans
        ``_NUM_RE`` : les deux modules lisent le même vocabulaire.
  [M4]  fenêtre d'ancrage conférence→décision par devise : JPY 240 min
        (mesuré : BoJ décision 02:30Z, conférence 05:30Z = 180 min > 120 ;
        la conférence BoJ n'était JAMAIS rattachée à sa décision) ; 120 min
        ailleurs (FOMC 14:00→14:30 verrouillé par test).
  [B8]  sessions ANZ : WELLINGTON (05:00-14:00 NZ) + SYDNEY (07:00-15:00 AU)
        en heures locales DST-aware ; ``SESSION_POLICY_VERSION`` → v2. Cas
        mesuré : GDP NZD mercredi 22:45Z = 10:45 Wellington / 08:45 Sydney,
        cœur de séance étiqueté « OFF » par la table à trois places.
  [compat-M2]  vocabulaire d'horizon exporté dans les métadonnées legacy,
        strictement aligné sur le macro : ``feed_horizon_h`` /
        ``feed_horizon_truncated`` (rouge SEULEMENT sur « degraded », jamais
        sur le rollover hebdo normal — la philosophie ND-013 de ce fichier,
        traduite dans les CLÉS que l'ENGINE lit déjà l.2851-2853) /
        ``feed_horizon_state`` / ``feed_start_utc`` / ``feed_end_utc`` /
        ``feed_coverage_detail`` / ``reachable`` (l'ENGINE lisait
        ``meta.get("reachable", True)`` sur un fichier qui ne l'exportait
        JAMAIS → fail-open permanent ; le flag est maintenant honnêtement
        calculé, et ``serving_mode=last_known_good`` distingue le secours).
  [compat-F7-hygiene]  ``events`` et ``events_engine`` ne sont plus le MÊME
        objet list (un consommateur qui mute l'un corrompait l'autre).
  [align-rows]  lignes legacy alignées sur le schema macro : +
        release_group_type, datetime_display, display_timezone, actual_value,
        priority (mêmes seuils 6h/48h, politique identique aux deux modules).

Versions de contrat portées : CONTENT_HASH_METHOD « economic_projection_v2 »,
SESSION_POLICY_VERSION « exchange_local_dst_aware_v2 »,
NUMERIC_PARSER_VERSION « ff_numeric_v2 » — identiques au calendar_layer macro
v6.2 (sha 2e4bf033a0c6). Fenêtre future : 192 h → 168 h (aligné macro [F7]
« la fenêtre annoncée et la fenêtre servie sont la MÊME » ; mesuré, le flux
thisweek n'atteint jamais 168 h hors dimanche : aucun événement retiré cette
semaine — verrouillé par test croisé).
=============================================================================

Contrat public :
    SelectionPolicy      - politique machine, versionnée, indépendante de l'UI
    CalendarEvent        - événement normalisé, immuable (frozen)
    CalendarPayload      - enveloppe canonique complète
    build_payload(...)   - raw source -> payload validé
    compute_time_context - champs volatils (countdown) calculés à la demande
    to_legacy_payload    - export rétro-compatible v1 pour migration
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo
import zoneinfo as _zoneinfo

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

# ─────────────────────────────────────────────────────────────────────────────
# [B2 audit OPUS 15/09/2026] SOURCE D'AUTORITÉ DES RÈGLES TZ — À EXÉCUTER
# AVANT TOUT ZoneInfo() DU MODULE.
# PEP 615 : zoneinfo consulte TZPATH (les données SYSTÈME en premier) et ne
# retombe sur le paquet PyPI `tzdata` qu'en l'absence de la zone. Le Maroc
# passe définitivement à GMT le 20/09/2026 (règle encodée dans tzdata>=2026.3)
# ; une image Debian/Alpine au tzdata système antérieur afficherait donc des
# heures fausses DÈS LE 20 SEPTEMBRE malgré le pin pip. En en-tête de TZPATH,
# le paquet épinglé devient la vérité observable, partout, reproductible.
# Try/except : sans le paquet (Windows propre, env exotique), on reste sur le
# comportement par défaut — tz_environment() le dira honnêtement.
# ─────────────────────────────────────────────────────────────────────────────
def _pin_pip_tzdata() -> None:
    try:
        import tzdata as _td
        _d = os.path.join(os.path.dirname(_td.__file__), "zoneinfo")
        if os.path.isdir(_d) and _d not in tuple(_zoneinfo.TZPATH):
            _zoneinfo.TZPATH = (_d,) + tuple(_zoneinfo.TZPATH)
    except Exception:                                       # noqa: BLE001
        pass


_pin_pip_tzdata()


def tz_environment() -> Dict[str, Any]:
    """Preuve forensique de la source des règles horaires, à journaliser au
    démarrage ET dans health.json (audit OPUS B2) : version pip, tête de
    TZPATH, et offsets Africa/Casablanca observés de part et d'autre de la
    transition marocaine du 20/09/2026 — un décalage +01:00 le 21/09 trahit
    des règles périmées, quelle que soit la machine."""
    try:
        import importlib.metadata as _md
        pip_version = _md.version("tzdata")
    except Exception:                                       # noqa: BLE001
        pip_version = None
    cas = ZoneInfo("Africa/Casablanca")
    probe = {}
    for label, d in (("2026-09-19", datetime(2026, 9, 19, 12, tzinfo=cas)),
                     ("2026-09-21", datetime(2026, 9, 21, 12, tzinfo=cas)),
                     ("2027-03-01", datetime(2027, 3, 1, 12, tzinfo=cas))):
        probe[label] = (d.utcoffset().total_seconds() / 3600.0, d.tzname())
    return {
        "tzdata_pip_version": pip_version,
        "tzpath_head": (str(_zoneinfo.TZPATH[0]) if len(_zoneinfo.TZPATH) else None),
        "pip_tzdata_authoritative": bool(len(_zoneinfo.TZPATH)) and pip_version is not None
        and str(_zoneinfo.TZPATH[0]).replace("\\", "/").endswith("tzdata/zoneinfo"),
        "casablanca_offsets": probe,
    }


# ─────────────────────────────────────────────────────────────────────────────
# VERSIONS DE CONTRAT — toute rupture doit incrémenter la majeure
# ─────────────────────────────────────────────────────────────────────────────
SCHEMA_VERSION = "2.4.1"  # 2.4.1 : audit OPUS — invariant B1 (rejet total vocabulaire → INVALID), autorité tzdata pip (B2), hash de fusion multi-flux aligné sur la formule macro, coverage courte jugée sur EVENTS. v2.5.0 (release) = overlay « actuals » FOREX FACTORY en VUE + helpers de jointure : schéma canonique VOLONTAIREMENT inchangé (le content_hash inter-apps ne doit pas bouger d'un iota quand un actual tombe).
                          # 2.4.0 : PORT calendar_layer v6.1/v6.2 (F2/F3/F5/F6/F7/M3/M4/B8 + vocab horizon compatible) — voir en-tête
                          # 2.3.0 : rollover hebdo neutre (ND-013) — COVERAGE_SHORTER_THAN_HORIZON + DEGRADED seulement si cause anormale ; flag meta week_rollover_pending
                          # 2.1.0 : ajout additif de CoverageInfo (aucune rupture v2.0.0)
PAIR_MAPPING_METHOD = "static_currency_membership_v1"
SESSION_POLICY_VERSION = "exchange_local_dst_aware_v2"  # v2 : +WELLINGTON/SYDNEY (port B8)
NUMERIC_PARSER_VERSION = "ff_numeric_v2"                # v2 : milliers + préfixes ≈≤≥± + APPROXIMATE (port F6/M3)
CONTENT_HASH_METHOD = "economic_projection_v2"          # identique au calendar_layer macro [F5]

UTC = timezone.utc

TZ_LONDON = ZoneInfo("Europe/London")
TZ_NEW_YORK = ZoneInfo("America/New_York")
TZ_TOKYO = ZoneInfo("Asia/Tokyo")
# [F2 port] plus de fuseau figé : override BLUESTAR_DISPLAY_TZ. La VALEUR PAR
# DÉFAUT reste celle de l'app TA (Africa/Casablanca) — aucun changement
# visible ici ; ce qui change, c'est que le content_hash [F5] ne dépend plus
# du fuseau et que l'app macro peut réconcilier ses vues avec les nôtres.
DEFAULT_DISPLAY_TZ = os.environ.get("BLUESTAR_DISPLAY_TZ", "Africa/Casablanca")

# Heures locales des places financières (politique explicite et versionnée).
# On ne fige JAMAIS d'heures UTC : zoneinfo applique le DST de chaque place,
# y compris pendant les périodes où Londres et New York ne sont pas alignés.
SESSION_HOURS = {
    "LONDON": (TZ_LONDON, 8 * 60, 16 * 60 + 30),
    "NEW_YORK": (TZ_NEW_YORK, 8 * 60, 17 * 60),
    "TOKYO": (TZ_TOKYO, 8 * 60, 17 * 60),
    # [B8 port] places ANZ (mesuré : GDP NZD mercredi 22:45Z = 10:45
    # Wellington / 08:45 Sydney — cœur de séance étiqueté « OFF » par la
    # table à trois places). Heures LOCALES, DST appliquée par zoneinfo.
    "WELLINGTON": (ZoneInfo("Pacific/Auckland"), 5 * 60, 14 * 60),
    "SYDNEY": (ZoneInfo("Australia/Sydney"), 7 * 60, 15 * 60),
}

# [F7 port] table fixe : strftime("%A") dépend de la locale du conteneur
# (« JEUDI » sur l'une, « THURSDAY » sur l'autre → même événement, deux
# représentations). Index = weekday() (0 = lundi).
_DAY_NAMES = ("MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY",
              "FRIDAY", "SATURDAY", "SUNDAY")


def day_name(dt: datetime) -> str:
    """Nom de jour ANGLAIS, majuscule, indépendant de la locale. Source
    unique pour le core et l'app (qui recalcule une vue dérivée)."""
    return _DAY_NAMES[dt.weekday()]

# ─────────────────────────────────────────────────────────────────────────────
# MATRICE PAIRES — dérivée mécaniquement, pas saisie à la main
# ─────────────────────────────────────────────────────────────────────────────
G10_PAIRS: Tuple[str, ...] = (
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "USD/CAD", "AUD/USD", "NZD/USD",
    "EUR/GBP", "EUR/JPY", "EUR/CHF", "EUR/CAD", "EUR/AUD", "EUR/NZD",
    "GBP/JPY", "GBP/CHF", "GBP/CAD", "GBP/AUD", "GBP/NZD",
    "AUD/JPY", "AUD/CHF", "AUD/CAD", "AUD/NZD",
    "NZD/JPY", "NZD/CHF", "NZD/CAD",
    "CAD/JPY", "CAD/CHF", "CHF/JPY",
)
EXTRA_PAIRS: Dict[str, Tuple[str, ...]] = {"CNY": ("USD/CNY", "EUR/CNY")}

KNOWN_CURRENCIES: Tuple[str, ...] = (
    "USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF", "CNY",
)
GLOBAL_COUNTRY_TOKENS = {"ALL", "GLOBAL", "WORLD", ""}


def pairs_for_currency(ccy: str) -> List[str]:
    """Appartenance mécanique de la devise à la paire. Aucune notion de causalité."""
    ccy = ccy.upper()
    out = [p for p in G10_PAIRS if ccy in p.split("/")]
    out.extend(p for p in EXTRA_PAIRS.get(ccy, ()) if p not in out)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# ENUMS
# ─────────────────────────────────────────────────────────────────────────────
class Impact(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    HOLIDAY = "HOLIDAY"
    UNKNOWN = "UNKNOWN"


class Session(str, Enum):
    ASIAN = "ASIAN"
    LONDON = "LONDON"
    NEW_YORK = "NEW_YORK"
    OVERLAP_ASIA_LONDON = "OVERLAP_ASIA_LONDON"
    OVERLAP_LONDON_NY = "OVERLAP_LONDON_NY"
    OFF = "OFF"


class TimeProximity(str, Enum):
    IMMINENT = "IMMINENT"
    SOON = "SOON"
    LATER = "LATER"
    PAST = "PAST"


class EventStatus(str, Enum):
    SCHEDULED = "SCHEDULED"
    DUE = "DUE"
    PAST_SCHEDULE = "PAST_SCHEDULE"
    HOLIDAY = "HOLIDAY"


class ActualStatus(str, Enum):
    UNSUPPORTED_BY_SOURCE = "UNSUPPORTED_BY_SOURCE"
    NOT_YET_RELEASED = "NOT_YET_RELEASED"
    RELEASED = "RELEASED"


class PairMappingStatus(str, Enum):
    MAPPED = "MAPPED"
    NO_MAPPING_GLOBAL_EVENT = "NO_MAPPING_GLOBAL_EVENT"
    NO_MAPPING_UNKNOWN_CURRENCY = "NO_MAPPING_UNKNOWN_CURRENCY"


class QualityStatus(str, Enum):
    VALID = "VALID"
    DEGRADED = "DEGRADED"
    INVALID = "INVALID"


class ReleaseGroupType(str, Enum):
    CENTRAL_BANK_DECISION = "CENTRAL_BANK_DECISION"
    LABOR_MARKET_RELEASE = "LABOR_MARKET_RELEASE"
    SIMULTANEOUS_RELEASE = "SIMULTANEOUS_RELEASE"


IMPACT_ALIASES = {
    "HIGH": Impact.HIGH, "RED": Impact.HIGH,
    "MEDIUM": Impact.MEDIUM, "ORANGE": Impact.MEDIUM, "MED": Impact.MEDIUM,
    "LOW": Impact.LOW, "YELLOW": Impact.LOW,
    "HOLIDAY": Impact.HOLIDAY, "NON-ECONOMIC": Impact.HOLIDAY, "GRAY": Impact.HOLIDAY,
    "GREY": Impact.HOLIDAY,
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS DÉTERMINISTES
# ─────────────────────────────────────────────────────────────────────────────
def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_hex(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(text: str) -> str:
    norm = unicodedata.normalize("NFKD", text or "")
    norm = "".join(c for c in norm if not unicodedata.combining(c))
    return _SLUG_RE.sub("-", norm.lower()).strip("-")


def parse_source_datetime(raw: Any) -> datetime:
    """Accepte 'Z', '+00:00', '-04:00', naïf (traité UTC). Retourne un aware UTC."""
    if isinstance(raw, datetime):
        dt = raw
    else:
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError("empty datetime")
        txt = raw.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


_NUM_RE = re.compile(
    # [M3 port] ≤ ≥ ± ≈ ajoutés : mêmes préfixes que le calendar_layer macro
    # et que ENGINE._parse_ff_value côté lecture. Le flux FF actuel n'émet
    # que < > ~ et nombres nus : zéro impact sur le contenu économique —
    # verrouillé par test (parité avec le macro, sha calendar_layer
    # 2e4bf033a0c6).
    r"^([<>~≈≤≥±]?)\s*(-?[\d]+(?:[.,][\d]+)*)\s*([KMBT]?)\s*(%?)$", re.IGNORECASE
)
_THOUSANDS_COMMA = re.compile(r"^-?\d{1,3}(?:,\d{3})+$")
_THOUSANDS_DOT = re.compile(r"^-?\d{1,3}(?:\.\d{3})+$")
_SCALES = {"": 1.0, "K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}


class NumericValue(BaseModel):
    """Valeur économique : brut conservé + interprétation numérique explicite."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    raw: Optional[str] = None
    value: Optional[float] = None
    unit: Optional[str] = None          # "percent" | "number" | None
    scale: Optional[str] = None         # "K" | "M" | "B" | "T" | None
    parse_status: str = "ABSENT"        # ABSENT | PARSED | COMPOSITE | UNPARSEABLE


ABSENT_NUMERIC = NumericValue()
# [F6/M3 port] placeholders alignés sur le calendar_layer macro (« n.a. »,
# « tentative » en plus) — même vocabulaire ABSENT des deux côtés.
_PLACEHOLDERS = {"", "-", "—", "–", "n/a", "n.a.", "na", "null", "none", "tentative"}
ABSENT_DISPLAY = "—"      # [F3 port] placeholder unique des lignes legacy


def _to_float(text: str) -> Optional[float]:
    """[F6 port] Distingue séparateur de milliers et séparateur décimal.

    Règles, dans l'ordre :
      1. Les deux séparateurs présents → le DERNIER rencontré est le
         décimal, l'autre est un séparateur de milliers ("1.234,5" → 1234.5,
         "1,234.5" → 1234.5).
      2. Un seul séparateur, en groupes de 3 exacts ("1,234", "12.345.678")
         → séparateur de milliers.
      3. Sinon → séparateur décimal ("1,5" → 1.5, "0.25" → 0.25).
    Retourne ``None`` si non convertible (le caller émet UNPARSEABLE).
    """
    t = text.strip()
    has_comma, has_dot = "," in t, "." in t
    try:
        if has_comma and has_dot:
            if t.rfind(",") > t.rfind("."):
                return float(t.replace(".", "").replace(",", "."))
            return float(t.replace(",", ""))
        if has_comma:
            if _THOUSANDS_COMMA.match(t):
                return float(t.replace(",", ""))
            return float(t.replace(",", "."))
        if has_dot:
            if _THOUSANDS_DOT.match(t):
                return float(t.replace(".", ""))
            return float(t)
        return float(t)
    except ValueError:
        return None


def normalize_numeric(raw: Any) -> NumericValue:
    """
    Ne devine jamais l'unité au-delà de ce que le suffixe garantit.
    '0' et 0 ne doivent JAMAIS devenir absents (piège du falsy Python).
    """
    if raw is None:
        return ABSENT_NUMERIC
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return NumericValue(raw=str(raw), value=float(raw), unit="number",
                            scale=None, parse_status="PARSED")
    if not isinstance(raw, str):
        return NumericValue(raw=str(raw), parse_status="UNPARSEABLE")

    text = raw.strip()
    if text.lower() in _PLACEHOLDERS:
        return ABSENT_NUMERIC

    status = "PARSED"
    candidate = text
    if "|" in candidate:                       # ex. adjudication "2.84|2.6"
        candidate = candidate.split("|", 1)[0].strip()
        status = "COMPOSITE"

    m = _NUM_RE.match(candidate)
    if not m:
        return NumericValue(raw=text, parse_status="UNPARSEABLE")

    prefix, number, suffix, pct = m.groups()
    base = _to_float(number)                   # [F6] plus de float() brut
    if base is None:
        return NumericValue(raw=text, parse_status="UNPARSEABLE")

    # [F6 port] préfixe < > ~ ≈ ≤ ≥ ± → statut APPROXIMATE (la valeur est
    # retenue, l'incertitude est NOMMÉE, pas silencieuse).
    if prefix and status == "PARSED":
        status = "APPROXIMATE"

    suffix = suffix.upper()
    if pct:
        return NumericValue(raw=text, value=base, unit="percent",
                            scale=None, parse_status=status)
    return NumericValue(raw=text, value=base * _SCALES[suffix], unit="number",
                        scale=suffix or None, parse_status=status)


def classify_session(dt_utc: datetime) -> Tuple[Session, List[str]]:
    """Sessions calculées en heure LOCALE de chaque place, DST inclus."""
    active: List[str] = []
    for name, (tz, start_min, end_min) in SESSION_HOURS.items():
        local = dt_utc.astimezone(tz)
        if local.weekday() >= 5:               # samedi / dimanche : place fermée
            continue
        minutes = local.hour * 60 + local.minute
        if start_min <= minutes < end_min:
            active.append(name)

    has_ldn = "LONDON" in active
    has_ny = "NEW_YORK" in active
    # [B8 port] Wellington/Sydney rejoignent le bloc asiatique : l'enum
    # Session est inchangée, seuls les centres actifs s'enrichissent.
    has_asia = bool({"TOKYO", "WELLINGTON", "SYDNEY"} & set(active))
    if has_ldn and has_ny:
        session = Session.OVERLAP_LONDON_NY
    elif has_ldn and has_asia:
        session = Session.OVERLAP_ASIA_LONDON
    elif has_ny:
        session = Session.NEW_YORK
    elif has_ldn:
        session = Session.LONDON
    elif has_asia:
        session = Session.ASIAN
    else:
        session = Session.OFF
    return session, sorted(active)


def fmt_until(hours: float) -> str:
    total_min = int(round(abs(hours) * 60))
    d, rem = divmod(total_min, 1440)
    h, m = divmod(rem, 60)
    if d:
        body = f"{d}d {h}h {m}m"
    elif h:
        body = f"{h}h {m}m"
    else:
        body = f"{m}m"
    return body if hours > 0 else f"{body} ago"


# ─────────────────────────────────────────────────────────────────────────────
# POLITIQUE MACHINE — jamais alimentée par des widgets
# ─────────────────────────────────────────────────────────────────────────────
class SelectionPolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    policy_version: str = "1.2.0"  # 1.2.0 : plafond d'âge DUR du last-known-good (audit calendrier 2026-09-11)
    # HIGH seul sous-couvre structurellement AUD/CAD/CHF/JPY/NZD : la source tague
    # bien plus souvent HIGH les publications USD/EUR/GBP (CPI, GDP, rate decisions)
    # que l'équivalent sur devises mineures (retail sales, trade balance, PMI...),
    # pourtant tout aussi market-moving. HIGH+MEDIUM réduit ce biais sans changer
    # la sémantique de la policy (LOW reste exclu par défaut).
    impact_levels: Tuple[Impact, ...] = (Impact.HIGH, Impact.MEDIUM)
    currencies: Optional[Tuple[str, ...]] = None      # None = toutes
    include_global_events: bool = True
    window_past_hours: float = 72.0
    # [F7 port] fenêtre servie == fenêtre annoncée == 168 h (watch horizon du
    # macro). Avant : 192 annoncés / flux réel ~144 max → incohérence de
    # fenêtre identique à celle corrigée côté macro (mesuré 15/09 : le flux
    # thisweek n'atteint 168 h AUCUN jour de la semaine → aucun événement
    # retiré par ce changement, verrouillé par test croisé).
    window_future_hours: float = 168.0
    imminent_hours: float = 6.0
    soon_hours: float = 48.0
    display_timezone: str = DEFAULT_DISPLAY_TZ
    max_source_age_seconds: int = 900
    # PLAFOND D'ÂGE DUR DU LAST-KNOWN-GOOD (audit calendrier 2026-09-11, F-2) :
    # avant ce champ, un LKG de J−30 restait publiable à vie (score plancher
    # 0,40 > seuil <0,4) avec generated_at_utc rajeuni — un consommateur aval
    # ne pouvait pas distinguer « frais » de « ressassement ». Au-delà : INVALID
    # → l'ingesteur refuse de réécrire calendar.json ; l'artefact précédent
    # reste sur disque avec SON horodatage honnête.
    max_last_known_good_age_seconds: int = 48 * 3600
    max_events: int = 2000

    @field_validator("currencies")
    @classmethod
    def _upper(cls, v):
        return None if v is None else tuple(sorted({c.upper() for c in v}))

    @model_validator(mode="after")
    def _coherent(self):
        if self.window_past_hours < 0 or self.window_future_hours <= 0:
            raise ValueError("window bounds must be positive")
        if not 0 < self.imminent_hours < self.soon_hours:
            raise ValueError("imminent_hours must be < soon_hours")
        return self

    def display_tz(self) -> ZoneInfo:
        return ZoneInfo(self.display_timezone)


DEFAULT_POLICY = SelectionPolicy()


# ─────────────────────────────────────────────────────────────────────────────
# MODÈLES
# ─────────────────────────────────────────────────────────────────────────────
class TimeContext(BaseModel):
    """VOLATIL. Exclu du content_hash. Recalculable à tout instant."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    computed_at_utc: datetime
    hours_until: float
    hours_until_display: str
    is_upcoming: bool
    time_proximity: TimeProximity
    status: EventStatus

    @field_serializer("computed_at_utc")
    def _ser(self, v: datetime, _info) -> str:
        return iso_z(v)


class CalendarEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    occurrence_id: str
    event_type_id: str
    release_group_id: Optional[str] = None
    release_group_type: Optional[ReleaseGroupType] = None

    currency: str
    is_global: bool = False
    name: str

    scheduled_at_utc: datetime
    scheduled_at_display: datetime
    display_timezone: str
    date_utc: str
    date_display: str
    day_of_week: str

    impact: Impact
    session: Session
    active_market_centers: Tuple[str, ...] = ()

    forecast: NumericValue = ABSENT_NUMERIC
    previous: NumericValue = ABSENT_NUMERIC
    actual: NumericValue = ABSENT_NUMERIC
    actual_status: ActualStatus = ActualStatus.UNSUPPORTED_BY_SOURCE

    pairs_with_currency_exposure: Tuple[str, ...] = ()
    pair_mapping_status: PairMappingStatus = PairMappingStatus.MAPPED
    pair_mapping_method: str = PAIR_MAPPING_METHOD

    source_index: int
    time_context: Optional[TimeContext] = None

    @field_validator("scheduled_at_utc")
    @classmethod
    def _aware_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("scheduled_at_utc must be timezone-aware")
        return v.astimezone(UTC)

    @field_validator("scheduled_at_display")
    @classmethod
    def _aware_display(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("scheduled_at_display must be timezone-aware")
        return v

    @field_serializer("scheduled_at_utc")
    def _ser_utc(self, v: datetime, _info) -> str:
        return iso_z(v)

    @field_serializer("scheduled_at_display")
    def _ser_display(self, v: datetime, _info) -> str:
        return v.isoformat()

    def with_time_context(self, ctx: TimeContext) -> "CalendarEvent":
        return self.model_copy(update={"time_context": ctx})


class SourceInfo(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: str
    url: str
    fetched_at_utc: datetime
    fetch_duration_ms: int = 0
    http_status: Optional[int] = None
    content_type: Optional[str] = None
    payload_bytes: int = 0
    payload_sha256: str
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    supports_actual: bool = False
    from_last_known_good: bool = False
    # HARNESS-CAL (audit 2026-09-11) : statut par flux fetché (« thisweek »,
    # « nextweek ») — ok | absent_404 | error:<CODE>. Exclu du content_hash
    # (root « source » déjà exclu) : la disponibilité variable d'un flux
    # supplémentaire ne doit pas faire dériver le hash économique.
    feed_status: Dict[str, str] = Field(default_factory=dict)
    # [audit OPUS] sha256 DES OCTETS par flux composant la fusion (« thisweek »,
    # « nextweek ») — payload_sha256 ci-dessus étant désormais l'agrégat à la
    # formule macro, cette carte conserve la traçabilité par flux d'origine.
    # Exclue du content_hash (racine « source » exclue par [F5]) : un flux
    # supplémentaire ou son indisponibilité ne déplacent pas le hash économique.
    feed_sha256: Dict[str, str] = Field(default_factory=dict)

    @field_serializer("fetched_at_utc")
    def _ser(self, v: datetime, _info) -> str:
        return iso_z(v)


class QualityInfo(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    status: QualityStatus
    is_stale: bool
    source_age_seconds: int
    raw_event_count: int
    accepted_event_count: int
    rejected_event_count: int
    duplicate_event_count: int
    coverage_start_utc: Optional[str] = None
    coverage_end_utc: Optional[str] = None
    data_quality_score: float = Field(ge=0.0, le=1.0)
    warnings: Tuple[str, ...] = ()
    rejections: Tuple[str, ...] = ()
    # ND-013 : horizon court CAUSÉ par le flux semaine suivante non publié =
    # état structurel neutre (flag méta + note), plus un signal d'alerte.
    week_rollover_pending: bool = False


class CoverageInfo(BaseModel):
    """
    Sépare deux causes bien distinctes d'absence de devise dans l'artefact final,
    pour que tout consommateur aval (rapport desk inclus) cesse de les confondre :

      - currencies_excluded_by_policy : la source contenait des événements pour
        cette devise sur la fenêtre temporelle, mais à un niveau d'impact (ou hors
        filtre devise) non retenu par la policy active. Artefact de configuration,
        PAS une information de marché. Ne doit déclencher aucune alerte de risque.

      - currencies_no_data_in_source : la source ne contenait aucun événement pour
        cette devise sur la fenêtre temporelle, quel que soit le niveau d'impact.
        Calendrier réellement creux pour cette devise sur cette période — un statut
        neutre à afficher tel quel, sans dramatisation.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    window_start_utc: str
    window_end_utc: str
    currencies_scope: Tuple[str, ...]
    currencies_covered: Tuple[str, ...]
    currencies_excluded_by_policy: Tuple[str, ...]
    currencies_no_data_in_source: Tuple[str, ...]


class CalendarPayload(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: str = SCHEMA_VERSION
    generated_at_utc: datetime
    generator: str = "bluestar-calendar-ingestor"
    content_hash: Optional[str] = None
    source: SourceInfo
    quality: QualityInfo
    selection_policy: SelectionPolicy
    coverage: CoverageInfo
    session_policy_version: str = SESSION_POLICY_VERSION
    numeric_parser_version: str = NUMERIC_PARSER_VERSION
    events: Tuple[CalendarEvent, ...]

    @field_serializer("generated_at_utc")
    def _ser(self, v: datetime, _info) -> str:
        return iso_z(v)

    @model_validator(mode="after")
    def _invariants(self):
        ids = [e.occurrence_id for e in self.events]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate occurrence_id in payload")
        times = [e.scheduled_at_utc for e in self.events]
        if times != sorted(times):
            raise ValueError("events must be sorted by scheduled_at_utc")
        if self.quality.accepted_event_count != len(self.events):
            raise ValueError("accepted_event_count mismatch")
        return self


# ─────────────────────────────────────────────────────────────────────────────
# REGROUPEMENT DE PUBLICATIONS LIÉES
# ─────────────────────────────────────────────────────────────────────────────
_RATE_KEYWORDS = (
    "official cash rate", "overnight rate", "rate statement", "cash rate",
    "monetary policy statement", "interest rate", "policy rate", "main refinancing",
    "federal funds", "bank rate", "fomc statement",
)
_PRESSER_KEYWORDS = ("press conference", "monetary policy press")
# [M4 port] fenêtre d'antériorité conférence→décision PAR DEVISE. Mesuré :
# BoJ décision 02:30Z, conférence 05:30Z = écart 180 min > 120 → la
# conférence n'était jamais rattachée à sa décision côté TA comme côté
# macro avant correctif. 120 min partout ailleurs (FOMC 14:00→14:30).
_PRESSER_ANCHOR_WINDOW_MIN: Dict[str, float] = {"JPY": 240.0}
_LABOR_KEYWORDS = (
    "non-farm employment change", "unemployment rate", "average hourly earnings",
    "employment change", "claimant count",
)


def _match(title: str, keywords: Sequence[str]) -> bool:
    low = title.lower()
    return any(k in low for k in keywords)


def assign_release_groups(rows: List[Dict[str, Any]]) -> None:
    """
    Groupe (mutation in place de la clé 'release_group_*') :
      1. publications strictement simultanées d'un même pays,
      2. conférence de presse rattachée à la décision de taux du même pays
         survenue dans la fenêtre [_PRESSER_ANCHOR_WINDOW_MIN] (120 min par
         défaut, JPY 240 min — port [M4] du calendar_layer macro v6.1).
    """
    buckets: Dict[Tuple[str, datetime], List[Dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault((row["currency"], row["scheduled_at_utc"]), []).append(row)

    ordered = sorted(buckets.items(), key=lambda kv: (kv[0][0], kv[0][1]))
    anchors: Dict[str, Tuple[datetime, str]] = {}

    for (ccy, when), members in ordered:
        titles = [m["name"] for m in members]
        is_cb = any(_match(t, _RATE_KEYWORDS) for t in titles)
        is_presser = all(_match(t, _PRESSER_KEYWORDS) for t in titles)
        is_labor = any(_match(t, _LABOR_KEYWORDS) for t in titles)

        gid: Optional[str] = None
        gtype: Optional[ReleaseGroupType] = None

        if is_presser and ccy in anchors:
            anchor_time, anchor_gid = anchors[ccy]
            # [M4 port] fenêtre par devise : JPY 240 min, 120 ailleurs.
            window_min = _PRESSER_ANCHOR_WINDOW_MIN.get(ccy, 120.0)
            if timedelta(0) <= (when - anchor_time) <= timedelta(minutes=window_min):
                gid, gtype = anchor_gid, ReleaseGroupType.CENTRAL_BANK_DECISION

        if gid is None and (is_cb or len(members) > 1):
            gid = "grp_" + sha256_hex(f"{ccy}|{iso_z(when)}")[:16]
            if is_cb:
                gtype = ReleaseGroupType.CENTRAL_BANK_DECISION
                anchors[ccy] = (when, gid)
            elif is_labor:
                gtype = ReleaseGroupType.LABOR_MARKET_RELEASE
            else:
                gtype = ReleaseGroupType.SIMULTANEOUS_RELEASE

        for m in members:
            m["release_group_id"] = gid
            m["release_group_type"] = gtype


# ─────────────────────────────────────────────────────────────────────────────
# PROJECTION « PRIORITY » + PLACEHOLDER D'AFFICHAGE — portés du macro
# [M3/F3] pour que les deux apps exportent la MÊME sémantique de ligne.
# ─────────────────────────────────────────────────────────────────────────────
PRIORITY_PAST = "PAST"
PRIORITY_CRITICAL = "CRITICAL"
PRIORITY_HIGH = "HIGH"
PRIORITY_MEDIUM = "MEDIUM"


def compute_priority(hours_until: float,
                     policy: SelectionPolicy = DEFAULT_POLICY) -> str:
    """Règle IDENTIQUE au calendar_layer macro (seuils = time_proximity :
    imminent 6 h / soon 48 h, communs aux deux politiques). Additif dans la
    ligne legacy : l'ENGINE lit « priority » en audit seulement (l.339,
    extra="ignore") — aucun verdict de blackout n'en dépend côté desk."""
    if hours_until <= 0:
        return PRIORITY_PAST
    if hours_until <= policy.imminent_hours:
        return PRIORITY_CRITICAL
    if hours_until <= policy.soon_hours:
        return PRIORITY_HIGH
    return PRIORITY_MEDIUM


def _disp(nv: NumericValue) -> Optional[str]:
    """[F3 port] placeholder unique « — » pour les lignes legacy, aligné
    models.from_enriched et ENGINE._parse_ff_value (qui traite déjà « — »
    comme absent). La valeur numérique brute reste dans ``*_value``."""
    return nv.raw if nv.raw is not None else ABSENT_DISPLAY


# ─────────────────────────────────────────────────────────────────────────────
# CONSTRUCTION DU PAYLOAD
# ─────────────────────────────────────────────────────────────────────────────
def compute_time_context(
    event: CalendarEvent, now_utc: datetime, policy: SelectionPolicy = DEFAULT_POLICY
) -> TimeContext:
    hours = (event.scheduled_at_utc - now_utc).total_seconds() / 3600.0
    if event.impact is Impact.HOLIDAY:
        status = EventStatus.HOLIDAY
    elif hours > 0:
        status = EventStatus.SCHEDULED
    elif hours > -0.5:
        status = EventStatus.DUE
    else:
        status = EventStatus.PAST_SCHEDULE

    if hours <= 0:
        proximity = TimeProximity.PAST
    elif hours <= policy.imminent_hours:
        proximity = TimeProximity.IMMINENT
    elif hours <= policy.soon_hours:
        proximity = TimeProximity.SOON
    else:
        proximity = TimeProximity.LATER

    return TimeContext(
        computed_at_utc=now_utc,        hours_until=round(hours, 4),
        hours_until_display=fmt_until(hours),
        is_upcoming=hours > 0,
        time_proximity=proximity,
        status=status,
    )


def _normalize_row(
    raw: Any, index: int, policy: SelectionPolicy, display_tz: ZoneInfo
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not isinstance(raw, dict):
        return None, f"idx={index}: root element is {type(raw).__name__}, expected object"

    title = str(raw.get("title", "") or "").strip()
    if not title:
        return None, f"idx={index}: missing title"

    country = str(raw.get("country", "") or "").strip().upper()
    impact = IMPACT_ALIASES.get(str(raw.get("impact", "") or "").strip().upper(), Impact.UNKNOWN)

    try:
        when = parse_source_datetime(raw.get("date"))
    except (ValueError, TypeError) as exc:
        return None, f"idx={index} '{title}': invalid date ({exc})"

    is_global = country in GLOBAL_COUNTRY_TOKENS
    if is_global:
        currency, pairs, mapping = "ALL", (), PairMappingStatus.NO_MAPPING_GLOBAL_EVENT
    elif country in KNOWN_CURRENCIES:
        currency, mapping = country, PairMappingStatus.MAPPED
        pairs = tuple(pairs_for_currency(country))
    else:
        currency, pairs = country, ()
        mapping = PairMappingStatus.NO_MAPPING_UNKNOWN_CURRENCY

    has_actual_key = "actual" in raw
    actual = normalize_numeric(raw.get("actual")) if has_actual_key else ABSENT_NUMERIC
    if actual.parse_status != "ABSENT":
        actual_status = ActualStatus.RELEASED
    elif has_actual_key:
        actual_status = ActualStatus.NOT_YET_RELEASED
    else:
        actual_status = ActualStatus.UNSUPPORTED_BY_SOURCE

    local = when.astimezone(display_tz)
    type_id = f"ff:{currency.lower()}:{slugify(title)}"

    return {
        "occurrence_id": sha256_hex(f"{type_id}|{iso_z(when)}")[:32],
        "event_type_id": type_id,
        "release_group_id": None,
        "release_group_type": None,
        "currency": currency,
        "is_global": is_global,
        "name": title,
        "scheduled_at_utc": when,
        "scheduled_at_display": local,
        "display_timezone": policy.display_timezone,
        "date_utc": when.strftime("%Y-%m-%d"),
        "date_display": local.strftime("%Y-%m-%d"),
        "day_of_week": day_name(local),            # [F7 port] locale-safe
        "impact": impact,
        "session": None,
        "active_market_centers": None,
        "forecast": normalize_numeric(raw.get("forecast")),
        "previous": normalize_numeric(raw.get("previous")),
        "actual": actual,
        "actual_status": actual_status,
        "pairs_with_currency_exposure": pairs,
        "pair_mapping_status": mapping,
        "pair_mapping_method": PAIR_MAPPING_METHOD,
        "source_index": index,
    }, None


def _coverage_diagnostics(
    rows: List[Dict[str, Any]],
    selected: List[Dict[str, Any]],
    policy: SelectionPolicy,
    lo: datetime,
    hi: datetime,
) -> CoverageInfo:
    in_window = [r for r in rows if not r["is_global"] and lo <= r["scheduled_at_utc"] <= hi]

    scope: Tuple[str, ...] = policy.currencies if policy.currencies is not None else KNOWN_CURRENCIES
    raw_currencies_in_window = {r["currency"] for r in in_window}
    covered = {r["currency"] for r in selected if not r["is_global"]}

    excluded_by_policy: List[str] = []
    no_data_in_source: List[str] = []
    for ccy in scope:
        if ccy in covered:
            continue
        if ccy in raw_currencies_in_window:
            excluded_by_policy.append(ccy)
        else:
            no_data_in_source.append(ccy)

    return CoverageInfo(
        window_start_utc=iso_z(lo),
        window_end_utc=iso_z(hi),
        currencies_scope=tuple(sorted(scope)),
        currencies_covered=tuple(sorted(covered)),
        currencies_excluded_by_policy=tuple(sorted(excluded_by_policy)),
        currencies_no_data_in_source=tuple(sorted(no_data_in_source)),
    )


def build_payload(
    raw_list: Any,
    *,
    source: SourceInfo,
    now_utc: datetime,
    policy: SelectionPolicy = DEFAULT_POLICY,
) -> CalendarPayload:
    """Transforme le payload brut en artefact canonique validé."""
    if not isinstance(raw_list, list):
        raise ValueError(f"source root must be a JSON array, got {type(raw_list).__name__}")
    # [F7 port] volume hors-norme : TRONCATURE + avertissement (flux frais
    # amputé du surplus) plutôt que ValueError → ingestor → LKG périmé servi
    # à la place d'un flux vivant. Choix identique au calendar_layer macro.
    _oversized = len(raw_list) > policy.max_events
    if _oversized:
        raw_list = list(raw_list[: policy.max_events])

    display_tz = policy.display_tz()
    warnings: List[str] = []
    if _oversized:
        warnings.append(f"RAW_PAYLOAD_TRUNCATED_TO_{policy.max_events}")
    rejections: List[str] = []
    rows: List[Dict[str, Any]] = []

    for index, raw in enumerate(raw_list):
        row, err = _normalize_row(raw, index, policy, display_tz)
        if err:
            rejections.append(err)
            continue
        rows.append(row)

    unknown = {r["impact"] for r in rows if r["impact"] is Impact.UNKNOWN}
    if unknown:
        warnings.append("SOURCE_IMPACT_VOCABULARY_CHANGED")
        # [B1 audit OPUS] Pénalité : la dérive de vocabulaire n'est jamais
        # neutre. Partielle → score 0.80 → DEGRADED (la conjonction quality
        # teste score < 0.85). Totale (tous les candidats retenables devenus
        # UNKNOWN) → INVALID explicite plus bas. Sans ceci, un renommage
        # amont « High → Critical » produisait un calendrier VIDE noté
        # VALID 1.000 (mesuré 15/09/2026 sur le flux réel : 105 lignes
        # normalisées, 0 événement, quality VALID).
        score_vocab_penalty = 0.2
    else:
        score_vocab_penalty = 0.0

    lo = now_utc - timedelta(hours=policy.window_past_hours)
    hi = now_utc + timedelta(hours=policy.window_future_hours)

    selected: List[Dict[str, Any]] = []
    for row in rows:
        if row["impact"] not in policy.impact_levels:
            continue
        if row["is_global"] and not policy.include_global_events:
            continue
        if (policy.currencies is not None and not row["is_global"]
                and row["currency"] not in policy.currencies):
            continue
        if not (lo <= row["scheduled_at_utc"] <= hi):
            continue
        selected.append(row)

    coverage = _coverage_diagnostics(rows, selected, policy, lo, hi)

    seen: Dict[str, Dict[str, Any]] = {}
    duplicates = 0
    for row in selected:
        if row["occurrence_id"] in seen:
            duplicates += 1
            continue
        seen[row["occurrence_id"]] = row
    selected = list(seen.values())
    if duplicates:
        warnings.append(f"DUPLICATE_OCCURRENCES_DROPPED:{duplicates}")

    selected.sort(key=lambda r: (r["scheduled_at_utc"], r["currency"], r["name"]))
    assign_release_groups(selected)

    events: List[CalendarEvent] = []
    for row in selected:
        session, centers = classify_session(row["scheduled_at_utc"])
        row["session"] = session
        row["active_market_centers"] = tuple(centers)
        event = CalendarEvent(**row)
        events.append(event.with_time_context(compute_time_context(event, now_utc, policy)))

    age = max(0, int((now_utc - source.fetched_at_utc).total_seconds()))
    is_stale = age > policy.max_source_age_seconds
    if is_stale:
        warnings.append(f"SOURCE_AGE_EXCEEDS_{policy.max_source_age_seconds}S")
    if source.from_last_known_good:
        warnings.append("SERVING_LAST_KNOWN_GOOD")
        if age > policy.max_last_known_good_age_seconds:
            lkg_expired = True
            warnings.append(
                f"LAST_KNOWN_GOOD_EXPIRED:{age}s>{policy.max_last_known_good_age_seconds}s")
        else:
            lkg_expired = False
    else:
        lkg_expired = False
    if not source.supports_actual:
        warnings.append("SOURCE_DOES_NOT_PROVIDE_ACTUAL")

    all_times = [r["scheduled_at_utc"] for r in rows]
    coverage_start = min(all_times) if all_times else None
    coverage_end = max(all_times) if all_times else None
    if coverage_end is not None and coverage_end < now_utc:
        warnings.append("ALL_SOURCE_EVENTS_IN_THE_PAST_WEEK_ROLLOVER_PENDING")
    if coverage_start is not None and coverage_start > now_utc + timedelta(days=9):
        warnings.append("SOURCE_COVERAGE_STARTS_TOO_FAR_IN_FUTURE")
    if coverage_end is not None and coverage_end > now_utc:
        horizon_useful_h = (coverage_end - now_utc).total_seconds() / 3600.0
    else:
        horizon_useful_h = None
    # F-2 (audit 2026-09-11) : détecter la troncature INTRA-hebdomadaire — la
    # source « this week » se tarit avant le horizon que la policy elle-même
    # promet (soon_hours = la fenêtre de veille du consommateur). Sans ce
    # warning, seul un rollover complet (coverage_end < now) sonnait, et le
    # run de jeudi à 44 h de couverture passait pour un flux sain.
    # [Câblage audit OPUS 15/09] La couverture qui COMPTE est celle des
    # événements RETENUS (events), pas de toutes les lignes normalisées :
    # l'aval lit data_coverage_end_utc = max(events). Baser le warning sur
    # max(rows) (LOW/Holiday inclus) le faisait sonner ~24 h trop tard
    # (mesuré 15/09 : rows-end ECOFIN 19/09 10:15Z vs events-end 18/09
    # 10:30Z = 23,75 h d'angle mort). Repli sur rows si events vide (semaine
    # calme = cas où l'horizon des events n'a rien à mesurer).
    _horizon_times = [e.scheduled_at_utc for e in events] or all_times
    _horizon_end = max(_horizon_times) if _horizon_times else None
    horizon_events_h = (
        (_horizon_end - now_utc).total_seconds() / 3600.0
        if _horizon_end is not None and _horizon_end > now_utc else None
    )
    coverage_short = bool(
        horizon_events_h is not None and horizon_events_h < policy.soon_hours
    )
    # [ND-013] Deux causes d'horizon court, deux traitements (sémantique
    # IDENTIQUE à la macro v6.1 — classification classify_feed_horizon) :
    #   - cause structurelle : flux primaire OK, secondaire (nextweek) non
    #     intégré (404 normal lundi-vendredi, ou erreur réseau) → « rollover
    #     hebdo » : PAS de warning, PAS de dégradation. Le desk recalcule son
    #     propre fail-closed F7 sur max(events) vs 168h — inchangé ; la
    #     visibilité reste portée par coverage_note + flag méta.
    #   - cause anormale (nextweek intégré et horizon court) ou granularité
    #     feed_status absente → comportement 2.2.0 conservé à l'identique.
    rollover_pending = False
    _fs = dict(getattr(source, "feed_status", None) or {})
    if (
        coverage_short
        and _fs
        and _fs.get("thisweek", "ok") == "ok"
        and (_fs.get("nextweek") or "ok") != "ok"
        and "ALL_SOURCE_EVENTS_IN_THE_PAST_WEEK_ROLLOVER_PENDING" not in warnings
    ):
        rollover_pending = True
    if coverage_short and not rollover_pending:
        warnings.append(
            f"COVERAGE_SHORTER_THAN_HORIZON:{horizon_events_h:.1f}h<{policy.soon_hours:.0f}h")
    if not rows:
        warnings.append("EMPTY_NORMALIZED_PAYLOAD")

    score = 1.0
    if is_stale:
        score -= 0.35
    if source.from_last_known_good:
        score -= 0.25
    if rejections:
        score -= min(0.25, 0.05 * len(rejections))
    score -= score_vocab_penalty                      # [B1 audit OPUS]
    if "ALL_SOURCE_EVENTS_IN_THE_PAST_WEEK_ROLLOVER_PENDING" in warnings:
        score -= 0.30
    if not rows:
        score = 0.0
    score = round(max(0.0, min(1.0, score)), 3)

    # [B1 audit OPUS] Rejet TOTAL par dérive de vocabulaire : des lignes ont
    # survécu à la normalisation mais AUCUN événement n'est retenu, et le
    # warning de vocabulaire est là pour l'attester. C'est le seul cas
    # « vide = artefact de parsing » ; une semaine calme sans High (aucun
    # UNKNOWN) reste un fait de marché publiable, avec ses garde-fous
    # existants (F-3 devise côté moteur, coverage, rollover).
    total_vocab_rejection = bool(rows) and not events \
        and "SOURCE_IMPACT_VOCABULARY_CHANGED" in warnings
    if total_vocab_rejection:
        warnings.append("IMPACT_VOCABULARY_ALL_REJECTED")

    if not rows or score < 0.4 or lkg_expired or total_vocab_rejection:
        status = QualityStatus.INVALID
    elif warnings and (is_stale or source.from_last_known_good
                       or (coverage_short and not rollover_pending)
                       or score < 0.85):
        status = QualityStatus.DEGRADED
    else:
        status = QualityStatus.VALID

    quality = QualityInfo(
        status=status,
        week_rollover_pending=rollover_pending,
        is_stale=is_stale,
        source_age_seconds=age,
        raw_event_count=len(raw_list),
        accepted_event_count=len(events),
        rejected_event_count=len(rejections),
        duplicate_event_count=duplicates,
        coverage_start_utc=iso_z(coverage_start) if coverage_start else None,
        coverage_end_utc=iso_z(coverage_end) if coverage_end else None,
        data_quality_score=score,
        warnings=tuple(warnings),
        rejections=tuple(rejections[:50]),
    )

    payload = CalendarPayload(
        generated_at_utc=now_utc,
        source=source,
        quality=quality,
        selection_policy=policy,
        coverage=coverage,
        events=tuple(events),
    )
    return payload.model_copy(update={"content_hash": canonical_content_hash(payload)})


# ─────────────────────────────────────────────────────────────────────────────
# HASH DE CONTENU — [F5 port] PROJECTION ÉCONOMIQUE EXPLICITE
# (méthode « economic_projection_v2 », identique au calendar_layer macro :
# mêmes champs, même sérialisation → même calendrier, même hash, quel que
# soit le fuseau d'affichage, la locale, l'ordre de fusion ou l'horloge.)
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# OVERLAY « ACTUALS » — SECONDE SORTIE PUBLIQUE DE FOREX FACTORY
# Additionnel PAR CONSTRUCTION : ne touche ni au payload canonique, ni au
# content_hash inter-apps, ni à l'ENGINE. Preuve live du 15/09 : le flux JSON
# hebdo ne contient AUCUNE clé « actual » (0/105) ; la page publique du
# calendrier du site FF, elle, embarque un objet JSON
# (window.calendarComponentStates) dont chaque event porte le même `dateline`
# (epoch UNIX, indépendant du fuseau d'affichage) que le flux. Jointure :
# (nom normalisé + devise, dateline ± tolérance) — seul écart observé en
# live : la minute affinée côté site (Westpac 22:04 vs 22:00 feed = 240 s).
# On ne réinvente pas la roue : c'est la même maison, sa seconde porte.
# ─────────────────────────────────────────────────────────────────────────────

FF_ACTUALS_TOLERANCE_S = 360          # ±6 min : mise à jour de minute du site


def _balanced_json_object(text: str, start: int) -> Optional[str]:
    """Extrait l'objet JSON équilibré depuis l'accolade d'index `start`
    (ignore les accolades des chaînes, gère les échappements)."""
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        c = text[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    return None


def parse_ff_embedded_calendar(html: str) -> List[Dict[str, Any]]:
    """Événements embarqués d'une page calendrier du site FF (données serveur
    `calendarComponentStates`). Les blocs qui ne sont pas des events (jour
    enveloppe, états de composants) sont ignorés ; un résultat VIDE sur une
    page qui contenait « timeLabel » signale un changement de structure —
    l'appelant doit traiter, jamais publier un overlay trompeur."""
    if '"timeLabel"' not in html:
        return []
    events: List[Dict[str, Any]] = []
    for m in re.finditer(r'\{"[^"]+"\s*:', html):
        blob = _balanced_json_object(html, m.start())
        if not blob or len(blob) > 4000:
            continue
        if '"timeLabel"' not in blob or '"actual"' not in blob or '"dateline"' not in blob:
            continue
        try:
            e = json.loads(blob)
        except ValueError:
            continue
        if not isinstance(e, dict) or "timeLabel" not in e or not e.get("dateline"):
            continue
        events.append({
            "name": str(e.get("name") or "").strip(),
            "currency": str(e.get("currency") or "").strip().upper(),
            "date_label": str(e.get("date") or "").strip(),
            "time_label": str(e.get("timeLabel") or "").strip(),
            "dateline": int(e["dateline"]),
            "actual": str(e.get("actual") or "").strip(),
            "forecast": str(e.get("forecast") or "").strip(),
            "previous": str(e.get("previous") or "").strip(),
            "revision": str(e.get("revision") or "").strip(),
            "actual_better_worse": e.get("actualBetterWorse"),
        })
    return events


def build_actuals_index(entries: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Index par nom normalisé, trié par dateline (déterministe)."""
    idx: Dict[str, List[Dict[str, Any]]] = {}
    for e in entries:
        idx.setdefault(str(e.get("name") or "").strip().lower(), []).append(e)
    for lst in idx.values():
        lst.sort(key=lambda x: int(x.get("dateline") or 0))
    return idx


def find_overlay_actual(
    index: Dict[str, List[Dict[str, Any]]],
    name: str,
    scheduled_at_utc: datetime,
    currency: Optional[str] = None,
    tolerance_s: int = FF_ACTUALS_TOLERANCE_S,
) -> Optional[Dict[str, Any]]:
    """Jointure déterministe : nom + epoch le plus proche sous tolérance ;
    la devise, présente des deux côtés, DOIT correspondre — les homonymes
    multi-pays (« Unemployment Rate » GBP ≠ CNY le même jour) ne peuvent pas
    se marcher dessus. Retourne l'entrée overlay ou None."""
    cands = index.get((name or "").strip().lower()) or []
    if not cands:
        return None
    target = int(scheduled_at_utc.timestamp())
    ccy = (currency or "").strip().upper()
    best: Optional[Any] = None
    for e in cands:
        if ccy and e.get("currency") and e["currency"] != ccy:
            continue
        d = abs(int(e.get("dateline") or 0) - target)
        if d <= tolerance_s and (best is None or d < best[0]):
            best = (d, e)
    return best[1] if best else None


def canonical_content_hash(payload: CalendarPayload) -> str:
    """[F5 port] SHA-256 d'une PROJECTION ÉCONOMIQUE explicite.

    Insensible à : fuseau d'affichage, jour en toutes lettres (locale),
    index d'origine dans le flux, sessions/centres actifs (dérivables de
    l'UTC), champs de vue (time_context, priority…), métadonnées de qualité
    et de source. Avant ce port, changer DEFAULT_DISPLAY_TZ ou l'ordre de
    fusion faisait bouger le hash — « les deux apps voient le même
    calendrier » était invérifiable. La liste des 10 champs est la copie
    exacte de la projection du calendar_layer macro (l. canonical_content_
    hash, v6.2 sha 2e4bf033a0c6) ; la stabilité inter-apps du BRUT reste
    ``source.payload_sha256``.
    """
    projection = [
        {
            "occurrence_id": e.occurrence_id,
            "event_type_id": e.event_type_id,
            "scheduled_at_utc": iso_z(e.scheduled_at_utc),
            "currency": e.currency,
            "name": e.name,
            "impact": e.impact.value,
            "forecast": e.forecast.raw,
            "previous": e.previous.raw,
            "actual": e.actual.raw,
            "release_group_id": e.release_group_id,
        }
        for e in payload.events
    ]
    canonical = json.dumps(
        {"method": CONTENT_HASH_METHOD, "events": projection},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )
    return "sha256:" + sha256_hex(canonical)


def refresh_time_contexts(
    payload: CalendarPayload, now_utc: datetime
) -> Tuple[CalendarEvent, ...]:
    """Recalcule les countdowns sans toucher au payload canonique."""
    return tuple(
        e.with_time_context(compute_time_context(e, now_utc, payload.selection_policy))
        for e in payload.events
    )


def render_coverage_note(coverage: CoverageInfo, impact_levels: Tuple[Impact, ...]) -> str:
    """
    Formulation neutre, destinée à un rendu client/desk. Principes :
      - jamais de vocabulaire de risque ("fail-closed", "non écarté", "cap
        prudentiel") pour un simple constat de périmètre de données ;
      - la distinction policy vs source réelle est explicite mais factuelle ;
      - couverture complète -> une ligne courte, pas de mise en avant.
    """
    levels = "+".join(lvl.value for lvl in impact_levels)

    if not coverage.currencies_excluded_by_policy and not coverage.currencies_no_data_in_source:
        return f"Couverture calendrier complète ({levels}) sur la fenêtre analysée."

    parts = [f"Couverture calendrier ({levels}) : {', '.join(coverage.currencies_covered) or '—'}."]

    if coverage.currencies_excluded_by_policy:
        parts.append(
            "Hors périmètre de sélection actif (données disponibles, non retenues) : "
            f"{', '.join(coverage.currencies_excluded_by_policy)}."
        )
    if coverage.currencies_no_data_in_source:
        parts.append(
            "Aucune publication programmée sur la fenêtre pour : "
            f"{', '.join(coverage.currencies_no_data_in_source)}."
        )
    return " ".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# EXPORT LEGACY v1 — pont de migration pour le pipeline existant
# ─────────────────────────────────────────────────────────────────────────────
_LEGACY_SESSION = {
    Session.OVERLAP_LONDON_NY: "OVERLAP",
    Session.OVERLAP_ASIA_LONDON: "LONDON",
    Session.NEW_YORK: "NEW YORK",
    Session.LONDON: "LONDON",
    Session.ASIAN: "ASIAN",
    Session.OFF: "OFF",
}


def _horizon_diagnosis(feed_horizon_h: Optional[float],
                       feed_status: Dict[str, Any],
                       watch_horizon_h: float) -> Tuple[str, bool]:
    """[compat-M2 port] Copie sémantique exacte du ``_horizon_diagnosis`` du
    calendar_layer macro (v6.1) : « flux normalement court » != « flux
    réellement dégradé ».

    Retourne ``(state, alerte_rouge)`` :
      * ("unreachable", False)     — horizon non mesurable (flux vide) ;
      * ("degraded", True)         — thisweek en échec (seul cas rouge) ;
      * ("nominal_weekly", False)  — thisweek ok, horizon < watch : nature
        hebdomadaire de la source (mention neutre, pas d'alerte) ;
      * ("full_watch_horizon", False) — ≥ watch horizon (second flux publié).

    ``feed_status`` vide (payload injecté, tests/rejeu) : donnée de confiance,
    pas de dégradation affirmée.
    """
    if feed_horizon_h is None:
        return "unreachable", False
    tw = str((feed_status or {}).get("thisweek") or "")
    if feed_status and tw and not tw.startswith("ok"):
        return "degraded", True
    if feed_horizon_h < watch_horizon_h:
        return "nominal_weekly", False
    return "full_watch_horizon", False


def to_legacy_payload(payload: CalendarPayload, now_utc: datetime) -> Dict[str, Any]:
    """
    Reproduit la forme v1 (metadata / events / events_engine / summary_by_day)
    avec DEUX corrections non négociables :
      - 'actual' vaut null, jamais '—', et 'actual_status' est explicite ;
      - summary_by_day est indexé sur la date d'affichage, comme la vue.
    'events' et 'events_engine' sont désormais strictement identiques :
    aucun clic humain n'intervient plus dans la production de cet artefact.
    """
    events = refresh_time_contexts(payload, now_utc)
    rows: List[Dict[str, Any]] = []
    summary: Dict[str, List[str]] = {}

    # F-3 (audit calendrier 2026-09-11) : le ENGINE dérive sa couverture de
    # `filters_applied.currencies` (l.2724) et NON des clés `currencies_covered`
    # qu'il ne lit pas (grep ENGINE = 0 occurrence). Avec `ui_filters_applied`
    # laissé à null, l'ENGINE conclut « flux global => couvert = les 8 devises
    # du desk » et le fail-closed « devise hors couverture » du f7 devient
    # inafectable : un setup AUD/CAD/JPY/NZD lit « aucun event S/A = risque nul »
    # alors que la policy a EXCLU ces devises. On publie donc la couverture que
    # le consommateur lit déjà : devises où le flux a réellement fait son travail
    # (= couvertes ∪ genuinely-empty), en RETIRANT les exclus-par-policy (angles
    # morts réels). C'est la seule correction qui traverse le pont legacy sans
    # toucher au moteur gelé.
    _cov = payload.coverage
    claimable = set(_cov.currencies_covered) | set(_cov.currencies_no_data_in_source)
    filters_currencies = sorted(claimable)

    for e in events:
        ctx = e.time_context
        rows.append({
            "occurrence_id": e.occurrence_id,
            "event_type_id": e.event_type_id,
            "release_group_id": e.release_group_id,
            # [align-rows] type de groupe exporté (le macro l'expose ; additif
            # pour l'ENGINE, extra="ignore").
            "release_group_type": (e.release_group_type.value
                                   if e.release_group_type else None),
            "currency": e.currency,
            "event_name": e.name,
            "datetime_utc": iso_z(e.scheduled_at_utc),
            "date_display": e.date_display,
            "time_display": f"{e.scheduled_at_display.strftime('%H:%M')} ({e.display_timezone})",
            "datetime_display": (f"{e.date_display} · "
                                 f"{e.scheduled_at_display.strftime('%H:%M')} ({e.display_timezone})"),
            "display_timezone": e.display_timezone,
            "day_of_week": e.day_of_week,
            "impact": e.impact.value.lower(),
            # [F3 port] placeholder « — » commun aux trois valeurs ; les
            # *_value numériques restent None quand absent (contrat ENGINE).
            "forecast": _disp(e.forecast),
            "forecast_value": e.forecast.value,
            # [M3 port] statut de parsing exporté (additif, consommé par les
            # diagnostics ; jamais par un verdict).
            "forecast_status": e.forecast.parse_status,
            "previous": _disp(e.previous),
            "previous_value": e.previous.value,
            "previous_status": e.previous.parse_status,
            "actual": _disp(e.actual),
            "actual_value": e.actual.value,
            "actual_status": e.actual_status.value,
            "hours_until": ctx.hours_until,
            # [align-rows] projection priority (mêmes seuils que le macro).
            "priority": compute_priority(ctx.hours_until, payload.selection_policy),
            "hours_until_display": ctx.hours_until_display,
            "is_upcoming": ctx.is_upcoming,
            "time_proximity": ctx.time_proximity.value,
            "status": ctx.status.value,
            "session": _LEGACY_SESSION.get(e.session, e.session.value),  # [F7 port]
            "session_v2": e.session.value,
            "pairs_affected": list(e.pairs_with_currency_exposure),
            "pair_mapping_status": e.pair_mapping_status.value,
        })
        summary.setdefault(e.date_display, []).append(f"{e.currency} – {e.name}")

    ev_times = [e.scheduled_at_utc for e in payload.events]
    data_start = min(ev_times) if ev_times else None
    data_end = max(ev_times) if ev_times else None
    data_horizon_h = (round((data_end - payload.generated_at_utc).total_seconds() / 3600.0, 2)
                      if data_end else None)

    # [compat-M2 port] diagnostic d'horizon dans le VOCABULAIRE que l'ENGINE
    # lit déjà (feed_horizon_h / feed_horizon_truncated / feed_coverage_detail,
    # ENGINE.V10 l.2851-2853) — sémantique identique au calendar_layer macro :
    # rouge UNIQUEMENT sur « degraded » (thisweek en échec), jamais sur le
    # rollover hebdo normal (la position ND-013 de ce fichier, traduite).
    _fstat = dict(payload.source.feed_status or {})
    _feeds_ok = sum(1 for v in _fstat.values() if str(v).startswith("ok"))
    _reachable = (not payload.source.from_last_known_good) and (
        _feeds_ok > 0 if _fstat else True)
    if not _reachable:
        _hstate, _hred = ("unreachable", False) if payload.source.from_last_known_good \
            else _horizon_diagnosis(data_horizon_h, _fstat,
                                    payload.selection_policy.window_future_hours)
    else:
        _hstate, _hred = _horizon_diagnosis(data_horizon_h, _fstat,
                                            payload.selection_policy.window_future_hours)

    _cov_note = (
        (render_coverage_note(payload.coverage, payload.selection_policy.impact_levels) or "")
        + (" Rollover hebdomadaire en attente : flux semaine suivante non publié "
           "(publication en fin de semaine) — couverture réelle jusqu'au "
           + str(iso_z(data_end) if data_end else "—")
           + " ; au-delà, silence non mesuré, pas risque nul."
           if payload.quality.week_rollover_pending else "")
    ).strip() or None

    return {
        "metadata": {
            "schema_version": f"legacy-1.2.0+core-{payload.schema_version}",
            "generated_at_utc": iso_z(payload.generated_at_utc),
            "content_hash": payload.content_hash,
            # [F5 port] méthode + [compat-M2] vocab d'horizon lisible par
            # l'ENGINE (l.2850-2853) — avant ce port, « reachable » n'était
            # JAMAIS exporté et l'ENGINE vivait sur son défaut True
            # (fail-open) ; « serving_mode » distingue le secours LKG.
            "content_hash_method": CONTENT_HASH_METHOD,
            "reachable": _reachable,
            "feeds_ok": _feeds_ok,
            "feeds_total": len(_fstat),
            "feed_horizon_h": data_horizon_h,
            "feed_horizon_state": _hstate,
            "feed_horizon_truncated": _hred,
            "feed_watch_horizon_h": payload.selection_policy.window_future_hours,
            "feed_start_utc": iso_z(data_start) if data_start else None,
            "feed_end_utc": iso_z(data_end) if data_end else None,
            "feed_coverage_detail": _cov_note,
            "source": payload.source.provider,
            "source_url": payload.source.url,
            "fetched_at_utc": iso_z(payload.source.fetched_at_utc),
            "supports_actual": payload.source.supports_actual,
            "timezone": f"UTC (backend) / {payload.selection_policy.display_timezone} (display)",
            "quality_status": payload.quality.status.value,
            "data_quality_score": payload.quality.data_quality_score,
            "is_stale": payload.quality.is_stale,
            "source_age_seconds": payload.quality.source_age_seconds,
            # HARNESS-CAL (audit 2026-09-11) : ce que sert le LKG devient visible —
            # un consommateur peut distinguer « frais » de « ressassement ».
            "serving_mode": "last_known_good" if payload.source.from_last_known_good else "live",
            "feeds_status": dict(payload.source.feed_status),
            "rejected_event_count": payload.quality.rejected_event_count,
            "warnings": list(payload.quality.warnings),
            "engine_events_count": len(rows),
            # DEPRECATED — nom historiquement trompeur (= len(rows) quel que soit le
            # policy réel, pas seulement HIGH). Conservé pour rétro-compatibilité
            # aval ; utiliser engine_events_count + impact_levels_included ci-dessous.
            "total_high_impact": len(rows),
            # F-2 (audit 2026-09-11) : compteur VRAI des HIGH, à côté du champ
            # deprecated ci-dessus qui compte HIGH+MEDIUM confondus.
            "high_impact_count": sum(1 for e in payload.events if e.impact is Impact.HIGH),
            "upcoming_count": sum(1 for r in rows if r["is_upcoming"]),
            "imminent_count": sum(1 for r in rows if r["time_proximity"] == "IMMINENT"),
            "summary_by_day_basis": "display_timezone",
            "ui_filters_applied": None,
            # F-3 (audit 2026-09-11) : LE contrat que le ENGINE lit. Dict présent
            # (et non null) = « flux non global » ; .currencies = devises que le
            # flux revendique (couvertes ∪ genuinely-empty), les exclus-par-policy
            # restent hors liste => le fail-closed devise du f7 redevient vivable.
            "filters_applied": {
                "basis": "machine_policy",
                "policy_version": payload.selection_policy.policy_version,
                "currencies": filters_currencies,
                "impact_levels": [lvl.value.lower() for lvl in payload.selection_policy.impact_levels],
            },
            # --- Périmètre & couverture : de quoi éviter à l'aval de confondre
            #     "filtré par policy" et "aucune donnée réelle". ---
            # Cassée autrefois en MAJUSCULES ici alors que les événements sont en
            # minuscules (l.940) — deux représentations du même vocabulaire dans
            # le même fichier ; un consommateur qui recoupait ne matchait jamais.
            "impact_levels_included": [lvl.value.lower() for lvl in payload.selection_policy.impact_levels],
            "currencies_filter": (
                list(payload.selection_policy.currencies)
                if payload.selection_policy.currencies is not None else "ALL"
            ),
            "coverage_window_start_utc": payload.coverage.window_start_utc,
            "coverage_window_end_utc": payload.coverage.window_end_utc,
            # F-2 (audit 2026-09-11) : les clés ci-dessus sont des bornes POLICY
            # (fenêtre souhaitée) et NON des données. Les bornes RÉELLES — dernier
            # événement réellement présent — étaient confinées au canonique
            # calendar.latest.json, jamais exportées : un jeudi, le fichier
            # promettait le 18/09 alors que les données finissaient au 12/09.
            # Bornes des ÉVÉNEMENTS RETENUS (ce que l'artefact contient vraiment,
            # et ce que le ENGINE recalcule lui-même via max(events.datetime_utc)
            # — les deux sources doivent coïncider), et non les bornes brutes du
            # flux qui incluraient des lignes filtrées par la policy.
            "data_coverage_start_utc": iso_z(data_start) if data_start else None,
            "data_coverage_end_utc": iso_z(data_end) if data_end else None,
            "data_coverage_horizon_h": data_horizon_h,
            "week_rollover_pending": payload.quality.week_rollover_pending,
            "currencies_scope": list(payload.coverage.currencies_scope),
            "currencies_covered": list(payload.coverage.currencies_covered),
            "currencies_excluded_by_policy": list(payload.coverage.currencies_excluded_by_policy),
            "currencies_no_data_in_source": list(payload.coverage.currencies_no_data_in_source),
            "coverage_note": _cov_note,
        },
        # [M5 port] les deux clés ne partagent plus le MÊME objet list : un
        # consommateur qui en mute une ne corrompt plus l'autre. Contenu
        # identique (Pro publie la population complète ; c'est l'ENGINE qui
        # applique sa fenêtre de lecture).
        "events": list(rows),
        "events_engine": list(rows),
        "summary_by_day": {k: summary[k] for k in sorted(summary)},
    }

"""
BLUESTAR · calendar_core
========================
Logique métier pure. Aucun import Streamlit, aucun I/O réseau, aucun accès disque.
Toutes les fonctions sont déterministes : le temps est TOUJOURS injecté.

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
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

# ─────────────────────────────────────────────────────────────────────────────
# VERSIONS DE CONTRAT — toute rupture doit incrémenter la majeure
# ─────────────────────────────────────────────────────────────────────────────
SCHEMA_VERSION = "2.0.0"
PAIR_MAPPING_METHOD = "static_currency_membership_v1"
SESSION_POLICY_VERSION = "exchange_local_dst_aware_v1"
NUMERIC_PARSER_VERSION = "ff_numeric_v1"

UTC = timezone.utc

TZ_LONDON = ZoneInfo("Europe/London")
TZ_NEW_YORK = ZoneInfo("America/New_York")
TZ_TOKYO = ZoneInfo("Asia/Tokyo")
DEFAULT_DISPLAY_TZ = "Africa/Casablanca"

# Heures locales des places financières (politique explicite et versionnée).
# On ne fige JAMAIS d'heures UTC : zoneinfo applique le DST de chaque place,
# y compris pendant les périodes où Londres et New York ne sont pas alignés.
SESSION_HOURS = {
    "LONDON": (TZ_LONDON, 8 * 60, 16 * 60 + 30),
    "NEW_YORK": (TZ_NEW_YORK, 8 * 60, 17 * 60),
    "TOKYO": (TZ_TOKYO, 8 * 60, 17 * 60),
}

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


_NUM_RE = re.compile(r"^([<>~]?)\s*(-?\d+(?:[.,]\d+)?)\s*([KMBT]?)\s*(%?)$", re.IGNORECASE)
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
_PLACEHOLDERS = {"", "-", "—", "–", "n/a", "na", "null", "none"}


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

    _prefix, number, suffix, pct = m.groups()
    try:
        base = float(number.replace(",", "."))
    except ValueError:
        return NumericValue(raw=text, parse_status="UNPARSEABLE")

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

    has_ldn, has_ny, has_tky = ("LONDON" in active, "NEW_YORK" in active, "TOKYO" in active)
    if has_ldn and has_ny:
        session = Session.OVERLAP_LONDON_NY
    elif has_ldn and has_tky:
        session = Session.OVERLAP_ASIA_LONDON
    elif has_ny:
        session = Session.NEW_YORK
    elif has_ldn:
        session = Session.LONDON
    elif has_tky:
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

    policy_version: str = "1.0.0"
    impact_levels: Tuple[Impact, ...] = (Impact.HIGH,)
    currencies: Optional[Tuple[str, ...]] = None      # None = toutes
    include_global_events: bool = True
    window_past_hours: float = 72.0
    window_future_hours: float = 192.0
    imminent_hours: float = 6.0
    soon_hours: float = 48.0
    display_timezone: str = DEFAULT_DISPLAY_TZ
    max_source_age_seconds: int = 900
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


class CalendarPayload(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: str = SCHEMA_VERSION
    generated_at_utc: datetime
    generator: str = "bluestar-calendar-ingestor"
    content_hash: Optional[str] = None
    source: SourceInfo
    quality: QualityInfo
    selection_policy: SelectionPolicy
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
         survenue dans les 120 minutes précédentes.
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
            if timedelta(0) <= (when - anchor_time) <= timedelta(minutes=120):
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
        computed_at_utc=now_utc,
        hours_until=round(hours, 4),
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
        "day_of_week": local.strftime("%A").upper(),
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
    if len(raw_list) > policy.max_events:
        raise ValueError(f"payload too large: {len(raw_list)} > {policy.max_events}")

    display_tz = policy.display_tz()
    warnings: List[str] = []
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
    if not source.supports_actual:
        warnings.append("SOURCE_DOES_NOT_PROVIDE_ACTUAL")

    all_times = [r["scheduled_at_utc"] for r in rows]
    coverage_start = min(all_times) if all_times else None
    coverage_end = max(all_times) if all_times else None
    if coverage_end is not None and coverage_end < now_utc:
        warnings.append("ALL_SOURCE_EVENTS_IN_THE_PAST_WEEK_ROLLOVER_PENDING")
    if coverage_start is not None and coverage_start > now_utc + timedelta(days=9):
        warnings.append("SOURCE_COVERAGE_STARTS_TOO_FAR_IN_FUTURE")
    if not rows:
        warnings.append("EMPTY_NORMALIZED_PAYLOAD")

    score = 1.0
    if is_stale:
        score -= 0.35
    if source.from_last_known_good:
        score -= 0.25
    if rejections:
        score -= min(0.25, 0.05 * len(rejections))
    if "ALL_SOURCE_EVENTS_IN_THE_PAST_WEEK_ROLLOVER_PENDING" in warnings:
        score -= 0.30
    if not rows:
        score = 0.0
    score = round(max(0.0, min(1.0, score)), 3)

    if not rows or score < 0.4:
        status = QualityStatus.INVALID
    elif warnings and (is_stale or source.from_last_known_good or score < 0.85):
        status = QualityStatus.DEGRADED
    else:
        status = QualityStatus.VALID

    quality = QualityInfo(
        status=status,
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
        events=tuple(events),
    )
    return payload.model_copy(update={"content_hash": canonical_content_hash(payload)})


# ─────────────────────────────────────────────────────────────────────────────
# HASH DE CONTENU — insensible aux champs volatils
# ─────────────────────────────────────────────────────────────────────────────
_VOLATILE_EVENT_FIELDS = ("time_context",)
_VOLATILE_ROOT_FIELDS = ("generated_at_utc", "content_hash", "source", "quality")


def canonical_content_hash(payload: CalendarPayload) -> str:
    """
    SHA-256 du contenu économique seul. Deux runs successifs sur des données
    identiques produisent le MÊME hash, même à des secondes différentes.
    """
    dumped = payload.model_dump(mode="json")
    for field in _VOLATILE_ROOT_FIELDS:
        dumped.pop(field, None)
    for event in dumped.get("events", []):
        for field in _VOLATILE_EVENT_FIELDS:
            event.pop(field, None)
    canonical = json.dumps(dumped, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + sha256_hex(canonical)


def refresh_time_contexts(
    payload: CalendarPayload, now_utc: datetime
) -> Tuple[CalendarEvent, ...]:
    """Recalcule les countdowns sans toucher au payload canonique."""
    return tuple(
        e.with_time_context(compute_time_context(e, now_utc, payload.selection_policy))
        for e in payload.events
    )


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

    for e in events:
        ctx = e.time_context
        rows.append({
            "occurrence_id": e.occurrence_id,
            "event_type_id": e.event_type_id,
            "release_group_id": e.release_group_id,
            "currency": e.currency,
            "event_name": e.name,
            "datetime_utc": iso_z(e.scheduled_at_utc),
            "date_display": e.date_display,
            "time_display": f"{e.scheduled_at_display.strftime('%H:%M')} ({e.display_timezone})",
            "day_of_week": e.day_of_week,
            "impact": e.impact.value.lower(),
            "forecast": e.forecast.raw,
            "forecast_value": e.forecast.value,
            "previous": e.previous.raw,
            "previous_value": e.previous.value,
            "actual": e.actual.raw,
            "actual_status": e.actual_status.value,
            "hours_until": ctx.hours_until,
            "hours_until_display": ctx.hours_until_display,
            "is_upcoming": ctx.is_upcoming,
            "time_proximity": ctx.time_proximity.value,
            "status": ctx.status.value,
            "session": _LEGACY_SESSION[e.session],
            "session_v2": e.session.value,
            "pairs_affected": list(e.pairs_with_currency_exposure),
            "pair_mapping_status": e.pair_mapping_status.value,
        })
        summary.setdefault(e.date_display, []).append(f"{e.currency} – {e.name}")

    return {
        "metadata": {
            "schema_version": f"legacy-1.1.0+core-{payload.schema_version}",
            "generated_at_utc": iso_z(payload.generated_at_utc),
            "content_hash": payload.content_hash,
            "source": payload.source.provider,
            "source_url": payload.source.url,
            "fetched_at_utc": iso_z(payload.source.fetched_at_utc),
            "supports_actual": payload.source.supports_actual,
            "timezone": f"UTC (backend) / {payload.selection_policy.display_timezone} (display)",
            "quality_status": payload.quality.status.value,
            "data_quality_score": payload.quality.data_quality_score,
            "is_stale": payload.quality.is_stale,
            "source_age_seconds": payload.quality.source_age_seconds,
            "warnings": list(payload.quality.warnings),
            "total_high_impact": len(rows),
            "upcoming_count": sum(1 for r in rows if r["is_upcoming"]),
            "imminent_count": sum(1 for r in rows if r["time_proximity"] == "IMMINENT"),
            "engine_events_count": len(rows),
            "summary_by_day_basis": "display_timezone",
            "ui_filters_applied": None,
        },
        "events": rows,
        "events_engine": rows,
        "summary_by_day": {k: summary[k] for k in sorted(summary)},
    }


_LEGACY_TIME_PROXIMITY_LABELS: Tuple[str, ...] = tuple(p.value for p in TimeProximity)
_LEGACY_SESSION_LABELS: Tuple[str, ...] = ("LONDON", "NEW YORK", "OVERLAP", "ASIAN", "OFF")


def to_bridge_payload(payload: CalendarPayload, now_utc: datetime) -> Dict[str, Any]:
    """
    Pont de compatibilité stricte pour les consommateurs v1 historiques
    (ex. application 'merge') qui attendent exactement la forme produite
    par l'ancien app.py monolithique : metadata.filters_applied présent,
    et 'actual' en placeholder '—' plutôt que null.

    Ne modifie PAS to_legacy_payload() : calendar.legacy.json conserve ses
    corrections v1.1.0 volontaires (actual=null explicite). Ce pont est une
    dérivation supplémentaire, écrite séparément dans calendar.json, donc
    aucune régression sur les artefacts existants.

    Note : selection_policy.currencies est None (= aucun filtre devise actif
    dans le pipeline v2). 'filters_applied.currencies' est donc reconstruit
    à partir des devises réellement présentes dans les événements retenus,
    et non recopié depuis une config figée qui n'existe plus côté ingestor.
    """
    bridge = to_legacy_payload(payload, now_utc)
    rows = bridge["events"]  # meme liste que events_engine

    for row in rows:
        if row["actual"] is None:
            row["actual"] = "\u2014"  # '—', comportement v1 historique

    currencies_filter = (
        sorted(payload.selection_policy.currencies)
        if payload.selection_policy.currencies
        else sorted({r["currency"] for r in rows})
    )

    meta = bridge["metadata"]
    meta.pop("ui_filters_applied", None)
    meta["filters_applied"] = {
        "currencies": currencies_filter,
        "sessions": list(_LEGACY_SESSION_LABELS),
        "show_past": payload.selection_policy.window_past_hours > 0,
        "time_proximity": list(_LEGACY_TIME_PROXIMITY_LABELS),
    }

    return bridge
      

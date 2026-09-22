"""bluestar calendar_compat -- socle COMMUN macro (calendar_layer.py) / desk
(calendar_core.py) / committee.

=============================================================================
RAISON D'ÊTRE
=============================================================================
Trois divergences macro/desk ne peuvent pas être résolues par des correctifs
symétriques dans deux fichiers : elles se re-créent à chaque évolution.

  [C12/C13] FUSEAU D'AFFICHAGE. macro lisait config.TZ_CET (Europe/Paris),
            desk codait "Africa/Casablanca". Un opérateur en Tunisie lisait
            donc deux heures différentes pour le même instant UTC. Ici :
            résolution UNIQUE, adaptative (système / raccourci pays / env),
            appliquée en AVAL du canonique — aucun instant UTC, aucun
            occurrence_id, aucun content_hash n'en dépend.

  [C14]     HASH DE PARITÉ. content_hash n'est PAS comparable entre les deux
            apps : c'est un hash de VUE, calculé APRÈS le filtre de policy
            (macro HIGH seul, desk HIGH+MEDIUM) et il projette
            release_group_id, qui dépend de la sélection. parity_hash()
            hashe le périmètre de CONTRAT (HIGH+MEDIUM, avant policy, fenêtre
            ancrée à l'heure ronde) : deux apps qui voient le même flux
            produisent le même parity_hash, quelle que soit leur policy.

  [C15]     SORTIE COMMITTEE. Le committee agrège macro + desk ; il doit lire
            une structure sans chaînes d'affichage, en UTC pur, avec les
            angles morts déclarés. to_committee_payload() + reconcile().

=============================================================================
GARANTIES
=============================================================================
* Ce module ne modifie JAMAIS une valeur canonique : il ajoute des clés et
  réécrit uniquement les champs d'AFFICHAGE (date_display / time_display /
  datetime_display / display_timezone / day_of_week).
* Il n'importe ni calendar_layer ni calendar_core (aucune circularité) : il
  travaille en duck typing sur les lignes legacy et sur les rows normalisées.
* Aucune I/O réseau, aucun état disque, aucune horloge implicite : now_utc
  est toujours passé par l'appelant.

Kill-switches (production) :
    BLUESTAR_TZ_AUTO=0    -> désactive la détection système (retour au
                             comportement historique : env puis fallback).
    BLUESTAR_TZ_PIN=0     -> désactive l'épinglage sur le tzdata pip.
    BLUESTAR_DISPLAY_TZ   -> force un fuseau (IANA ou raccourci pays).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import zoneinfo
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

logger = logging.getLogger(__name__)

COMPAT_MODULE_VERSION = "calendar-compat-1.0.0"
UTC = timezone.utc

# Table des jours identique à calendar_layer._DAY_NAMES / calendar_core :
# strftime("%A") dépend de la locale du conteneur (« JEUDI » vs « THURSDAY »),
# ce qui suffirait à faire diverger deux rapports sur la même donnée.
DAY_NAMES: Tuple[str, ...] = ("MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY",
                              "FRIDAY", "SATURDAY", "SUNDAY")

FALLBACK_DISPLAY_TZ = "Europe/Paris"


# =============================================================================
# 1. ÉPINGLAGE DE LA BASE tzdata
# =============================================================================
def pin_tzdata(force: Optional[bool] = None) -> Dict[str, Any]:
    """Place le tzdata pip EN TÊTE de TZPATH (stratégie validée côté desk,
    [OPUS-TZ 16/09/2026]) plutôt que de vider TZPATH : le repli système reste
    disponible pour toute zone absente du paquet.

    ``clear_cache()`` est indispensable : reset_tzpath() n'invalide PAS les
    instances ZoneInfo déjà construites (PEP 615). Sans lui, un import
    antérieur (config.TZ_CET côté macro) fige les anciennes règles et le
    garde annonce son propre succès en laissant le tzdata système faire
    autorité — la panne exacte mesurée sur Africa/Casablanca le 20/09/2026.

    IDEMPOTENT : si le répertoire est déjà en tête (cas où calendar_core a
    déjà appelé son propre _pin_pip_tzdata), on ne retouche rien. Les deux
    modules peuvent donc coexister dans le même processus sans se défaire
    mutuellement, quel que soit l'ordre d'import.
    """
    if force is None:
        force = os.getenv("BLUESTAR_TZ_PIN", "1") != "0"

    diag: Dict[str, Any] = {
        "pinned": False, "reason": "disabled" if not force else "not_attempted",
        "tzdata_version": None, "tzpath_head": None,
    }
    if not force:
        diag["tzpath_head"] = str(zoneinfo.TZPATH[0]) if zoneinfo.TZPATH else None
        return diag

    try:
        import tzdata as _td
        _dir = os.path.abspath(os.path.join(os.path.dirname(_td.__file__), "zoneinfo"))
        if not os.path.isdir(_dir):
            diag.update(reason="pip_dir_missing")
            return diag
        try:
            import importlib.metadata as _md
            diag["tzdata_version"] = _md.version("tzdata")
        except Exception:                                     # noqa: BLE001
            diag["tzdata_version"] = getattr(_td, "IANA_VERSION", None)

        current = [str(p) for p in zoneinfo.TZPATH]
        if current and os.path.abspath(current[0]) == _dir:
            diag.update(pinned=True, reason="already_pinned", tzpath_head=current[0])
            return diag

        zoneinfo.ZoneInfo.clear_cache()
        zoneinfo.reset_tzpath([_dir] + [p for p in current
                                        if os.path.abspath(p) != _dir])
        ZoneInfo("Africa/Tunis"); ZoneInfo("America/Toronto"); ZoneInfo("Europe/Paris")
        diag.update(pinned=True, reason="ok", tzpath_head=_dir)
    except Exception as exc:                                  # noqa: BLE001
        diag.update(pinned=False, reason=f"unavailable:{type(exc).__name__}",
                    tzpath_head=str(zoneinfo.TZPATH[0]) if zoneinfo.TZPATH else None)
        logger.info("tzdata pip indisponible (%s) — base système conservée", exc)
    return diag


TZDATA_PIN = pin_tzdata()


def tz_environment() -> Dict[str, Any]:
    """Empreinte de l'environnement fuseau, à exposer dans les métadonnées des
    DEUX apps. Si macro et desk n'affichent pas la même heure, c'est ce bloc
    qui le prouve en une lecture, sans avoir à rejouer un flux."""
    return {
        "compat_module_version": COMPAT_MODULE_VERSION,
        "tzdata_pinned": TZDATA_PIN.get("pinned"),
        "tzdata_source": TZDATA_PIN.get("reason"),
        "tzdata_version": TZDATA_PIN.get("tzdata_version"),
        "tzpath_head": TZDATA_PIN.get("tzpath_head"),
    }


# =============================================================================
# 2. RÉSOLUTION ADAPTATIVE DU FUSEAU D'AFFICHAGE
# =============================================================================
# Raccourcis opérateur : on tape "TN", "FR", "CA" — pas une clé IANA.
# Les pays à fuseaux multiples exigent une précision (CA-ON, CA-BC...) ;
# "CA" seul retombe sur Toronto, qui est le fuseau des heures de marché
# nord-américaines de référence du desk — choix explicite, pas un défaut.
TZ_SHORTCUTS: Dict[str, str] = {
    "TN": "Africa/Tunis",
    "TUNISIA": "Africa/Tunis", "TUNISIE": "Africa/Tunis",
    "FR": "Europe/Paris",
    "FRANCE": "Europe/Paris", "PARIS": "Europe/Paris",
    "MA": "Africa/Casablanca", "MAROC": "Africa/Casablanca",
    "CA": "America/Toronto",
    "CA-ON": "America/Toronto", "CA-QC": "America/Toronto",
    "CA-AB": "America/Edmonton", "CA-BC": "America/Vancouver",
    "CA-MB": "America/Winnipeg", "CA-NS": "America/Halifax",
    "CANADA": "America/Toronto", "MONTREAL": "America/Toronto",
    "UK": "Europe/London", "GB": "Europe/London", "LONDON": "Europe/London",
    "US": "America/New_York", "US-ET": "America/New_York",
    "US-CT": "America/Chicago", "US-PT": "America/Los_Angeles",
    "NY": "America/New_York",
    "CH": "Europe/Zurich", "BE": "Europe/Brussels", "DE": "Europe/Berlin",
    "AE": "Asia/Dubai", "SG": "Asia/Singapore", "JP": "Asia/Tokyo",
    "AU": "Australia/Sydney", "NZ": "Pacific/Auckland",
    "UTC": "UTC", "Z": "UTC",
}


def _valid_tz(key: Optional[str]) -> Optional[str]:
    if not key:
        return None
    try:
        ZoneInfo(key)
        return key
    except (ZoneInfoNotFoundError, ValueError, TypeError, OSError):
        return None


def normalize_tz_token(token: Optional[str]) -> Optional[str]:
    """Accepte indifféremment une clé IANA ("Africa/Tunis") ou un raccourci
    ("TN", "ca-qc"). Retourne une clé IANA valide, ou None."""
    if not token:
        return None
    raw = str(token).strip()
    direct = _valid_tz(raw)
    if direct:
        return direct
    return _valid_tz(TZ_SHORTCUTS.get(raw.upper().replace("_", "-")))


def _system_iana_key() -> Optional[str]:
    """Fuseau IANA de la machine hôte, sans dépendance externe.

    Trois sources, par ordre de fiabilité décroissante :
      1. variable TZ (posée explicitement par l'exploitant / le conteneur) ;
      2. /etc/timezone (Debian/Ubuntu, contenu = clé IANA en clair) ;
      3. cible du lien /etc/localtime (partout ailleurs sous Unix/macOS).
    Retourne None sous Windows sans TZ, ou si rien n'est concluant : la
    couche appelante retombe alors sur le comportement historique.
    """
    env_tz = normalize_tz_token(os.getenv("TZ"))
    if env_tz:
        return env_tz

    try:
        p = Path("/etc/timezone")
        if p.is_file():
            key = _valid_tz(p.read_text(encoding="utf-8").strip())
            if key:
                return key
    except OSError:
        pass

    try:
        link = Path("/etc/localtime")
        if link.exists():
            target = str(link.resolve())
            if "zoneinfo" in target:
                candidate = target.split("zoneinfo", 1)[1].lstrip("/")
                # /usr/share/zoneinfo/posix/Africa/Tunis -> Africa/Tunis
                for prefix in ("posix/", "right/"):
                    if candidate.startswith(prefix):
                        candidate = candidate[len(prefix):]
                key = _valid_tz(candidate)
                if key:
                    return key
    except OSError:
        pass
    return None


def resolve_display_tz(explicit: Optional[str] = None,
                       fallback: str = FALLBACK_DISPLAY_TZ) -> Tuple[str, str]:
    """Fuseau d'AFFICHAGE unique pour macro, desk et committee.

    Ordre de priorité, du plus fort au plus faible :
      1. ``explicit``            -> origin "explicit"   (argument de code/test)
      2. ``BLUESTAR_DISPLAY_TZ`` -> origin "env"        (comportement v6 conservé)
      3. fuseau système          -> origin "system"     (désactivable, TZ_AUTO=0)
      4. ``fallback``            -> origin "fallback"
      5. "UTC"                   -> origin "utc_last_resort"

    Retourne ``(cle_iana, origine)``. L'origine est exportée dans les
    métadonnées : une heure affichée doit toujours pouvoir être expliquée.

    NOTE D'EXPLOITATION — c'est le SEUL changement de comportement visible de
    tout ce chantier. Sur un poste réglé sur Africa/Tunis, l'affichage passe
    de Europe/Paris à Africa/Tunis. Aucun instant UTC, aucune décision
    (priority / is_blackout / hours_until), aucun hash n'est concerné. Pour
    figer l'ancien comportement : BLUESTAR_TZ_AUTO=0, ou poser
    BLUESTAR_DISPLAY_TZ=Europe/Paris (ce que les tests DOIVENT faire).
    """
    key = normalize_tz_token(explicit)
    if key:
        return key, "explicit"

    key = normalize_tz_token(os.getenv("BLUESTAR_DISPLAY_TZ"))
    if key:
        return key, "env"

    if os.getenv("BLUESTAR_TZ_AUTO", "1") != "0":
        key = _system_iana_key()
        if key:
            return key, "system"

    key = normalize_tz_token(fallback)
    if key:
        return key, "fallback"

    logger.error("Aucun fuseau d'affichage résoluble — repli UTC")
    return "UTC", "utc_last_resort"


def utc_offset_label(dt: datetime) -> str:
    """« UTC+1 », « UTC-4 », « UTC+5:30 » calculé sur l'offset RÉEL du
    datetime localisé. Jamais codé en dur : un « +1 » fixe serait faux en
    Europe/Paris l'été comme à Casablanca pendant le Ramadan."""
    offset = dt.utcoffset()
    if offset is None:
        return "UTC"
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    hh, mm = divmod(abs(total_minutes), 60)
    return f"UTC{sign}{hh}" + (f":{mm:02d}" if mm else "")


def _parse_utc(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)


def localize_rows(rows: Sequence[Dict[str, Any]], tz_key: str,
                  origin: str = "unknown") -> int:
    """Réécrit EN PLACE les seuls champs d'affichage des lignes legacy.

    Champs réécrits : date_display, time_display, datetime_display,
    display_timezone, day_of_week.
    Champs AJOUTÉS  : datetime_local_iso, display_tz_origin, utc_offset_label.
    Champs INTOUCHÉS : datetime_utc, hours_until, priority, tier, is_blackout,
    impact, occurrence_id, *_value, *_status — tout le décisionnel.

    Format strictement identique à celui de calendar_layer.to_legacy_payload
    v6.2 (« YYYY-MM-DD · HH:MM (UTC+X) ») : aucun gabarit de rendu à toucher.
    Retourne le nombre de lignes effectivement localisées.
    """
    try:
        tz = ZoneInfo(tz_key)
    except (ZoneInfoNotFoundError, ValueError, TypeError, OSError):
        logger.error("localize_rows: fuseau '%s' inconnu — lignes inchangées", tz_key)
        return 0

    done = 0
    for row in rows:
        when = _parse_utc(row.get("datetime_utc"))
        if when is None:
            continue
        local = when.astimezone(tz)
        label = utc_offset_label(local)
        row["date_display"] = local.strftime("%Y-%m-%d")
        row["time_display"] = f"{local.strftime('%H:%M')} ({label})"
        row["datetime_display"] = f"{local.strftime('%Y-%m-%d')} · {local.strftime('%H:%M')} ({label})"
        row["display_timezone"] = tz_key
        row["day_of_week"] = DAY_NAMES[local.weekday()]
        row["datetime_local_iso"] = local.isoformat()
        row["display_tz_origin"] = origin
        row["utc_offset_label"] = label
        done += 1
    return done


def rebuild_summary_by_day(rows: Sequence[Dict[str, Any]]) -> Dict[str, List[str]]:
    """summary_by_day est indexé par date d'AFFICHAGE : changer de fuseau peut
    déplacer un événement de veille au lendemain (Tokyo 23:00 UTC = jour+1 à
    Paris). À rappeler après localize_rows, sinon le sommaire et les lignes se
    contredisent d'une journée."""
    out: Dict[str, List[str]] = {}
    for row in rows:
        out.setdefault(str(row.get("date_display", "")), []).append(
            f"{row.get('currency', '?')} – {row.get('event_name', '?')}")
    return {k: out[k] for k in sorted(out)}


# =============================================================================
# 3. HASH DE PARITÉ (périmètre de CONTRAT, avant policy)
# =============================================================================
PARITY_HASH_METHOD = "economic_projection_v2:contract_scope_no_group"
CONTRACT_IMPACT_SCOPE: Tuple[str, ...] = ("HIGH", "MEDIUM")
PARITY_WINDOW_PAST_H = 72.0
PARITY_WINDOW_FUTURE_H = 168.0


def parity_window(now_utc: datetime) -> Tuple[datetime, datetime, datetime]:
    """Fenêtre de parité ANCRÉE À L'HEURE RONDE.

    Sans cet ancrage, le hash serait inutilisable : macro tournant à 10:02 et
    desk à 10:07 n'auraient pas les mêmes bornes, et tout événement situé au
    voisinage de now-72h ou now+168h ferait diverger le hash pour une raison
    purement horlogère. Avec l'ancrage, deux exécutions dans la MÊME heure UTC
    comparent exactement le même périmètre.

    Retourne (anchor, lo, hi).
    """
    anchor = now_utc.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    return (anchor,
            anchor - timedelta(hours=PARITY_WINDOW_PAST_H),
            anchor + timedelta(hours=PARITY_WINDOW_FUTURE_H))


def _impact_str(value: Any) -> str:
    return str(getattr(value, "value", value) or "").upper()


def _raw_of(value: Any) -> Optional[str]:
    """Accepte un NumericValue (attribut .raw), une chaîne, ou None."""
    if value is None:
        return None
    raw = getattr(value, "raw", value)
    return raw if raw is None else str(raw)


def parity_projection(rows: Iterable[Dict[str, Any]], lo: datetime,
                      hi: datetime) -> List[Dict[str, Any]]:
    """Projection économique du périmètre de contrat, à partir des rows
    NORMALISÉES (sortie de _normalize_row, AVANT le filtre de policy).

    Trois exclusions délibérées :
      * release_group_id : le groupement dépend de la sélection (un HIGH
        simultané d'un MEDIUM est groupé côté desk, pas côté macro) ;
      * tout champ d'affichage : le fuseau ne doit rien changer ;
      * tout champ volatil (hours_until, priority) : recalculables.
    """
    out: List[Dict[str, Any]] = []
    for r in rows:
        impact = _impact_str(r.get("impact"))
        if impact not in CONTRACT_IMPACT_SCOPE:
            continue
        when = _parse_utc(r.get("scheduled_at_utc") or r.get("datetime_utc"))
        if when is None or not (lo <= when <= hi):
            continue
        out.append({
            "occurrence_id": r.get("occurrence_id"),
            "event_type_id": r.get("event_type_id"),
            "scheduled_at_utc": when.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "currency": r.get("currency"),
            "name": r.get("name") or r.get("event_name"),
            "impact": impact,
            "forecast": _raw_of(r.get("forecast")),
            "previous": _raw_of(r.get("previous")),
            "actual": _raw_of(r.get("actual")),
        })
    out.sort(key=lambda d: (d["scheduled_at_utc"], d["currency"] or "",
                            d["name"] or "", d["occurrence_id"] or ""))
    return out


def parity_hash(rows: Iterable[Dict[str, Any]], lo: datetime,
                hi: datetime) -> str:
    projection = parity_projection(rows, lo, hi)
    canonical = json.dumps(
        {"method": PARITY_HASH_METHOD, "scope": list(CONTRACT_IMPACT_SCOPE),
         "events": projection},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# =============================================================================
# 4. SORTIE STRUCTURÉE POUR LE COMMITTEE
# =============================================================================
COMMITTEE_SCHEMA = "bluestar-committee-calendar-1.0"

# Champs repris tels quels depuis la ligne legacy : UTC et valeurs brutes
# uniquement. Aucune chaîne localisée ne franchit cette frontière — le
# committee compare des instants, pas des libellés.
_COMMITTEE_EVENT_KEYS = (
    "occurrence_id", "event_type_id", "release_group_id", "release_group_type",
    "currency", "event_name", "datetime_utc", "impact", "priority",
    "tier", "is_blackout", "hours_until", "is_upcoming", "time_proximity",
    "status", "session_v2", "pairs_affected",
    "forecast_value", "forecast_status", "previous_value", "previous_status",
    "actual_value", "actual_status",
)


def to_committee_payload(legacy: Dict[str, Any], now_utc: datetime, *,
                         module: str) -> Dict[str, Any]:
    """Vue machine du calendrier, destinée à l'app committee.

    ``module`` : "macro" ou "desk". ``legacy`` : sortie de build_calendar().

    Contrat : pas une seule chaîne d'affichage, pas un seul fuseau local.
    Le committee doit pouvoir empiler macro et desk sans jamais avoir à
    deviner dans quel fuseau une heure a été écrite.
    """
    meta: Dict[str, Any] = dict(legacy.get("metadata") or {})
    rows: List[Dict[str, Any]] = list(legacy.get("events_engine")
                                      or legacy.get("events") or [])

    events: List[Dict[str, Any]] = []
    for r in rows:
        events.append({k: r.get(k) for k in _COMMITTEE_EVENT_KEYS if k in r})
    events.sort(key=lambda d: (str(d.get("datetime_utc") or ""),
                               str(d.get("currency") or ""),
                               str(d.get("occurrence_id") or "")))

    blackout = [e for e in events if e.get("is_blackout")]
    return {
        "committee_schema": COMMITTEE_SCHEMA,
        "module": module,
        "computed_at_utc": now_utc.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "identity": {
            "parity_hash": meta.get("parity_hash"),
            "parity_hash_method": meta.get("parity_hash_method"),
            "parity_window_anchor_utc": meta.get("parity_window_anchor_utc"),
            "high_content_hash": meta.get("high_content_hash"),
            "view_content_hash": meta.get("content_hash"),
            "source_payload_sha256": meta.get("source_payload_sha256")
                                     or meta.get("payload_sha256"),
            "feed_sha256": meta.get("feed_sha256") or {},
            "contract_version": meta.get("contract_version"),
            "schema_version": meta.get("schema_version"),
            "actuals_overlay_applied": bool(meta.get("actuals_overlay_applied")),
            "actuals_overlay_source": meta.get("actuals_overlay_source"),
            "actuals_overlay_matched": meta.get("actuals_overlay_matched"),
        },
        "scope": {
            "impact_levels_included": meta.get("impact_levels_included") or [],
            "contract_impact_scope": list(CONTRACT_IMPACT_SCOPE),
            "currencies_filter": meta.get("currencies_filter"),
            "currencies_covered": meta.get("currencies_covered") or [],
            # Angles morts : une devise « exclue par policy » n'est PAS une
            # devise sans risque. Le committee doit pondérer, pas ignorer.
            "currencies_excluded_by_policy":
                meta.get("currencies_excluded_by_policy") or [],
            "currencies_no_data_in_source":
                meta.get("currencies_no_data_in_source") or [],
            "window_past_hours": meta.get("window_past_hours"),
            "window_future_hours": meta.get("window_future_hours"),
            "priority_thresholds": meta.get("priority_thresholds"),
        },
        "quality": {
            "status": meta.get("quality_status"),
            "data_quality_score": meta.get("data_quality_score"),
            "reachable": meta.get("reachable"),
            "is_stale": meta.get("is_stale"),
            "source_age_seconds": meta.get("source_age_seconds"),
            "serving_mode": meta.get("serving_mode"),
            "feed_horizon_state": meta.get("feed_horizon_state"),
            "feed_horizon_h": meta.get("feed_horizon_h"),
            "week_rollover_pending": meta.get("week_rollover_pending"),
            "warnings": meta.get("warnings") or [],
        },
        "timezone_environment": {**tz_environment(),
                                 "display_timezone": meta.get("display_timezone"),
                                 "display_tz_origin": meta.get("display_tz_origin")},
        "counters": {
            "events_total": len(events),
            "high_impact_count": sum(1 for e in events
                                     if _impact_str(e.get("impact")) == "HIGH"),
            "critical_count": meta.get("critical_count"),
            "blackout_count": len(blackout),
            "blackout_currencies": sorted({e["currency"] for e in blackout
                                           if e.get("currency")}),
        },
        "events": events,
    }


def reconcile(macro: Dict[str, Any], desk: Dict[str, Any]) -> Dict[str, Any]:
    """Compare deux sorties de to_committee_payload.

    ``aligned=True`` exige l'égalité du parity_hash ET de l'ancre de fenêtre :
    deux hash calculés sur deux heures différentes ne prouvent rien.

    En cas de divergence, on ne se contente pas de la signaler : on livre les
    occurrence_id en cause, côté par côté. C'est la seule forme de rapport
    exploitable à 7h du matin avant ouverture.
    """
    mi, di = macro.get("identity", {}), desk.get("identity", {})
    m_ids = {e.get("occurrence_id") for e in macro.get("events", [])}
    d_ids = {e.get("occurrence_id") for e in desk.get("events", [])}

    same_anchor = mi.get("parity_window_anchor_utc") == di.get("parity_window_anchor_utc")
    same_hash = (mi.get("parity_hash") is not None
                 and mi.get("parity_hash") == di.get("parity_hash"))

    report: Dict[str, Any] = {
        "aligned": bool(same_hash and same_anchor),
        "parity_hash_match": same_hash,
        "anchor_match": same_anchor,
        "macro_parity_hash": mi.get("parity_hash"),
        "desk_parity_hash": di.get("parity_hash"),
        "macro_anchor_utc": mi.get("parity_window_anchor_utc"),
        "desk_anchor_utc": di.get("parity_window_anchor_utc"),
        "source_payload_match": (mi.get("source_payload_sha256")
                                 == di.get("source_payload_sha256")),
        "counts": {"macro": len(m_ids), "desk": len(d_ids),
                   "intersection": len(m_ids & d_ids)},
        "only_in_macro": sorted(i for i in (m_ids - d_ids) if i),
        "only_in_desk": sorted(i for i in (d_ids - m_ids) if i),
        "divergences": [],
    }

    if not same_anchor:
        report["divergences"].append(
            "ANCHOR_MISMATCH: exécutions dans deux heures UTC différentes — "
            "rejouer les deux modules dans la même heure avant de conclure.")
    elif not same_hash:
        if not report["source_payload_match"]:
            report["divergences"].append(
                "SOURCE_PAYLOAD_MISMATCH: les deux modules n'ont pas lu le "
                "même flux (fetch décalé, miroir, ou cache). Cause probable.")
        else:
            report["divergences"].append(
                "CONTRACT_SCOPE_MISMATCH: même flux, périmètre de contrat "
                "différent — vérifier le parseur numérique et le filtre de "
                "fenêtre avant de suspecter la policy.")

    # L'overlay « actuals » du desk lit une SECONDE source (page HTML FF) que
    # le macro n'a pas : un actual présent d'un seul côté est une asymétrie
    # de périmètre ASSUMÉE, pas une divergence de données. On ne compare donc
    # actual_value que si les deux modules ont le même état d'overlay — sinon
    # on le déclare explicitement comme angle mort, jamais silencieusement.
    overlay_asym = bool(mi.get("actuals_overlay_applied")) != bool(
        di.get("actuals_overlay_applied"))
    compared = ["datetime_utc", "impact", "forecast_value", "previous_value",
                "tier", "is_blackout"]
    if not overlay_asym:
        compared.append("actual_value")
    report["actual_value_compared"] = not overlay_asym
    if overlay_asym:
        report.setdefault("known_asymmetries", []).append(
            "ACTUALS_OVERLAY_ONE_SIDED: le desk publie des actuals issus de la "
            "page HTML FF, absents du flux JSON lu par le macro. "
            "actual_value exclu de la comparaison — le committee doit lire "
            "l'actual du module qui le porte, jamais conclure à un écart.")

    m_by_id = {e.get("occurrence_id"): e for e in macro.get("events", [])}
    d_by_id = {e.get("occurrence_id"): e for e in desk.get("events", [])}
    field_diffs: List[Dict[str, Any]] = []
    for oid in sorted(i for i in (m_ids & d_ids) if i):
        me, de = m_by_id[oid], d_by_id[oid]
        for field in compared:
            if field in me and field in de and me[field] != de[field]:
                field_diffs.append({"occurrence_id": oid, "field": field,
                                    "macro": me[field], "desk": de[field]})
    report["field_divergences"] = field_diffs[:200]
    report["field_divergence_count"] = len(field_diffs)
    if field_diffs:
        report["aligned"] = False
        report["divergences"].append(
            f"FIELD_MISMATCH: {len(field_diffs)} écart(s) sur des événements "
            "pourtant partagés — à traiter avant toute décision de trading.")
    return report


__all__ = [
    "COMPAT_MODULE_VERSION", "DAY_NAMES", "FALLBACK_DISPLAY_TZ",
    "TZ_SHORTCUTS", "TZDATA_PIN",
    "pin_tzdata", "tz_environment", "normalize_tz_token", "resolve_display_tz",
    "utc_offset_label", "localize_rows", "rebuild_summary_by_day",
    "PARITY_HASH_METHOD", "CONTRACT_IMPACT_SCOPE", "PARITY_WINDOW_PAST_H",
    "PARITY_WINDOW_FUTURE_H", "parity_window", "parity_projection", "parity_hash",
    "COMMITTEE_SCHEMA", "to_committee_payload", "reconcile",
]

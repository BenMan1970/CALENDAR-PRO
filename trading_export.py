#!/usr/bin/env python3
"""
BLUESTAR Calendar Pro - Trading Export Generator
================================================
Génère un JSON optimisé pour les applications de trading.

Durcissements [2.7] :
  * ``is_upcoming`` est RECALCULÉ depuis ``scheduled_at_utc`` : la valeur
    stockée dans l'artefact est figée à l'ingestion, donc fausse dès que
    l'artefact vieillit — ce qui est permanent pendant un cooldown ;
  * accès défensif aux sous-objets : ``{"time_context": null}`` faisait lever
    un AttributeError, de même que ``forecast``/``previous``/``actual`` nuls ;
  * l'export déclare sa fraîcheur (``artifact_age_s``, ``quality_status``,
    ``is_stale``, ``fetched_at_utc``) et REFUSE (code retour non nul) de
    produire un fichier au-delà d'un seuil configurable — un consommateur
    aval doit pouvoir faire confiance à ce fichier ;
  * chemins résolus depuis ``__file__``, pas depuis le répertoire courant ;
  * ``pairs_with_currency_exposure`` reste un champ de CONTEXTE, jamais un
    signal de trading.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

UTC = timezone.utc

ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = ROOT / "data"

# Seuil de refus : au-delà, l'export n'est pas produit. Un artefact « trade »
# plus vieux que deux fois la fraîcheur exigée par la policy (900 s) n'a plus
# de valeur décisionnelle.
DEFAULT_MAX_AGE_S = 3600

# [F8] Version du CONTRAT de ce document. À incrémenter dès qu'une clé
# racine ou un champ d'événement change de sens : un consommateur aval doit
# pouvoir refuser un schéma qu'il ne connaît pas plutôt que le mal lire.
EXPORT_SCHEMA_VERSION = "trading-export-1.0.0"


def iso_z(dt: datetime) -> str:
    """Instant UTC au format ``...Z`` — identique au reste du contrat."""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(raw: Any) -> Optional[datetime]:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _raw_of(node: Any) -> Optional[str]:
    """``node`` peut être absent, ``None`` ou un mapping : jamais d'exception."""
    if isinstance(node, dict):
        value = node.get("raw")
        return None if value is None else str(value)
    if node is None:
        return None
    return str(node)


def _dict_of(node: Any) -> Dict[str, Any]:
    """``event.get("time_context", {})`` renvoie ``None`` si la clé existe avec
    la valeur null : le défaut ne s'applique QUE si la clé est absente."""
    return node if isinstance(node, dict) else {}


def load_canonical(data_dir: Path) -> Optional[Dict[str, Any]]:
    path = data_dir / "calendar.latest.json"
    if not path.exists():
        return None
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def artifact_facts(data: Dict[str, Any], now: Optional[datetime] = None
                   ) -> Dict[str, Any]:
    """Fraîcheur et qualité de l'artefact — indispensables à un usage trading."""
    now = now or datetime.now(UTC)
    src = _dict_of(data.get("source"))
    quality = _dict_of(data.get("quality"))

    fetched = _parse_utc(src.get("fetched_at_utc")
                         or data.get("generated_at_utc"))
    age: Optional[int] = None
    if fetched is not None:
        age = max(0, int((now - fetched).total_seconds()))

    # ``is_stale`` est figé à l'ingestion : on le recalcule, sinon un artefact
    # vieux de trois jours s'annoncerait encore « frais ».
    declared_stale = bool(quality.get("is_stale"))
    max_age = None
    policy = _dict_of(data.get("selection_policy"))
    try:
        max_age = float(policy.get("max_source_age_seconds"))
    except (TypeError, ValueError):
        max_age = None
    computed_stale = age is not None and max_age is not None and age > max_age

    return {
        "generated_at_utc": data.get("generated_at_utc"),
        "fetched_at_utc": src.get("fetched_at_utc"),
        "artifact_age_s": age,
        "quality_status": quality.get("status"),
        "is_stale": bool(declared_stale or computed_stale),
        "max_source_age_seconds": max_age,
        "content_hash": data.get("content_hash"),
        "parity_hash": data.get("parity_hash"),
        "event_count": len(data.get("events") or []),
    }


def build_export(data: Dict[str, Any], now: Optional[datetime] = None
                 ) -> Tuple[Dict[str, Any], int]:
    """(document, nombre d'événements à venir)."""
    now = now or datetime.now(UTC)
    events = data.get("events")
    events = events if isinstance(events, list) else []

    upcoming: List[Dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        # [2.7] Recacul depuis scheduled_at_utc : is_upcoming est figé à
        # l'ingestion et devient faux dès que l'artefact vieillit.
        scheduled = _parse_utc(event.get("scheduled_at_utc"))
        if scheduled is None:
            continue
        hours_until = (scheduled - now).total_seconds() / 3600.0
        if hours_until <= 0:
            continue
        upcoming.append({
            "id": event.get("occurrence_id"),
            "currency": event.get("currency"),
            "name": event.get("name"),
            "impact": event.get("impact"),
            "datetime_utc": event.get("scheduled_at_utc"),
            "datetime_display": event.get("scheduled_at_display"),
            "hours_until": round(hours_until, 4),
            "forecast": _raw_of(event.get("forecast")),
            "previous": _raw_of(event.get("previous")),
            "actual": _raw_of(event.get("actual")),
            "actual_status": event.get("actual_status"),
            # [2.7] Champ de CONTEXTE : la liste des paires exposées n'est pas
            # un signal directionnel et ne doit pas être consommée comme tel.
            "pairs_with_currency_exposure": list(
                event.get("pairs_with_currency_exposure") or []),
            "pairs_semantics": "context_only_not_a_signal",
        })
    upcoming.sort(key=lambda e: e["datetime_utc"] or "")

    facts = artifact_facts(data, now)
    document = {
        # [F8] Un consommateur institutionnel doit pouvoir identifier le
        # CONTRAT qu'il lit et savoir QUAND le document a été produit. Sans
        # ``exported_at_utc``, il devait dériver l'heure d'export de
        # ``fetched_at_utc + artifact_age_s`` — fragile et indirect.
        "schema_version": EXPORT_SCHEMA_VERSION,
        "exported_at_utc": iso_z(now),
        # [2.9h] Les deux artefacts portent le même generated_at_utc : c'est ce
        # qui permet à un consommateur aval de détecter un couple incohérent.
        "generated_at_utc": facts["generated_at_utc"],
        "fetched_at_utc": facts["fetched_at_utc"],
        "artifact_age_s": facts["artifact_age_s"],
        "quality_status": facts["quality_status"],
        "is_stale": facts["is_stale"],
        "content_hash": facts["content_hash"],
        "parity_hash": facts["parity_hash"],
        "source": _dict_of(data.get("source")).get("provider"),
        "source_url": _dict_of(data.get("source")).get("resolved_url")
                      or _dict_of(data.get("source")).get("url"),
        "total_events": len(events),
        "upcoming_events": len(upcoming),
        "events": upcoming,
    }
    return document, len(upcoming)


def generate_trading_export(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Export JSON pour applications de trading.")
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR),
                        help="répertoire des artefacts (défaut : ./data)")
    parser.add_argument("--out", default=None,
                        help="fichier de sortie (défaut : <data-dir>/trading_export.json)")
    parser.add_argument("--max-age-s", type=float,
                        default=float(os.getenv("BLUESTAR_TRADING_MAX_AGE_S",
                                                DEFAULT_MAX_AGE_S)),
                        help="refuse l'export au-delà de cet âge (secondes)")
    parser.add_argument("--now", default=None,
                        help="instant de référence ISO (tests)")
    parser.add_argument("--allow-stale", action="store_true",
                        help="produire l'export malgré un artefact périmé")
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir).resolve()
    now = _parse_utc(args.now) or datetime.now(UTC)

    data = load_canonical(data_dir)
    if data is None:
        print(f"ERROR: {data_dir / 'calendar.latest.json'} absent ou illisible. "
              "Lancez pipeline_calendar.py --once d'abord.", file=sys.stderr)
        return 1

    facts = artifact_facts(data, now)
    if facts["quality_status"] == "INVALID":
        print("ERROR: artefact INVALID — export refusé (données non fiables).",
              file=sys.stderr)
        return 2

    age = facts["artifact_age_s"]
    if age is None:
        print("ERROR: âge de l'artefact indéterminable (aucun horodatage) — "
              "export refusé.", file=sys.stderr)
        return 2
    if age > args.max_age_s and not args.allow_stale:
        print(
            f"ERROR: artefact périmé — {age}s > seuil {args.max_age_s}s "
            f"(fetched_at_utc={facts['fetched_at_utc']}, "
            f"quality={facts['quality_status']}). "
            "Relancez l'ingestion, ou utilisez --allow-stale en connaissance "
            "de cause.", file=sys.stderr)
        return 3

    document, n_upcoming = build_export(data, now)
    out_path = Path(args.out).resolve() if args.out else data_dir / "trading_export.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(document, fh, indent=2, ensure_ascii=False)
    tmp.replace(out_path)

    print(f"SUCCESS: {n_upcoming} upcoming events exported to {out_path} "
          f"(age={age}s, quality={facts['quality_status']}, "
          f"stale={facts['is_stale']})")
    return 0


if __name__ == "__main__":
    sys.exit(generate_trading_export())
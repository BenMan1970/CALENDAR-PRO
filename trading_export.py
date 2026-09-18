#!/usr/bin/env python3
"""
BLUESTAR Calendar Pro - Trading Export Generator
Génère un JSON optimisé pour les applications de trading.
"""

import json
import sys
from pathlib import Path

def generate_trading_export():
    """Extrait les événements à venir depuis le fichier canonique."""
    
    calendar_path = Path("data/calendar.latest.json")
    if not calendar_path.exists():
        print("ERROR: calendar.latest.json not found. Run pipeline_calendar.py --once first.")
        return 1
    
    with open(calendar_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    events = data.get("events", [])
    
    # Filtrer les événements à venir (upcoming)
    upcoming = []
    for event in events:
        time_context = event.get("time_context", {})
        if time_context.get("is_upcoming", False):
            upcoming.append({
                "id": event.get("occurrence_id"),
                "currency": event.get("currency"),
                "name": event.get("name"),
                "impact": event.get("impact"),
                "datetime_utc": event.get("scheduled_at_utc"),
                "datetime_display": event.get("scheduled_at_display"),
                "forecast": event.get("forecast", {}).get("raw"),
                "previous": event.get("previous", {}).get("raw"),
                "actual": event.get("actual", {}).get("raw"),
                "actual_status": event.get("actual_status"),
                "pairs": event.get("pairs_with_currency_exposure", [])
            })
    
    # Export JSON optimisé pour le trading
    output = {
        "generated_at": data.get("generated_at_utc"),
        "source": data.get("source", {}).get("provider"),
        "total_events": len(events),
        "upcoming_events": len(upcoming),
        "events": upcoming
    }
    
    output_path = Path("data/trading_export.json")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"SUCCESS: {len(upcoming)} upcoming events exported to {output_path}")
    return 0

if __name__ == "__main__":
    sys.exit(generate_trading_export())

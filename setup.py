#!/usr/bin/env python3
"""
BLUESTAR Calendar Pro — Setup Script
====================================
Génère le seed nécessaire au déploiement Streamlit Cloud.

[2.8] Auparavant, ``run_once`` renvoyant ``None`` (cooldown 429, artefact
encore récent, mode lecteur), le seed n'était pas écrit et ce script
concluait « vérifiez votre connexion » — un diagnostic FAUX : l'artefact
canonique était bien présent sur le disque. On réutilise donc cet artefact
s'il est valide, et on affiche la vraie raison de l'absence de cycle.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pipeline_calendar as pc


def main() -> int:
    print("BLUESTAR Calendar Pro - Setup")
    print("=" * 40)

    data_dir = Path(__file__).resolve().parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    print(f"[OK] data/ directory: {data_dir}")

    print("\nGenerating seed file...")
    seed_path = pc.emit_seed(data_dir)

    if seed_path:
        print(f"[OK] Seed generated: {seed_path}")
        data = pc.read_json(seed_path)
        if isinstance(data, dict) and "events" in data:
            print(f"[OK] {len(data['events'])} events included")
            print(f"[OK] generated_at_utc: {data.get('generated_at_utc')}")
        return 0

    # La VRAIE raison, jamais « vérifiez votre connexion ».
    reason = pc.seed_failure_reason(data_dir)
    print("[ERROR] Seed non généré.")
    print(f"  raison : {reason}")
    canonical = data_dir / "calendar.latest.json"
    if canonical.exists():
        print(f"  note  : {canonical} existe pourtant — inspectez-le "
              "(CalendarPayload.model_validate a échoué ou il est INVALID).")
    else:
        print("  note  : aucun artefact local. Lancez "
              "`python pipeline_calendar.py --once` pour en produire un.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
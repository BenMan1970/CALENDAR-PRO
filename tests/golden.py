"""GOLDEN : hashes de contenu produits par build_payload SUR LA FIXTURE.

Ces quatre valeurs sont le contrat de non-regression le plus fort du chantier :
toute modification de calendar_core.py, calendar_compat.py ou
pipeline_calendar.py qui les ferait bouger romprait la parite inter-apps.

Le fichier est ecrit par tests/_make_golden.py et lu par tests/test_golden.py.
"""
from __future__ import annotations

import json
from pathlib import Path

GOLDEN_PATH = Path(__file__).resolve().parent / "fixtures" / "golden.json"


def load_golden() -> dict:
    with GOLDEN_PATH.open(encoding="utf-8") as fh:
        return json.load(fh)


__all__ = ["GOLDEN_PATH", "load_golden"]
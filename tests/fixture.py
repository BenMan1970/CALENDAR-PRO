"""Fixture de flux Forex Factory / Fair Economy -- LOCALE, jamais reseau.

Format du flux public hebdomadaire, tel qu'observe : ``title``, ``country``,
``date``, ``impact``, ``forecast``, ``previous`` -- **sans cle ``actual``**
(c'est precisement ce que constate l'app en production, et ce que le warning
``SOURCE_DOES_NOT_PROVIDE_ACTUAL`` constate).

Le fichier est un JSON plat, volontairement stable : les hashes « golden »
(``content_hash``, ``parity_hash``, ``high_content_hash``) en dependent, et
ces hashes doivent rester IDENTIQUES apres tous les correctifs.
"""
from __future__ import annotations

import json
from pathlib import Path

FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "ff_thisweek.json"


def load_fixture() -> list[dict]:
    """Charge la fixture brute (une nouvelle liste a chaque appel : le code de
    normalisation peut la tronquer)."""
    with FIXTURE_PATH.open(encoding="utf-8") as fh:
        return json.load(fh)


__all__ = ["FIXTURE_PATH", "load_fixture"]
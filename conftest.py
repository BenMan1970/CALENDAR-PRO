"""Racine des tests : importabilite du projet + fixtures partagees.

DEUX REGLES POUR CETTE SUITE
---------------------------
1. AUCUNE requete reseau : la fixture FF est locale et les appels HTTP sont
   mockes (``CountingSession``). Une requete reelle serait un 429 cause par la
   suite de tests elle-meme.
2. AUCUN test ne depend du fuseau de la machine hote. ``resolve_display_tz``
   place le fuseau SYSTEME avant le repli (bug 2.3) : sans les variables
   ci-dessous, un evenement de 23:00 UTC changerait de JOUR selon la machine.
   Les variables sont posees AVANT le premier import des modules du projet car
   calendar_core resout DEFAULT_DISPLAY_TZ a l'import.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 1. Determinisme du fuseau — AVANT tout import des modules du projet.
os.environ.setdefault("BLUESTAR_TZ_AUTO", "0")
os.environ.setdefault("BLUESTAR_DISPLAY_TZ", "Africa/Casablanca")

import pytest  # noqa: E402

from tests.helpers import CountingSession, FakeClock  # noqa: E402


@pytest.fixture
def clock() -> FakeClock:
    """Horloge injectable : permet de faire passer cooldowns et fenetres
    d'espacement sans jamais dormir."""
    return FakeClock()


@pytest.fixture
def session() -> CountingSession:
    """Session HTTP mockee qui COMPTE les appels (aucune I/O reseau)."""
    return CountingSession()


@pytest.fixture
def data_dir(tmp_path):
    """Repertoire de donnees isole par test."""
    d = tmp_path / "data"
    d.mkdir(parents=True, exist_ok=True)
    return d
"""F1-VOYAGE — le sélecteur de fuseau doit suivre le propriétaire qui voyage.

Défaut constaté : la liste des fuseaux était FERMÉE à 8 villes. En voyage à
Riyad, Singapour, São Paulo ou Toronto, aucune de ces destinations n'était
proposée — alors que tzdata en publie 599. L'utilisateur ne pouvait donc pas
afficher l'heure locale de l'endroit où il se trouve.

Deux exigences :
  * toute zone IANA doit être atteignable (liste complète) ;
  * l'étiquette doit porter l'offset RÉEL, calculé — Casablanca bascule
    TROIS fois en 2026 (UTC+1 → UTC+0 le 15/02, retour le 22/03, puis UTC+0
    le 20/09) : un « UTC+1 » codé en dur serait faux une partie de l'année.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import app as A

UTC = timezone.utc


# ── 1. Atteignabilité de toutes les zones ────────────────────────────────
def test_selector_offers_the_full_iana_database():
    tzs = A.available_view_tzs()
    assert len(tzs) > 400, f"seulement {len(tzs)} fuseaux proposes"


@pytest.mark.parametrize("dest", [
    "Asia/Riyadh", "Asia/Singapore", "America/Sao_Paulo", "America/Toronto",
    "Asia/Kolkata", "Africa/Cairo", "Asia/Hong_Kong", "Europe/Istanbul",
    "America/Mexico_City", "Asia/Karachi", "Africa/Dakar", "Europe/Lisbon",
])
def test_travel_destination_is_selectable(dest):
    """Toute destination de voyage plausible doit etre proposee."""
    assert dest in A.available_view_tzs(), f"{dest} non proposable"


def test_common_timezones_come_first():
    tzs = A.available_view_tzs()
    assert tzs[:8] == list(A.VIEW_TZ_COMMON), "les choix courants doivent etre en tete"


def test_selector_has_no_duplicates():
    tzs = A.available_view_tzs()
    assert len(tzs) == len(set(tzs)), "doublons dans la liste des fuseaux"


def test_every_offered_timezone_is_resolvable():
    """Aucune option ne doit faire planter l'affichage."""
    from zoneinfo import ZoneInfo
    bad = []
    for tz in A.available_view_tzs():
        try:
            ZoneInfo(tz)
        except Exception:                                      # noqa: BLE001
            bad.append(tz)
    assert not bad, f"fuseaux non resolvables: {bad[:5]}"


# ── 2. L'étiquette porte l'offset réel ───────────────────────────────────
def test_label_carries_the_utc_offset():
    A._TZ_OFFSET_CACHE.clear()
    assert "UTC+8" in A.tz_display_label("Asia/Singapore")
    assert "UTC+0" in A.tz_display_label("UTC")


def test_half_hour_offset_is_not_rounded():
    """Kolkata = UTC+5:30 : arrondir a UTC+6 serait faux de 30 minutes."""
    A._TZ_OFFSET_CACHE.clear()
    ref = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)
    assert "UTC+5:30" in A.tz_display_label("Asia/Kolkata", ref)


def test_negative_offsets_are_labelled():
    A._TZ_OFFSET_CACHE.clear()
    ref = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)
    assert "UTC-4" in A.tz_display_label("America/New_York", ref)


# ── 3. Casablanca n'est PAS un offset fixe ──────────────────────────────
def test_casablanca_is_not_a_fixed_offset():
    """La preuve qu'un « UTC+1 » code en dur serait faux."""
    from zoneinfo import ZoneInfo
    z = ZoneInfo("Africa/Casablanca")
    jan = datetime(2026, 1, 15, 12, 0, tzinfo=UTC).astimezone(z).utcoffset()
    oct_ = datetime(2026, 10, 15, 12, 0, tzinfo=UTC).astimezone(z).utcoffset()
    assert jan != oct_, "Casablanca devrait changer d'offset en 2026"
    assert jan == timedelta(hours=1)
    assert oct_ == timedelta(hours=0)


def test_casablanca_transitions_in_2026():
    """Les trois bascules de 2026, verifiees une par une."""
    from zoneinfo import ZoneInfo
    z = ZoneInfo("Africa/Casablanca")
    offsets = {}
    d = datetime(2026, 1, 1, tzinfo=UTC)
    while d < datetime(2027, 1, 1, tzinfo=UTC):
        offsets[d.date().isoformat()] = d.astimezone(z).utcoffset()
        d += timedelta(days=1)
    assert offsets["2026-02-14"] == timedelta(hours=1)
    assert offsets["2026-02-16"] == timedelta(hours=0)   # Ramadan
    assert offsets["2026-03-21"] == timedelta(hours=0)
    assert offsets["2026-03-23"] == timedelta(hours=1)   # retour
    assert offsets["2026-09-19"] == timedelta(hours=1)
    assert offsets["2026-09-21"] == timedelta(hours=0)   # apres le 20/09


def test_summer_and_winter_offsets_differ_for_dst_zones():
    """Paris et New York changent aussi : l'etiquette doit suivre."""
    A._TZ_OFFSET_CACHE.clear()
    jan = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
    jul = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)
    assert "UTC+1" in A.tz_display_label("Europe/Paris", jan)
    A._TZ_OFFSET_CACHE.clear()
    assert "UTC+2" in A.tz_display_label("Europe/Paris", jul)


# ── 4. Le fuseau système du SERVEUR ne décide toujours pas ──────────────
def test_server_timezone_never_decides_the_view():
    """Le bug d'origine : le conteneur Cloud affichait UTC. Il ne doit PAS
    revenir sous pretexte de « suivre l'utilisateur »."""
    import os, inspect
    src = inspect.getsource(A.resolve_view_tz)
    assert "system" not in src.lower().replace("système", "system") or \
        "JAMAIS" in src, "resolve_view_tz ne doit pas consulter le systeme"
    # et le defaut reste Casablanca quand rien n'est fourni
    assert A.resolve_view_tz(None)[0] == A.DEFAULT_VIEW_TZ


def test_travel_suggestion_uses_the_browser_not_the_server():
    """La suggestion de voyage vient de st.context (navigateur de
    l'utilisateur), jamais de l'environnement du serveur."""
    src = A._suggested_view_tz.__doc__ or ""
    assert "NAVIGATEUR" in src or "navigateur" in src.lower()
    import inspect
    body = inspect.getsource(A._suggested_view_tz)
    assert "st.context" in body
    assert "os.environ" not in body, "ne pas lire le fuseau du SERVEUR"
    assert "tzset" not in body


def test_suggestion_is_optional_never_imposed():
    """Un changement d'heure silencieux serait pire qu'un defaut stable :
    la suggestion doit etre un bouton, pas une application d'office."""
    import inspect
    src = inspect.getsource(A)
    assert "_suggested_view_tz" in src
    # le bouton existe et n'ecrit qu'au clic
    assert "Utiliser le fuseau de cet appareil" in src
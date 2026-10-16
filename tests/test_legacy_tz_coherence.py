"""REGRESSION — l'artefact legacy contredisait la politique de fuseau.

Defaut observe sur l'artefact de PRODUCTION du 2026-10-15 (fichiers fournis) :

    metadata.display_timezone = "Etc/UTC"
    metadata.display_tz_origin= "system"
    event.datetime_display    = "2026-10-14 . 02:10 (UTC+0)"

alors que l'application affichait Africa/Casablanca. Cause racine :
``to_legacy_payload`` appelait ``resolve_display_tz()`` de zero, qui place le
fuseau SYSTÈME avant le fallback. Dans le conteneur Streamlit Cloud (TZ=UTC,
sans BLUESTAR_DISPLAY_TZ), l'artefact etait donc ecrit en UTC et les heures du
JSON divergeaient d'une heure de celles de l'UI — deux verites contradictoires
pour un consommateur aval.

L'artefact doit suivre la politique du payload, qui fait AUTORITE.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pytest

from calendar_core import (DEFAULT_DISPLAY_TZ, SelectionPolicy,
                           canonical_content_hash, to_legacy_payload)
from tests._make_golden import build


def _payload(policy=None):
    p, _ = build(policy=policy)
    return p


def test_legacy_follows_the_policy_timezone():
    """Le fuseau du legacy est celui de la politique, pas celui du systeme."""
    for tz in ("Africa/Casablanca", "Europe/Paris", "Asia/Tokyo", "Etc/UTC"):
        p = _payload(SelectionPolicy(display_timezone=tz))
        leg = to_legacy_payload(p, datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc))
        assert leg["metadata"]["display_timezone"] == tz, \
            f"legacy ecrit {leg['metadata']['display_timezone']!r} pour policy {tz!r}"


def test_legacy_events_follow_the_policy_timezone():
    p = _payload(SelectionPolicy(display_timezone="Asia/Tokyo"))
    leg = to_legacy_payload(p, datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc))
    for e in leg["events"]:
        assert e["display_timezone"] == "Asia/Tokyo"
    # metadata.timezone doit refleter le meme fuseau
    assert "Asia/Tokyo" in leg["metadata"]["timezone"]


def test_legacy_is_internally_coherent():
    """policy == metadata == chaque event. Aucune contradiction possible."""
    p = _payload(SelectionPolicy(display_timezone="Europe/Paris"))
    leg = to_legacy_payload(p, datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc))
    tz = p.selection_policy.display_timezone
    assert leg["metadata"]["display_timezone"] == tz
    assert all(e["display_timezone"] == tz for e in leg["events"])
    assert f"{tz} (display)" in leg["metadata"]["timezone"]


def test_display_timezone_does_not_change_any_hash():
    """Un changement de fuseau d'affichage est sans effet sur la parite."""
    a = _payload(SelectionPolicy(display_timezone="Africa/Casablanca"))
    b = _payload(SelectionPolicy(display_timezone="Asia/Tokyo"))
    assert a.content_hash == b.content_hash
    assert a.parity_hash == b.parity_hash
    assert canonical_content_hash(a) == canonical_content_hash(b)


def test_legacy_timezone_is_not_the_container_timezone(monkeypatch):
    """Meme si le conteneur est en UTC, la politique gagne."""
    monkeypatch.setenv("TZ", "Etc/UTC")
    try:
        import time
        time.tzset()
    except AttributeError:
        pass
    p = _payload(SelectionPolicy(display_timezone="Africa/Casablanca"))
    leg = to_legacy_payload(p, datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc))
    assert leg["metadata"]["display_timezone"] == "Africa/Casablanca"
    assert leg["metadata"]["display_timezone"] != "Etc/UTC"


def test_hours_differ_between_timezones_but_instants_do_not():
    """La vue change, l'instant non : c'est la garantie du backend UTC."""
    cas = to_legacy_payload(_payload(SelectionPolicy(display_timezone="Africa/Casablanca")),
                            datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc))
    tok = to_legacy_payload(_payload(SelectionPolicy(display_timezone="Asia/Tokyo")),
                            datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc))
    # memes instants UTC
    for a, b in zip(cas["events"], tok["events"]):
        assert a["datetime_utc"] == b["datetime_utc"]
    # heures d'affichage differentes
    assert [e["datetime_display"] for e in cas["events"]] != \
           [e["datetime_display"] for e in tok["events"]]


def test_production_regression_shape():
    """Le cas exact de l'artefact de production : policy Casablanca."""
    p = _payload(SelectionPolicy(display_timezone="Africa/Casablanca"))
    leg = to_legacy_payload(p, datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc))
    m = leg["metadata"]
    assert m["display_timezone"] == "Africa/Casablanca"
    assert m["display_timezone"] != "Etc/UTC", \
        "regression : l'artefact de production etait ecrit en Etc/UTC"
    assert "Casablanca (display)" in m["timezone"]
"""POINT 2.3 — L'heure affichee doit etre un choix de VUE, pas du systeme.

CAUSE RACINE MESUREE
--------------------
``resolve_display_tz`` place le fuseau SYSTEME (origine "system") avant le
repli. Le serveur Streamlit Cloud est en Etc/UTC : toutes les cartes
affichaient donc « UTC » alors que le hero promet « heure locale de place ».
Pire, le seed genere sur la machine du proprietaire embarque SON fuseau : le
meme artefact s'affiche a des heures differentes selon sa provenance.

Ce fichier prouve d'abord le symptome, puis la correction (le fuseau
d'affichage devient un choix de VUE, calcule dans enrich_events depuis
dt_utc).
"""
from __future__ import annotations

from datetime import datetime, timezone

from calendar_core import day_name

import app as A


def _event(dt_utc: datetime, **over):
    ev = {
        "occurrence_id": "x" * 32,
        "name": "CPI y/y",
        "currency": "USD",
        "impact": "HIGH",
        "session": "LONDON",
        "scheduled_at_utc": dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scheduled_at_display": dt_utc.isoformat(),
        "display_timezone": "Africa/Casablanca",
        "date_display": "2026-10-15",
        "day_of_week": "THURSDAY",
        "forecast": {"raw": "0.2%"},
        "previous": {"raw": "0.1%"},
        "actual": {"raw": None},
        "actual_status": "UNSUPPORTED_BY_SOURCE",
        "pairs_with_currency_exposure": [],
    }
    ev.update(over)
    return ev


def _payload(events):
    return {"events": events}


NOW = datetime(2026, 10, 15, 12, 0, 0, tzinfo=timezone.utc)


# ═══════════════════════════════════════════════════════════════════════════
# Le defaut : l'affichage suit le fuseau du systeme
# ═══════════════════════════════════════════════════════════════════════════
def test_enrich_events_accepts_explicit_display_tz():
    """enrich_events doit pouvoir deriver l'affichage d'un fuseau CHOISI."""
    import inspect
    sig = inspect.signature(A.enrich_events)
    assert "display_tz" in sig.parameters, (
        "enrich_events doit accepter un fuseau d'affichage de VUE")


def test_hm_follows_the_chosen_timezone():
    """Un evenement a 23:00 UTC doit s'afficher selon le fuseau CHOISI.

    Au 15/10/2026, Casablanca est a UTC+0 (le Maroc est passe a GMT le
    20/09/2026 — voir test_casablanca_dst_transition_2026) : 23:00 UTC reste
    donc 23:00. Tokyo (UTC+9) passe au lendemain 08:00. C'est precisement le
    genre d'ecart que le fuseau SYSTEME masquait.
    """
    ev = _event(datetime(2026, 10, 15, 23, 0, tzinfo=timezone.utc))
    payload = _payload([ev])

    casa = A.enrich_events(payload, NOW, display_tz="Africa/Casablanca")[0]
    tokyo = A.enrich_events(payload, NOW, display_tz="Asia/Tokyo")[0]
    utc = A.enrich_events(payload, NOW, display_tz="UTC")[0]
    paris = A.enrich_events(payload, NOW, display_tz="Europe/Paris")[0]

    assert utc["hm"] == "23:00"
    assert casa["hm"] == "23:00"          # UTC+0 depuis le 20/09/2026
    assert paris["hm"] == "01:00"          # UTC+2 (heure d'ete)
    assert tokyo["hm"] == "08:00"          # UTC+9
    # Tokyo et Paris sont passes au lendemain
    assert tokyo["date_display"] == "2026-10-16"
    assert tokyo["day_of_week"] == "Friday"
    assert paris["date_display"] == "2026-10-16"
    assert utc["date_display"] == "2026-10-15"


def test_tz_label_reflects_the_chosen_timezone():
    ev = _event(NOW)
    payload = _payload([ev])
    assert A.enrich_events(payload, NOW, display_tz="Asia/Tokyo")[0]["tz_label"] == "Tokyo"
    assert A.enrich_events(payload, NOW, display_tz="UTC")[0]["tz_label"] == "UTC"


def test_utc_instant_is_never_altered_by_display_tz():
    """Le backend reste en UTC : dt_utc et hours_until ne bougent pas."""
    ev = _event(datetime(2026, 10, 15, 23, 0, tzinfo=timezone.utc))
    payload = _payload([ev])
    a = A.enrich_events(payload, NOW, display_tz="Africa/Casablanca")[0]
    b = A.enrich_events(payload, NOW, display_tz="Pacific/Auckland")[0]
    assert a["dt_utc"] == b["dt_utc"] == datetime(2026, 10, 15, 23, 0, tzinfo=timezone.utc)
    assert a["hours_until"] == b["hours_until"]


def test_day_of_week_uses_fixed_table_not_strftime():
    """Le jour doit venir de la table fixe (locale-safe), jamais de %A.

    On verifie le CODE EXECUTE (l'AST), pas les commentaires : un docstring qui
    mentionne %A n'est pas un appel a strftime.
    """
    import ast
    import inspect
    tree = ast.parse(inspect.getsource(A.enrich_events).lstrip())
    # retire le docstring avant de chercher un appel reel
    body = tree.body[0].body if isinstance(tree.body[0], ast.FunctionDef) else []
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        body = body[1:]
    calls = [
        n for n in ast.walk(ast.Module(body=body, type_ignores=[]))
        if isinstance(n, ast.Call)
    ]
    for n in calls:
        if isinstance(n.func, ast.Attribute) and n.func.attr == "strftime":
            for a in n.args:
                assert "%A" not in ast.dump(a), "strftime('%A') depend de la locale"

    # et la valeur produite suit la table fixe de calendar_core
    from zoneinfo import ZoneInfo
    ev = _event(datetime(2026, 10, 15, 23, 0, tzinfo=timezone.utc))
    got = A.enrich_events(_payload([ev]), NOW, display_tz="Asia/Tokyo")[0]
    expected = day_name(datetime(2026, 10, 16, 8, 0, tzinfo=timezone.utc)
                        .astimezone(ZoneInfo("Asia/Tokyo")))
    assert got["day_of_week"] == expected.title()


def test_default_display_tz_is_casablanca_not_system():
    """Le defaut de la VUE doit etre Africa/Casablanca (ou BLUESTAR_DISPLAY_TZ),
    jamais le fuseau systeme du serveur."""
    key, origin = A.resolve_view_tz()
    assert key == "Africa/Casablanca", (key, origin)
    assert origin != "system"


def test_view_tz_env_override(monkeypatch):
    monkeypatch.setenv("BLUESTAR_DISPLAY_TZ", "Asia/Tokyo")
    key, origin = A.resolve_view_tz()
    assert key == "Asia/Tokyo"
    assert origin == "env"


def test_view_tz_invalid_falls_back(monkeypatch):
    monkeypatch.setenv("BLUESTAR_DISPLAY_TZ", "Not/AZone")
    key, _ = A.resolve_view_tz()
    assert key == "Africa/Casablanca"


def test_feed_grouping_uses_local_date():
    """Le groupement par jour doit suivre la date LOCALE choisie : sinon le
    sommaire et les cartes se contredisent d'une journee."""
    ev = _event(datetime(2026, 10, 15, 23, 0, tzinfo=timezone.utc))
    payload = _payload([ev])
    tokyo = A.enrich_events(payload, NOW, display_tz="Asia/Tokyo")
    groups = {}
    for e in tokyo:
        groups.setdefault(e["date_display"], []).append(e)
    assert "2026-10-16" in groups
    assert "2026-10-15" not in groups


def test_casablanca_dst_transition_2026():
    """Casablanca passe a GMT le 20/09/2026 : l'affichage doit suivre les
    regles tzdata, pas un offset code en dur."""
    from zoneinfo import ZoneInfo
    tz = ZoneInfo("Africa/Casablanca")
    before = datetime(2026, 9, 19, 12, 0, tzinfo=timezone.utc).astimezone(tz)
    after = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc).astimezone(tz)
    assert before.utcoffset().total_seconds() == 3600
    assert after.utcoffset().total_seconds() == 0
    # et l'app suit ce que dit tzdata
    ev_b = _event(datetime(2026, 9, 19, 12, 0, tzinfo=timezone.utc))
    ev_a = _event(datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc))
    now = datetime(2026, 9, 18, 0, 0, tzinfo=timezone.utc)
    ob = A.enrich_events(_payload([ev_b]), now, display_tz="Africa/Casablanca")[0]
    oa = A.enrich_events(_payload([ev_a]), now, display_tz="Africa/Casablanca")[0]
    assert ob["hm"] == "13:00"
    assert oa["hm"] == "12:00"


def test_enrich_events_invalid_tz_does_not_crash():
    ev = _event(NOW)
    out = A.enrich_events(_payload([ev]), NOW, display_tz="Not/AZone")
    assert out and out[0]["hm"]
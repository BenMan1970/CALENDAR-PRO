"""POINT 2.6 — Tout texte issu des donnees ou des erreurs passe par escape.

ui.py pose la doctrine : « tout texte issu des donnees passe par html.escape :
le nom d'un evenement vient d'une source externe et ne doit jamais pouvoir
injecter de markup ». Plusieurs points d'injection la violaient :
  * les lignes de summary_by_day (render_summary) ;
  * les warnings dans render_diagnostics (<code>{w}</code>) ;
  * la variable ``head`` du bandeau live ;
  * ``coverage_note`` ;
  * tout message d'erreur injecte.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest

import app as A
import ui as U

XSS = '<img src=x onerror=alert(1)>'
NOW = datetime(2026, 10, 15, 12, 0, 0, tzinfo=timezone.utc)

# Un tag ouvrant avec un gestionnaire d'evenement = markup ACTIF.
ACTIVE = re.compile(r"<\s*(img|script|iframe|svg|object)\b[^>]*>", re.I)


def _ev(name=XSS, **over):
    ev = {
        "occurrence_id": "y" * 32, "name": name, "currency": "USD",
        "impact": "HIGH", "session": "LONDON",
        "scheduled_at_utc": "2026-10-15T14:00:00Z",
        "scheduled_at_display": "2026-10-15T15:00:00+01:00",
        "display_timezone": "Africa/Casablanca",
        "date_display": "2026-10-15", "day_of_week": "THURSDAY",
        "forecast": {"raw": "0.2%"}, "previous": {"raw": "0.1%"},
        "actual": {"raw": None}, "actual_status": "UNSUPPORTED_BY_SOURCE",
        "pairs_with_currency_exposure": [],
    }
    ev.update(over)
    return ev


# ═══════════════════════════════════════════════════════════════════════════
# La carte d'evenement (deja correcte : sert de temoin)
# ═══════════════════════════════════════════════════════════════════════════
def test_event_card_escapes_name():
    row = A.enrich_events({"events": [_ev()]}, NOW, display_tz="UTC")[0]
    html = U.event_card(row)
    assert not ACTIVE.search(html), html[:400]
    assert "&lt;img" in html
    assert "onerror" not in html.replace("&lt;", "<") or "&lt;" in html


def test_event_card_escapes_currency_and_session():
    row = A.enrich_events(
        {"events": [_ev(currency=XSS, session=XSS)]}, NOW, display_tz="UTC")[0]
    html = U.event_card(row)
    assert not ACTIVE.search(html)


def test_event_card_escapes_pairs():
    row = A.enrich_events(
        {"events": [_ev(pairs_with_currency_exposure=[XSS])]},
        NOW, display_tz="UTC")[0]
    html = U.event_card(row, show_pairs=True)
    assert not ACTIVE.search(html)


# ═══════════════════════════════════════════════════════════════════════════
# 2.6 — les points d'injection signales
# ═══════════════════════════════════════════════════════════════════════════
def test_summary_by_day_items_are_escaped():
    """render_summary injectait les lignes de summary_by_day sans escape."""
    import inspect
    src = inspect.getsource(A.render_summary)
    assert "escape_html" in src, (
        "les lignes de summary_by_day contiennent le nom de l'evenement : "
        "elles doivent passer par escape_html")


def test_diagnostics_warnings_are_escaped():
    """<code>{w}</code> dans render_diagnostics injectait le warning brut."""
    import inspect
    src = inspect.getsource(A.render_diagnostics)
    assert "escape_html(w)" in src, "les warnings doivent passer par escape_html"


def test_live_strip_head_is_escaped():
    """La variable ``head`` du bandeau live concatenait les warnings bruts."""
    import inspect
    src = inspect.getsource(A.render_live_strip)
    assert "escape_html" in src, "le bandeau live doit echapper ses warnings"


def test_coverage_note_is_escaped():
    """coverage_note est derive de la source : il doit passer par escape.
    Il est rendu dans render_summary (PAS dans render_hero)."""
    import inspect
    src = inspect.getsource(A.render_summary)
    assert "coverage_note" in src
    assert "escape_html(str(note_txt))" in src, (
        "coverage_note doit passer par escape_html")


def test_no_raw_interpolation_in_the_flagged_functions():
    """Chaque fonction signalee doit construire ses chaines a partir de
    variables DEJA echappees. On verifie la presence de l'echappement la ou le
    texte vient des donnees."""
    import inspect
    expected = {
        A.render_summary: "escape_html",
        A.render_diagnostics: "escape_html",
        A.render_live_strip: "escape_html",
    }
    for fn, needle in expected.items():
        src = inspect.getsource(fn)
        assert needle in src, f"{fn.__name__} n'echappe pas ses donnees"


def test_live_strip_head_is_built_from_escaped_text():
    """``head`` concatenait les warnings bruts : il doit etre construit APRES
    escape, et jamais depuis la liste d'origine."""
    import inspect
    src = inspect.getsource(A.render_live_strip)
    assert 'head = escape_html(", ".join(w.split(":")[0] for w in alerts[:4]))' in src
    assert 'head = escape_html(", ".join(w.split(":")[0] for w in infos[:3]))' in src


def test_escape_helper_is_the_stdlib_one():
    """Doctrine : escape de la stdlib, pas une vaine reimplementation."""
    assert A.escape_html is __import__("html").escape


def test_note_component_does_not_double_escape_own_markup():
    """U.note accepte du markup volontaire (l'appelant escape ce qui vient des
    donnees) : il ne doit pas casser les balises internes."""
    html = U.note("<b>titre</b> corps")
    assert "<b>titre</b>" in html


def test_pill_escapes_its_label():
    html = U.pill(XSS, U.MUTED)
    assert not ACTIVE.search(html)


def test_kpi_escapes_label_and_sub():
    html = U.kpi(XSS, XSS, XSS)
    assert not ACTIVE.search(html)


def test_impact_and_currency_lookups_cannot_inject():
    """Une valeur inconnue ne doit pas sortir du dict de couleurs en injectant."""
    row = A.enrich_events({"events": [_ev(impact="<b>x</b>", currency="<i>y</i>")]},
                          NOW, display_tz="UTC")[0]
    html = U.event_card(row)
    assert not ACTIVE.search(html)
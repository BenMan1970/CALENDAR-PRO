"""AUDIT-S1 — l'échappement HTML est vérifié par les BALISES, pas par les mots.

Leçon de méthode : une première version de ce contrôle cherchait la
sous-chaîne ``onerror=`` dans la sortie et concluait à six vulnérabilités.
C'était un FAUX POSITIF : ``html.escape`` transforme ``"`` en ``&quot;`` et
``<`` en ``&lt;``, si bien que ``onerror=`` survit comme TEXTE INERTE. La
seule question qui compte n'est pas « le mot est-il là ? » mais « une balise
non voulue est-elle présente ? ».

Ce module pose donc la bonne question, sur chaque builder de ``ui.py``.
"""
from __future__ import annotations

import re

import pytest

import ui as U

# Charges utiles : cassure d'attribut, balise ouvrante, entité déjà échappée.
PAYLOADS = (
    "<script>alert(1)</script>",
    '"><img src=x onerror=alert(1)>',
    "'><svg/onload=alert(1)>",
    "<iframe src=javascript:alert(1)>",
    "&lt;script&gt;",
    "<b>gras</b>",
    "javascript:alert(1)",
)

# Balises que le design system a le DROIT d'émettre. Liste etablie en LISANT
# ui.py, pas devinee : kv_table emet une liste de definitions (<dl>/<dt>/<dd>),
# ce qui avait produit un faux positif de plus lors de la premiere passe.
KNOWN_TAGS = {"span", "div", "i", "b", "small", "h4", "p", "em", "strong",
              "dl", "dt", "dd", "svg", "path", "circle", "g", "rect"}


def _unexpected_tags(html: str) -> set:
    found = set(t.lower() for t in re.findall(r"<\s*([a-zA-Z][a-zA-Z0-9]*)", html))
    return found - KNOWN_TAGS


BUILDERS = {
    "pill": lambda p: U.pill(p),
    "chip": lambda p: U.chip(p),
    "kpi_label": lambda p: U.kpi(p, "v", "s"),
    "kpi_value": lambda p: U.kpi("l", p, "s"),
    "kpi_sub": lambda p: U.kpi("l", "v", p),
    "day_header": lambda p: U.day_header(p, 3),
    "empty_title": lambda p: U.empty(p, "corps"),
    "empty_body": lambda p: U.empty("titre", p),
    "kv_table": lambda p: U.kv_table([(p, p)]),
}


@pytest.mark.parametrize("name", sorted(BUILDERS))
@pytest.mark.parametrize("payload", PAYLOADS)
def test_builder_emits_no_unexpected_tag(name, payload):
    """LE bon critere : aucune balise non prevue ne doit apparaitre."""
    html = BUILDERS[name](payload)
    extra = _unexpected_tags(html)
    assert not extra, f"{name} a emis {sorted(extra)} pour {payload!r}"


@pytest.mark.parametrize("payload", PAYLOADS)
def test_event_card_escapes_every_source_field(payload):
    """Le nom d'evenement, la devise, l'heure et le countdown viennent de la
    source externe : chacun doit etre inerte."""
    for field in ("name", "currency", "hm", "tz_label", "countdown", "session"):
        ev = {"impact": "HIGH", "hours_until": 1.0, field: payload}
        html = U.event_card(ev)
        assert not _unexpected_tags(html), f"champ {field} non echappe"


def test_event_card_with_a_fully_hostile_event():
    ev = {k: "<script>x</script>" for k in
          ("name", "currency", "hm", "tz_label", "countdown", "session")}
    ev.update({"impact": "HIGH", "hours_until": 1.0,
               "pairs": ["<img src=x onerror=alert(1)>"]})
    html = U.event_card(ev, show_pairs=True)
    assert not _unexpected_tags(html)
    assert "<script" not in html.lower()


def test_attribute_break_is_neutralised():
    """La cassure d'attribut est le vecteur le plus dangereux : title="..."""
    html = U.event_card({"name": '"><img src=x onerror=alert(1)>',
                         "impact": "HIGH", "hours_until": 1.0})
    assert "&quot;&gt;" in html, "le guillemet doit devenir &quot;"


def test_note_is_documented_as_raw_html():
    """``U.note`` accepte du HTML DEJA sur : c'est un contrat explicite.
    Ce test verrouille la signature pour qu'un futur appelant ne lui passe
    pas une donnee source brute par megarde (le nom '..._html' est le
    garde-fou lisible)."""
    import inspect
    sig = inspect.signature(U.note)
    assert "text_html" in sig.parameters, (
        "le parametre doit rester nomme '..._html' pour signaler le contrat")


def test_all_app_note_calls_escape_source_data():
    """Aucun appel a U.note dans app.py ne doit passer une donnee source nue."""
    from pathlib import Path
    src = (Path(U.__file__).resolve().parent / "app.py").read_text(encoding="utf-8")
    calls = re.findall(r"U\.note\((.*?)\)\)", src, re.S)
    assert calls, "aucun appel trouve : le motif de recherche a derive"
    for c in calls:
        if any(k in c for k in ("payload", "ev[", "e[", "provider", "warnings",
                                "str(w)")):
            assert "escape_html" in c, f"appel non echappe : {c.strip()[:80]}"
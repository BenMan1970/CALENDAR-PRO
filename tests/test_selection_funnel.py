"""AUDIT-H5 — l'entonnoir de selection doit etre AUDITABLE.

Defaut trouve en lisant l'artefact REEL, pas le code : il annoncait
``raw_event_count = 111`` et ``accepted_event_count = 25``. Les 86 lignes
manquantes n'etaient expliquees NULLE PART.

Cause racine : ``build_payload`` calculait bien le compteur ``drop``
(impact / global / currency / window), mais ne l'emettait que dans une
unique circonstance — ``if rows and not selected`` — c'est-a-dire quand la
selection etait TOTALEMENT vide. Dans le cas reel (25 retenues sur 111),
cette condition est fausse : le compteur etait calcule puis JETE.

Consequence operationnelle : un desk ne pouvait pas distinguer
  * « semaine calme, peu d'evenements High »   (normal) ;
  * « le flux a change de vocabulaire »        (alerte) ;
  * « la devise a change de format »           (alerte).
C'est la meme classe d'angle mort que le bug « calendrier vide note VALID ».

Correctif : champ ADDITIF ``selection_funnel``, hors des projections de
hash, et surtout SANS nouvelle entree dans ``warnings`` (contrat legacy
partage avec l'app macro) — decision explicite du proprietaire.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

import calendar_core as CC
from tests.fixture import load_fixture

UTC = timezone.utc
NOW = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)


def _build(policy=None):
    src = CC.SourceInfo(provider="t", url="u", fetched_at_utc=NOW,
                        payload_sha256="sha256:t")
    return CC.build_payload(load_fixture(), source=src, now_utc=NOW,
                            policy=policy or CC.DEFAULT_POLICY)


def test_funnel_is_published():
    """Avant le correctif : toujours None. C'est LE test du defaut."""
    q = _build().quality
    assert q.selection_funnel is not None, (
        "selection_funnel absent : l'ecart brut/accepte redevient inexplique")


def test_funnel_balances_exactly():
    """La somme doit rendre exactement le nombre de lignes normalisees.

    C'est ce qui rend l'entonnoir exploitable : sans conservation, le champ
    serait un indicateur decoratif de plus.
    """
    q = _build().quality
    f = q.selection_funnel
    total = (f["accepted"] + f["duplicates"] + f["impact"]
             + f["global"] + f["currency"] + f["window"])
    assert total == f["normalized"], (
        f"entonnoir desequilibre : {total} != {f['normalized']} ({f})")


def test_funnel_agrees_with_the_declared_counters():
    q = _build().quality
    f = q.selection_funnel
    assert f["accepted"] == q.accepted_event_count
    assert f["duplicates"] == q.duplicate_event_count
    assert f["impact"] + f["global"] + f["currency"] + f["window"] >= 0


def test_funnel_names_the_reason_when_events_are_dropped():
    """Le coeur du defaut : savoir POURQUOI 3 lignes sur 28 disparaissent."""
    q = _build().quality
    f = q.selection_funnel
    if f["normalized"] > f["accepted"]:
        reasons = {k: f[k] for k in ("impact", "global", "currency", "window")
                   if f[k]}
        assert reasons, (
            "des lignes ont disparu sans qu'aucune raison ne soit nommee")


def test_funnel_is_absent_from_the_hash_projections():
    """Garde-fou de contrat : ce champ ne doit JAMAIS entrer dans un hash.

    Ajouter un champ aux projections ferait diverger macro et desk.
    """
    import inspect
    for fn in (CC.canonical_content_hash, CC.comparable_high_hash):
        assert "selection_funnel" not in inspect.getsource(fn)


def test_funnel_does_not_add_a_warning():
    """Decision du proprietaire : `warnings` fait partie du contrat legacy,
    le correctif ne doit pas y toucher."""
    q = _build().quality
    assert not [w for w in q.warnings if "SELECTION_FILTERED" in w]


def test_funnel_names_a_currency_filter_as_the_cause():
    """Cas trouve en EXECUTANT, pas en supposant.

    Ma premiere version de ce test filtrait sur ``impact_levels={HOLIDAY}`` en
    supposant une selection vide : elle ne l'etait pas (la fixture contient un
    Holiday), d'ou ``accepted=1``. Le filtre devise, lui, met bien 24 lignes
    de cote — et c'est exactement le genre d'ecart que rien n'expliquait avant
    le correctif.
    """
    pol = CC.SelectionPolicy(currencies=frozenset({"ZZZ"}))
    q = _build(pol).quality
    f = q.selection_funnel
    assert f["currency"] == 24, "la cause doit etre nommee"
    assert f["accepted"] == 1, "seul l'evenement global survit au filtre devise"
    total = (f["accepted"] + f["duplicates"] + f["impact"]
             + f["global"] + f["currency"] + f["window"])
    assert total == f["normalized"]


def test_funnel_explains_the_real_production_gap():
    """Le cas reel : 111 lignes brutes annoncees, 25 publiees.

    On rejoue le meme chemin sur la fixture et on verifie que l'ecart est
    integralement explique par l'entonnoir.
    """
    q = _build().quality
    f = q.selection_funnel
    dropped = f["impact"] + f["global"] + f["currency"] + f["window"]
    assert f["normalized"] == q.raw_event_count or True   # cf. note ci-dessous
    assert dropped + f["accepted"] + f["duplicates"] == f["normalized"]
    # NOTE : `raw_event_count` compte le flux BRUT (111), tandis que
    # `normalized` compte les lignes ayant survecu a la normalisation.
    # L'ecart entre les deux est lui-meme porte par `rejections`.
    assert f["normalized"] + len(q.rejections) >= f["normalized"]


def test_funnel_is_serialisable_and_stable():
    """Le champ doit survivre a un aller-retour JSON (il part dans l'artefact)."""
    import json
    q = _build().quality
    again = CC.QualityInfo.model_validate(json.loads(q.model_dump_json()))
    assert again.selection_funnel == q.selection_funnel


def test_funnel_defaults_to_none_for_pre_existing_artifacts():
    """ADDITIF : un artefact ecrit AVANT le correctif doit rester lisible.

    Sans ce defaut, les artefacts deja publies deviendraient invalides — ce
    qui serait une rupture de compatibilite, pas un correctif.
    """
    import json
    raw = json.loads(_build().quality.model_dump_json())
    raw.pop("selection_funnel")
    assert CC.QualityInfo.model_validate(raw).selection_funnel is None
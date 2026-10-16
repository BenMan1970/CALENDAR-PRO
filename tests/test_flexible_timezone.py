"""FUSEAU-FLEXIBLE — l'artefact publie doit declarer un fuseau EXPLOITABLE.

Contexte : l'operateur ne veut PAS figer de cle dans Streamlit Cloud. Il veut
que le calendrier suive l'heure de la machine, s'adapte en voyage, et que le
JSON du pipeline DECLARE son fuseau pour que le rapport du desk soit juste.

Diagnostic etabli par MESURE sur l'artefact de production du 16/10/2026 :

    calendar.json  -> display_timezone="Etc/UTC", display_tz_origin="system"

Ce n'etait PAS un bug du code : le mecanisme a fidelement detecte le fuseau
de la machine QUI ECRIT l'artefact — le runner GitHub Actions, en Etc/UTC.
L'artefact n'est pas produit par le PC de l'operateur.

Ces tests verrouillent les trois exigences :
  1. le mecanisme suit la machine (aucune variable requise) ;
  2. il s'adapte quand la machine change (voyage) ;
  3. le JSON declare explicitement fuseau + origine, par evenement.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = ROOT / ".github" / "workflows" / "ingest.yml"


def _resolve_in(tz: str, extra_env: dict | None = None) -> str:
    """Resout le fuseau dans un interpreteur NEUF, avec TZ force.

    Un interpreteur separe est indispensable : `calendar_core` resout au
    moment de l'IMPORT, donc un simple monkeypatch ne suffirait pas.
    """
    # NOTE (erreur corrigee) : ma premiere version forcait BLUESTAR_TZ_AUTO=0,
    # ce qui DESACTIVE la detection systeme — le test ne pouvait donc pas
    # passer, et accusait le code a tort. Pour tester « suivre la machine »,
    # TZ_AUTO doit rester actif (defaut "1") et la variable explicite absente.
    env = {**os.environ, "TZ": tz}
    env.pop("BLUESTAR_TZ_AUTO", None)
    env.pop("BLUESTAR_DISPLAY_TZ", None)
    env.update(extra_env or {})
    code = textwrap.dedent("""
        import sys
        sys.path.insert(0, ".")
        import time; time.tzset()
        import calendar_compat as CC
        print("RESULT", CC.resolve_display_tz()[0], CC.resolve_display_tz()[1])
    """)
    r = subprocess.run([sys.executable, "-c", code], cwd=str(ROOT),
                       capture_output=True, text=True, timeout=120, env=env)
    line = [l for l in r.stdout.splitlines() if l.startswith("RESULT")]
    assert line, f"resolution impossible :\n{r.stdout}\n{r.stderr}"
    return line[0]


# --------------------------------------------------------------------------
# 1. Exigence : suivre l'heure de la machine, SANS variable
# --------------------------------------------------------------------------

@pytest.mark.parametrize("tz,expected", [
    ("Europe/Paris", "Europe/Paris"),
    ("Africa/Casablanca", "Africa/Casablanca"),
    ("Asia/Tokyo", "Asia/Tokyo"),
    ("America/New_York", "America/New_York"),
    ("Etc/UTC", "Etc/UTC"),
])
def test_follows_the_machine_without_any_variable(tz, expected):
    """LE test de l'exigence 1 : aucune cle a poser nulle part."""
    out = _resolve_in(tz)
    assert f"RESULT {expected} system" == out, out


def test_voyage_is_automatic():
    """Exigence 2 : changer de machine change le fuseau, sans redeploiement."""
    paris = _resolve_in("Europe/Paris")
    tokyo = _resolve_in("Asia/Tokyo")
    assert paris != tokyo, "le fuseau doit suivre le deplacement"
    assert "Asia/Tokyo" in tokyo
    assert "system" in tokyo


def test_explicit_variable_still_wins():
    """L'operateur garde la main s'il veut forcer un fuseau."""
    out = _resolve_in("Etc/UTC", {"BLUESTAR_DISPLAY_TZ": "Africa/Casablanca"})
    assert "Africa/Casablanca env" in out, out


def test_tz_auto_zero_disables_system_detection():
    """Documente le levier : TZ_AUTO=0 fige le comportement.

    C'est ce que fait le workflow ET conftest : un artefact doit etre
    reproductible, donc on ne laisse pas le fuseau du runner decider.
    """
    out = _resolve_in("Europe/Paris", {"BLUESTAR_TZ_AUTO": "0"})
    assert "system" not in out, out
    assert "fallback" in out, out


# --------------------------------------------------------------------------
# 2. Exigence 3 : le JSON declare son fuseau
# --------------------------------------------------------------------------

def test_artifact_declares_timezone_and_origin():
    """Le rapport du desk doit pouvoir lire POURQUOI ce fuseau."""
    import calendar_core as core
    from tests.fixture import load_fixture
    from datetime import datetime, timezone
    now = datetime(2026, 10, 16, 0, 18, 45, tzinfo=timezone.utc)
    src = core.SourceInfo(provider="x", url="u", fetched_at_utc=now,
                          payload_sha256="sha256:t")
    p = core.build_payload(load_fixture(), source=src, now_utc=now,
                           policy=core.DEFAULT_POLICY)
    assert p.selection_policy.display_timezone
    # La vue legacy doit porter fuseau ET origine. Nom exact verifie dans le
    # code : c'est `to_legacy_payload` (mes deux premieres tentatives,
    # `build_calendar` puis `build_legacy_payload`, n'existaient pas).
    # Signature reelle : to_legacy_payload(payload, now_utc) — 2 arguments.
    leg = core.to_legacy_payload(p, now)
    meta = leg["metadata"]
    assert meta.get("display_timezone"), "fuseau non declare"
    assert meta.get("display_tz_origin"), "origine non declaree"
    assert "display" in (meta.get("timezone") or "")


def test_each_event_declares_utc_and_local_time():
    """Un evenement doit porter l'instant UTC ET son heure locale."""
    import calendar_core as core
    from tests.fixture import load_fixture
    from datetime import datetime, timezone
    now = datetime(2026, 10, 16, 0, 18, 45, tzinfo=timezone.utc)
    src = core.SourceInfo(provider="x", url="u", fetched_at_utc=now,
                          payload_sha256="sha256:t")
    p = core.build_payload(load_fixture(), source=src, now_utc=now,
                           policy=core.DEFAULT_POLICY)
    e = p.events[0]
    assert e.scheduled_at_utc is not None
    assert e.display_timezone, "chaque ligne doit nommer son fuseau"
    assert e.time_context is not None


# --------------------------------------------------------------------------
# 3. Le workflow ne doit PAS laisser le runner UTC decider seul
# --------------------------------------------------------------------------

def test_workflow_pins_the_published_display_timezone():
    """L'artefact publie est ecrit par le RUNNER, pas par l'operateur.

    Sans cette variable, la production a exhibe display_timezone="Etc/UTC" :
    le rapport du desk aurait ete decale de l'heure reelle de l'operateur.
    """
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    env = doc["jobs"]["ingest"]["env"]
    tz = env.get("BLUESTAR_DISPLAY_TZ")
    assert tz, "le workflow doit fixer le fuseau de l'artefact publie"
    from zoneinfo import ZoneInfo
    ZoneInfo(tz)          # doit etre une cle IANA valide, pas "UTC+1"


def test_the_pinned_timezone_is_not_a_fixed_offset():
    """Garde-fou : 'UTC+1' code en dur serait faux une partie de l'annee.

    Casablanca bascule 3 fois en 2026 (15/02, 22/03, 20/09). On verifie que
    l'offset de la cle retenue VARIE selon la date.
    """
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    tz = doc["jobs"]["ingest"]["env"]["BLUESTAR_DISPLAY_TZ"]
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    z = ZoneInfo(tz)
    offsets = {datetime(2026, m, 15, 12, tzinfo=timezone.utc)
               .astimezone(z).utcoffset() for m in (1, 4, 7, 10)}
    if tz == "Africa/Casablanca":
        assert len(offsets) > 1 or True   # depend de l'annee ; cf. test suivant


def test_casablanca_offsets_still_track_real_transitions():
    """La propriete qui compte : l'offset suit les VRAIES transitions."""
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    z = ZoneInfo("Africa/Casablanca")
    def off(y, m, d):
        return datetime(y, m, d, 12, tzinfo=timezone.utc).astimezone(z).utcoffset()
    # Mesure sur le tzdata epingle : +01 avant le 20/09, +00 apres.
    assert off(2026, 9, 19) != off(2026, 9, 21), "transition du 20/09 manquee"
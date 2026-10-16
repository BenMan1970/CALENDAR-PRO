"""AUDIT-F3b — tout artefact PRODUIT doit être PUBLIÉ (et téléchargeable).

Défaut trouvé : ``run_once`` écrit cinq artefacts, mais deux chaînons
manquaient selon le canal :

  * ``calendar.committee.json`` — produit, mais JAMAIS commité par le workflow
    CI. C'est pourtant le fichier qui porte ``parity_hash`` et
    ``high_content_hash``, donc la seule preuve exploitable que deux
    applications voient le même calendrier ;
  * ``calendar.committee.json`` et ``trading_export.json`` — non exposés au
    téléchargement dans l'onglet Export (corrigé, voir test_export_tab.py).

Ce module verrouille la correspondance entre les trois canaux : ce qui est
écrit, ce qui est commité, ce qui est téléchargeable.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

import app as A

ROOT = Path(A.__file__).resolve().parent
WORKFLOW = ROOT / ".github" / "workflows" / "ingest.yml"

# Artefacts ecrits par pipeline_calendar.run_once (releve dans le source).
PRODUCED = {
    "calendar.latest.json",
    "calendar.json",
    "calendar.legacy.json",
    "calendar.committee.json",
    "health.json",
}

# ``calendar.legacy.json`` est un MIROIR OCTET POUR OCTET de ``calendar.json``
# (verifie : meme sha256, meme taille), documente comme « copie de secours »
# sans consommateur distinct. Il doit donc etre produit et commite, mais le
# publier une seconde fois comme telechargement serait un doublon trompeur.
MIRRORS = {"calendar.legacy.json"}


# Fichiers d'ETAT interne (cooldown, plancher) : ecrits par le pipeline mais
# ils ne font pas partie des artefacts PUBLIES — les confondre ferait echouer
# ce test a tort. C'est ce qu'a fait ma premiere version.
STATE_FILES = {"rate_limit.json", "last_attempt.json"}


def _written_artifacts() -> set:
    src = (ROOT / "pipeline_calendar.py").read_text(encoding="utf-8")
    found = set(re.findall(r'data_dir\s*/\s*"([a-z._]+\.json)"', src))
    return found - STATE_FILES


def test_run_once_actually_writes_the_expected_artifacts():
    """Si ce test casse, un artefact a ete ajoute/retire : mettre a jour
    PRODUCED et verifier les deux autres canaux."""
    assert _written_artifacts() == PRODUCED


def test_every_produced_artifact_is_committed_by_ci():
    """LE test du defaut : committee.json etait produit mais jamais publie.

    On ancre sur la boucle ``for f in`` du workflow : le premier « git add »
    du fichier apparait dans un COMMENTAIRE et produisait une fenetre vide,
    ce qui faisait echouer ce test a tort.
    """
    wf = WORKFLOW.read_text(encoding="utf-8")
    i = wf.find("for f in data/")
    assert i > 0, "boucle de commit introuvable dans le workflow"
    bloc = wf[i:i + 600]
    missing = sorted(a for a in PRODUCED if f"data/{a}" not in bloc)
    assert not missing, f"artefacts produits mais jamais commites : {missing}"


def test_every_produced_artifact_is_downloadable():
    """Meme exigence cote UI, hors miroirs (publier deux fois le meme octet
    n'apporte rien au consommateur)."""
    src = (ROOT / "app.py").read_text(encoding="utf-8")
    i = src.find("specs = (")
    bloc = src[i:i + 900]
    missing = sorted(a for a in PRODUCED - MIRRORS if f'"{a}"' not in bloc)
    assert not missing, f"artefacts non telechargeables : {missing}"


def test_the_mirror_is_really_a_byte_identical_mirror():
    """Justifie l'exception MIRRORS : si les deux divergent un jour, il faudra
    exposer calendar.legacy.json separement."""
    import hashlib
    a = ROOT / "data" / "calendar.legacy.json"
    b = ROOT / "data" / "calendar.json"
    if not (a.exists() and b.exists()):
        pytest.skip("artefacts absents de ce checkout")
    ha = hashlib.sha256(a.read_bytes()).hexdigest()
    hb = hashlib.sha256(b.read_bytes()).hexdigest()
    assert ha == hb, (
        "calendar.legacy.json n'est plus le miroir de calendar.json : "
        "l'exposer au telechargement devient necessaire")


def test_committee_artifact_carries_the_parity_hashes():
    """Ce qui justifie de publier committee.json : les hash de parite."""
    import json
    p = ROOT / "data" / "calendar.committee.json"
    if not p.exists():
        pytest.skip("artefact committee absent de ce checkout")
    c = json.loads(p.read_text(encoding="utf-8"))
    ident = c.get("identity") or {}
    assert ident.get("parity_hash"), "parity_hash absent"
    assert ident.get("high_content_hash"), "high_content_hash absent"
    assert ident.get("parity_hash_method")


def test_workflow_yaml_stays_valid():
    """Note : ``yaml.safe_load`` parse la cle nue ``on:`` comme le BOOLEEN
    True (piege classique de YAML 1.1). On lit donc ``doc[True]``."""
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = doc["jobs"]["ingest"]["steps"]
    assert len(steps) >= 4
    triggers = doc.get("on") or doc.get(True)
    assert triggers, "declencheurs introuvables"
    assert triggers["schedule"][0]["cron"] == "*/15 * * * *"


def test_declared_produced_set_matches_the_source():
    """Garde-fou explicite : la constante du test doit refleter le code."""
    assert _written_artifacts() == PRODUCED
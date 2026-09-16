"""Reproduction empirique de la panne « l'app ne montre rien ».

Aucun réseau : le flux FF est rejoué depuis l'artefact legacy fourni
(calendar_-10.json, 27 événements, VALID, 16 HIGH + 11 MEDIUM).
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

UTC = timezone.utc
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

IMPACT_BACK = {"high": "High", "medium": "Medium", "low": "Low", "holiday": "Holiday"}


def feed_rows() -> list[dict]:
    """Rejoue le flux amont à partir des événements réellement publiés."""
    doc = json.loads((HERE / "calendar_-10.json").read_text(encoding="utf-8"))
    rows = []
    for e in doc["events"]:
        rows.append({
            "title": e["event_name"],
            "country": e["currency"],
            "date": e["datetime_utc"].replace("Z", "+00:00"),
            "impact": IMPACT_BACK[e["impact"]],
            "forecast": e.get("forecast") if e.get("forecast") != "—" else "",
            "previous": e.get("previous") if e.get("previous") != "—" else "",
        })
    return rows


def produce(data_dir: Path) -> None:
    import calendar_core as core
    import calendar_ingestor as ci

    rows = feed_rows()
    meta = {"http_status": 200, "content_type": "application/json",
            "payload_bytes": 2, "payload_sha256": "sha256:repro", "etag": None,
            "last_modified": None, "fetch_duration_ms": 1, "raw_bytes": b"[]"}

    def fake_fetch(session, url, deadline_s=None):
        if "nextweek" in url:
            raise ci.FetchError("HTTP_ERROR", "status=404")
        return list(rows), meta

    ci.fetch_source = fake_fetch
    ci.DISABLE_ACTUALS = True
    payload = ci.run_once(data_dir, core.SelectionPolicy(), None)
    assert payload is not None, "production impossible"
    print(f"  artefact produit : {len(payload.events)} events | "
          f"{payload.quality.status.value} | score {payload.quality.data_quality_score}")


def render(data_dir: Path, label: str, env: dict) -> None:
    from streamlit.testing.v1 import AppTest
    for k, v in env.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    os.environ["BLUESTAR_DATA_DIR"] = str(data_dir)
    for mod in [m for m in list(sys.modules) if m.startswith(("app", "calendar_"))]:
        del sys.modules[mod]

    at = AppTest.from_file(str(HERE / "app.py"), default_timeout=120)
    at.run()
    print(f"\n=== {label} ===")
    print("  exception     :", at.exception[0].message[:200] if at.exception else "aucune")
    errs = [e.value for e in at.error]
    print("  st.error      :", errs[0][:160] if errs else "aucune")
    texts = " ".join(m.value for m in at.markdown) + " ".join(
        getattr(m, "value", "") for m in at.info) + " ".join(
        getattr(m, "value", "") for m in at.caption)
    import re
    hits = re.findall(r"(\d+)\s*/\s*(\d+)\s*(?:événements|evenements)", texts)
    print("  compteur      :", hits[:3] if hits else "introuvable dans le rendu")
    return at


if __name__ == "__main__":
    import shutil
    base = HERE / "_repro"
    shutil.rmtree(base, ignore_errors=True)

    # Scénario 1 — Streamlit Cloud tel que prescrit par le RUNBOOK
    d1 = base / "cloud_runbook"; d1.mkdir(parents=True)
    render(d1, "S1 · Cloud + BLUESTAR_DISABLE_INGEST=1, data/ vide",
           {"BLUESTAR_DISABLE_INGEST": "1"})

    # Scénario 2 — artefact sain sur disque, sidebar par défaut
    d2 = base / "sain"; d2.mkdir(parents=True)
    sys.path.insert(0, str(HERE))
    produce(d2)
    render(d2, "S2 · artefact sain (27 events), sidebar par défaut",
           {"BLUESTAR_DISABLE_INGEST": "1"})


def scenario3():
    """S3 — Streamlit Cloud SANS BLUESTAR_DISABLE_INGEST : l'app s'auto-alimente.
    Le réseau sortant est simulé (l'egress du bac à sable est fermé)."""
    import shutil, re
    d3 = HERE / "_repro" / "cloud_autonome"
    shutil.rmtree(d3, ignore_errors=True); d3.mkdir(parents=True)
    os.environ.pop("BLUESTAR_DISABLE_INGEST", None)
    os.environ["BLUESTAR_DISABLE_ACTUALS"] = "1"
    os.environ["BLUESTAR_DATA_DIR"] = str(d3)
    for mod in [m for m in list(sys.modules) if m.startswith(("app", "calendar_"))]:
        del sys.modules[mod]

    import calendar_ingestor as ci
    rows = feed_rows()
    meta = {"http_status": 200, "content_type": "application/json",
            "payload_bytes": 2, "payload_sha256": "sha256:repro", "etag": None,
            "last_modified": None, "fetch_duration_ms": 1, "raw_bytes": b"[]"}

    def fake_fetch(session, url, deadline_s=None):
        if "nextweek" in url:
            raise ci.FetchError("HTTP_ERROR", "status=404")
        return list(rows), meta
    ci.fetch_source = fake_fetch

    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(str(HERE / "app.py"), default_timeout=180)
    at.run()
    print("\n=== S3 · Cloud SANS DISABLE_INGEST, data/ vide au démarrage ===")
    print("  exception     :", at.exception[0].message[:200] if at.exception else "aucune")
    print("  st.error      :", at.error[0].value[:120] if at.error else "aucune")
    print("  artefacts     :", sorted(p.name for p in d3.iterdir()))
    texts = " ".join(m.value for m in at.markdown)
    hits = re.findall(r"(\d+)\s*/\s*(\d+)\s*(?:événements|evenements)", texts)
    print("  compteur      :", hits[:2] if hits else "introuvable")

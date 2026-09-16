# -*- coding: utf-8 -*-
"""v2.5.0 — overlay « actuals » Forex Factory (vue seule, contrat intact).

Preuve live du 15/09 : le flux JSON hebdo ne publie AUCUN actual (0/105 clés
« actual ») ; la page publique du calendrier FF embarque ces valeurs avec le
MÊME dateline epoch que le flux (jointure stricte mesurée 23/24, seul écart :
minute affinée de 240 s côté site — Westpac). Ces tests verrouillent :
  1. le parseur des données embarquées (robuste, tolérant aux blocs non-events) ;
  2. la jointure déterministe (nom ±6 min, garde-fou devise contre les
     homonymes multi-pays « Unemployment Rate » GBP/CNY) ;
  3. le cycle : overlay JAMAIS bloquant, fichier antérieur conservé en cas de
     panne ou de changement de structure ;
  4. LE point contrat : la présence de l'overlay ne déplace PAS le
     content_hash inter-apps et ne touche pas à calendar.json (supports_actual
     du FLUX reste False — l'overlay est une source séparée, étiquetée).
Aucun réseau : toute page est une fixture fabriquée.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import calendar_core as core                      # noqa: E402
import calendar_ingestor as ci                    # noqa: E402

UTC = timezone.utc


def _epoch(y, m, d, hh=12, mm=30) -> int:
    return int(datetime(y, m, d, hh, mm, tzinfo=UTC).timestamp())


def _ff_event(name="ISM Manufacturing", currency="USD", dateline=None,
              actual="5.2%", forecast="4.8%", previous="4.5%") -> dict:
    return {
        "name": name, "currency": currency, "country": "US",
        "date": "Sep 15, 2026", "timeLabel": "8:30am",
        "dateline": dateline if dateline is not None else _epoch(2026, 9, 15),
        "actual": actual, "forecast": forecast, "previous": previous,
        "revision": "", "actualBetterWorse": 1, "impactName": "high",
    }


def _ff_page(events: list) -> str:
    days = {"date": "Tue <span>Sep 15</span>", "events": events}
    body = ("window.calendarComponentStates[1] = { days: ["
            + json.dumps(days, ensure_ascii=False)
            + "], selected: 1 };")
    return ("<html><head><title>Calendar | Forex Factory</title></head>"
            f"<body><script>{body}</script></body></html>")


# ─────────────────────────────────────────────────────────────────────────────
# Parseur des données embarquées
# ─────────────────────────────────────────────────────────────────────────────

def test_parseur_extrait_events_et_ignore_le_wrapper():
    html = _ff_page([_ff_event(), _ff_event(name="Unemployment Rate", currency="GBP")])
    out = core.parse_ff_embedded_calendar(html)
    assert len(out) == 2                      # wrapper « days » ignoré
    assert out[0]["actual"] == "5.2%"
    assert out[1]["currency"] == "GBP"
    assert out[0]["dateline"] == _epoch(2026, 9, 15)


def test_parseur_page_sans_donnees_embarquees():
    assert core.parse_ff_embedded_calendar("<html>rien</html>") == []
    # page présente mais structure méconnue (timeLabel sans objet parseable) :
    assert core.parse_ff_embedded_calendar('x"timeLabel"y') == []


def test_parseur_tolerant_aux_objets_measles():
    html = ('{"name":"Casse","dateline":1,"timeLabel":"1:00am","actual":"1"'
            + _ff_page([_ff_event()]) + '"fin"')
    out = core.parse_ff_embedded_calendar(html)
    assert [e["name"] for e in out] == ["ISM Manufacturing"]


# ─────────────────────────────────────────────────────────────────────────────
# Jointure déterministe
# ─────────────────────────────────────────────────────────────────────────────

def _index(*events):
    return core.build_actuals_index(core.parse_ff_embedded_calendar(_ff_page(list(events))))


def test_jointure_exacte_par_epoch():
    idx = _index(_ff_event())
    hit = core.find_overlay_actual(idx, "ISM Manufacturing",
                                   datetime(2026, 9, 15, 12, 30, tzinfo=UTC), "USD")
    assert hit and hit["actual"] == "5.2%"


def test_jointure_tolerance_minute_affinee_et_rejet_au_dela():
    ep = _epoch(2026, 9, 15, 22, 0)
    idx = _index(_ff_event(name="Westpac Consumer Sentiment", currency="NZD", dateline=ep + 240))
    ok = core.find_overlay_actual(idx, "Westpac Consumer Sentiment",
                                  datetime.fromtimestamp(ep, UTC), "NZD")
    assert ok and ok["actual"]                                   # 240 s : le cas réel
    lo = core.find_overlay_actual(idx, "Westpac Consumer Sentiment",
                                  datetime.fromtimestamp(ep - 400, UTC), "NZD")
    assert lo is None                                            # hors tolérance


def test_jointure_garde_devise_sur_homonymes():
    ep = _epoch(2026, 9, 15, 6, 0)
    idx = _index(_ff_event(name="Unemployment Rate", currency="GBP", dateline=ep),
                 _ff_event(name="Unemployment Rate", currency="CNY", dateline=ep,
                           actual="5.1%"))
    gbp = core.find_overlay_actual(idx, "Unemployment Rate",
                                   datetime.fromtimestamp(ep, UTC), "GBP")
    assert gbp["actual"] == "5.2%"
    cny = core.find_overlay_actual(idx, "Unemployment Rate",
                                   datetime.fromtimestamp(ep, UTC), "CNY")
    assert cny["actual"] == "5.1%"
    assert core.find_overlay_actual(idx, "Unemployment Rate",
                                    datetime.fromtimestamp(ep, UTC), "CHF") is None


def test_jointure_nom_absent_aucune_collision():
    idx = _index(_ff_event())
    assert core.find_overlay_actual(idx, "Inconnu", datetime.now(UTC), "USD") is None


def test_ff_day_url_format_figee_sans_locale():
    u = ci.ff_day_url(datetime(2026, 9, 5, tzinfo=UTC))
    assert u.endswith("?day=sep05.2026") and "sept" not in u
    assert ci.ff_day_url(datetime(2027, 5, 1, tzinfo=UTC)).endswith("?day=may01.2027")


# ─────────────────────────────────────────────────────────────────────────────
# refresh_actuals_overlay : écriture, fraîcheur, conservation en panne
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def _env(tmp_path, monkeypatch):
    monkeypatch.setattr(ci, "DISABLE_ACTUALS", False)
    monkeypatch.setattr(ci, "ACTUALS_REFRESH_S", 0)
    return tmp_path


def test_refresh_writes_and_keeps_failed_pages(_env, monkeypatch):
    ok_html = _ff_page([_ff_event(name="Alday", dateline=_epoch(2026, 9, 15))])
    def fake(session, url):
        if "sep16" in url:
            raise ci.FetchError("HTTP_ERROR", "status=500")
        return ok_html
    monkeypatch.setattr(ci, "fetch_ff_page", fake)
    summary = ci.refresh_actuals_overlay(_env, None, datetime(2026, 9, 16, 9, 0, tzinfo=UTC))
    obj = json.loads((_env / "actuals_overlay.json").read_text(encoding="utf-8"))
    assert obj["schema_version"] == "actuals-1.0.0"
    assert any(e["actual"] == "5.2%" for e in obj["entries"])
    assert summary["pages_failed"] and summary["entries"] >= 1
    assert "sep16" not in "".join(summary["pages_ok"])


def test_refresh_skips_when_fresh(_env, monkeypatch):
    (_env / "actuals_overlay.json").write_text(json.dumps(
        {"schema_version": "actuals-1.0.0", "fetched_at_utc": "2026-09-16T08:55:00Z",
         "entries": [{"name": "X", "dateline": _epoch(2026, 9, 15), "actual": "1%"}]}),
        encoding="utf-8")
    monkeypatch.setattr(ci, "ACTUALS_REFRESH_S", 10 ** 9)
    def boom(session, url):
        raise AssertionError("ne doit pas requêter : overlay frais")
    monkeypatch.setattr(ci, "fetch_ff_page", boom)
    summary = ci.refresh_actuals_overlay(_env, None, datetime.now(UTC))
    assert summary.get("skipped_fresh") and summary.get("entries") == 1


def test_refresh_desactive(_env, monkeypatch):
    monkeypatch.setattr(ci, "DISABLE_ACTUALS", True)
    def boom(session, url):
        raise AssertionError("BLUESTAR_DISABLE_ACTUALS=on interdit toute requête")
    monkeypatch.setattr(ci, "fetch_ff_page", boom)
    summary = ci.refresh_actuals_overlay(_env, None, datetime.now(UTC))
    assert summary.get("disabled") is True


def test_refresh_tout_en_echec_conserve_le_fichier(_env, monkeypatch):
    previous = json.dumps({"schema_version": "actuals-1.0.0",
                           "entries": [{"name": "X", "dateline": 1, "actual": "1%"}]})
    path = _env / "actuals_overlay.json"
    path.write_text(previous, encoding="utf-8")
    def fake(session, url):
        raise ci.FetchError("NETWORK_ERROR", "dégonflé")
    monkeypatch.setattr(ci, "fetch_ff_page", fake)
    summary = ci.refresh_actuals_overlay(_env, None, datetime.now(UTC))
    assert summary.get("kept_previous") is True
    assert path.read_text(encoding="utf-8") == previous


def test_refresh_structure_changee_ne_publie_rien(_env, monkeypatch):
    path = _env / "actuals_overlay.json"
    previous = json.dumps({"entries": [{"name": "X", "dateline": 1}]})
    path.write_text(previous, encoding="utf-8")
    def fake(session, url):
        # page présente, marqueur présent, mais plus aucun objet parseable :
        # signal de refonte du site — interdiction de publier un overlay vide.
        return '<html><script>x = { days: [{"timeLabel":"8:30am"'
    monkeypatch.setattr(ci, "fetch_ff_page", fake)
    summary = ci.refresh_actuals_overlay(_env, None, datetime.now(UTC))
    assert any("STRUCTURE_CHANGED" in p for p in summary["pages_failed"])
    assert summary.get("kept_previous") is True
    assert path.read_text(encoding="utf-8") == previous


# ─────────────────────────────────────────────────────────────────────────────
# CONTRAT : l'overlay ne déplace jamais le content_hash ni calendar.json
# ─────────────────────────────────────────────────────────────────────────────

def _rows():
    now = datetime.now(UTC)
    return [{"title": "ISM Manufacturing", "country": "USD",
             "date": (now + timedelta(hours=6)).isoformat(),
             "impact": "High", "forecast": "4.8%", "previous": "4.5%"}]


def _meta(body: bytes = b"[]"):
    return {"http_status": 200, "content_type": "application/json",
            "payload_bytes": len(body), "payload_sha256": "sha256:t",
            "etag": None, "last_modified": None, "fetch_duration_ms": 1,
            "raw_bytes": body}


def test_run_once_overlay_present_hash_identique(tmp_path, monkeypatch):
    rows = _rows()
    monkeypatch.setattr(ci, "DISABLE_ACTUALS", False)
    monkeypatch.setattr(ci, "ACTUALS_REFRESH_S", 0)
    monkeypatch.setattr(ci, "fetch_source", lambda session, url, deadline_s=None: (list(rows), _meta()))

    # cycle A : overlay disponible
    monkeypatch.setattr(ci, "fetch_ff_page",
                        lambda s, u: _ff_page([_ff_event(dateline=int((datetime.now(UTC)
                                                                       + timedelta(hours=6)).timestamp()))]))
    dir_a = tmp_path / "a"; dir_a.mkdir()
    pa = ci.run_once(dir_a, core.SelectionPolicy(), None)
    assert pa is not None and (dir_a / "actuals_overlay.json").exists()

    # cycle B : page du site inaccessible
    def boom(s, u):
        raise ci.FetchError("NETWORK_ERROR", "site injoignable")
    monkeypatch.setattr(ci, "fetch_ff_page", boom)
    dir_b = tmp_path / "b"; dir_b.mkdir()
    pb = ci.run_once(dir_b, core.SelectionPolicy(), None)

    assert pb is not None
    assert pa.content_hash == pb.content_hash
    assert not (dir_b / "actuals_overlay.json").exists()
    # le contrat canonique ignore l'overlay : supports_actual reste celui du FLUX
    canon = json.loads((dir_a / "calendar.latest.json").read_text(encoding="utf-8"))
    assert canon["source"]["supports_actual"] is False
    # et health.json rend compte des deux états (v2.5.0, health-1.2.0)
    health = json.loads((dir_a / "health.json").read_text(encoding="utf-8"))
    assert health["schema_version"] == "health-1.2.0"
    assert health["actuals_overlay"]["entries"] >= 1


def test_run_once_panne_overlay_ne_bloque_jamais_la_publication(tmp_path, monkeypatch):
    rows = _rows()
    monkeypatch.setattr(ci, "DISABLE_ACTUALS", False)
    monkeypatch.setattr(ci, "ACTUALS_REFRESH_S", 0)
    monkeypatch.setattr(ci, "fetch_source", lambda session, url, deadline_s=None: (list(rows), _meta()))
    def explode(s, u):
        raise RuntimeError("bug inattendu dans la collecte")
    monkeypatch.setattr(ci, "fetch_ff_page", explode)
    p = ci.run_once(tmp_path, core.SelectionPolicy(), None)
    assert p is not None                                   # publication réussie
    health = json.loads((tmp_path / "health.json").read_text(encoding="utf-8"))
    assert "UNEXPECTED" in (health["actuals_overlay"] or {}).get("error", "")


# ─────────────────────────────────────────────────────────────────────────────
# Côté app : chargeur d'index (vue)
# ─────────────────────────────────────────────────────────────────────────────

def test_app_load_actuals_state(tmp_path, monkeypatch):
    import app
    overlay = tmp_path / "actuals_overlay.json"
    overlay.write_text(json.dumps({
        "schema_version": "actuals-1.0.0", "fetched_at_utc": "x",
        "entries": [dict(_ff_event(), name="ISM Manufacturing")]}), encoding="utf-8")
    monkeypatch.setattr(app, "ACTUALS_PATH", overlay)
    idx, meta = app.load_actuals_state()
    hit = core.find_overlay_actual(idx, "ISM Manufacturing",
                                   datetime.fromtimestamp(_epoch(2026, 9, 15), UTC), "USD")
    assert hit and hit["actual"] == "5.2%"
    # fichier absent -> index vide, affichage d'avant reproduit à l'identique
    overlay.unlink()
    idx2, meta2 = app.load_actuals_state()
    assert idx2 == {} and meta2 == {}

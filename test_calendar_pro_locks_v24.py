"""Verrous v2.4.0 — port des correctifs calendar_layer MACRO (v6.1/v6.2) dans
le calendar_core PRO (app TA).

Règle d'or de ce fichier : chaque verrou nomme le tag du correctif porté
([F3]/[F5]/[F6]/[F7]/[M3]/[M4]/[B8]/[M2-compat]/[M5]) et teste la PROPRIÉTÉ
inter-apps visée, pas l'implémentation. Retirer un port rougit ces tests —
c'est le but. Parité de méthode = parité de hash pour données identiques.

Contexte ENGINE : calendar.json (légatif) est consommé par ENGINE.V10 via
CalendarEvent(extra="ignore") et _parse_ff_value qui traite déjà « — » comme
absent (l.1318) — les ports additifs ne peuvent donc pas casser le desk ;
les ports F5/M2 changent des clés LUES, verrouillées ici.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from calendar_core import (
    CONTENT_HASH_METHOD,
    DEFAULT_DISPLAY_TZ,
    DEFAULT_POLICY,
    NUMERIC_PARSER_VERSION,
    SESSION_POLICY_VERSION,
    SelectionPolicy,
    Session,
    SourceInfo,
    build_payload,
    classify_session,
    normalize_numeric,
    to_legacy_payload,
)

UTC = timezone.utc
NOW = datetime(2026, 9, 1, 10, 8, 44, tzinfo=UTC)


def make_source(**overrides) -> SourceInfo:
    base = dict(
        provider="test",
        url="https://example.invalid/feed.json",
        fetched_at_utc=NOW,
        payload_sha256="sha256:test",
        supports_actual=False,
    )
    base.update(overrides)
    return SourceInfo(**base)


def build(raw=None, policy=None, now=NOW, source=None):
    return build_payload(
        raw if raw is not None else SAMPLE,
        source=source or make_source(),
        now_utc=now,
        policy=policy or SelectionPolicy(),
    )


SAMPLE = [
    {"title": "ISM Manufacturing PMI", "country": "USD",
     "date": "2026-09-01T10:00:00-04:00", "impact": "High",
     "forecast": "55.2", "previous": "55.6"},
    {"title": "G20 Meetings", "country": "All",
     "date": "2026-09-01T15:15:00-04:00", "impact": "High",
     "forecast": "", "previous": ""},
    {"title": "Non-Farm Employment Change", "country": "USD",
     "date": "2026-09-04T08:30:00-04:00", "impact": "High",
     "forecast": "55K", "previous": "-23K"},
    {"title": "Bank Holiday", "country": "GBP",
     "date": "2026-08-31T03:00:00-04:00", "impact": "Holiday",
     "forecast": "", "previous": ""},
]


# ── Parité de contrat avec le calendar_layer macro (v6.2, sha 2e4bf033a0c6) ──
def test_contract_versions_paired_with_macro():
    """Les trois versions de méthode + les seuils décisionnels doivent être
    TEXTELEMENT identiques aux constantes du calendar_layer macro. Si l'un
    des deux modules bump une méthode sans l'autre, ce verrou rougit : c'est
    LE signal de coordination inter-apps avant tout déploiement."""
    assert CONTENT_HASH_METHOD == "economic_projection_v2"
    assert SESSION_POLICY_VERSION == "exchange_local_dst_aware_v2"
    assert NUMERIC_PARSER_VERSION == "ff_numeric_v2"
    p = DEFAULT_POLICY
    assert (p.imminent_hours, p.soon_hours) == (6.0, 48.0)
    assert (p.window_past_hours, p.window_future_hours) == (72.0, 168.0)


# ── [F6] parseur numérique ───────────────────────────────────────────────────
def test_thousands_separator_not_read_as_decimal():
    """« 1,234 » = 1 234 et non 1.234 (bug facteur-1000 pré-port sur les
    valeurs sans suffixe K/M/B). Cas mesurés identiques au verrou macro."""
    assert normalize_numeric("1,234").value == pytest.approx(1234.0)
    assert normalize_numeric("1.234,5").value == pytest.approx(1234.5)
    assert normalize_numeric("12.345.678").value == pytest.approx(12345678.0)
    assert normalize_numeric("1,5").value == pytest.approx(1.5)
    assert normalize_numeric("0.25").value == pytest.approx(0.25)
    assert normalize_numeric("1,234").parse_status == "PARSED"


def test_prefix_is_named_not_silent():
    """[F6/M3] tout préfixe d'approximation (< > ~ ≈ ≤ ≥ ±) doit NOMMER
    l'incertitude (APPROXIMATE) au lieu de la noyer dans un PARSED."""
    for text, expected in [("≈2.5%", 2.5), ("<0.1", 0.1), ("~3", 3.0),
                           ("≤3", 3.0), ("≥ -2", -2.0), ("±0.5%", 0.5)]:
        nv = normalize_numeric(text)
        assert nv.value == pytest.approx(expected), text
        assert nv.parse_status == "APPROXIMATE", text
    assert normalize_numeric("2.5%").parse_status == "PARSED"
    assert normalize_numeric("2.5%").unit == "percent"


def test_placeholders_alignes_sur_le_macro():
    assert normalize_numeric("n.a.").parse_status == "ABSENT"
    assert normalize_numeric("tentative").parse_status == "ABSENT"
    assert normalize_numeric("—").parse_status == "ABSENT"
    assert normalize_numeric("0").value == 0.0          # piège falsy
    assert normalize_numeric("").parse_status == "ABSENT"


# ── [B8] sessions ANZ ────────────────────────────────────────────────────────
def test_nzd_gdp_coeur_de_seance_anz_nest_plus_off():
    """Cas MESURÉ du correctif macro : « GDP q/q » NZD mercredi 22:45 UTC =
    10:45 Wellington / 08:45 Sydney. Pré-port : Session.OFF."""
    dt = datetime(2026, 9, 2, 22, 45, tzinfo=UTC)
    session, active = classify_session(dt)
    assert session is Session.ASIAN
    assert {"SYDNEY", "WELLINGTON"} <= set(active)


def test_zone_anz_sans_tokyo_compte_comme_asie():
    """Wellington/Sydney ouverts, Tokyo fermé (22:00Z = 08:00 Sydney /
    10:00 NZST jeudi, Tokyo 07:00 avant ouverture) → ASIAN, pas OFF.
    NB : Wellington « seule » est structurellement impossible en jour
    ouvré (New York couvre 12:00-21:00Z, Sydney prend le relais à 21:00Z)
    — c'est bien la PAIRE ANZ qui était étiquetée OFF pré-port."""
    dt = datetime(2026, 9, 2, 22, 0, tzinfo=UTC)
    session, active = classify_session(dt)
    assert session is Session.ASIAN
    assert active == ["SYDNEY", "WELLINGTON"]


# ── [M4] fenêtre presseur par devise ─────────────────────────────────────────
JPY_RATE = {"title": "Interest Rate Decision", "country": "JPY",
            "date": "2026-09-03T11:30:00+09:00", "impact": "High"}   # 02:30Z
JPY_PRESSER_180 = {"title": "BOJ Press Conference", "country": "JPY",
                   "date": "2026-09-03T14:30:00+09:00", "impact": "High"}  # 05:30Z


def test_boj_presser_rattache_a_180_min_jpy():
    """Mesuré sur calendrier BoJ réel : décision 02:30Z → conférence 05:30Z
    = 180 min > 120. Pré-port : jamais rattachée. JPY = 240 min."""
    p = build(raw=[JPY_RATE, JPY_PRESSER_180])
    groups = {e.name: e.release_group_id for e in p.events}
    assert groups["Interest Rate Decision"] is not None
    assert groups["BOJ Press Conference"] == groups["Interest Rate Decision"]


def test_hors_jpy_la_fenetre_reste_120_min():
    """GBP 150 min → PAS rattaché (la fenêtre élargie est justifiée SEULEMENT
    pour JPY ; un élargissement global aurait groupé des événements distincts).
    Et 30 min → rattaché (comportement historique conservé)."""
    beyond = build(raw=[
        {"title": "Interest Rate Decision", "country": "GBP",
         "date": "2026-09-03T12:00:00+00:00", "impact": "High"},
        {"title": "BoE Press Conference", "country": "GBP",
         "date": "2026-09-03T14:30:00+00:00", "impact": "High"},
    ])
    gb = {e.name: e.release_group_id for e in beyond.events}
    assert gb["Interest Rate Decision"] is not None
    assert gb["BoE Press Conference"] is None

    within = build(raw=[
        {"title": "Interest Rate Decision", "country": "GBP",
         "date": "2026-09-03T12:00:00+00:00", "impact": "High"},
        {"title": "BoE Press Conference", "country": "GBP",
         "date": "2026-09-03T12:30:00+00:00", "impact": "High"},
    ])
    gw = {e.name: e.release_group_id for e in within.events}
    assert gw["BoE Press Conference"] == gw["Interest Rate Decision"]


# ── [F5] projection économique explicite ────────────────────────────────────
def test_hash_insensible_au_fuseau_daffichage():
    """Le point du port : deux instances configurées dans des fuseaux
    DIFFÉRENTS doivent produire le MÊME hash pour le MÊME calendrier —
    tout en affichant des dates différentes (preuve que le test n'est pas
    un no-op : G20 15:15 EST = 1er sept. à Casablanca, 2 sept. à Tokyo)."""
    p1 = build(policy=SelectionPolicy(display_timezone="Africa/Casablanca"))
    p2 = build(policy=SelectionPolicy(display_timezone="Asia/Tokyo"))
    assert p1.content_hash == p2.content_hash
    d1 = {e.occurrence_id: e.date_display for e in p1.events}
    d2 = {e.occurrence_id: e.date_display for e in p2.events}
    assert d1 != d2


def test_hash_insensible_a_ordre_de_fusion():
    """Même lot de lignes dans le désordre → même projection (les événements
    sont triés à la construction ; source_index et l'ordre brut sont exclus)."""
    assert build(raw=SAMPLE).content_hash == build(raw=list(reversed(SAMPLE))).content_hash


def test_hash_reagit_toujours_au_contenu_economique():
    """L'insensibilité ne doit jamais devenir de l'indifférence : changer la
    prévision d'UN événement change le hash."""
    moved = [{**r, "forecast": "99.9"} if r["title"] == "ISM Manufacturing PMI"
             else r for r in SAMPLE]
    assert build(raw=SAMPLE).content_hash != build(raw=moved).content_hash


# ── [F7] locale & robustesse ─────────────────────────────────────────────────
def test_day_of_week_table_fixe_anglais():
    """strftime("%A") suit la LOCALE du conteneur ; la table fixe non."""
    p = build()
    anglais = {"MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY",
               "FRIDAY", "SATURDAY", "SUNDAY"}
    assert {e.day_of_week for e in p.events} <= anglais


def test_fenetre_servie_egale_fenetre_annoncee_168h():
    """Port [F7] macro : 192 h annoncés / jamais atteints → 168 h servies ET
    annoncées. Un event à +167 h entre, un event à +170 h sort."""
    inside = {"title": "In Window Probe", "country": "USD",
              "date": "2026-09-08T05:00:00+00:00", "impact": "High"}   # +162,9 h
    outside = {"title": "Out Window Probe", "country": "USD",
               "date": "2026-09-08T12:30:00+00:00", "impact": "High"}  # +170,4 h
    names = {e.name for e in build(raw=[inside, outside]).events}
    assert names == {"In Window Probe"}


# ── [F3/M3] lignes legacy ────────────────────────────────────────────────────
def _rows(legacy):
    return {r["event_name"]: r for r in legacy["events_engine"]}


def test_placeholder_absent_et_statuts_exports():
    legacy = to_legacy_payload(build(), NOW)
    r = _rows(legacy)
    ism = r["ISM Manufacturing PMI"]
    assert ism["forecast"] == "55.2"
    assert ism["forecast_status"] == "PARSED"
    assert ism["previous"] == "55.6"
    assert ism["previous_status"] == "PARSED"
    assert ism["actual"] == "—"                     # [F3] plus None
    assert ism["actual_value"] is None              # le numérique reste None
    g20 = r["G20 Meetings"]
    assert g20["forecast"] == "—"
    assert g20["forecast_status"] == "ABSENT"
    for row in legacy["events_engine"]:
        assert row["session"]                       # [F7] jamais KeyError,
        assert row["release_group_type"] in (None, "CENTRAL_BANK_DECISION",
                                             "SIMULTANEOUS_RELEASE",
                                             "LABOR_MARKET_RELEASE")


def test_priority_meme_seuil_que_le_macro():
    """ISM ≈ +3,9 h → CRITICAL ; NFP ≈ +74 h → MEDIUM ; un HIGH passé de
    46 h (dans la fenêtre résiduelle 72 h) → PAST. La Holiday de SAMPLE est
    écartée par la policy défaut (HIGH only) — d'où la sonde dédiée."""
    past = {"title": "Past Probe", "country": "USD",
            "date": "2026-08-31T12:00:00+00:00", "impact": "High"}
    r = _rows(to_legacy_payload(build(raw=[*SAMPLE, past]), NOW))
    assert r["ISM Manufacturing PMI"]["priority"] == "CRITICAL"
    assert r["Non-Farm Employment Change"]["priority"] == "MEDIUM"
    assert r["Past Probe"]["priority"] == "PAST"


# ── [compat-M2] vocabulaire d'horizon lu par l'ENGINE ───────────────────────
def test_live_ok_horizon_court_nominal_jamais_rouge():
    src = make_source(feed_status={"thisweek": "ok", "nextweek": "absent_404"})
    m = to_legacy_payload(build(source=src), NOW)["metadata"]
    assert m["reachable"] is True
    assert (m["feeds_ok"], m["feeds_total"]) == (1, 2)
    assert m["feed_horizon_state"] == "nominal_weekly"
    assert m["feed_horizon_truncated"] is False     # jamais 7 jours/7 (leçon M2)
    assert m["feed_horizon_h"] is not None
    assert m["content_hash_method"] == "economic_projection_v2"


def test_thisweek_en_echec_seul_cas_rouge():
    src = make_source(feed_status={"thisweek": "error:500", "nextweek": "ok"})
    m = to_legacy_payload(build(source=src), NOW)["metadata"]
    assert m["feed_horizon_state"] == "degraded"
    assert m["feed_horizon_truncated"] is True


def test_lkg_secours_honnete_jamais_rouge():
    """LKG servi : reachable=False (cycle sans source), MAIS pas de drapeau
    rouge — la donnée affichée est de confiance, l'incident est signalé par
    serving_mode. Avant le port, « reachable » n'était pas exporté du tout et
    l'ENGINE vivait sur son défaut True : fail-open invisible."""
    src = make_source(from_last_known_good=True,
                      feed_status={"thisweek": "error:520"})
    m = to_legacy_payload(build(source=src), NOW)["metadata"]
    assert m["reachable"] is False
    assert m["serving_mode"] == "last_known_good"
    assert m["feed_horizon_state"] == "unreachable"
    assert m["feed_horizon_truncated"] is False


def test_payload_injecte_sans_feed_status_est_de_confiance():
    """Conventions des tests/rejeu (build() sans feed_status) : reachable
    True, aucune dégradation affirmée — verrou contre une régression qui
    rougirait les rejeux hors production."""
    m = to_legacy_payload(build(), NOW)["metadata"]
    assert m["reachable"] is True
    assert m["feed_horizon_truncated"] is False


# ── [M5] découplage events / events_engine ──────────────────────────────────
def test_events_et_engine_ne_partagent_plus_le_meme_objet():
    legacy = to_legacy_payload(build(), NOW)
    assert legacy["events"] == legacy["events_engine"]
    assert legacy["events"] is not legacy["events_engine"]
    legacy["events"].append("sentinelle")
    assert "sentinelle" not in legacy["events_engine"]


# ── [F2] fuseau : câblage sans changement de défaut ─────────────────────────
def test_display_tz_defaut_inchange_et_cable():
    assert DEFAULT_DISPLAY_TZ == "Africa/Casablanca"
    assert SelectionPolicy().display_timezone == DEFAULT_DISPLAY_TZ

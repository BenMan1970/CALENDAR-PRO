"""Suite de non-régression. CI bloquante : aucun déploiement si un test échoue.

NOM : ce fichier s'appelait `tests-test_calendar_core.py` — un pattern que
pytest ne collecte PAS (ni `test_*.py`, ni `*_test.py`) : la « CI bloquante »
revendiquée collectait silencieusement 0 test (audit calendrier 2026-09-11).
Renommé ; ajouter ce dépôt à la racine de collecte de la CI.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from calendar_core import (
    ActualStatus,
    Impact,
    PairMappingStatus,
    QualityStatus,
    SelectionPolicy,
    Session,
    SourceInfo,
    TimeProximity,
    build_payload,
    canonical_content_hash,
    classify_session,
    compute_time_context,
    normalize_numeric,
    pairs_for_currency,
    parse_source_datetime,
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


RAW = [
    {"title": "ISM Manufacturing PMI", "country": "USD",
     "date": "2026-09-01T10:00:00-04:00", "impact": "High",
     "forecast": "55.2", "previous": "55.6"},
    {"title": "Official Cash Rate", "country": "NZD",
     "date": "2026-09-01T22:00:00-04:00", "impact": "High",
     "forecast": "2.75%", "previous": "2.50%"},
    {"title": "RBNZ Rate Statement", "country": "NZD",
     "date": "2026-09-01T22:00:00-04:00", "impact": "High",
     "forecast": "", "previous": ""},
    {"title": "RBNZ Press Conference", "country": "NZD",
     "date": "2026-09-01T23:00:00-04:00", "impact": "High",
     "forecast": "", "previous": ""},
    {"title": "Non-Farm Employment Change", "country": "USD",
     "date": "2026-09-04T08:30:00-04:00", "impact": "High",
     "forecast": "55K", "previous": "-23K"},
    {"title": "G20 Meetings", "country": "All",
     "date": "2026-09-01T11:15:00-04:00", "impact": "High",
     "forecast": "", "previous": ""},
    {"title": "Bank Holiday", "country": "GBP",
     "date": "2026-08-31T03:00:00-04:00", "impact": "Holiday",
     "forecast": "", "previous": ""},
    {"title": "JOLTS Job Openings", "country": "USD",
     "date": "2026-09-01T10:00:00-04:00", "impact": "Medium",
     "forecast": "7.33M", "previous": "7.36M"},
]


def build(raw=None, policy=None, now=NOW, source=None):
    return build_payload(
        raw if raw is not None else RAW,
        source=source or make_source(),
        now_utc=now,
        policy=policy or SelectionPolicy(),
    )


# ── Temps ────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("raw,expected_hour", [
    ("2026-09-01T10:00:00-04:00", 14),
    ("2026-09-01T14:00:00Z", 14),
    ("2026-09-01T14:00:00+00:00", 14),
    ("2026-09-01T14:00:00", 14),
])
def test_datetime_parsing_normalizes_to_utc(raw, expected_hour):
    parsed = parse_source_datetime(raw)
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == timedelta(0)
    assert parsed.hour == expected_hour


def test_display_timezone_conversion_matches_casablanca():
    payload = build()
    ism = next(e for e in payload.events if e.name.startswith("ISM"))
    assert ism.scheduled_at_utc.strftime("%H:%M") == "14:00"
    assert ism.scheduled_at_display.strftime("%H:%M") == "15:00"


# ── Sessions DST-aware ───────────────────────────────────────────────────────
def test_nfp_summer_release_is_london_ny_overlap_not_london():
    """Régression historique : 12:30 UTC en septembre = 08:30 EDT, NY est ouvert."""
    session, centers = classify_session(datetime(2026, 9, 4, 12, 30, tzinfo=UTC))
    assert session is Session.OVERLAP_LONDON_NY
    assert "NEW_YORK" in centers and "LONDON" in centers


def test_winter_release_at_same_utc_hour_is_london_only():
    session, centers = classify_session(datetime(2026, 1, 15, 12, 30, tzinfo=UTC))
    assert session is Session.LONDON
    assert "NEW_YORK" not in centers


def test_weekend_timestamp_is_off_session():
    session, centers = classify_session(datetime(2026, 9, 5, 12, 30, tzinfo=UTC))
    assert session is Session.OFF and centers == []


# ── Parsing numérique ────────────────────────────────────────────────────────
@pytest.mark.parametrize("raw,value,unit", [
    ("55K", 55_000.0, "number"),
    ("-23K", -23_000.0, "number"),
    ("7.33M", 7_330_000.0, "number"),
    ("-29.7B", -29_700_000_000.0, "number"),
    ("0.3%", 0.3, "percent"),
    ("-0.7%", -0.7, "percent"),
    ("55.2", 55.2, "number"),
])
def test_numeric_parsing(raw, value, unit):
    parsed = normalize_numeric(raw)
    assert parsed.parse_status == "PARSED"
    assert parsed.value == pytest.approx(value)
    assert parsed.unit == unit
    assert parsed.raw == raw


def test_zero_is_not_swallowed_by_falsy_check():
    for candidate in ("0", 0, "0.0", "0%"):
        assert normalize_numeric(candidate).value == 0.0
        assert normalize_numeric(candidate).parse_status == "PARSED"


def test_composite_auction_value_is_flagged():
    parsed = normalize_numeric("2.84|2.6")
    assert parsed.parse_status == "COMPOSITE"
    assert parsed.value == pytest.approx(2.84)
    assert parsed.raw == "2.84|2.6"


def test_placeholders_map_to_absent_never_to_em_dash():
    for candidate in ("", "—", "-", "N/A", None):
        parsed = normalize_numeric(candidate)
        assert parsed.parse_status == "ABSENT"
        assert parsed.value is None
        assert parsed.raw is None


# ── Sélection & politique ────────────────────────────────────────────────────
def test_only_policy_impact_levels_are_retained():
    """Policy 1.1.0+ : HIGH+MEDIUM retenus, LOW/Holiday exclus par défaut.
    (JOLTS Medium était l'ancienne victime de l'assertion HIGH-only.)"""
    payload = build()
    assert {e.impact for e in payload.events} == {Impact.HIGH, Impact.MEDIUM}
    assert any("JOLTS" in e.name for e in payload.events)
    assert all("Bank Holiday" not in e.name for e in payload.events)


def test_holiday_impact_is_modelled_not_crashing():
    policy = SelectionPolicy(impact_levels=(Impact.HOLIDAY,), window_past_hours=240)
    payload = build(policy=policy)
    assert [e.name for e in payload.events] == ["Bank Holiday"]
    assert payload.events[0].impact is Impact.HOLIDAY


def test_events_outside_window_are_excluded():
    policy = SelectionPolicy(window_past_hours=1, window_future_hours=6)
    payload = build(policy=policy)
    # JOLTS (Medium) est retenu depuis policy 1.1.0 — il partage la minute d'ISM.
    assert [e.name for e in payload.events] == [
        "ISM Manufacturing PMI", "JOLTS Job Openings", "G20 Meetings"]


# ── Événements globaux ───────────────────────────────────────────────────────
def test_global_event_is_kept_and_explicitly_flagged():
    payload = build()
    g20 = next(e for e in payload.events if e.name == "G20 Meetings")
    assert g20.is_global is True
    assert g20.pairs_with_currency_exposure == ()
    assert g20.pair_mapping_status is PairMappingStatus.NO_MAPPING_GLOBAL_EVENT


def test_no_high_impact_event_has_silent_empty_pairs():
    """Une liste vide doit TOUJOURS être justifiée par un statut explicite."""
    payload = build()
    for event in payload.events:
        if not event.pairs_with_currency_exposure:
            assert event.pair_mapping_status is not PairMappingStatus.MAPPED


def test_pairs_are_derived_by_membership():
    pairs = pairs_for_currency("USD")
    assert len(pairs) == 7
    assert all("USD" in p.split("/") for p in pairs)
    assert pairs == pairs_for_currency("usd")


# ── Identité & idempotence ───────────────────────────────────────────────────
def test_occurrence_ids_are_unique():
    payload = build()
    ids = [e.occurrence_id for e in payload.events]
    assert len(ids) == len(set(ids))


def test_occurrence_id_is_stable_across_runs():
    first = build(now=NOW)
    later = build(now=NOW + timedelta(hours=4))
    assert [e.occurrence_id for e in first.events] == [e.occurrence_id for e in later.events]


def test_content_hash_ignores_volatile_time_fields():
    first = build(now=NOW)
    later = build(now=NOW + timedelta(minutes=37),
                  source=make_source(fetched_at_utc=NOW + timedelta(minutes=37)))
    assert first.content_hash == later.content_hash
    assert first.events[0].time_context != later.events[0].time_context


def test_content_hash_changes_when_forecast_changes():
    mutated = [dict(row) for row in RAW]
    mutated[0]["forecast"] = "56.0"
    assert build().content_hash != build(raw=mutated).content_hash


def test_duplicate_source_rows_are_deduplicated():
    payload = build(raw=RAW + [dict(RAW[0])])
    assert payload.quality.duplicate_event_count == 1
    assert len({e.occurrence_id for e in payload.events}) == len(payload.events)


# ── Groupes de publication ───────────────────────────────────────────────────
def test_rbnz_cluster_forms_one_release_group_including_presser():
    payload = build()
    rbnz = [e for e in payload.events if e.currency == "NZD"]
    groups = {e.release_group_id for e in rbnz}
    assert len(rbnz) == 3
    assert len(groups) == 1 and None not in groups
    assert all(e.release_group_type.value == "CENTRAL_BANK_DECISION" for e in rbnz)


def test_unrelated_events_do_not_share_a_group():
    payload = build()
    ism = next(e for e in payload.events if e.name.startswith("ISM"))
    nfp = next(e for e in payload.events if e.name.startswith("Non-Farm"))
    assert ism.release_group_id != nfp.release_group_id


# ── Champ actual ─────────────────────────────────────────────────────────────
def test_actual_is_null_and_status_is_explicit_when_source_omits_it():
    payload = build()
    assert payload.source.supports_actual is False
    for event in payload.events:
        assert event.actual.raw is None
        assert event.actual_status is ActualStatus.UNSUPPORTED_BY_SOURCE
    serialized = payload.model_dump(mode="json")
    assert all(e["actual"]["raw"] is None for e in serialized["events"])
    assert "—" not in str(serialized)


def test_actual_is_honoured_if_source_starts_providing_it():
    mutated = [dict(row) for row in RAW]
    mutated[0]["actual"] = "56.1"
    payload = build(raw=mutated, source=make_source(supports_actual=True))
    ism = next(e for e in payload.events if e.name.startswith("ISM"))
    assert ism.actual.value == pytest.approx(56.1)
    assert ism.actual_status is ActualStatus.RELEASED


# ── Staleness & qualité ──────────────────────────────────────────────────────
def test_stale_source_is_flagged_and_degrades_score():
    stale = make_source(fetched_at_utc=NOW - timedelta(hours=3))
    payload = build(source=stale)
    assert payload.quality.is_stale is True
    assert payload.quality.status is QualityStatus.DEGRADED
    assert payload.quality.data_quality_score < 1.0
    assert any("SOURCE_AGE_EXCEEDS" in w for w in payload.quality.warnings)


def test_week_rollover_is_detected_when_all_events_are_past():
    late = NOW + timedelta(days=20)
    payload = build(now=late, source=make_source(fetched_at_utc=late))
    assert "ALL_SOURCE_EVENTS_IN_THE_PAST_WEEK_ROLLOVER_PENDING" in payload.quality.warnings


def test_empty_payload_is_invalid_not_silently_published():
    payload = build(raw=[])
    assert payload.quality.status is QualityStatus.INVALID
    assert payload.quality.data_quality_score == 0.0


def test_last_known_good_is_surfaced():
    payload = build(source=make_source(from_last_known_good=True))
    assert "SERVING_LAST_KNOWN_GOOD" in payload.quality.warnings
    assert payload.quality.status is QualityStatus.DEGRADED


# ── Robustesse schéma ────────────────────────────────────────────────────────
def test_non_array_root_is_rejected():
    with pytest.raises(ValueError, match="JSON array"):
        build(raw={"events": []})


def test_malformed_rows_are_rejected_individually_with_context():
    payload = build(raw=RAW + ["<html>oops</html>", {"title": "", "country": "USD",
                                                     "date": "2026-09-01T10:00:00-04:00",
                                                     "impact": "High"},
                               {"title": "Bad date", "country": "USD",
                                "date": "not-a-date", "impact": "High"}])
    assert payload.quality.rejected_event_count == 3
    assert any("Bad date" in r for r in payload.quality.rejections)
    # 7 retenus = 8 fixtures − Bank Holiday (impact hors policy) ; JOLTS Medium
    # est compté depuis policy 1.1.0 (l'ancienne valeur 5 datait de HIGH-only).
    assert len(payload.events) == 7


def test_unknown_impact_vocabulary_raises_warning_not_exception():
    payload = build(raw=RAW + [{"title": "Mystery", "country": "USD",
                                "date": "2026-09-01T12:00:00-04:00", "impact": "Critical"}])
    assert "SOURCE_IMPACT_VOCABULARY_CHANGED" in payload.quality.warnings


def test_oversized_payload_is_rejected():
    policy = SelectionPolicy(max_events=3)
    with pytest.raises(ValueError, match="payload too large"):
        build(policy=policy)


# ── Immutabilité & invariants ────────────────────────────────────────────────
def test_events_are_frozen():
    payload = build()
    with pytest.raises(Exception):
        payload.events[0].currency = "EUR"


def test_events_are_sorted_chronologically():
    payload = build()
    times = [e.scheduled_at_utc for e in payload.events]
    assert times == sorted(times)


def test_time_context_thresholds():
    payload = build()
    ism = next(e for e in payload.events if e.name.startswith("ISM"))
    ctx = compute_time_context(ism, NOW, payload.selection_policy)
    assert ctx.time_proximity is TimeProximity.IMMINENT
    assert ctx.hours_until == pytest.approx(3.8544, abs=1e-3)
    assert ctx.hours_until_display == "3h 51m"

    boundary = ism.scheduled_at_utc - timedelta(hours=6, seconds=1)
    assert compute_time_context(ism, boundary, payload.selection_policy).time_proximity \
        is TimeProximity.SOON
    exactly_now = compute_time_context(ism, ism.scheduled_at_utc, payload.selection_policy)
    assert exactly_now.time_proximity is TimeProximity.PAST
    assert exactly_now.status.value == "DUE"


# ── Déterminisme & legacy ────────────────────────────────────────────────────
def test_identical_inputs_produce_identical_serialization():
    a = build().model_dump(mode="json")
    b = build().model_dump(mode="json")
    assert a == b


def test_legacy_events_and_engine_are_identical():
    """Le bug historique : deux listes divergentes sans trace d'audit."""
    legacy = to_legacy_payload(build(), NOW)
    assert legacy["events"] == legacy["events_engine"]
    assert legacy["metadata"]["ui_filters_applied"] is None


def test_legacy_summary_is_indexed_on_display_dates():
    legacy = to_legacy_payload(build(), NOW)
    display_dates = {e["date_display"] for e in legacy["events"]}
    assert set(legacy["summary_by_day"]) == display_dates
    assert legacy["metadata"]["summary_by_day_basis"] == "display_timezone"


# ── HARNESS-CAL : verrous du contrat legacy (audit calendrier 2026-09-11) ───
def test_legacy_filters_applied_is_the_claimable_coverage():
    """F-3 : le ENGINE lit `filters_applied.currencies` (et IGNORE currencies_covered).
    La liste publiée doit être exactement ce que le flux revendique :
    couvertes ∪ sans-publication, JAMAIS les devises exclues par la policy."""
    legacy = to_legacy_payload(build(), NOW)
    meta = legacy["metadata"]
    assert meta["ui_filters_applied"] is None            # pureté humaine verrouillée
    fa = meta["filters_applied"]
    assert fa["basis"] == "machine_policy"
    claimable = set(meta["currencies_covered"]) | set(meta["currencies_no_data_in_source"])
    assert set(fa["currencies"]) == claimable
    assert not (set(fa["currencies"]) & set(meta["currencies_excluded_by_policy"]))
    ev_ccys = {e["currency"] for e in legacy["events"]} - {"ALL"}  # ALL = sentinel global
    assert ev_ccys <= claimable                          # aucun événement hors couverture revendiquée


def test_legacy_impact_casing_is_uniform():
    """F-4 : metadata et événements parlaient deux casses du même vocabulaire."""
    legacy = to_legacy_payload(build(), NOW)
    assert set(legacy["metadata"]["impact_levels_included"]) == {"high", "medium"}
    assert set(legacy["metadata"]["filters_applied"]["impact_levels"]) == {"high", "medium"}
    for row in legacy["events"]:
        assert row["impact"] == row["impact"].lower()


def test_legacy_publishes_real_data_coverage_bounds():
    """F-2 : les bornes POLICY ne doivent plus être la seule fenêtre visible
    de l'aval — les bornes RÉELLES de données sont désormais exportées."""
    legacy = to_legacy_payload(build(), NOW)
    meta = legacy["metadata"]
    times = [e["datetime_utc"] for e in legacy["events"]]
    assert meta["data_coverage_start_utc"] == min(times)
    assert meta["data_coverage_end_utc"] == max(times)
    assert meta["data_coverage_horizon_h"] == pytest.approx(
        (datetime.fromisoformat(max(times).replace("Z", "+00:00"))
         - datetime.fromisoformat(meta["generated_at_utc"].replace("Z", "+00:00"))
         ).total_seconds() / 3600.0, abs=0.02)


def test_coverage_shorter_than_soon_horizon_is_warned_and_degrades():
    """Régime « jeudi après-midi » : la source se tarit avant soon_hours (48 h)."""
    payload = build(raw=[dict(RAW[0])])               # seul ISM : fin ~3,9 h devant NOW
    assert any(w.startswith("COVERAGE_SHORTER_THAN_HORIZON") for w in payload.quality.warnings)
    assert payload.quality.status is QualityStatus.DEGRADED


def test_expired_last_known_good_is_invalid_not_recycled():
    """F-2 : un LKG au-delà du plafond d'âge dur doit INVALIDER le publish
    (avant ce champ, le plancher de score 0,40 le laissait republiable à vie)."""
    old_lkg = make_source(from_last_known_good=True,
                          fetched_at_utc=NOW - timedelta(hours=49))
    payload = build(source=old_lkg)
    assert any(w.startswith("LAST_KNOWN_GOOD_EXPIRED") for w in payload.quality.warnings)
    assert payload.quality.status is QualityStatus.INVALID


def test_fresh_last_known_good_stays_degradable_only():
    lkg = make_source(from_last_known_good=True, fetched_at_utc=NOW - timedelta(hours=2))
    payload = build(source=lkg)
    assert not any(w.startswith("LAST_KNOWN_GOOD_EXPIRED") for w in payload.quality.warnings)
    assert payload.quality.status is QualityStatus.DEGRADED


def test_high_impact_count_is_no_longer_the_row_count():
    """total_high_impact (deprecated) comptait HIGH+MEDIUM ; le nouveau champ
    doit compter les vrais HIGH."""
    legacy = to_legacy_payload(build(), NOW)
    meta = legacy["metadata"]
    assert meta["total_high_impact"] == len(legacy["events"])          # deprecated conservé
    assert meta["high_impact_count"] == sum(1 for e in legacy["events"] if e["impact"] == "high")
    assert meta["high_impact_count"] < meta["total_high_impact"]

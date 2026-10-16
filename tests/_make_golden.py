"""Generation du GOLDEN a partir de la fixture locale.

Usage :  .venv/bin/python tests/_make_golden.py

Ecrit tests/fixtures/golden.json : content_hash, parity_hash,
high_content_hash et le JSON legacy complet, produits par build_payload sur la
fixture FF, a instant UTC FIXE. Aucune I/O reseau.

Ce module est AUSSI la fabrique partagee des tests : ``build()`` rejoue
exactement la meme construction, donc les tests comparent des choses
comparables.

POINT CRITIQUE DE DETERMINISME
------------------------------
calendar_core resout DEFAULT_DISPLAY_TZ a l'IMPORT (ligne ~156), et
to_legacy_payload re-resout le fuseau d'affichage a chaque appel via
resolve_display_tz(), qui place le fuseau SYSTEME avant le repli. Sur une
machine en Africa/Lagos, le legacy sortait donc localise a Lagos ; sur le
serveur Cloud en Etc/UTC, en UTC. Les variables ci-dessous doivent donc etre
posees AVANT le premier import des modules du projet — sinon le golden depend
de la machine qui le genere, ce qui est exactement le bug 2.3.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── AVANT tout import des modules du projet ─────────────────────────────────
os.environ.setdefault("BLUESTAR_TZ_AUTO", "0")
os.environ.setdefault("BLUESTAR_DISPLAY_TZ", "Africa/Casablanca")
# ────────────────────────────────────────────────────────────────────────────

from calendar_core import (  # noqa: E402
    SelectionPolicy,
    SourceInfo,
    build_payload,
    comparable_high_hash,
    to_legacy_payload,
)

FIXTURE = ROOT / "tests" / "fixtures" / "ff_thisweek.json"
OUT = ROOT / "tests" / "fixtures" / "golden.json"

# Instant FIGE : le golden ne doit pas dependre de l'horloge de la machine.
NOW = datetime(2026, 10, 15, 12, 0, 0, tzinfo=timezone.utc)

# TZ d'affichage FIGE et EXPLICITE.
POLICY = SelectionPolicy(display_timezone="Africa/Casablanca")


def build(now=None, policy=None):
    """Construit (payload, legacy) depuis la fixture, de facon deterministe."""
    now = now or NOW
    policy = policy or POLICY
    raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
    source = SourceInfo(
        provider="Forex Factory / Fair Economy weekly feed",
        url="https://nfs.faireconomy.media/ff_calendar_thisweek.json",
        fetched_at_utc=now,
        payload_sha256="sha256:fixture",
        supports_actual=any("actual" in r for r in raw),
        feed_status={"thisweek": "ok", "nextweek": "absent_404"},
    )
    payload = build_payload(raw, source=source, now_utc=now, policy=policy)
    legacy = to_legacy_payload(payload, now)
    return payload, legacy


def main() -> int:
    payload, legacy = build()
    golden = {
        "purpose": "Non-regression inter-apps : ces hashes ne doivent JAMAIS bouger.",
        "fixture": "tests/fixtures/ff_thisweek.json",
        "now_utc": NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "policy_display_timezone": POLICY.display_timezone,
        "content_hash": payload.content_hash,
        "parity_hash": payload.parity_hash,
        "parity_hash_method": payload.parity_hash_method,
        "parity_window_anchor_utc": payload.parity_window_anchor_utc,
        "parity_scope_event_count": payload.parity_scope_event_count,
        "high_content_hash": comparable_high_hash(payload.events),
        "quality_status": payload.quality.status.value,
        "data_quality_score": payload.quality.data_quality_score,
        "accepted_event_count": payload.quality.accepted_event_count,
        "warnings": list(payload.quality.warnings),
        "legacy_display_timezone": legacy["metadata"].get("display_timezone"),
        "legacy_display_tz_origin": legacy["metadata"].get("display_tz_origin"),
        "legacy": legacy,
    }
    OUT.write_text(json.dumps(golden, indent=2, ensure_ascii=False) + "\n",
                   encoding="utf-8")
    print("golden written ->", OUT)
    print("  content_hash     :", golden["content_hash"])
    print("  parity_hash      :", golden["parity_hash"])
    print("  high_content_hash:", golden["high_content_hash"])
    print("  status/score     :", golden["quality_status"], golden["data_quality_score"])
    print("  accepted events  :", golden["accepted_event_count"])
    print("  warnings         :", golden["warnings"])
    print("  legacy tz        :", golden["legacy_display_timezone"],
          "/", golden["legacy_display_tz_origin"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
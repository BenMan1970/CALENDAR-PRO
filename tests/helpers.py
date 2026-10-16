"""Outillage partage : horloge injectable + session HTTP mockee.

AUCUNE requete reelle n'est jamais emise. ``CountingSession`` remplace
requests.Session et compte les appels : c'est la mesure qui prouve les
corrections du point 2.2 (une seule requete par fenetre d'espacement, zero
pendant un cooldown).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pytest

from tests.fixture import load_fixture

UTC = timezone.utc


# ─────────────────────────────────────────────────────────────────────────────
# HORLOGE INJECTABLE
# ─────────────────────────────────────────────────────────────────────────────
class FakeClock:
    """Horloge controlee : ``time()`` et ``datetime.now(UTC)`` suivent la meme
    avancee, pour que les tests puissent faire passer des cooldowns et des
    fenetres d'espacement sans dormir."""

    def __init__(self, start: Optional[datetime] = None):
        self._now = start or datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC)

    def time(self) -> float:
        return self._now.timestamp()

    def monotonic(self) -> float:
        return self._now.timestamp()

    def now(self, tz=None) -> datetime:
        return self._now if tz is None else self._now.astimezone(tz)

    def advance(self, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)

    def set(self, when: datetime) -> None:
        self._now = when


# ─────────────────────────────────────────────────────────────────────────────
# SESSION HTTP MOCKEE
# ─────────────────────────────────────────────────────────────────────────────
class FakeResponse:
    def __init__(self, status_code: int, body: bytes,
                 headers: Optional[Dict[str, str]] = None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def iter_content(self, chunk_size: int = 65536):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i:i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@dataclass
class CountingSession:
    """Session factice : enregistre CHAQUE appel et rejoue un scenario."""

    calls: List[str] = field(default_factory=list)
    plan: Dict[str, Any] = field(default_factory=dict)
    fixture_raw: Optional[List[dict]] = None

    def get(self, url: str, **kwargs):
        self.calls.append(url)
        status = self.plan.get("status", 200)
        if callable(status):
            status = status(url, len(self.calls))
        if status == 429:
            return FakeResponse(429, b"{}", {"Retry-After": str(
                self.plan.get("retry_after", 300))})
        body = self.plan.get("body")
        if body is None:
            raw = self.fixture_raw if self.fixture_raw is not None else load_fixture()
            body = json.dumps(raw).encode("utf-8")
        return FakeResponse(status, body, {"Content-Type": "application/json"})

    def close(self) -> None:
        pass

    @property
    def n_calls(self) -> int:
        return len(self.calls)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def session() -> CountingSession:
    return CountingSession()


@pytest.fixture
def data_dir(tmp_path):
    d = tmp_path / "data"
    d.mkdir(parents=True, exist_ok=True)
    return d
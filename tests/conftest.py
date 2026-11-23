from __future__ import annotations

from collections.abc import Sequence

import pytest

from parallel_ops.models import WorkerKind, WorkItem
from parallel_ops.resilience.errors import PermanentError, WorkerError
from parallel_ops.workers.base import Worker


class FakeClock:
    """
    Manually advanced monotonic clock.

    Every timing assertion in this suite is exact because of this. Sleeping for real
    makes tests slow and flaky, and asserting `0.19 < waited < 0.21` hides off-by-one
    errors in backoff arithmetic that an exact check catches.
    """

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeSleep:
    """
    Sleep replacement that advances a FakeClock instead of waiting.

    Recording every requested delay means a test can assert the *shape* of a backoff
    sequence, not just that some waiting happened.
    """

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        self.clock.advance(seconds)

    @property
    def total(self) -> float:
        return sum(self.calls)


class ScriptedWorker(Worker):
    """
    Worker that replays a fixed script, one entry per physical call.

    An entry is either a dict to return or an exception to raise. Running off the end of
    the script raises, which turns "the code called more times than the test expected"
    into a failure rather than silent success.
    """

    kind = WorkerKind.API

    def __init__(self, script: Sequence[object], *, kind: WorkerKind = WorkerKind.API) -> None:
        self.script = list(script)
        self.calls: list[str] = []
        self.kind = kind
        self.closed = False

    async def execute(self, item: WorkItem) -> dict:
        index = len(self.calls)
        self.calls.append(item.item_id)
        if index >= len(self.script):
            raise AssertionError(
                f"ScriptedWorker called {index + 1} times but the script has "
                f"{len(self.script)} entries"
            )
        entry = self.script[index]
        if isinstance(entry, Exception):
            raise entry
        if isinstance(entry, dict):
            return entry
        raise AssertionError(f"script entries must be dict or Exception, got {type(entry)}")

    @property
    def call_count(self) -> int:
        return len(self.calls)

    async def aclose(self) -> None:
        self.closed = True


class AlwaysFailWorker(Worker):
    """Fails every call with a retryable error. Counts calls."""

    kind = WorkerKind.API

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error or WorkerError("HTTP 503")
        self.call_count = 0

    async def execute(self, item: WorkItem) -> dict:
        self.call_count += 1
        raise self.error


class AlwaysOkWorker(Worker):
    """Returns a valid catalog record for every call."""

    kind = WorkerKind.API

    def __init__(self, payload: dict | None = None) -> None:
        self.payload = payload
        self.call_count = 0

    async def execute(self, item: WorkItem) -> dict:
        self.call_count += 1
        if self.payload is not None:
            return dict(self.payload)
        return {
            "product_id": item.item_id,
            "status": "active",
            "region": "us",
            "updated_at": "2026-01-01",
        }


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def sleeper(clock: FakeClock) -> FakeSleep:
    return FakeSleep(clock)


@pytest.fixture
def no_jitter():
    """rng that returns 0.5, which makes proportional jitter exactly zero."""
    return lambda: 0.5


@pytest.fixture
def item() -> WorkItem:
    return WorkItem(item_id="T-1", host="https://catalog.example.com", path="items/SKU-1")


def make_items(count: int, host: str = "https://catalog.example.com") -> list[WorkItem]:
    return [
        WorkItem(item_id=f"T-{n}", host=host, path=f"items/SKU-{n}") for n in range(1, count + 1)
    ]


__all__ = [
    "AlwaysFailWorker",
    "AlwaysOkWorker",
    "FakeClock",
    "FakeSleep",
    "PermanentError",
    "ScriptedWorker",
    "WorkerError",
    "make_items",
]

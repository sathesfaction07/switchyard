from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class WorkerKind(str, Enum):
    """Which executor handles an item. Cheap API call or expensive browser visit."""

    API = "api"
    BROWSER = "browser"


class Outcome(str, Enum):
    """
    Terminal state of one work item.

    The distinction between FAILED and REJECTED is load-bearing: a FAILED item was
    genuinely attempted and the dependency did not satisfy it, while a REJECTED item
    was never sent because the circuit was open. Rejected work is safe to re-run once
    the dependency recovers; failed work needs a human to look at it.
    """

    OK = "ok"
    INVALID = "invalid"
    FAILED = "failed"
    REJECTED = "rejected"


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class WorkItem(BaseModel):
    """One unit of work. `host` is the key for the rate limiter and circuit breaker."""

    item_id: str
    host: str
    kind: WorkerKind = WorkerKind.API
    path: str = ""
    extras: dict[str, str] = Field(default_factory=dict)

    def target(self) -> str:
        if not self.path:
            return self.host
        return f"{self.host.rstrip('/')}/{self.path.lstrip('/')}"


class WorkResult(BaseModel):
    """
    Outcome of one item, carrying enough detail to explain itself without the logs.

    `attempts` counts physical calls made. `breaker_state` is the state observed when
    the item was admitted, which is how you tell a REJECTED item from a FAILED one
    after the fact.
    """

    item_id: str
    host: str
    kind: WorkerKind
    outcome: Outcome
    attempts: int = 0
    waited_s: float = 0.0
    duration_s: float = 0.0
    breaker_state: BreakerState = BreakerState.CLOSED
    data: dict | None = None
    error: str = ""

    @property
    def succeeded(self) -> bool:
        return self.outcome is Outcome.OK


class HostReport(BaseModel):
    """Per-dependency rollup. Trips and rejects are the interesting columns."""

    host: str
    total: int = 0
    ok: int = 0
    invalid: int = 0
    failed: int = 0
    rejected: int = 0
    attempts: int = 0
    waited_s: float = 0.0
    trips: int = 0
    final_state: BreakerState = BreakerState.CLOSED


class BatchReport(BaseModel):
    total: int = 0
    ok: int = 0
    invalid: int = 0
    failed: int = 0
    rejected: int = 0
    attempts: int = 0
    waited_s: float = 0.0
    duration_s: float = 0.0
    hosts: list[HostReport] = Field(default_factory=list)

    @property
    def amplification(self) -> float:
        """
        Physical calls per item. Above 1.0 means retries are firing.

        Worth watching next to `rejected`: high amplification with rising rejections
        means retries are feeding the breaker rather than riding out a blip.
        """
        if self.total == 0:
            return 0.0
        return self.attempts / self.total


ROW_FIELDS = (
    "item_id",
    "host",
    "kind",
    "outcome",
    "attempts",
    "waited_s",
    "duration_s",
    "breaker_state",
    "error",
    "data",
)


def as_row(result: WorkResult) -> dict[str, object]:
    """Flat dict used by every writer, so CSV, JSON and Excel cannot drift apart."""
    return {
        "item_id": result.item_id,
        "host": result.host,
        "kind": result.kind.value,
        "outcome": result.outcome.value,
        "attempts": result.attempts,
        "waited_s": round(result.waited_s, 3),
        "duration_s": round(result.duration_s, 3),
        "breaker_state": result.breaker_state.value,
        "error": result.error,
        "data": "" if result.data is None else str(result.data),
    }

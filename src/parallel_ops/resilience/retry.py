from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .errors import CircuitOpenError, PermanentError, SchemaError

NON_RETRYABLE: tuple[type[Exception], ...] = (CircuitOpenError, PermanentError, SchemaError)


@dataclass(frozen=True)
class RetryPolicy:
    """
    Exponential backoff with proportional jitter.

    Jitter is not decoration. A batch that fans out 200 items against one host will see
    them fail at very nearly the same instant when that host goes down, and without
    jitter every one of them waits the identical backoff and retries in the same
    millisecond. The retry storm is then perfectly synchronised, which is the worst
    possible shape for a dependency that is trying to recover.
    """

    attempts: int = 3
    base_delay: float = 0.2
    max_delay: float = 5.0
    jitter: float = 0.1

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError(f"attempts must be >= 1, got {self.attempts}")
        if self.base_delay < 0:
            raise ValueError(f"base_delay must be >= 0, got {self.base_delay}")
        if self.max_delay < self.base_delay:
            raise ValueError(
                f"max_delay ({self.max_delay}) must be >= base_delay ({self.base_delay})"
            )
        if not 0.0 <= self.jitter <= 1.0:
            raise ValueError(f"jitter must be within 0..1, got {self.jitter}")

    def delay_for(self, attempt: int, *, rng: Callable[[], float] = random.random) -> float:
        """
        Delay to wait *after* attempt number `attempt` (1-based), before the next one.

        Doubling is capped at `max_delay` before jitter is applied, so the cap bounds
        the base rather than the jittered value and the spread stays proportional at
        every step.
        """
        if attempt < 1:
            raise ValueError(f"attempt must be >= 1, got {attempt}")
        base = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
        if self.jitter == 0.0:
            return base
        spread = self.jitter * (2.0 * rng() - 1.0)
        return max(0.0, base * (1.0 + spread))


@dataclass
class RetryOutcome:
    """
    Result of one logical operation, however many physical calls it took.

    `attempts` is the number of calls actually made, which is what the breaker must
    *not* be told about — it wants one verdict per operation. `delays` is kept so tests
    can assert the backoff shape without patching the clock.
    """

    value: Any = None
    attempts: int = 0
    slept_s: float = 0.0
    error: Exception | None = None
    delays: list[float] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return self.error is None


async def call_with_retry(
    fn: Callable[[], Awaitable[Any]],
    policy: RetryPolicy,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    rng: Callable[[], float] = random.random,
    non_retryable: tuple[type[Exception], ...] = NON_RETRYABLE,
) -> RetryOutcome:
    """
    Call `fn` until it succeeds or the policy is exhausted.

    This function deliberately knows nothing about the circuit breaker. Composition is
    the caller's job, in `core/executor.py`, and keeping the two apart is what makes it
    possible to state the ordering rule in one readable place instead of having it
    emerge from two classes calling each other.

    Errors in `non_retryable` abort immediately with the attempt count that had been
    reached. `SchemaError` is in that set because a malformed response will be malformed
    again, `PermanentError` because a 404 stays a 404, and `CircuitOpenError` because
    retrying into an open breaker actively undermines it.
    """
    outcome = RetryOutcome()

    for attempt in range(1, policy.attempts + 1):
        outcome.attempts = attempt
        try:
            outcome.value = await fn()
            outcome.error = None
            return outcome
        except non_retryable as exc:
            outcome.error = exc
            return outcome
        except Exception as exc:  # noqa: BLE001 - deliberate: any worker failure is retryable
            outcome.error = exc
            if attempt >= policy.attempts:
                return outcome
            delay = policy.delay_for(attempt, rng=rng)
            outcome.delays.append(delay)
            outcome.slept_s += delay
            await sleep(delay)

    return outcome


def monotonic() -> float:
    """Indirection so the whole package shares one clock source that tests can replace."""
    return time.monotonic()

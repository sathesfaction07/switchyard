from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from ..models import BreakerState


class CircuitBreaker:
    """
    Three-state circuit breaker, one per host.

    CLOSED     traffic flows; consecutive failures accumulate
    OPEN       traffic is refused outright until `reset_timeout` has elapsed
    HALF_OPEN  a single probe is admitted to test whether the dependency recovered

    Two details in here are the reason this class exists rather than a counter and an
    `if`.

    **Half-open admits exactly one probe.** The obvious implementation flips to
    half-open when the timer expires and lets waiting work through. With a batch, that
    means the entire backlog arrives at a dependency that has just come back up, which
    knocks it over again and re-trips the breaker. The recovery probe has to be a single
    request, and everything else has to keep being refused until that one request
    reports back. `_probe_in_flight` is what enforces it.

    **One outcome per logical operation, not per attempt.** `record_failure` is called
    once per work item after its retries are exhausted — never once per retry. See the
    note on `record_failure` for what went wrong when it was the other way round.

    `clock` is injected so tests can step time exactly rather than sleeping.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: float = 30.0,
        success_threshold: int = 1,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError(f"failure_threshold must be >= 1, got {failure_threshold}")
        if reset_timeout <= 0:
            raise ValueError(f"reset_timeout must be positive, got {reset_timeout}")
        if success_threshold < 1:
            raise ValueError(f"success_threshold must be >= 1, got {success_threshold}")

        self.failure_threshold = failure_threshold
        self.reset_timeout = float(reset_timeout)
        self.success_threshold = success_threshold
        self._clock = clock

        self._state = BreakerState.CLOSED
        self._failures = 0
        self._successes = 0
        self._opened_at: float | None = None
        self._probe_in_flight = False
        self._lock = asyncio.Lock()

        self.trips = 0
        self.rejections = 0

    @property
    def state(self) -> BreakerState:
        """
        Effective state without mutating anything.

        Reports HALF_OPEN once an OPEN breaker's timeout has elapsed, so a status
        display agrees with what `admit()` would do next. `admit()` performs the actual
        transition under the lock.
        """
        if self._state is BreakerState.OPEN and self._open_elapsed() >= self.reset_timeout:
            return BreakerState.HALF_OPEN
        return self._state

    @property
    def failures(self) -> int:
        return self._failures

    def _open_elapsed(self) -> float:
        if self._opened_at is None:
            return 0.0
        return max(0.0, self._clock() - self._opened_at)

    def _trip(self) -> None:
        self._state = BreakerState.OPEN
        self._opened_at = self._clock()
        self._failures = 0
        self._successes = 0
        self._probe_in_flight = False
        self.trips += 1

    def _close(self) -> None:
        self._state = BreakerState.CLOSED
        self._failures = 0
        self._successes = 0
        self._opened_at = None
        self._probe_in_flight = False

    async def admit(self) -> tuple[bool, BreakerState]:
        """
        Ask permission to make one call.

        Returns `(admitted, state_observed)`. The observed state is returned rather than
        read separately because by the time a caller looked again it could have changed,
        and the result row wants to record the state this specific item actually saw.
        """
        async with self._lock:
            if self._state is BreakerState.OPEN and self._open_elapsed() >= self.reset_timeout:
                self._state = BreakerState.HALF_OPEN
                self._successes = 0
                self._probe_in_flight = False

            observed = self._state

            if observed is BreakerState.CLOSED:
                return True, observed

            if observed is BreakerState.OPEN:
                self.rejections += 1
                return False, observed

            if self._probe_in_flight:
                self.rejections += 1
                return False, observed

            self._probe_in_flight = True
            return True, observed

    async def record_success(self) -> None:
        """Report that one logical operation succeeded."""
        async with self._lock:
            if self._state is BreakerState.HALF_OPEN:
                self._probe_in_flight = False
                self._successes += 1
                if self._successes >= self.success_threshold:
                    self._close()
                return
            self._failures = 0

    async def record_failure(self) -> None:
        """
        Report that one logical operation failed, after its retries are exhausted.

        Calling this per physical attempt instead of per operation is the bug that took
        the longest to find here. With a 3-attempt retry policy and a 5-failure
        threshold, two bad items produce six recorded failures and trip the breaker —
        so the breaker ends up measuring the retry policy rather than the dependency's
        health, and tightening retries silently loosens the breaker. Worse, the two
        settings are usually owned by different people.

        A failure while HALF_OPEN re-opens immediately and restarts the timer. One
        failed probe is sufficient evidence that the dependency has not recovered;
        waiting for `failure_threshold` probes would send exactly the burst that
        half-open exists to prevent.
        """
        async with self._lock:
            if self._state is BreakerState.HALF_OPEN:
                self._trip()
                return
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._trip()

    def snapshot(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "failures": self._failures,
            "successes": self._successes,
            "trips": self.trips,
            "rejections": self.rejections,
            "failure_threshold": self.failure_threshold,
            "reset_timeout": self.reset_timeout,
        }


class GuardRegistry:
    """
    Lazily creates and holds one breaker and one limiter per host.

    Per-host rather than global, because a global breaker means a single sick
    dependency halts work against every healthy one. The batch would report itself as
    broken when in fact one of three hosts was down.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        reset_timeout: float = 30.0,
        success_threshold: int = 1,
        rate: float = 10.0,
        burst: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], object] | None = None,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._success_threshold = success_threshold
        self._rate = rate
        self._burst = burst
        self._clock = clock
        self._sleep = sleep
        self._breakers: dict[str, CircuitBreaker] = {}
        self._limiters: dict[str, object] = {}

    def breaker(self, host: str) -> CircuitBreaker:
        if host not in self._breakers:
            self._breakers[host] = CircuitBreaker(
                failure_threshold=self._failure_threshold,
                reset_timeout=self._reset_timeout,
                success_threshold=self._success_threshold,
                clock=self._clock,
            )
        return self._breakers[host]

    def limiter(self, host: str):
        from .limiter import TokenBucket

        if host not in self._limiters:
            kwargs: dict[str, object] = {"clock": self._clock}
            if self._sleep is not None:
                kwargs["sleep"] = self._sleep
            self._limiters[host] = TokenBucket(self._rate, self._burst, **kwargs)  # type: ignore[arg-type]
        return self._limiters[host]

    def hosts(self) -> list[str]:
        return sorted(set(self._breakers) | set(self._limiters))

    def snapshot(self) -> dict[str, dict[str, object]]:
        out: dict[str, dict[str, object]] = {}
        for host in self.hosts():
            entry: dict[str, object] = {}
            if host in self._breakers:
                entry["breaker"] = self._breakers[host].snapshot()
            if host in self._limiters:
                entry["limiter"] = self._limiters[host].snapshot()  # type: ignore[attr-defined]
            out[host] = entry
        return out

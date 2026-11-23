from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

# Tokens are a continuous quantity derived from a float clock, so an exact `>=` comparison
# is not a safe loop condition. See the note in `acquire`.
TOKEN_EPSILON = 1e-9


class TokenBucket:
    """
    Token bucket rate limiter, one per host.

    Tokens accrue at `rate` per second up to `burst`, and every call spends one. When
    the bucket is empty a caller waits exactly as long as it takes to accrue what it
    needs, so a batch settles into a steady `rate` rather than firing everything at
    once and relying on the dependency to push back.

    The lock is deliberately held across the wait. That serialises waiters into FIFO
    order, which costs a little parallelism at the limiter but means an unlucky task
    cannot be starved indefinitely while newer tasks keep taking tokens. For a client
    trying not to exceed someone else's quota, predictable ordering is worth more than
    squeezing the last few percent of throughput.

    `clock` and `sleep` are injected so tests can drive time directly instead of
    sleeping. Every timing assertion in the suite is exact for that reason.
    """

    def __init__(
        self,
        rate: float,
        burst: float | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if rate <= 0:
            raise ValueError(f"rate must be positive, got {rate}")
        resolved_burst = max(1.0, rate) if burst is None else burst
        if resolved_burst <= 0:
            raise ValueError(f"burst must be positive, got {resolved_burst}")

        self.rate = float(rate)
        self.burst = float(resolved_burst)
        self._clock = clock
        self._sleep = sleep
        self._tokens = self.burst
        self._updated = clock()
        self._lock = asyncio.Lock()
        self.granted = 0
        self.total_waited = 0.0

    @property
    def tokens(self) -> float:
        """Currently available tokens, refilled to now. Read-only, does not spend."""
        now = self._clock()
        elapsed = max(0.0, now - self._updated)
        return min(self.burst, self._tokens + elapsed * self.rate)

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._updated
        if elapsed > 0:
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._updated = now

    async def acquire(self, tokens: float = 1.0) -> float:
        """
        Spend `tokens`, waiting if necessary. Returns how long this caller waited.

        The returned wait is reported per item rather than only aggregated, because
        "the batch took 40 seconds" and "the batch spent 38 of those seconds waiting on
        our own limiter" are very different findings and only the second one tells you
        the rate is set too low.

        **One sleep, not a loop.** The lock is held for the whole method, so nothing can
        take a token while this caller waits. The wait is therefore uncontended and can
        be computed exactly once: sleep for the deficit divided by the rate, and by the
        time that returns the tokens have accrued.

        This replaced a `while True` that re-checked `self._tokens >= tokens` after each
        sleep, which was defending against a race the lock already prevents and could
        spin forever. Tokens come from `elapsed * rate` on a float clock, so the exact
        comparison would sometimes land a few parts in 1e13 short of the target. The
        loop would then ask for a corrective sleep of ~5e-14 seconds -- smaller than the
        float64 spacing at a clock value of 1000, so the clock could not represent it,
        `elapsed` came back as zero, no tokens accrued, and the next delay was identical.
        A real clock escapes because time passes independently, but it burns a hot spin
        of event-loop round trips to do it, and any deterministic clock hangs outright.

        `TOKEN_EPSILON` covers the same float slack on the near-satisfied case: a deficit
        of 1e-13 tokens means the caller has effectively been paid and should proceed.
        """
        if tokens <= 0:
            raise ValueError(f"tokens must be positive, got {tokens}")
        if tokens > self.burst:
            raise ValueError(
                f"cannot acquire {tokens} tokens from a bucket with burst {self.burst}; "
                "this would block forever"
            )

        async with self._lock:
            self._refill()
            waited = 0.0
            deficit = tokens - self._tokens

            if deficit > TOKEN_EPSILON:
                waited = deficit / self.rate
                await self._sleep(waited)
                self._refill()

            # Clamped because the accrual above can land a hair under `tokens`, and a
            # negative balance would quietly overcharge the next caller.
            self._tokens = max(0.0, self._tokens - tokens)
            self.granted += 1
            self.total_waited += waited
            return waited

    def snapshot(self) -> dict[str, float]:
        return {
            "rate": self.rate,
            "burst": self.burst,
            "tokens": round(self.tokens, 3),
            "granted": float(self.granted),
            "total_waited": round(self.total_waited, 3),
        }

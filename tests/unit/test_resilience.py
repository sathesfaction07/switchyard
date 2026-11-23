from __future__ import annotations

import asyncio

import pytest

from parallel_ops.models import BreakerState
from parallel_ops.resilience.breaker import CircuitBreaker, GuardRegistry
from parallel_ops.resilience.limiter import TokenBucket

from ..conftest import FakeClock, FakeSleep


class TestTokenBucketBasics:
    def test_starts_full_so_a_small_batch_never_waits(self, clock, sleeper):
        bucket = TokenBucket(10, clock=clock, sleep=sleeper)
        assert bucket.tokens == 10.0

    async def test_spending_within_burst_costs_no_wait(self, clock, sleeper):
        bucket = TokenBucket(10, clock=clock, sleep=sleeper)
        waits = [await bucket.acquire() for _ in range(10)]
        assert waits == [0.0] * 10
        assert sleeper.calls == []

    async def test_eleventh_call_waits_exactly_one_interval(self, clock, sleeper):
        bucket = TokenBucket(10, clock=clock, sleep=sleeper)
        for _ in range(10):
            await bucket.acquire()
        waited = await bucket.acquire()
        assert waited == pytest.approx(0.1)

    async def test_tokens_refill_over_time(self, clock, sleeper):
        bucket = TokenBucket(10, clock=clock, sleep=sleeper)
        for _ in range(10):
            await bucket.acquire()
        assert bucket.tokens == pytest.approx(0.0)
        clock.advance(0.5)
        assert bucket.tokens == pytest.approx(5.0)

    async def test_refill_is_capped_at_burst(self, clock, sleeper):
        bucket = TokenBucket(10, burst=10, clock=clock, sleep=sleeper)
        await bucket.acquire()
        clock.advance(1_000)
        assert bucket.tokens == 10.0

    async def test_sustained_rate_matches_configuration(self, clock, sleeper):
        """30 calls at 10/s from a burst of 10 should take 2s of waiting, not 3."""
        bucket = TokenBucket(10, burst=10, clock=clock, sleep=sleeper)
        for _ in range(30):
            await bucket.acquire()
        assert sleeper.total == pytest.approx(2.0, abs=1e-6)

    async def test_reports_cumulative_wait_for_diagnosis(self, clock, sleeper):
        bucket = TokenBucket(5, burst=1, clock=clock, sleep=sleeper)
        for _ in range(4):
            await bucket.acquire()
        assert bucket.granted == 4
        assert bucket.total_waited == pytest.approx(0.6)


class TestTokenBucketValidation:
    def test_rejects_non_positive_rate(self, clock):
        with pytest.raises(ValueError, match="rate must be positive"):
            TokenBucket(0, clock=clock)

    def test_rejects_non_positive_burst(self, clock):
        with pytest.raises(ValueError, match="burst must be positive"):
            TokenBucket(10, burst=0, clock=clock)

    async def test_rejects_a_request_larger_than_burst_instead_of_hanging(self, clock, sleeper):
        """
        A request bigger than the bucket can ever hold would block forever.

        Failing loudly beats a batch that appears to hang with no explanation.
        """
        bucket = TokenBucket(10, burst=5, clock=clock, sleep=sleeper)
        with pytest.raises(ValueError, match="would block forever"):
            await bucket.acquire(6)

    async def test_rejects_non_positive_token_request(self, clock, sleeper):
        bucket = TokenBucket(10, clock=clock, sleep=sleeper)
        with pytest.raises(ValueError, match="tokens must be positive"):
            await bucket.acquire(0)

    def test_burst_defaults_to_rate(self, clock):
        assert TokenBucket(7, clock=clock).burst == 7.0

    def test_burst_floor_is_one_for_sub_unit_rates(self, clock):
        """At 0.5/s a burst of 0.5 could never satisfy a single call."""
        assert TokenBucket(0.5, clock=clock).burst == 1.0


class TestTokenBucketFairness:
    async def test_waiters_are_served_in_arrival_order(self, clock, sleeper):
        """
        FIFO ordering is why the lock is held across the wait.

        Without it a late arrival can take a token that an earlier waiter was queued for,
        and on a long batch some item waits indefinitely.
        """
        bucket = TokenBucket(10, burst=1, clock=clock, sleep=sleeper)
        order: list[int] = []

        async def contend(n: int) -> None:
            await bucket.acquire()
            order.append(n)

        await asyncio.gather(*(contend(n) for n in range(5)))
        assert order == [0, 1, 2, 3, 4]


class TestBreakerStates:
    def test_starts_closed(self, clock):
        assert CircuitBreaker(clock=clock).state is BreakerState.CLOSED

    async def test_admits_while_closed(self, clock):
        breaker = CircuitBreaker(clock=clock)
        admitted, state = await breaker.admit()
        assert admitted is True
        assert state is BreakerState.CLOSED

    async def test_opens_at_the_threshold_not_before(self, clock):
        breaker = CircuitBreaker(failure_threshold=3, clock=clock)
        await breaker.record_failure()
        await breaker.record_failure()
        assert breaker.state is BreakerState.CLOSED
        await breaker.record_failure()
        assert breaker.state is BreakerState.OPEN
        assert breaker.trips == 1

    async def test_refuses_while_open(self, clock):
        breaker = CircuitBreaker(failure_threshold=1, clock=clock)
        await breaker.record_failure()
        admitted, state = await breaker.admit()
        assert admitted is False
        assert state is BreakerState.OPEN
        assert breaker.rejections == 1

    async def test_a_success_resets_the_failure_run(self, clock):
        """
        Consecutive failures, not cumulative. A dependency that fails, recovers and
        fails again is not the same as one that is failing steadily.
        """
        breaker = CircuitBreaker(failure_threshold=3, clock=clock)
        await breaker.record_failure()
        await breaker.record_failure()
        await breaker.record_success()
        assert breaker.failures == 0
        await breaker.record_failure()
        await breaker.record_failure()
        assert breaker.state is BreakerState.CLOSED

    async def test_becomes_half_open_after_the_reset_timeout(self, clock):
        breaker = CircuitBreaker(failure_threshold=1, reset_timeout=30, clock=clock)
        await breaker.record_failure()
        clock.advance(29.9)
        assert breaker.state is BreakerState.OPEN
        clock.advance(0.2)
        assert breaker.state is BreakerState.HALF_OPEN

    async def test_a_successful_probe_closes_the_circuit(self, clock):
        breaker = CircuitBreaker(failure_threshold=1, reset_timeout=30, clock=clock)
        await breaker.record_failure()
        clock.advance(31)
        admitted, state = await breaker.admit()
        assert admitted is True
        assert state is BreakerState.HALF_OPEN
        await breaker.record_success()
        assert breaker.state is BreakerState.CLOSED

    async def test_success_threshold_above_one_requires_several_probes(self, clock):
        breaker = CircuitBreaker(
            failure_threshold=1, reset_timeout=10, success_threshold=2, clock=clock
        )
        await breaker.record_failure()
        clock.advance(11)

        admitted, _ = await breaker.admit()
        assert admitted is True
        await breaker.record_success()
        assert breaker.state is BreakerState.HALF_OPEN

        admitted, _ = await breaker.admit()
        assert admitted is True
        await breaker.record_success()
        assert breaker.state is BreakerState.CLOSED


class TestHalfOpenStampede:
    """
    The reason `_probe_in_flight` exists.

    Letting a whole backlog through the moment the timer expires is the classic circuit
    breaker bug: the dependency that just came back up is handed the entire queue and
    falls over again.
    """

    async def test_half_open_admits_exactly_one_probe(self, clock):
        breaker = CircuitBreaker(failure_threshold=1, reset_timeout=10, clock=clock)
        await breaker.record_failure()
        clock.advance(11)

        first, first_state = await breaker.admit()
        assert first is True
        assert first_state is BreakerState.HALF_OPEN

        for _ in range(20):
            admitted, state = await breaker.admit()
            assert admitted is False, "half-open admitted a second concurrent probe"
            assert state is BreakerState.HALF_OPEN

    async def test_concurrent_admits_while_half_open_yield_one_winner(self, clock):
        breaker = CircuitBreaker(failure_threshold=1, reset_timeout=10, clock=clock)
        await breaker.record_failure()
        clock.advance(11)

        outcomes = await asyncio.gather(*(breaker.admit() for _ in range(50)))
        admitted = [ok for ok, _ in outcomes if ok]
        assert len(admitted) == 1, f"expected exactly one probe, got {len(admitted)}"

    async def test_a_failed_probe_reopens_immediately_and_restarts_the_timer(self, clock):
        breaker = CircuitBreaker(failure_threshold=5, reset_timeout=10, clock=clock)
        for _ in range(5):
            await breaker.record_failure()
        assert breaker.trips == 1

        clock.advance(11)
        admitted, _ = await breaker.admit()
        assert admitted is True

        await breaker.record_failure()
        assert breaker.state is BreakerState.OPEN, "one bad probe must re-open the circuit"
        assert breaker.trips == 2

        clock.advance(9)
        assert breaker.state is BreakerState.OPEN, "the reset timer should have restarted"
        clock.advance(2)
        assert breaker.state is BreakerState.HALF_OPEN

    async def test_probe_slot_is_released_after_a_failure(self, clock):
        """A failed probe must not leave the slot stuck, or recovery deadlocks."""
        breaker = CircuitBreaker(failure_threshold=1, reset_timeout=10, clock=clock)
        await breaker.record_failure()

        clock.advance(11)
        await breaker.admit()
        await breaker.record_failure()

        clock.advance(11)
        admitted, _ = await breaker.admit()
        assert admitted is True, "a later probe was refused, so recovery is impossible"


class TestBreakerValidation:
    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"failure_threshold": 0}, "failure_threshold must be >= 1"),
            ({"reset_timeout": 0}, "reset_timeout must be positive"),
            ({"success_threshold": 0}, "success_threshold must be >= 1"),
        ],
    )
    def test_rejects_nonsense_configuration(self, kwargs, match, clock):
        with pytest.raises(ValueError, match=match):
            CircuitBreaker(clock=clock, **kwargs)

    def test_snapshot_exposes_the_fields_a_report_needs(self, clock):
        snap = CircuitBreaker(failure_threshold=4, reset_timeout=12, clock=clock).snapshot()
        assert snap["state"] == "closed"
        assert snap["failure_threshold"] == 4
        assert snap["reset_timeout"] == 12.0
        assert snap["trips"] == 0


class TestGuardRegistry:
    def test_returns_the_same_breaker_for_a_host(self, clock):
        registry = GuardRegistry(clock=clock)
        assert registry.breaker("a.example.com") is registry.breaker("a.example.com")

    def test_hosts_get_distinct_breaker_instances(self, clock):
        registry = GuardRegistry(failure_threshold=1, clock=clock)
        assert registry.breaker("a.example.com") is not registry.breaker("b.example.com")

    async def test_tripping_one_host_leaves_another_closed(self, clock):
        registry = GuardRegistry(failure_threshold=1, clock=clock)
        await registry.breaker("sick.example.com").record_failure()

        assert registry.breaker("sick.example.com").state is BreakerState.OPEN
        assert registry.breaker("healthy.example.com").state is BreakerState.CLOSED

    def test_limiters_are_also_per_host(self, clock):
        sleeper = FakeSleep(clock)
        registry = GuardRegistry(rate=5, clock=clock, sleep=sleeper)
        first = registry.limiter("a.example.com")
        assert first is registry.limiter("a.example.com")
        assert first is not registry.limiter("b.example.com")

    async def test_snapshot_covers_every_touched_host(self, clock):
        sleeper = FakeSleep(clock)
        registry = GuardRegistry(rate=5, clock=clock, sleep=sleeper)
        await registry.limiter("a.example.com").acquire()
        registry.breaker("b.example.com")

        snap = registry.snapshot()
        assert set(snap) == {"a.example.com", "b.example.com"}
        assert snap["a.example.com"]["limiter"]["granted"] == 1.0
        assert snap["b.example.com"]["breaker"]["state"] == "closed"


def test_fake_clock_is_wired_into_the_registry():
    """Guards against a regression where the registry built its own real clock."""
    clock = FakeClock(start=500.0)
    registry = GuardRegistry(clock=clock)
    breaker = registry.breaker("x.example.com")
    assert breaker._clock() == 500.0
    clock.advance(10)
    assert breaker._clock() == 510.0


class TestWhyOneSleepNotALoop:
    """
    Regression cover for a hang that only a deterministic clock could expose.

    `acquire` used to re-check `self._tokens >= tokens` in a `while True` after each
    sleep. Tokens accrue as `elapsed * rate` on a float clock, so that exact comparison
    would periodically land a few parts in 1e13 short. The loop then asked for a
    corrective sleep of ~5e-14s, which is below the float64 spacing at a clock value of
    1000 -- the clock could not represent it, no time passed, no tokens accrued, and the
    next iteration computed the identical delay. The suite hung on acquire #13.

    The fix was to delete the loop rather than widen the comparison. The lock is held
    for the whole method, so no other caller can take the tokens this one is waiting
    for; the wait is uncontended and one computed sleep is therefore exact.
    """

    async def test_thirty_acquires_terminate_and_bill_the_right_total(self, clock, sleeper):
        """The exact case that used to hang. 30 calls at 10/s from a burst of 10."""
        bucket = TokenBucket(10, burst=10, clock=clock, sleep=sleeper)
        for _ in range(30):
            await bucket.acquire()
        assert bucket.granted == 30
        assert sleeper.total == pytest.approx(2.0, abs=1e-6)

    async def test_a_contended_acquire_sleeps_exactly_once(self, clock, sleeper):
        """One sleep per wait is the invariant that makes the loop unnecessary."""
        bucket = TokenBucket(10, burst=1, clock=clock, sleep=sleeper)
        await bucket.acquire()
        sleeper.calls.clear()
        await bucket.acquire()
        assert len(sleeper.calls) == 1

    async def test_a_sub_epsilon_deficit_does_not_sleep_at_all(self, clock, sleeper):
        """
        A deficit of 1e-13 tokens is float noise, not backpressure.

        Treating it as a real shortfall is what generated the unrepresentable delay.
        """
        bucket = TokenBucket(10, burst=10, clock=clock, sleep=sleeper)
        for _ in range(10):
            await bucket.acquire()
        bucket._tokens = 1.0 - 1e-13

        waited = await bucket.acquire()

        assert waited == 0.0
        assert sleeper.calls == []

    async def test_terminates_even_when_the_clock_refuses_to_advance(self, clock):
        """
        A clock too coarse to represent the delay must not cause a spin.

        This is the property the old loop lacked. Sleeping without advancing time is
        exactly what a float clock does when asked to add a sub-ulp interval, and it is
        also what a coarse platform clock does for any very short sleep.
        """
        calls: list[float] = []

        async def frozen_sleep(seconds: float) -> None:
            calls.append(seconds)
            if len(calls) > 50:
                raise AssertionError(f"spun {len(calls)} times on a frozen clock")

        bucket = TokenBucket(10, burst=1, clock=clock, sleep=frozen_sleep)
        await bucket.acquire()

        waited = await asyncio.wait_for(bucket.acquire(), timeout=1.0)

        assert len(calls) == 1
        assert waited == pytest.approx(0.1)

    async def test_the_balance_never_goes_negative(self, clock, sleeper):
        """
        Accrual can land a hair under the requested amount, and an unclamped subtraction
        would leave a negative balance that silently overcharges the next caller.
        """
        bucket = TokenBucket(10, burst=10, clock=clock, sleep=sleeper)
        for _ in range(40):
            await bucket.acquire()
            assert bucket._tokens >= 0.0

    async def test_fifo_order_survives_the_rewrite(self, clock, sleeper):
        """The lock is still held across the wait, so arrival order is still honoured."""
        bucket = TokenBucket(10, burst=1, clock=clock, sleep=sleeper)
        order: list[int] = []

        async def contend(n: int) -> None:
            await bucket.acquire()
            order.append(n)

        await asyncio.gather(*(contend(n) for n in range(6)))
        assert order == [0, 1, 2, 3, 4, 5]

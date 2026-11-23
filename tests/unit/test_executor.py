from __future__ import annotations

import pytest

from parallel_ops.core.executor import ExecutionPolicy, counts_against_breaker, execute_item
from parallel_ops.core.schema import CatalogRecord
from parallel_ops.models import BreakerState, Outcome, WorkItem
from parallel_ops.resilience.breaker import CircuitBreaker
from parallel_ops.resilience.errors import (
    CircuitOpenError,
    PermanentError,
    SchemaError,
    WorkerError,
)
from parallel_ops.resilience.limiter import TokenBucket
from parallel_ops.resilience.retry import RetryPolicy, call_with_retry

from ..conftest import AlwaysFailWorker, AlwaysOkWorker, ScriptedWorker


@pytest.fixture
def policy():
    return ExecutionPolicy(retry=RetryPolicy(attempts=3, base_delay=0.2, jitter=0.0))


@pytest.fixture
def validating_policy():
    return ExecutionPolicy(
        retry=RetryPolicy(attempts=3, base_delay=0.2, jitter=0.0),
        schema=CatalogRecord,
    )


def guards(clock, sleeper, *, failure_threshold=5, reset_timeout=30.0, rate=100.0):
    breaker = CircuitBreaker(
        failure_threshold=failure_threshold, reset_timeout=reset_timeout, clock=clock
    )
    limiter = TokenBucket(rate, clock=clock, sleep=sleeper)
    return breaker, limiter


class TestWhichFailuresCountAgainstTheBreaker:
    """
    The classification that stops the breaker from measuring the wrong thing.

    A breaker is meant to answer "is this dependency unhealthy". Only errors that are
    evidence of that may count.
    """

    def test_worker_error_counts(self):
        assert counts_against_breaker(WorkerError("HTTP 503")) is True

    def test_permanent_error_does_not(self):
        """
        A 404 means the dependency answered correctly and fast. The problem is our input.

        Counting it meant a batch of stale product IDs reported the catalog as down.
        """
        assert counts_against_breaker(PermanentError("HTTP 404")) is False

    def test_schema_error_does_not(self):
        """
        A contract change makes every item fail. Tripping on that converts the run into
        a wall of `rejected` rows and destroys the field-level detail that explains what
        actually changed -- which was the entire reason for validating.
        """
        assert counts_against_breaker(SchemaError("CatalogRecord: status: bad")) is False

    def test_circuit_open_error_does_not(self):
        assert counts_against_breaker(CircuitOpenError("open")) is False

    def test_an_unclassified_exception_does_not(self):
        """A bug in a worker is not evidence about the dependency."""
        assert counts_against_breaker(RuntimeError("typo in the worker")) is False


class TestGuardOrdering:
    """
    Breaker before limiter, retries inside the breaker, validation last and outside.

    Each of these is a separate observable consequence, so each gets its own test.
    """

    async def test_an_open_circuit_costs_no_token(self, clock, sleeper, item, policy):
        """
        Checking the limiter first would spend a token on a call that never happens.

        On a fully rejected batch that is the entire run's quota, burned to achieve
        nothing.
        """
        breaker, limiter = guards(clock, sleeper, failure_threshold=1)
        await breaker.record_failure()
        worker = AlwaysOkWorker()

        result = await execute_item(item, worker, breaker, limiter, policy, sleep=sleeper)

        assert result.outcome is Outcome.REJECTED
        assert limiter.granted == 0, "a token was spent on a call that was never made"
        assert worker.call_count == 0
        assert result.attempts == 0
        assert "not attempted" in result.error

    async def test_a_rejected_item_records_the_state_it_observed(
        self, clock, sleeper, item, policy
    ):
        breaker, limiter = guards(clock, sleeper, failure_threshold=1)
        await breaker.record_failure()

        result = await execute_item(item, AlwaysOkWorker(), breaker, limiter, policy, sleep=sleeper)

        assert result.breaker_state is BreakerState.OPEN

    async def test_a_successful_call_spends_exactly_one_token(self, clock, sleeper, item, policy):
        breaker, limiter = guards(clock, sleeper)

        await execute_item(item, AlwaysOkWorker(), breaker, limiter, policy, sleep=sleeper)

        assert limiter.granted == 1

    async def test_retries_spend_one_token_for_the_whole_operation(
        self, clock, sleeper, item, policy
    ):
        """
        The token is per logical item, not per physical attempt.

        Debatable, and documented as such: it means a retrying batch can exceed the
        nominal rate. The alternative -- a token per attempt -- makes a partial outage
        throttle the healthy work queued behind it.
        """
        breaker, limiter = guards(clock, sleeper)
        worker = AlwaysFailWorker()

        result = await execute_item(item, worker, breaker, limiter, policy, sleep=sleeper)

        assert worker.call_count == 3
        assert limiter.granted == 1
        assert result.attempts == 3


class TestOneVerdictPerOperation:
    """
    The bug that took longest to find: the breaker was told once per *attempt*.

    With 3 attempts and a 5-failure threshold, two bad items produce six recorded
    failures and trip the circuit -- so the breaker measures the retry policy rather
    than the dependency, and tightening retries silently loosens the breaker.
    """

    async def test_three_attempts_record_one_failure(self, clock, sleeper, item, policy):
        breaker, limiter = guards(clock, sleeper)
        worker = AlwaysFailWorker()

        await execute_item(item, worker, breaker, limiter, policy, sleep=sleeper)

        assert worker.call_count == 3, "the retry policy should have made three calls"
        assert breaker.failures == 1, "the breaker must hear one verdict, not three"

    async def test_two_bad_items_do_not_trip_a_five_failure_breaker(self, clock, sleeper, policy):
        """The exact scenario from the bug, asserted end to end."""
        breaker, limiter = guards(clock, sleeper, failure_threshold=5)

        for n in range(2):
            await execute_item(
                WorkItem(item_id=f"T-{n}", host="https://h.example.com"),
                AlwaysFailWorker(),
                breaker,
                limiter,
                policy,
                sleep=sleeper,
            )

        assert breaker.failures == 2
        assert breaker.state is BreakerState.CLOSED
        assert breaker.trips == 0

    async def test_five_bad_items_do_trip_it(self, clock, sleeper, policy):
        breaker, limiter = guards(clock, sleeper, failure_threshold=5)

        for n in range(5):
            await execute_item(
                WorkItem(item_id=f"T-{n}", host="https://h.example.com"),
                AlwaysFailWorker(),
                breaker,
                limiter,
                policy,
                sleep=sleeper,
            )

        assert breaker.state is BreakerState.OPEN
        assert breaker.trips == 1

    async def test_a_permanent_failure_clears_the_failure_run(self, clock, sleeper, item, policy):
        """
        A 404 is positive evidence the dependency is answering, so it is recorded as a
        success against the breaker even though the item failed.
        """
        breaker, limiter = guards(clock, sleeper, failure_threshold=5)
        await breaker.record_failure()
        await breaker.record_failure()
        assert breaker.failures == 2

        result = await execute_item(
            item,
            AlwaysFailWorker(PermanentError("HTTP 404")),
            breaker,
            limiter,
            policy,
            sleep=sleeper,
        )

        assert result.outcome is Outcome.FAILED
        assert breaker.failures == 0

    async def test_a_permanent_failure_is_not_retried(self, clock, sleeper, item, policy):
        breaker, limiter = guards(clock, sleeper)
        worker = AlwaysFailWorker(PermanentError("HTTP 404"))

        result = await execute_item(item, worker, breaker, limiter, policy, sleep=sleeper)

        assert worker.call_count == 1, "a 404 was retried"
        assert result.attempts == 1


class TestValidationIsOutsideTheRetry:
    async def test_a_valid_payload_is_returned_normalised(
        self, clock, sleeper, item, validating_policy
    ):
        breaker, limiter = guards(clock, sleeper)

        result = await execute_item(
            item, AlwaysOkWorker(), breaker, limiter, validating_policy, sleep=sleeper
        )

        assert result.outcome is Outcome.OK
        assert result.data["product_id"] == "T-1"
        assert result.data["status"] == "active"

    async def test_a_bad_payload_yields_invalid_not_failed(
        self, clock, sleeper, item, validating_policy
    ):
        """
        INVALID is a distinct outcome from FAILED because the remedy differs: one is a
        conversation with whoever owns the upstream contract, the other is a retry.
        """
        breaker, limiter = guards(clock, sleeper)
        worker = AlwaysOkWorker({"product_id": "T-1", "status": "ACTIVE", "region": "us"})

        result = await execute_item(
            item, worker, breaker, limiter, validating_policy, sleep=sleeper
        )

        assert result.outcome is Outcome.INVALID
        assert "status" in result.error

    async def test_an_invalid_payload_is_not_retried(self, clock, sleeper, item, validating_policy):
        breaker, limiter = guards(clock, sleeper)
        worker = AlwaysOkWorker({"product_id": "T-1", "status": "nope", "region": "us"})

        result = await execute_item(
            item, worker, breaker, limiter, validating_policy, sleep=sleeper
        )

        assert worker.call_count == 1, "validation happened inside the retry loop"
        assert result.attempts == 1

    async def test_an_invalid_payload_does_not_count_against_the_breaker(
        self, clock, sleeper, item, validating_policy
    ):
        breaker, limiter = guards(clock, sleeper)
        worker = AlwaysOkWorker({"product_id": "T-1", "status": "nope", "region": "us"})

        await execute_item(item, worker, breaker, limiter, validating_policy, sleep=sleeper)

        assert breaker.failures == 0

    async def test_the_raw_payload_is_kept_on_an_invalid_row(
        self, clock, sleeper, item, validating_policy
    ):
        """Without the raw body, diagnosing a contract change means re-running the call."""
        breaker, limiter = guards(clock, sleeper)
        bad = {"product_id": "T-1", "status": "ACTIVE", "region": "us"}

        result = await execute_item(
            item, AlwaysOkWorker(bad), breaker, limiter, validating_policy, sleep=sleeper
        )

        assert result.data == bad

    async def test_no_schema_passes_the_payload_through(self, clock, sleeper, item, policy):
        breaker, limiter = guards(clock, sleeper)
        odd = {"whatever": "shape", "count": 3}

        result = await execute_item(
            item, AlwaysOkWorker(odd), breaker, limiter, policy, sleep=sleeper
        )

        assert result.outcome is Outcome.OK
        assert result.data == odd


class TestRecoveryAfterSuccess:
    async def test_a_success_after_failures_resets_the_run(self, clock, sleeper, item, policy):
        breaker, limiter = guards(clock, sleeper, failure_threshold=5)
        worker = ScriptedWorker(
            [
                WorkerError("503"),
                WorkerError("503"),
                WorkerError("503"),
                {"product_id": "T-1", "status": "active", "region": "us"},
            ]
        )

        first = await execute_item(item, worker, breaker, limiter, policy, sleep=sleeper)
        assert first.outcome is Outcome.FAILED
        assert breaker.failures == 1

        second = await execute_item(item, worker, breaker, limiter, policy, sleep=sleeper)
        assert second.outcome is Outcome.OK
        assert breaker.failures == 0

    async def test_a_retry_that_eventually_succeeds_reports_ok(self, clock, sleeper, item, policy):
        breaker, limiter = guards(clock, sleeper)
        worker = ScriptedWorker(
            [WorkerError("503"), {"product_id": "T-1", "status": "active", "region": "us"}]
        )

        result = await execute_item(item, worker, breaker, limiter, policy, sleep=sleeper)

        assert result.outcome is Outcome.OK
        assert result.attempts == 2, "attempt count should show the retry happened"


class TestBackoffShape:
    def test_delays_double_from_the_base(self):
        policy = RetryPolicy(attempts=5, base_delay=0.2, max_delay=10.0, jitter=0.0)
        assert [policy.delay_for(n) for n in (1, 2, 3, 4)] == [0.2, 0.4, 0.8, 1.6]

    def test_doubling_is_capped(self):
        policy = RetryPolicy(attempts=8, base_delay=1.0, max_delay=4.0, jitter=0.0)
        assert [policy.delay_for(n) for n in (1, 2, 3, 4, 5)] == [1.0, 2.0, 4.0, 4.0, 4.0]

    def test_jitter_is_proportional_and_bounded(self):
        policy = RetryPolicy(base_delay=1.0, jitter=0.1)
        assert policy.delay_for(1, rng=lambda: 1.0) == pytest.approx(1.1)
        assert policy.delay_for(1, rng=lambda: 0.0) == pytest.approx(0.9)
        assert policy.delay_for(1, rng=lambda: 0.5) == pytest.approx(1.0)

    def test_the_cap_bounds_the_base_not_the_jittered_value(self):
        """
        Capping after jitter would let the spread shrink to nothing at the ceiling,
        re-synchronising every retry exactly where a storm is most likely.
        """
        policy = RetryPolicy(base_delay=1.0, max_delay=2.0, jitter=0.5)
        assert policy.delay_for(9, rng=lambda: 1.0) == pytest.approx(3.0)

    def test_jitter_never_produces_a_negative_delay(self):
        policy = RetryPolicy(base_delay=1.0, jitter=1.0)
        assert policy.delay_for(1, rng=lambda: 0.0) == 0.0

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"attempts": 0}, "attempts must be >= 1"),
            ({"base_delay": -1}, "base_delay must be >= 0"),
            ({"base_delay": 5, "max_delay": 1}, "must be >= base_delay"),
            ({"jitter": 1.5}, "jitter must be within"),
        ],
    )
    def test_rejects_nonsense_configuration(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            RetryPolicy(**kwargs)

    def test_rejects_a_zero_attempt_number(self):
        with pytest.raises(ValueError, match="attempt must be >= 1"):
            RetryPolicy().delay_for(0)


class TestCallWithRetry:
    async def test_returns_on_first_success_without_sleeping(self, sleeper, no_jitter):
        async def ok():
            return {"fine": True}

        outcome = await call_with_retry(ok, RetryPolicy(), sleep=sleeper, rng=no_jitter)

        assert outcome.succeeded is True
        assert outcome.attempts == 1
        assert sleeper.calls == []

    async def test_records_the_backoff_sequence(self, sleeper, no_jitter):
        calls = {"n": 0}

        async def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise WorkerError("503")
            return {"ok": True}

        outcome = await call_with_retry(
            flaky,
            RetryPolicy(attempts=4, base_delay=0.1, jitter=0.0),
            sleep=sleeper,
            rng=no_jitter,
        )

        assert outcome.succeeded is True
        assert outcome.attempts == 3
        assert outcome.delays == [0.1, 0.2]
        assert outcome.slept_s == pytest.approx(0.3)

    async def test_exhausts_and_keeps_the_last_error(self, sleeper, no_jitter):
        async def broken():
            raise WorkerError("503")

        outcome = await call_with_retry(
            broken, RetryPolicy(attempts=3, jitter=0.0), sleep=sleeper, rng=no_jitter
        )

        assert outcome.succeeded is False
        assert outcome.attempts == 3
        assert isinstance(outcome.error, WorkerError)

    async def test_does_not_sleep_after_the_final_attempt(self, sleeper, no_jitter):
        """Sleeping after the last try delays the batch to achieve nothing."""

        async def broken():
            raise WorkerError("503")

        outcome = await call_with_retry(
            broken, RetryPolicy(attempts=3, jitter=0.0), sleep=sleeper, rng=no_jitter
        )

        assert len(outcome.delays) == 2, "3 attempts means 2 gaps"

    @pytest.mark.parametrize(
        "error",
        [PermanentError("404"), SchemaError("bad shape"), CircuitOpenError("open")],
    )
    async def test_non_retryable_errors_abort_immediately(self, error, sleeper, no_jitter):
        calls = {"n": 0}

        async def fails():
            calls["n"] += 1
            raise error

        outcome = await call_with_retry(
            fails, RetryPolicy(attempts=5), sleep=sleeper, rng=no_jitter
        )

        assert calls["n"] == 1
        assert outcome.attempts == 1
        assert sleeper.calls == []

from __future__ import annotations

import pytest

from parallel_ops.core.executor import ExecutionPolicy
from parallel_ops.core.scheduler import (
    Scheduler,
    outcome_counts,
    plan_summary,
    worst_state,
)
from parallel_ops.core.schema import CatalogRecord
from parallel_ops.models import BreakerState, Outcome, WorkerKind, WorkItem, WorkResult
from parallel_ops.resilience.breaker import GuardRegistry
from parallel_ops.resilience.errors import PermanentError, WorkerError
from parallel_ops.resilience.retry import RetryPolicy

from ..conftest import AlwaysFailWorker, AlwaysOkWorker, make_items


def build(clock, sleeper, worker, *, concurrency=4, failure_threshold=5, rate=1000.0, schema=None):
    guards = GuardRegistry(
        failure_threshold=failure_threshold,
        rate=rate,
        clock=clock,
        sleep=sleeper,
    )
    policy = ExecutionPolicy(
        retry=RetryPolicy(attempts=3, base_delay=0.1, jitter=0.0),
        schema=schema,
    )
    scheduler = Scheduler(
        guards,
        {WorkerKind.API: worker},
        policy,
        concurrency=concurrency,
        sleep=sleeper,
        rng=lambda: 0.5,
        clock=clock,
    )
    return scheduler, guards


class TestSchedulerBasics:
    async def test_an_empty_batch_returns_an_empty_report(self, clock, sleeper):
        scheduler, _ = build(clock, sleeper, AlwaysOkWorker())
        results, report = await scheduler.run([])
        assert results == []
        assert report.total == 0

    async def test_results_come_back_in_input_order(self, clock, sleeper):
        """
        `asyncio.gather` preserves argument order regardless of completion order, and the
        output file has to line up with the input file for anyone to correlate the two.
        """
        scheduler, _ = build(clock, sleeper, AlwaysOkWorker())
        items = make_items(12)

        results, _ = await scheduler.run(items)

        assert [r.item_id for r in results] == [i.item_id for i in items]

    async def test_every_item_gets_exactly_one_result(self, clock, sleeper):
        scheduler, _ = build(clock, sleeper, AlwaysOkWorker())
        results, report = await scheduler.run(make_items(25))
        assert len(results) == 25
        assert report.total == 25
        assert report.ok == 25

    async def test_rejects_nonsense_concurrency(self, clock, sleeper):
        with pytest.raises(ValueError, match="concurrency must be >= 1"):
            build(clock, sleeper, AlwaysOkWorker(), concurrency=0)

    async def test_progress_hook_fires_once_per_item(self, clock, sleeper):
        seen: list[str] = []
        guards = GuardRegistry(rate=1000.0, clock=clock, sleep=sleeper)
        scheduler = Scheduler(
            guards,
            {WorkerKind.API: AlwaysOkWorker()},
            ExecutionPolicy(retry=RetryPolicy(attempts=1)),
            concurrency=4,
            sleep=sleeper,
            clock=clock,
            on_result=lambda r: seen.append(r.item_id),
        )

        await scheduler.run(make_items(9))

        assert len(seen) == 9


class TestOneBadItemNeverEndsTheBatch:
    async def test_a_missing_worker_fails_only_its_own_item(self, clock, sleeper):
        """A browser item in an API-only run must not abort the other 4 items."""
        guards = GuardRegistry(rate=1000.0, clock=clock, sleep=sleeper)
        scheduler = Scheduler(
            guards,
            {WorkerKind.API: AlwaysOkWorker()},
            ExecutionPolicy(retry=RetryPolicy(attempts=1)),
            sleep=sleeper,
            clock=clock,
        )
        items = make_items(4) + [
            WorkItem(item_id="B-1", host="https://h.example.com", kind=WorkerKind.BROWSER)
        ]

        results, report = await scheduler.run(items)

        assert report.ok == 4
        assert report.failed == 1
        assert "no worker configured" in results[-1].error

    async def test_a_worker_that_raises_an_unexpected_exception_becomes_a_row(self, clock, sleeper):
        class Exploding(AlwaysOkWorker):
            async def execute(self, item):
                raise ZeroDivisionError("bug in the worker")

        scheduler, _ = build(clock, sleeper, Exploding())

        results, report = await scheduler.run(make_items(3))

        assert report.failed == 3
        assert all("ZeroDivisionError" in r.error for r in results)

    async def test_the_exception_text_lands_in_the_row(self, clock, sleeper):
        """Someone reading the CSV should not have to go and find the logs."""

        class Exploding(AlwaysOkWorker):
            async def execute(self, item):
                raise RuntimeError("selector 'div.price' matched nothing")

        scheduler, _ = build(clock, sleeper, Exploding())
        results, _ = await scheduler.run(make_items(1))

        assert "div.price" in results[0].error


class TestReportRollup:
    async def test_counts_split_by_outcome(self, clock, sleeper):
        scheduler, _ = build(clock, sleeper, AlwaysFailWorker(), failure_threshold=3)

        _, report = await scheduler.run(make_items(6))

        assert report.total == 6
        assert report.failed + report.rejected == 6
        assert report.rejected > 0, "the breaker should have started refusing"

    async def test_amplification_shows_retries_firing(self, clock, sleeper):
        scheduler, _ = build(clock, sleeper, AlwaysFailWorker(), failure_threshold=100)

        _, report = await scheduler.run(make_items(4))

        assert report.attempts == 12, "4 items x 3 attempts"
        assert report.amplification == pytest.approx(3.0)

    async def test_amplification_is_one_when_nothing_retries(self, clock, sleeper):
        scheduler, _ = build(clock, sleeper, AlwaysOkWorker())
        _, report = await scheduler.run(make_items(5))
        assert report.amplification == pytest.approx(1.0)

    async def test_amplification_of_an_empty_report_is_zero_not_a_crash(self):
        from parallel_ops.models import BatchReport

        assert BatchReport().amplification == 0.0

    async def test_hosts_are_reported_separately(self, clock, sleeper):
        scheduler, _ = build(clock, sleeper, AlwaysOkWorker())
        items = make_items(3, host="https://a.example.com") + make_items(
            2, host="https://b.example.com"
        )

        _, report = await scheduler.run(items)

        assert [h.host for h in report.hosts] == [
            "https://a.example.com",
            "https://b.example.com",
        ]
        assert report.hosts[0].total == 3
        assert report.hosts[1].total == 2

    async def test_a_sick_host_does_not_stop_a_healthy_one(self, clock, sleeper):
        """
        Per-host guards, asserted end to end. A global breaker would report the whole
        batch as broken when one of two dependencies was down.
        """

        class PerHost(AlwaysOkWorker):
            async def execute(self, item):
                self.call_count += 1
                if "sick" in item.host:
                    raise WorkerError("HTTP 503")
                return {"product_id": item.item_id, "status": "active", "region": "us"}

        scheduler, guards = build(clock, sleeper, PerHost(), failure_threshold=2)
        items = make_items(6, host="https://sick.example.com") + make_items(
            4, host="https://healthy.example.com"
        )

        _, report = await scheduler.run(items)

        by_host = {h.host: h for h in report.hosts}
        assert by_host["https://healthy.example.com"].ok == 4
        assert by_host["https://sick.example.com"].ok == 0
        assert by_host["https://sick.example.com"].trips >= 1
        assert guards.breaker("https://healthy.example.com").state is BreakerState.CLOSED

    async def test_trips_and_final_state_are_recorded_per_host(self, clock, sleeper):
        scheduler, _ = build(clock, sleeper, AlwaysFailWorker(), failure_threshold=2)

        _, report = await scheduler.run(make_items(8))

        assert report.hosts[0].trips >= 1
        assert report.hosts[0].final_state in (BreakerState.OPEN, BreakerState.HALF_OPEN)

    async def test_invalid_rows_are_counted_separately_from_failures(self, clock, sleeper):
        scheduler, _ = build(
            clock,
            sleeper,
            AlwaysOkWorker({"product_id": "x", "status": "NOPE", "region": "us"}),
            schema=CatalogRecord,
        )

        _, report = await scheduler.run(make_items(4))

        assert report.invalid == 4
        assert report.failed == 0

    async def test_a_permanent_failure_never_trips_the_breaker_across_a_batch(self, clock, sleeper):
        """
        Six stale product IDs against a five-failure breaker. This is the scenario that
        produced a misleading "catalog is down" verdict before 404s were reclassified.
        """
        scheduler, guards = build(
            clock,
            sleeper,
            AlwaysFailWorker(PermanentError("HTTP 404")),
            failure_threshold=5,
        )

        _, report = await scheduler.run(make_items(6))

        assert report.failed == 6
        assert report.rejected == 0, "a 404 batch must not start refusing its own items"
        assert guards.breaker("https://catalog.example.com").trips == 0


class TestPlanSummary:
    def test_counts_items_hosts_and_kinds(self):
        items = make_items(3, host="https://a.example.com") + [
            WorkItem(item_id="B-1", host="https://b.example.com", kind=WorkerKind.BROWSER)
        ]

        summary = plan_summary(items)

        assert summary["total"] == 4
        assert summary["hosts"] == 2
        assert summary["by_kind"] == {"api": 3, "browser": 1}

    def test_breaks_kinds_down_per_host(self):
        items = [
            WorkItem(item_id="1", host="https://a.example.com"),
            WorkItem(item_id="2", host="https://a.example.com", kind=WorkerKind.BROWSER),
            WorkItem(item_id="3", host="https://b.example.com"),
        ]

        summary = plan_summary(items)

        assert summary["by_host"]["https://a.example.com"] == {"api": 1, "browser": 1}
        assert summary["by_host"]["https://b.example.com"] == {"api": 1}

    def test_an_empty_plan_is_all_zeroes(self):
        assert plan_summary([]) == {"total": 0, "hosts": 0, "by_kind": {}, "by_host": {}}


class TestSummaryHelpers:
    def test_outcome_counts_includes_every_outcome_even_at_zero(self):
        results = [
            WorkResult(item_id="1", host="h", kind=WorkerKind.API, outcome=Outcome.OK),
            WorkResult(item_id="2", host="h", kind=WorkerKind.API, outcome=Outcome.FAILED),
        ]

        counts = outcome_counts(results)

        assert counts == {"ok": 1, "invalid": 0, "failed": 1, "rejected": 0}

    def test_worst_state_of_nothing_is_closed(self):
        assert worst_state([]) is BreakerState.CLOSED

    def test_worst_state_picks_the_most_degraded(self):
        results = [
            WorkResult(
                item_id="1",
                host="h",
                kind=WorkerKind.API,
                outcome=Outcome.OK,
                breaker_state=BreakerState.CLOSED,
            ),
            WorkResult(
                item_id="2",
                host="h",
                kind=WorkerKind.API,
                outcome=Outcome.REJECTED,
                breaker_state=BreakerState.OPEN,
            ),
            WorkResult(
                item_id="3",
                host="h",
                kind=WorkerKind.API,
                outcome=Outcome.OK,
                breaker_state=BreakerState.HALF_OPEN,
            ),
        ]

        assert worst_state(results) is BreakerState.OPEN

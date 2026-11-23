from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable, Iterable, Sequence

from ..models import (
    BatchReport,
    BreakerState,
    HostReport,
    Outcome,
    WorkerKind,
    WorkItem,
    WorkResult,
)
from ..resilience.breaker import GuardRegistry
from ..workers.base import Worker
from .executor import ExecutionPolicy, execute_item

ProgressHook = Callable[[WorkResult], None]


class Scheduler:
    """
    Fans work out across one event loop and gathers it back in input order.

    Concurrency is bounded by a semaphore rather than by chunking, so a slow item does
    not hold up a whole chunk the way a barrier between batches would. Both worker kinds
    share the one semaphore: a browser item and an API item are the same kind of thing to
    the scheduler, differing only in how long they take and which limiter they draw from.

    **Known limitation, and it is inherent rather than an oversight.** With concurrency
    C, the first C items are all admitted before any of them has reported back, so a
    breaker with a threshold of 5 cannot possibly trip before those C calls are in
    flight. A circuit breaker limits *sustained* traffic to a failing dependency, not the
    initial burst. Lowering concurrency tightens that window; the honest framing is that
    the breaker bounds how long an outage costs, not whether the first wave hits.
    """

    def __init__(
        self,
        guards: GuardRegistry,
        workers: dict[WorkerKind, Worker],
        policy: ExecutionPolicy,
        *,
        concurrency: int = 8,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: Callable[[], float] = random.random,
        clock: Callable[[], float] = time.monotonic,
        on_result: ProgressHook | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")
        self._guards = guards
        self._workers = workers
        self._policy = policy
        self._concurrency = concurrency
        self._sleep = sleep
        self._rng = rng
        self._clock = clock
        self._on_result = on_result

    async def run(self, items: Iterable[WorkItem]) -> tuple[list[WorkResult], BatchReport]:
        batch = list(items)
        if not batch:
            return [], BatchReport()

        started = self._clock()
        semaphore = asyncio.Semaphore(self._concurrency)

        async def bounded(item: WorkItem) -> WorkResult:
            async with semaphore:
                return await self._run_one(item)

        results = await asyncio.gather(*(bounded(item) for item in batch))
        duration = self._clock() - started

        return list(results), self._build_report(list(results), duration)

    async def _run_one(self, item: WorkItem) -> WorkResult:
        """
        Execute one item, converting any unexpected exception into a failed row.

        A single bad item never takes the batch down. The cost of that choice is the usual
        one: a misconfiguration produces N failures that each look like a data problem, so
        `doctor` and `plan` exist to catch that class of thing before a run starts.
        """
        worker = self._workers.get(item.kind)
        if worker is None:
            return WorkResult(
                item_id=item.item_id,
                host=item.host,
                kind=item.kind,
                outcome=Outcome.FAILED,
                error=f"no worker configured for kind '{item.kind.value}'",
            )

        try:
            result = await execute_item(
                item,
                worker,
                self._guards.breaker(item.host),
                self._guards.limiter(item.host),  # type: ignore[arg-type]
                self._policy,
                sleep=self._sleep,
                rng=self._rng,
                clock=self._clock,
            )
        except Exception as exc:  # noqa: BLE001 - one bad item must not end the batch
            result = WorkResult(
                item_id=item.item_id,
                host=item.host,
                kind=item.kind,
                outcome=Outcome.FAILED,
                error=f"{type(exc).__name__}: {exc}",
            )

        if self._on_result is not None:
            self._on_result(result)
        return result

    def _build_report(self, results: Sequence[WorkResult], duration: float) -> BatchReport:
        report = BatchReport(total=len(results), duration_s=duration)
        per_host: dict[str, HostReport] = {}

        for result in results:
            host = per_host.setdefault(result.host, HostReport(host=result.host))
            host.total += 1
            host.attempts += result.attempts
            host.waited_s += result.waited_s
            report.attempts += result.attempts
            report.waited_s += result.waited_s

            if result.outcome is Outcome.OK:
                report.ok += 1
                host.ok += 1
            elif result.outcome is Outcome.INVALID:
                report.invalid += 1
                host.invalid += 1
            elif result.outcome is Outcome.REJECTED:
                report.rejected += 1
                host.rejected += 1
            else:
                report.failed += 1
                host.failed += 1

        for host_name, host_report in per_host.items():
            breaker = self._guards.breaker(host_name)
            host_report.trips = breaker.trips
            host_report.final_state = breaker.state
            host_report.waited_s = round(host_report.waited_s, 3)

        report.waited_s = round(report.waited_s, 3)
        report.duration_s = round(report.duration_s, 3)
        report.hosts = [per_host[name] for name in sorted(per_host)]
        return report


def plan_summary(items: Sequence[WorkItem]) -> dict[str, object]:
    """
    Describe what a run would do, without doing it.

    Exists because the expensive mistakes on a batch tool are all discovered in the first
    thirty seconds of a run that should never have started: every item pointed at one
    host, or a browser item in a file you thought was all API calls.
    """
    by_host: dict[str, dict[str, int]] = {}
    by_kind: dict[str, int] = {}

    for item in items:
        host_entry = by_host.setdefault(item.host, {})
        host_entry[item.kind.value] = host_entry.get(item.kind.value, 0) + 1
        by_kind[item.kind.value] = by_kind.get(item.kind.value, 0) + 1

    return {
        "total": len(items),
        "hosts": len(by_host),
        "by_kind": dict(sorted(by_kind.items())),
        "by_host": {host: dict(sorted(kinds.items())) for host, kinds in sorted(by_host.items())},
    }


def outcome_counts(results: Sequence[WorkResult]) -> dict[str, int]:
    counts = {outcome.value: 0 for outcome in Outcome}
    for result in results:
        counts[result.outcome.value] += 1
    return counts


def worst_state(results: Sequence[WorkResult]) -> BreakerState:
    """Most degraded breaker state observed, for a one-line run verdict."""
    order = {BreakerState.CLOSED: 0, BreakerState.HALF_OPEN: 1, BreakerState.OPEN: 2}
    worst = BreakerState.CLOSED
    for result in results:
        if order[result.breaker_state] > order[worst]:
            worst = result.breaker_state
    return worst

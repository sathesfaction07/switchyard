from __future__ import annotations

from ..config import Settings
from ..models import BatchReport, WorkerKind, WorkItem, WorkResult
from ..resilience.breaker import GuardRegistry
from ..resilience.retry import RetryPolicy
from ..workers import get_worker
from ..workers.base import Worker
from .executor import ExecutionPolicy
from .scheduler import ProgressHook, Scheduler
from .schema import get_schema


def build_guards(settings: Settings) -> GuardRegistry:
    return GuardRegistry(
        failure_threshold=settings.failure_threshold,
        reset_timeout=settings.reset_timeout,
        success_threshold=settings.success_threshold,
        rate=settings.rate,
        burst=settings.effective_burst,
    )


def build_policy(settings: Settings) -> ExecutionPolicy:
    return ExecutionPolicy(
        retry=RetryPolicy(
            attempts=settings.attempts,
            base_delay=settings.base_delay,
            max_delay=settings.max_delay,
            jitter=settings.jitter,
        ),
        schema=get_schema(settings.schema_name),
    )


def build_workers(settings: Settings, kinds: set[WorkerKind]) -> dict[WorkerKind, Worker]:
    """
    Construct only the workers the batch actually needs.

    Deliberately driven by what is present in the input rather than by configuration.
    A file of pure API items must not require Playwright to be installed, and building
    every known worker up front would make it required for everybody.
    """
    workers: dict[WorkerKind, Worker] = {}
    for kind in sorted(kinds, key=lambda k: k.value):
        if kind is WorkerKind.BROWSER:
            workers[kind] = get_worker(kind, timeout=settings.timeout, headless=settings.headless)
        else:
            workers[kind] = get_worker(kind, timeout=settings.timeout)
    return workers


async def run_batch(
    settings: Settings,
    items: list[WorkItem],
    *,
    on_result: ProgressHook | None = None,
) -> tuple[list[WorkResult], BatchReport]:
    """
    Run a batch end to end, always closing workers.

    The `finally` matters more than it looks: a browser worker that is not closed leaves
    a chromium process behind, and a run that raised halfway through a 500-item batch
    would otherwise leak one per invocation until the machine notices.
    """
    kinds = {item.kind for item in items}
    workers = build_workers(settings, kinds)

    scheduler = Scheduler(
        build_guards(settings),
        workers,
        build_policy(settings),
        concurrency=settings.concurrency,
        on_result=on_result,
    )

    try:
        return await scheduler.run(items)
    finally:
        for worker in workers.values():
            await worker.aclose()

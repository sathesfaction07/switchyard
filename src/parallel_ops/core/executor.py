from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from pydantic import BaseModel

from ..models import Outcome, WorkItem, WorkResult
from ..resilience.breaker import CircuitBreaker
from ..resilience.errors import SchemaError, WorkerError
from ..resilience.limiter import TokenBucket
from ..resilience.retry import RetryPolicy, call_with_retry
from ..workers.base import Worker
from .schema import validate_payload


@dataclass(frozen=True)
class ExecutionPolicy:
    retry: RetryPolicy
    schema: type[BaseModel] | None = None


def counts_against_breaker(error: Exception) -> bool:
    """
    Whether a failure is evidence that the *dependency* is unhealthy.

    Only `WorkerError` qualifies — timeouts, 5xx, 429, connection resets.

    `PermanentError` does not. A 404 means the dependency answered correctly and
    promptly; the problem is our input. Counting it was the first version of this code
    and it produced a genuinely misleading result: a batch of six items with stale
    product IDs tripped a five-failure breaker and rejected everything after item five,
    so the run reported the catalog as down when the catalog was entirely healthy and the
    input file was three weeks old.

    `SchemaError` does not either, for the same reason plus a sharper one. If upstream
    ships a breaking contract change, every item fails validation. Were that to trip the
    breaker, the run would convert into a wall of `rejected` rows and the field-level
    detail explaining *what* changed would only exist for the first few items. The
    diagnostic is the entire value of validating; discarding it to protect a dependency
    that is not under stress is a bad trade.

    An unclassified exception from a worker is a bug in the worker, so it also does not
    count. It fails its item loudly with the exception text in the row, which is where
    someone can act on it.
    """
    return isinstance(error, WorkerError)


async def execute_item(
    item: WorkItem,
    worker: Worker,
    breaker: CircuitBreaker,
    limiter: TokenBucket,
    policy: ExecutionPolicy,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    rng: Callable[[], float] = random.random,
    clock: Callable[[], float] = time.monotonic,
) -> WorkResult:
    """
    Run one item through the full guard stack.

    The ordering below is the single most important thing in this package, and each step
    is where it is for a reason:

    1. **Breaker first.** If the circuit is open the item is refused here, having spent
       nothing. Checking the limiter first would mean waiting for a token — and burning
       it — for a call that was never going to be made. On a batch that is fully rejected
       the difference is the entire run's quota.

    2. **Limiter second.** A token is spent only once the call is actually going to
       happen.

    3. **Retries inside, breaker outside.** `call_with_retry` loops without touching the
       breaker, and the breaker is told exactly once, afterwards, whether this *logical*
       operation succeeded. Reporting each physical attempt is the trap: with 3 attempts
       and a 5-failure threshold, two bad items produce six recorded failures and trip
       the circuit, so the breaker ends up measuring the retry policy instead of the
       dependency. Tightening retries would then silently loosen the breaker, and the two
       settings are normally owned by different people.

    4. **Validation last, and outside the retry.** A `SchemaError` is not retryable, so
       nothing is gained by validating inside the loop, and keeping it outside makes the
       ordering legible: the call either reached the dependency or it did not, and only
       then do we ask whether the answer was the shape we expected.
    """
    started = clock()

    admitted, observed_state = await breaker.admit()
    if not admitted:
        return WorkResult(
            item_id=item.item_id,
            host=item.host,
            kind=item.kind,
            outcome=Outcome.REJECTED,
            attempts=0,
            breaker_state=observed_state,
            error=f"circuit {observed_state.value} for {item.host}; not attempted",
            duration_s=clock() - started,
        )

    waited = await limiter.acquire()

    retry_outcome = await call_with_retry(
        lambda: worker.execute(item),
        policy.retry,
        sleep=sleep,
        rng=rng,
    )

    if retry_outcome.error is not None:
        error = retry_outcome.error
        if counts_against_breaker(error):
            await breaker.record_failure()
        else:
            await breaker.record_success()
        return WorkResult(
            item_id=item.item_id,
            host=item.host,
            kind=item.kind,
            outcome=Outcome.FAILED,
            attempts=retry_outcome.attempts,
            waited_s=waited,
            breaker_state=observed_state,
            error=f"{type(error).__name__}: {error}",
            duration_s=clock() - started,
        )

    await breaker.record_success()

    payload = retry_outcome.value if isinstance(retry_outcome.value, dict) else {}
    try:
        validated = validate_payload(payload, policy.schema)
    except SchemaError as exc:
        return WorkResult(
            item_id=item.item_id,
            host=item.host,
            kind=item.kind,
            outcome=Outcome.INVALID,
            attempts=retry_outcome.attempts,
            waited_s=waited,
            breaker_state=observed_state,
            data=payload,
            error=str(exc),
            duration_s=clock() - started,
        )

    return WorkResult(
        item_id=item.item_id,
        host=item.host,
        kind=item.kind,
        outcome=Outcome.OK,
        attempts=retry_outcome.attempts,
        waited_s=waited,
        breaker_state=observed_state,
        data=validated,
        duration_s=clock() - started,
    )

# ADR 002: Only dependency-health failures reach the circuit breaker, once per item

## Status

Accepted

## Context

A circuit breaker exists to answer one question: **is this dependency unhealthy enough that I should stop sending it traffic?** Everything about what to count, and how often, follows from that.

The first implementation got both parts wrong, in ways that only showed up when the output was read rather than when the code was reviewed.

### Counting every failure

`record_failure()` was called for any error. Then a batch of six items with three-week-old product IDs hit a five-failure threshold: items 1–5 returned 404, the circuit opened, and item 6 came back `rejected`. The run reported the catalog as down.

The catalog was entirely healthy. It had answered all five requests correctly, quickly, and with the right answer — *those products do not exist*. The problem was the input file. The breaker had been wired to a signal that says nothing about dependency health, so it produced a confidently wrong diagnosis and hid the real one.

Schema errors have the same defect with a sharper edge. If upstream ships a breaking contract change, *every* item fails validation. Tripping the breaker on that converts the run into a wall of `rejected` rows — and the field-level detail explaining what actually changed exists only for the handful of items that ran before the circuit opened. Validating a response is worth doing precisely because it produces `status: Input should be 'active', 'suppressed', 'pending' or 'removed'` instead of "invalid". Throwing that away to protect a dependency that is not under any stress is a bad trade.

### Counting every attempt

`record_failure()` was called inside the retry loop, once per physical call. This took longest to find because the arithmetic is not obviously wrong until you write it out:

| Attempts | Threshold | Bad items needed to trip |
|:--|:--|:--|
| 3 | 5 | 2 |
| 1 | 5 | 5 |
| 5 | 5 | 1 |

**Two bad items trip a five-failure breaker.** The breaker is not measuring the dependency, it is measuring the retry policy. Tightening retries from 3 to 1 silently *loosens* the breaker from 2 items to 5, and vice versa. Those two settings are normally owned by different people, so the coupling is invisible from either side.

## Decision

Two rules, implemented in two separate places.

**1. `counts_against_breaker(error)` — only `WorkerError` counts.**

```python
def counts_against_breaker(error: Exception) -> bool:
    return isinstance(error, WorkerError)
```

| Error | Counts? | Why |
|:--|:--|:--|
| `WorkerError` | yes | timeout, 5xx, 429, connection reset — plausibly transient, genuinely about the dependency |
| `PermanentError` | no | a 404 means it answered correctly and promptly. Our input is stale |
| `SchemaError` | no | a contract problem. Tripping destroys the diagnostic that is the point of validating |
| `CircuitOpenError` | no | the breaker's own output. Feeding it back is circular |
| anything else | no | a bug in the worker, not evidence about the dependency |

A `PermanentError` is recorded as a `record_success()`, because a prompt correct answer *is* evidence of health. That is deliberate and it is why a batch of 404s leaves the circuit closed.

**2. One verdict per logical operation.**

`call_with_retry` does not touch the breaker at all. It loops, exhausts, and returns a `RetryOutcome`. The executor then reports exactly once:

```python
retry_outcome = await call_with_retry(lambda: worker.execute(item), policy.retry, ...)

if retry_outcome.error is not None:
    if counts_against_breaker(retry_outcome.error):
        await breaker.record_failure()
    else:
        await breaker.record_success()
    return WorkResult(outcome=Outcome.FAILED, attempts=retry_outcome.attempts, ...)

await breaker.record_success()
```

Keeping the retry loop ignorant of the breaker means the composition rule lives in one readable place instead of emerging from two classes calling each other. `attempts` is still reported per item, so the physical call count remains visible in the output and in `BatchReport.amplification`.

## Verified behaviour

Against a local server returning 404 for every item, threshold 5:

```
total           6
failed          6
rejected        0      <- the circuit never opened
attempts        6      (1.00 per item, so nothing was retried either)
```

Against the same server returning 503, threshold 2, concurrency 1:

```
total           8      host              ok  inv  fail  rej  trips  state
failed          2      127.0.0.1:18771    0    0     2    6      1  open
rejected        6
attempts        4      (0.50 per item)
```

Amplification below 1.0 is the breaker working: six items were refused before spending a token or a call.

`tests/unit/test_executor.py::TestOneVerdictPerOperation` asserts three attempts produce exactly one recorded failure, and that two bad items leave a five-failure breaker closed.

## Consequences

**Good**

- The breaker measures dependency health and nothing else, so `trips` and `rejected` mean what a reader assumes they mean
- Retry and breaker settings are independent. Changing `--attempts` cannot move the effective trip point
- A stale input file reports as `failed` with 404s rather than as an outage, which points at the actual problem
- A contract change reports as `invalid` on every row, each carrying its own field-level detail
- The four outcomes map onto four different remedies, so triage does not require opening the logs

**Bad**

- Four error classes is more taxonomy than a single `RetryableError` boolean, and every new worker has to classify correctly. A worker that raises bare `Exception` gets the safe-but-useless default of "does not count"
- `PermanentError` recorded as a breaker *success* reads oddly. It is right — a prompt correct answer is evidence of health — but it needs the comment that is on it
- The rule lives in a free function separate from both the retry loop and the breaker. Understanding "why did this not trip" means reading `executor.py`, not `breaker.py`

**Neutral**

- 429 is a 4xx that means "try again later", so `RETRYABLE_STATUS` is checked before the generic 4xx rule. Easy to get wrong when adding a status
- The breaker cannot prevent the first wave. With concurrency C, the first C items are admitted before any reports back, so a threshold below C cannot trip in time. Inherent to concurrent execution: a breaker bounds *sustained* traffic, not the initial burst. Lowering concurrency narrows the window
- Nothing distinguishes a 503 from a connection refusal — both are `WorkerError`. Finer classification would let the breaker react faster to a hard down, at the cost of more taxonomy

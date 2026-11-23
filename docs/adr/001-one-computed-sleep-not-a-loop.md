# ADR 001: The limiter waits once, computed, rather than looping

## Status

Accepted

## Context

`TokenBucket.acquire` originally had the shape almost every token bucket has:

```python
async with self._lock:
    waited = 0.0
    while True:
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return waited
        delay = (tokens - self._tokens) / self.rate
        await self._sleep(delay)
        waited += delay
```

Sleep for the shortfall, wake, re-check, spend. It reads as obviously correct.

The test suite hung. Not slowly — indefinitely. 36 tests, 35 of which finished in about 0.01s each, and one that never returned. Killed after seven minutes.

Instrumenting the real class with a sleep that reported whether the clock had moved produced the answer:

```
acquire #13: clock refused to move: delay=4.5474735088646414e-14
             added to 1000.3 changed nothing (after 7 sleeps)
             tokens=0.9999999999995453
```

Tokens accrue as `elapsed * rate`, both floats. Depending on where the clock lands, that product is sometimes a few parts in 1e13 *below* the integer target. `0.9999999999995453 >= 1.0` is false, so the loop computed a corrective delay of `4.5e-14` seconds. Float64 spacing at 1000.3 is `1.14e-13`, so `1000.3 + 4.5e-14 == 1000.3` exactly. The clock could not represent the interval. `elapsed` came back zero, no tokens accrued, the next iteration computed the identical delay, forever.

Two properties of this are worth stating plainly.

**A real clock hides it.** `time.monotonic()` advances whether or not anyone asked, so real time passing during the event-loop round trip supplies the missing tokens and the loop escapes. What ships is a hot spin of a handful of event-loop iterations, occasionally, under load — invisible in any test that uses real time, and close to undiagnosable in production.

**My first hypothesis was wrong.** I predicted the failure would occur on acquire #11, on the theory that `(1000.0 + 0.1) - 1000.0` underestimates 0.1. It overestimates it — `0.10000000000002274` — so acquire #11 succeeds. The direction of float error varies with the clock value, and the failure needed 13 acquires to reach a value where it went the other way. Verifying rather than assuming was what found it.

## Decision

Delete the loop. The lock is held for the entire method, so **no other caller can take the tokens this one is waiting for.** The wait is uncontended, which means it can be computed exactly once:

```python
async with self._lock:
    self._refill()
    waited = 0.0
    deficit = tokens - self._tokens

    if deficit > TOKEN_EPSILON:
        waited = deficit / self.rate
        await self._sleep(waited)
        self._refill()

    self._tokens = max(0.0, self._tokens - tokens)
    self.granted += 1
    self.total_waited += waited
    return waited
```

Three parts:

- **No loop.** At most one sleep per call, so non-termination is impossible by construction rather than by argument. The loop had been defending against a race the lock already prevented.
- **`TOKEN_EPSILON = 1e-9`.** A deficit of 1e-13 tokens is float noise, not backpressure. Treating it as a real shortfall is what generated the unrepresentable delay in the first place.
- **`max(0.0, ...)`.** Accrual can land a hair under the requested amount, and an unclamped subtraction would leave a negative balance that silently overcharges the next caller.

The alternative — keep the loop and widen the comparison to `self._tokens + EPSILON >= tokens` — fixes the observed case but leaves the loop, and with it the possibility of some other input finding a delay the clock cannot represent. At a rate of 1e6/s, an epsilon-sized deficit yields a delay of 1e-15s, which is still below float spacing at any realistic clock value. Removing the loop removes the whole class.

## Consequences

**Good**

- Termination is structural. There is no iteration to fail to converge
- Shorter and easier to read than what it replaced
- FIFO fairness is unchanged, since the lock is still held across the wait
- `tests/unit/test_resilience.py::TestWhyOneSleepNotALoop` pins it six ways, including a sleep that never advances the clock at all and asserts `acquire` still returns after exactly one call
- The suite went from hanging past 442 seconds to 241 tests in 0.32s

**Bad**

- Correctness now depends on an invariant that is not locally visible: *the lock must be held across the wait*. Someone optimising for throughput by releasing it would reintroduce a real race, and the single computed sleep would then be wrong rather than merely unfair. The docstring says so; nothing enforces it
- `TOKEN_EPSILON` is a magic constant. 1e-9 tokens is obviously negligible for any real rate limit, but it is a judgement call with no measurement behind it
- The bucket trusts the injected sleep to have actually slept roughly as long as asked. A deliberately lying sleep produces a slightly generous bucket. Acceptable, because the only such implementation is the test double, and the clamp bounds the error

**Neutral**

- Under-delivery is bounded at `TOKEN_EPSILON` tokens per call, which at 10/s is 100 picoseconds of rate. Not worth accounting for
- The general lesson, which I would apply elsewhere: **a loop whose exit condition is an exact float comparison, and whose progress depends on a float clock advancing, has two independent ways to fail to converge.** If the loop can be replaced by a computed quantity, it should be

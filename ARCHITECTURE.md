# ParallelOps — Technical Architecture

## Overview

ParallelOps runs a batch of heterogeneous operations concurrently on a single asyncio event loop. Every item passes through a per-host circuit breaker and token bucket before the call is made, is retried according to an error taxonomy that distinguishes transient from settled failures, and has its response validated against a declared schema. HTTP and browser work are scheduled by the same governor and draw on the same guards.

**Design principle:** the guard ordering is the product. Breaker before limiter so a refused item spends nothing; retries inside the breaker so it hears one verdict per item, not per attempt; validation last and outside the retry so a contract problem is never mistaken for an outage.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            CLI (parallel-ops)                                │
│                                                                             │
│  ┌──────────────────┐  ┌────────────────────┐  ┌────────────────────────┐  │
│  │ run              │  │ plan               │  │ doctor                 │  │
│  │ execute a batch  │  │ counts, policy,    │  │ worker availability    │  │
│  │ live per-item    │  │ duration floor.    │  │ + install commands,    │  │
│  │ progress, per-   │  │ Builds no workers, │  │ schemas, settings,     │  │
│  │ host rollup,     │  │ makes no calls     │  │ the retry rules in     │  │
│  │ exit 1 if not    │  │                    │  │ words                  │  │
│  │ all ok           │  │                    │  │                        │  │
│  └────────┬─────────┘  └─────────┬──────────┘  └────────────────────────┘  │
└───────────┼──────────────────────┼─────────────────────────────────────────┘
            │                      │
            │                      └──> plan_summary()  +  build_policy()
            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          IO (io.py)                                          │
│                                                                             │
│  load_items()   CSV or JSON -> list[WorkItem]                                │
│                 unknown columns -> WorkItem.extras, carried to the output    │
│                 unknown kind    -> api, so one typo cannot abort the run     │
│                 no host         -> skipped, it cannot be scheduled           │
│                                                                             │
│  write_results(results, report, dir, fmt, *, emit=None)                      │
│                 `results` is complete and always reaches the JSON report     │
│                 `emit` optionally narrows the CSV/Excel  <- see Audit Trail  │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       PIPELINE (core/pipeline.py)                            │
│                                                                             │
│  build_guards()   GuardRegistry from Settings                                │
│  build_policy()   RetryPolicy + resolved schema                              │
│  build_workers()  ONLY the kinds present in the input, so a pure-API file    │
│                   never requires Playwright                                  │
│  run_batch()      try/finally -> every worker closed, or a browser process   │
│                   leaks per invocation                                       │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      SCHEDULER (core/scheduler.py)                           │
│                                                                             │
│  asyncio.Semaphore(concurrency)   bounded fan-out, not chunked, so a slow    │
│                                   item delays only itself                    │
│  asyncio.gather(*bounded)         fan-in, input order preserved              │
│  _run_one: try/except             any exception -> a FAILED row, never a     │
│                                   dead batch                                 │
│  _build_report                    per-host rollup: ok/invalid/failed/        │
│                                   rejected, attempts, waited, trips, state   │
│                                                                             │
│  Inherent limit: with concurrency C the first C items are admitted before    │
│  any reports back, so a threshold below C cannot trip in time.               │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │  one item
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      EXECUTOR (core/executor.py)                             │
│                                                                             │
│  1. breaker.admit()        -> (admitted, observed_state)                     │
│        refused             -> REJECTED. No token, no call, attempts=0        │
│  2. limiter.acquire()      -> waited_s. One token per LOGICAL item           │
│  3. call_with_retry(...)   -> RetryOutcome. Touches the breaker not at all   │
│  4. one verdict:                                                             │
│        counts_against_breaker(err) ? record_failure() : record_success()      │
│  5. validate_payload()     -> INVALID on SchemaError, breaker untouched      │
│                                                                             │
│  counts_against_breaker(): WorkerError only.                                 │
│    not PermanentError  (a 404 means it answered correctly)                    │
│    not SchemaError     (a contract change would trip on every row)           │
│    not CircuitOpenError(the breaker's own output)                            │
│    not anything else   (a worker bug is not dependency evidence)             │
└────────┬────────────────────────────────────────────┬───────────────────────┘
         │                                            │
         ▼                                            ▼
┌──────────────────────────────────────┐  ┌──────────────────────────────────┐
│  RESILIENCE (resilience/)             │  │  WORKERS (workers/)               │
│                                       │  │                                   │
│  limiter.py   TokenBucket             │  │  base.py   Worker ABC             │
│   per host, lock held across the wait │  │    execute() -> dict, or raise    │
│    -> FIFO, no starvation             │  │    is_available()/reason()        │
│   ONE computed sleep, never a loop    │  │    aclose() idempotent            │
│   TOKEN_EPSILON guards float slack    │  │                                   │
│                                       │  │  api.py    ApiWorker              │
│  breaker.py   CircuitBreaker          │  │    one pooled httpx client per    │
│   CLOSED -> OPEN at threshold         │  │    batch, not per item            │
│   OPEN -> HALF_OPEN after timeout     │  │    RETRYABLE_STATUS checked       │
│   HALF_OPEN admits exactly ONE probe  │  │    before the generic 4xx rule    │
│    (_probe_in_flight)                 │  │    so 429 stays retryable         │
│   a failed probe re-opens immediately │  │                                   │
│                                       │  │  browser.py BrowserWorker         │
│  GuardRegistry  lazily one breaker +  │  │    optional, deferred import      │
│   one limiter per host, so a sick     │  │    one browser+context per batch  │
│   dependency cannot halt a healthy one│  │    shares the event loop, not a   │
│                                       │  │    thread pool                    │
│  retry.py     RetryPolicy             │  └──────────────────────────────────┘
│   exponential, cap bounds the BASE    │
│   then proportional jitter applied    │  ┌──────────────────────────────────┐
│   NON_RETRYABLE aborts immediately    │  │  SCHEMA (core/schema.py)          │
│                                       │  │                                   │
│  errors.py    taxonomy                │  │  CatalogRecord  strict vocabulary │
│   WorkerError       retryable         │  │  StatusRecord   loose, for hosts  │
│   PermanentError    settled           │  │                 we do not control │
│   SchemaError       contract          │  │  extra="ignore" additive upstream │
│   CircuitOpenError  refused           │  │    changes are not our outage     │
│   WorkerUnavailable optional dep      │  │  field-level detail preserved     │
└──────────────────────────────────────┘  └──────────────────────────────────┘
```

---

## Components

### 1. CLI (`cli.py`)

Three commands. Entry point `parallel-ops`.

| Command | Purpose |
|:--|:--|
| `run INPUT` | Executes the batch. Streams a line per result, prints the rollup, writes output, exits 1 unless every item succeeded |
| `plan INPUT` | Counts, resolved policy, and a per-host duration floor. Builds no workers and makes no calls |
| `doctor` | Worker availability with install commands, schemas and their fields, active settings, and the two retry rules in prose |

CLI flags are collected into a dict, `None` entries dropped, and the rest passed to `Settings(**overrides)` so environment variables stay in force for anything not supplied.

`_progress_hook` streams per result rather than rendering a percentage bar, because the useful signal on a guarded batch is *which* outcome is accumulating. A run filling up with `rejected` is a completely different situation from one filling up with `invalid`, and a bar at 60% looks identical in both.

`run` exits non-zero unless `report.ok == report.total`, so a scheduler or shell script cannot mistake a partial batch for success.

### 2. Models (`models.py`)

Three enums and four models.

**`Outcome`** — `ok`, `invalid`, `failed`, `rejected`. The `failed`/`rejected` split is load-bearing: a rejected item was never sent, so it is safe to re-run once the dependency recovers; a failed item was genuinely attempted and needs someone to look at it.

**`WorkItem`** — `item_id`, `host` (the key for both guards), `kind`, `path`, `extras`. `target()` joins host and path without doubling slashes.

**`WorkResult`** — outcome plus `attempts` (physical calls), `waited_s` (limiter), `duration_s`, `breaker_state` *as observed at admission*, `data`, `error`. Recording the observed state rather than reading it later is what lets you tell a rejected item from a failed one after the fact, since the state may have changed by the time anything looked again.

**`HostReport`** / **`BatchReport`** — per-dependency and overall rollup. `amplification` is `attempts / total`: above 1.0 means retries are firing, below 1.0 means the breaker refused work before it cost anything. Reading it next to `rejected` distinguishes "retries are riding out a blip" from "retries are feeding a breaker".

`as_row()` produces the flat dict every writer consumes, so CSV, JSON and Excel cannot drift apart.

### 3. Resilience (`resilience/`)

**`TokenBucket`** — per host. Tokens accrue at `rate` up to `burst`; each call spends one.

The lock is deliberately held across the wait. That serialises waiters into FIFO order, costing a little parallelism at the limiter, but preventing an unlucky task from being starved while newer tasks keep taking tokens. For a client trying not to exceed someone else's quota, predictable ordering is worth more than the last few percent of throughput.

`acquire` performs **one computed sleep, never a loop** — see Concurrency Correctness below, and ADR-001. `TOKEN_EPSILON = 1e-9` absorbs float slack on the near-satisfied case.

A request larger than `burst` raises rather than blocking forever, because a batch that appears to hang with no explanation is worse than a loud failure.

**`CircuitBreaker`** — three states, one per host, `clock` injected.

`state` reports `HALF_OPEN` once an open breaker's timeout has elapsed, so a status display agrees with what `admit()` would do next; `admit()` performs the actual transition under the lock.

**Half-open admits exactly one probe.** The obvious implementation flips to half-open on timer expiry and lets waiting work through, handing the whole backlog to a dependency that has just recovered and knocking it over again. `_probe_in_flight` allows one; everything else keeps being refused until that one reports. A failed probe re-opens immediately and restarts the timer — one failure is sufficient evidence that recovery has not happened, and waiting for `failure_threshold` probes would send exactly the burst half-open exists to prevent.

**`GuardRegistry`** — lazily creates one breaker and one limiter per host. Per-host rather than global, because a global breaker means one sick dependency halts work against every healthy one, and the batch reports itself as broken when in fact one of three hosts was down.

**`RetryPolicy`** — exponential with proportional jitter. The cap bounds the *base* before jitter is applied, so the spread stays proportional at every step; capping the jittered value instead would shrink the spread to nothing at the ceiling, re-synchronising retries exactly where a storm is most likely.

Jitter is not decoration. A batch that fans 200 items at one host sees them all fail at nearly the same instant when it goes down, and without jitter every one waits the identical backoff and retries in the same millisecond.

`call_with_retry` knows nothing about the circuit breaker. Composition is the executor's job, which keeps the ordering rule stated in one readable place instead of emerging from two classes calling each other.

### 4. Executor (`core/executor.py`)

`execute_item` runs one item through the guard stack. The ordering is the most important thing in the package:

1. **Breaker first.** An open circuit refuses the item having spent nothing. Checking the limiter first would mean waiting for a token, and burning it, for a call that was never going to be made — on a fully rejected batch, the entire run's quota.
2. **Limiter second.** A token is spent only once the call will actually happen.
3. **Retries inside, breaker outside.** One verdict per logical operation.
4. **Validation last and outside the retry.** A `SchemaError` is not retryable, so nothing is gained by validating inside the loop, and keeping it outside makes the ordering legible: the call either reached the dependency or it did not, and only then do we ask whether the answer was the expected shape.

`counts_against_breaker()` is a free function so the classification is testable in isolation and readable without following the control flow.

### 5. Scheduler (`core/scheduler.py`)

Bounded fan-out via `asyncio.Semaphore`, fan-in via `asyncio.gather`, which preserves argument order regardless of completion order — the output file has to line up with the input file for anyone to correlate them.

Both worker kinds share one semaphore: a browser item and an API item are the same kind of thing to the scheduler, differing only in duration and which limiter they draw from.

`_run_one` wraps execution so any unexpected exception becomes a `FAILED` row carrying the exception text. One bad item never takes the batch down. The cost is the usual one — a misconfiguration produces N failures that each look like a data problem — which is why `doctor` and `plan` exist as pre-flight checks.

`plan_summary`, `outcome_counts` and `worst_state` are pure functions over items or results, used by the CLI and directly testable.

### 6. Schema (`core/schema.py`)

`CatalogRecord` constrains `status` to a known vocabulary; `StatusRecord` deliberately does not, because a bare status probe runs against endpoints whose vocabulary we do not control.

Both use `extra="ignore"`. Upstream adding a field is not a reason to fail a batch, and a strict model turns every harmless additive change into an outage on our side. Fields disappearing or changing type is what we actually want to hear about.

`get_schema("none")` disables validation, which matters for a first run against an unfamiliar endpoint: you want to see what actually comes back before committing to a contract.

`validate_payload` raises `SchemaError` carrying Pydantic's field-level detail, truncated at three errors with a count. "3 items invalid" is not actionable; `status: Input should be 'active', 'suppressed', 'pending' or 'removed'` tells you immediately that upstream started returning uppercase. Losing that detail to a generic message is the most common way response validation ends up switched off by whoever is on call.

### 7. Workers (`workers/`)

`Worker` ABC: `execute()` returns a dict or raises from the taxonomy; `is_available()` / `unavailable_reason()` support `doctor`; `aclose()` is idempotent.

**`ApiWorker`** — one `httpx.AsyncClient` reused for the whole batch, so pooling and keep-alive actually apply. Creating a client per item is the common mistake and it turns a batch of 500 into 500 TLS handshakes. An injected client is not closed by the worker, since closing something the caller owns breaks the next batch that shares it and the bug surfaces far from here.

Status classification: `RETRYABLE_STATUS` (408, 425, 429, 5xx subset) is checked *before* the generic 4xx rule, so 429 — the one 4xx that means "try again later" — stays retryable. Other 4xx are `PermanentError`. Unlisted 5xx are retryable. A non-JSON body or a non-object top level is a `WorkerError`, because that is usually a proxy error page or a truncated response.

**`BrowserWorker`** — optional. The Playwright import is deferred so `doctor` and the entire API path work on a machine that has never installed a browser. One browser and context shared across the batch; a fresh page per item is cheap, a fresh browser costs about a second of process startup and would dominate everything the guards are managing.

It runs on the shared event loop rather than a thread pool. A page load takes hundreds of milliseconds and yields throughout, so API items keep flowing while it waits. A thread pool would mean two concurrency budgets, two places to reason about the rate limit, and per-host guards that must be thread-safe as well as async-safe.

`get_worker()` checks availability before construction, so a missing optional dependency reads as an instruction rather than an `ImportError` from three frames down inside a coroutine.

### 8. IO (`io.py`)

`load_items` reads CSV (with `utf-8-sig`, since Excel writes a BOM) or JSON. Columns outside `KNOWN_ITEM_FIELDS` become `extras` and are carried to the output, so a `region` or `owner` column survives the run — without that, correlating output to input means a spreadsheet join somebody will get wrong. An unknown `kind` falls back to `api` rather than aborting; a row with no host is skipped because a host is the key for both guards.

Writers share `ROW_FIELDS` for a stable column order. Excel colours the outcome column by value and wraps the error column. `openpyxl` is imported lazily with an error naming the extra to install.

`write_results` takes the complete result set plus an optional `emit` narrowing — see Audit Trail below.

---

## Data Flow

```
1.  parallel-ops run items.csv -o out/ --rate 20 --failure-threshold 2
2.  CLI drops None overrides -> Settings(rate=20.0, failure_threshold=2, ...)
3.  load_items() -> 6 WorkItem; a `region` column lands in extras
4.  run_batch(): kinds = {API} -> build_workers builds ONLY ApiWorker
      (Playwright is never imported)
5.  GuardRegistry(rate=20, failure_threshold=2); Scheduler(concurrency=8)
6.  Semaphore(8) + gather over 6 bounded coroutines
7.  For OK-1:
      breaker.admit()          CLOSED -> (True, CLOSED)
      limiter.acquire()        bucket full -> waited 0.0, granted=1
      call_with_retry          ApiWorker GET .../items/OK-1 -> 200 JSON
      breaker.record_success()
      validate_payload         CatalogRecord -> normalised dict
      -> Outcome.OK, attempts=1
8.  For MISSING-1:
      404 -> PermanentError; NON_RETRYABLE so no second attempt
      counts_against_breaker(PermanentError) is False -> record_success()
      -> Outcome.FAILED, attempts=1, circuit still CLOSED
9.  For BAD-1:
      200 with status "ACTIVE" -> SchemaError from validate_payload
      breaker already told success; not retried
      -> Outcome.INVALID, data = the raw payload, error names the field
10. _build_report: per-host rollup, trips and final state from the registry
11. write_results(results, report, out/, "csv")   <- full set, no emit
      out/results.csv        6 rows
      out/full_report.json   summary + amplification + all 6 rows
12. report.ok (4) != report.total (6) -> sys.exit(1)
```

Observed, against a local server:

```
total           6                     total           8   (503s, threshold 2, conc 1)
ok              4                     ok              0
invalid         1                     failed          2
failed          1                     rejected        6
rejected        0                     attempts        4  (0.50 per item)
attempts        6  (1.00 per item)    trips           1   state open
```

Amplification of exactly 1.00 in the first run confirms neither the 404 nor the schema violation was retried. Below 1.0 in the second confirms the breaker refused six items before they cost a call.

---

## Concurrency Correctness

### The limiter's loop could not terminate

`acquire` originally re-checked `self._tokens >= tokens` in a `while True` after each sleep — the shape almost every token bucket has. The suite hung indefinitely: 35 of 36 tests finished in about 0.01s each and one never returned.

Tokens accrue as `elapsed * rate`, both floats. Depending on the clock value that product sometimes lands a few parts in 1e13 *below* the target:

```
acquire #13: tokens=0.9999999999995453   0.9999999999995453 >= 1.0 -> False
             delay = (1.0 - tokens)/10   = 4.5474735088646414e-14
             ulp(1000.3)                 = 1.1368683772161603e-13
             1000.3 + 4.5e-14 == 1000.3  -> the clock cannot represent it
             elapsed = 0 -> no tokens accrue -> identical delay next iteration
```

Two things make this worth recording. **A real clock hides it** — `time.monotonic()` advances regardless, so real time passing during the event-loop round trip supplies the missing tokens. What ships is an occasional hot spin, invisible to any test using real time. The deterministic clock built for *exact assertions* turned out to be a correctness tool.

And **the first hypothesis was wrong**: I predicted failure at acquire #11 on the theory that `(1000.0 + 0.1) - 1000.0` underestimates 0.1. It overestimates it (`0.10000000000002274`), so #11 succeeds. The error direction varies with the clock value. Verifying instead of assuming is what found it.

The fix deletes the loop. The lock is held for the whole method, so no other caller can take the tokens this one waits for — the wait is uncontended and one computed sleep is exact. The loop had been defending against a race the lock already prevented. Non-termination is now impossible by construction rather than by argument.

Widening the comparison instead would fix the observed case and leave the class open: at 1e6/s an epsilon-sized deficit yields a 1e-15s delay, still below float spacing at any realistic clock value.

### FIFO fairness costs throughput deliberately

The lock spans the wait, so waiters are served in arrival order. Releasing it while sleeping would let a late arrival take a token an earlier waiter was queued for, and on a long batch some item waits indefinitely. `test_waiters_are_served_in_arrival_order` and `test_fifo_order_survives_the_rewrite` pin it.

This is also the invariant the single computed sleep depends on. Someone optimising for throughput by releasing the lock would reintroduce a real race and the single sleep would become wrong rather than merely unfair. The docstring says so; nothing enforces it.

### Half-open admits exactly one probe

`test_concurrent_admits_while_half_open_yield_one_winner` fires 50 concurrent `admit()` calls at a half-open breaker and asserts exactly one is admitted. `test_probe_slot_is_released_after_a_failure` asserts a failed probe does not leave the slot stuck, since that would deadlock recovery permanently.

### The breaker cannot stop the first wave

With concurrency C, the first C items are admitted before any has reported back, so a threshold below C cannot trip in time. This is inherent to concurrent execution rather than an oversight: a breaker bounds *sustained* traffic to a failing dependency, not the initial burst. Lowering concurrency narrows the window. The honest framing is that the breaker bounds how long an outage costs, not whether the first wave lands.

---

## Audit Trail

`write_json` carries a docstring promising the JSON report always contains every row even when the CSV is trimmed with `--ok-only`. It did not.

`write_results` took one list and handed the same list to both `write_csv` and `write_json`, while the CLI passed already-filtered rows. `--ok-only` therefore dropped the failures out of the artifact whose entire purpose is to record them. An integration test caught it.

The fix inverted the signature. The complete set is the required positional argument and always reaches the report; filtering is an explicit keyword narrowing:

```python
def write_results(results, report, directory, fmt, *, emit=None):
    rows = results if emit is None else emit
    ...                                    # csv/excel get `rows`
    write_json(results, report, report_path)   # the report always gets everything
```

That inverts the failure mode. A caller who forgets now fails to filter, which is immediately visible, instead of dropping rows from an audit file, which is not. The docstring had been right and the code wrong — a comment describing an invariant is not the same as enforcing one, and this is the second time in this project that a stated intention and the implementation disagreed.

---

## Configuration

`PO_`-prefixed environment variables or a `.env` file. No list-typed fields, because Pydantic Settings parses those as JSON and `PO_HOSTS=a,b` yields a confusing decode error.

```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PO_", env_file=".env", extra="ignore")

    concurrency: int = 8

    rate: float = 10.0
    burst: float = 0.0            # 0 means "derive from rate"

    failure_threshold: int = 5
    reset_timeout: float = 30.0
    success_threshold: int = 1

    attempts: int = 3
    base_delay: float = 0.2
    max_delay: float = 5.0
    jitter: float = 0.1

    timeout: float = 10.0
    headless: bool = True
    schema_name: str = "catalog"

    output_dir: Path = Path("output")
    output_format: str = "csv"
    log_level: str = "INFO"
```

`effective_burst` returns `None` for zero, letting the bucket derive burst from rate. Zero means "not set" rather than "no burst", because a burst of zero would block every call forever and there is no sensible reason to configure that.

The defaults are conservative on purpose. A tool whose entire job is to avoid hammering someone else's service should not ship with a rate of 100/s and 64-way concurrency, because the first run anybody does will be with the defaults.

---

## Running

```bash
bash setup.sh                 # asserts the limiter terminates and the guards fire
bash setup.sh --browser       # + Playwright and chromium
bash setup.sh --all
source .venv/bin/activate

parallel-ops doctor
parallel-ops plan data/sample_items.csv --rate 2
parallel-ops run data/sample_items.csv -o out/
parallel-ops run items.csv -o out/ --rate 2 --concurrency 4 --failure-threshold 3
parallel-ops run items.csv -o out/ --schema none
parallel-ops run items.csv -o out/ --ok-only --format excel
```

`setup.sh` does more than install. It reports worker availability, then asserts: 30 sequential acquires terminate and bill 2.0s of waiting; a clock that refuses to advance produces exactly one sleep rather than a spin; a bad payload is `INVALID` and not retried; three attempts record one breaker verdict; six 404s leave a five-failure breaker closed; an open circuit spends no token; and `--ok-only` narrows the CSV while the report keeps every row.

---

## Testing

```bash
pytest                          # 241 tests, ~0.33s
pytest tests/unit -v
pytest tests/integration -v
pytest --cov=parallel_ops --cov-report=term-missing
```

| File | Covers |
|:--|:--|
| `test_resilience.py` | bucket accrual, cap, validation, FIFO; breaker states, thresholds, half-open stampede, probe release; registry per-host isolation; `TestWhyOneSleepNotALoop` pins the hang six ways |
| `test_executor.py` | `counts_against_breaker` per error class; guard ordering incl. an open circuit spending no token; one verdict per operation; validation outside the retry; backoff shape and the cap-before-jitter rule |
| `test_scheduler.py` | input-order fan-in, missing worker, exploding worker, report rollup, amplification, per-host isolation end to end, `plan_summary`, `worst_state` |
| `test_schema.py` | schema resolution, disabling validation, field-level error detail, truncation at the limit, `extra="ignore"` on every shipped schema |
| `test_workers.py` | every retryable and permanent status via `httpx.MockTransport`, 429 precedence, transport and body failures, client lifecycle, registry availability |
| `test_io_and_models.py` | CSV/JSON loading, extras, BOM, skipped rows; row shape; all three writers; `emit` narrowing without touching the report |
| `integration/test_cli.py` | all three commands, `plan` proving it builds no workers, exit codes, `--ok-only` end to end, progress streaming |

Time is injected everywhere — `clock` and `sleep` are constructor arguments on the bucket, breaker and retry policy. Nothing sleeps, so timing assertions are exact rather than `0.19 < waited < 0.21`, which is what made the float-precision bug visible at all.

CI bounds the suite with `timeout 120`. A hang is a real failure mode in this codebase, and the runner should report it rather than burn twenty minutes discovering it.

Not covered: real network calls and a real browser. Status classification and body handling are tested against a mock transport; `BrowserWorker`'s availability and deferred import are tested, its Playwright interaction is not.

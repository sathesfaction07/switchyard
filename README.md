# ParallelOps

Runs a batch of heterogeneous operations concurrently on one event loop, behind a per-host token bucket and circuit breaker, validating every response against a declared schema. HTTP and browser work are scheduled by the same governor and share the same guards.

---

## The Problem

A batch tool that talks to someone else's service has two jobs beyond the obvious one, and both are usually missing.

**It has to not hurt the dependency.** Fan 500 items out with `asyncio.gather` and you send 500 requests as fast as the event loop can manage. If the service is fine, you have spiked its error rate. If it is already struggling, you have finished it off — and then your retry logic sends the same 500 again with backoff, arriving in a synchronised wave.

**It has to tell you which of several things went wrong.** "412 items failed" is not a finding. There are at least four distinct situations behind that number and they need different responses:

| What happened | What it means | What to do |
|:--|:--|:--|
| Timeouts and 5xx | the dependency is unhealthy | wait, retry, escalate |
| 404s | your input is stale | fix the input file |
| Responses in the wrong shape | the contract changed | talk to whoever owns it |
| Never sent | the circuit was open | re-run once it recovers |

Collapsing these into `failed` is how a run against a perfectly healthy service gets reported as an outage — which is not hypothetical; it is one of the bugs described below.

---

## How It Works

```
item -> breaker.admit() -> limiter.acquire() -> retry(worker.execute) -> validate -> result
         |                                                                |
         refused -> rejected                                     bad shape -> invalid
```

The order is the substance of this package:

**Breaker first.** If the circuit is open the item is refused having spent nothing — no token, no call. Checking the limiter first would mean waiting for a token, and burning it, for a call that was never going to happen. On a batch that gets fully rejected, that is the entire run's quota spent on nothing.

**Limiter second.** A token is spent only once the call is actually going to be made.

**Retries inside, breaker outside.** The retry loop knows nothing about the breaker. The breaker is told exactly once, afterwards, whether this *logical* operation succeeded.

**Validation last, and outside the retry.** A malformed response will be malformed again, so retrying it burns quota to learn nothing.

Every result carries the reason it ended up where it did, so the output file explains itself.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CLI (parallel-ops)                                │
│                                                                         │
│   run                    plan                   doctor                  │
│   execute a batch        what would happen,     what this machine can   │
│   + live per-item        incl. a duration       run, + the active       │
│     progress             floor. Calls nothing.  policy in words         │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     SCHEDULER (core/scheduler.py)                        │
│                                                                         │
│   asyncio.Semaphore(concurrency) -> bounded fan-out                     │
│   asyncio.gather                 -> fan-in, input order preserved       │
│   per-item try/except            -> one bad item never ends the batch   │
│   _build_report                  -> per-host rollup, trips, final state │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      EXECUTOR (core/executor.py)                         │
│                                                                         │
│   1. breaker.admit()          open      -> REJECTED, nothing spent      │
│   2. limiter.acquire()        one token per logical item                │
│   3. call_with_retry(worker)  retries do not touch the breaker          │
│   4. record_success/failure   exactly one verdict per item              │
│   5. validate_payload()       bad shape -> INVALID, breaker untouched   │
│                                                                         │
│   counts_against_breaker(): only WorkerError. Not 404, not schema.      │
└───────┬─────────────────────────────────────────┬───────────────────────┘
        │                                         │
        ▼                                         ▼
┌────────────────────────────────┐   ┌────────────────────────────────────┐
│  RESILIENCE (resilience/)       │   │  WORKERS (workers/)                 │
│                                 │   │                                     │
│  limiter.py  TokenBucket        │   │  base.py     Worker ABC             │
│   - per host                    │   │              + availability         │
│   - lock held across the wait   │   │                                     │
│     -> FIFO, no starvation      │   │  api.py      ApiWorker              │
│   - ONE computed sleep, no loop │   │              one shared httpx       │
│                                 │   │              client per batch       │
│  breaker.py  CircuitBreaker     │   │              status -> error class  │
│   - closed / open / half_open   │   │                                     │
│   - half-open admits exactly    │   │  browser.py  BrowserWorker          │
│     ONE probe (_probe_in_flight)│   │              optional Playwright,   │
│   - GuardRegistry: per-host     │   │              deferred import, one   │
│                                 │   │              context per batch      │
│  retry.py    RetryPolicy        │   └────────────────────────────────────┘
│   - exponential + proportional  │
│     jitter, cap bounds the base │   ┌────────────────────────────────────┐
│   - non-retryable set           │   │  SCHEMA (core/schema.py)            │
│                                 │   │                                     │
│  errors.py   taxonomy:          │   │  CatalogRecord, StatusRecord        │
│   WorkerError      retryable    │   │  extra="ignore" -> additive         │
│   PermanentError   settled      │   │    upstream changes are not         │
│   SchemaError      contract     │   │    outages on our side              │
│   CircuitOpenError refused      │   │  field-level errors preserved       │
└────────────────────────────────┘   └────────────────────────────────────┘
```

---

## Project Structure

```
parallel-ops/
├── src/parallel_ops/
│   ├── cli.py                 run / plan / doctor
│   ├── config.py              Pydantic Settings, PO_ prefix
│   ├── models.py              WorkItem, WorkResult, HostReport, BatchReport
│   ├── io.py                  CSV/JSON in, CSV/JSON/Excel out
│   ├── core/
│   │   ├── executor.py        the guard ordering, one item
│   │   ├── scheduler.py       fan-out/fan-in, report rollup, plan
│   │   ├── schema.py          response contracts
│   │   └── pipeline.py        wiring, and closing workers
│   ├── resilience/
│   │   ├── limiter.py         TokenBucket
│   │   ├── breaker.py         CircuitBreaker + GuardRegistry
│   │   ├── retry.py           RetryPolicy + call_with_retry
│   │   └── errors.py          error taxonomy
│   └── workers/
│       ├── base.py            Worker ABC
│       ├── api.py             httpx
│       └── browser.py         Playwright, optional
├── data/sample_items.csv
├── tests/                     241 tests
└── docs/adr/
```

---

## Running

```bash
bash setup.sh                 # verifies the guards actually fire
bash setup.sh --browser       # + Playwright and chromium
bash setup.sh --all
source .venv/bin/activate

# What can this machine run, and what is the policy?
parallel-ops doctor

# What would this batch do? Calls nothing.
parallel-ops plan data/sample_items.csv --rate 2

# Run it
parallel-ops run data/sample_items.csv -o out/

# Slower, stricter, and shout sooner
parallel-ops run items.csv -o out/ --rate 2 --concurrency 4 --failure-threshold 3

# See what an unfamiliar endpoint actually returns before committing to a contract
parallel-ops run items.csv -o out/ --schema none

# Emit only the clean rows. The JSON report still contains everything.
parallel-ops run items.csv -o out/ --ok-only --format excel
```

Input is CSV or JSON with `item_id` and `host`; `path` and `kind` are optional. Any other column is carried through to the output, so a `region` or `owner` column survives the run.

`run` exits non-zero unless every item succeeded, so a scheduler or shell script sees a partial batch as a failure.

---

## Usage Reference

### `run INPUT_PATH`

| Option | Purpose |
|:--|:--|
| `-o, --output` | Results directory. Default `output/` |
| `-f, --format` | `csv`, `json`, `excel` |
| `-c, --concurrency` | Items in flight. Default 8 |
| `--rate`, `--burst` | Per-host token bucket. Default 10/s, burst = rate |
| `--attempts` | Tries per item. Default 3 |
| `--failure-threshold` | Failures before a circuit opens. Default 5 |
| `--reset-timeout` | Seconds before a probe is allowed. Default 30 |
| `--schema` | `catalog`, `status`, or `none` |
| `--ok-only` | Trim the tabular output. The report keeps everything |
| `--progress/--no-progress` | Stream each result as it lands |

### `plan INPUT_PATH`

Item, host and kind counts, the resolved policy, and a per-host duration floor from items ÷ rate. Seeing *"at least 50s"* before starting a 500-item run at 10/s is the difference between choosing a rate and discovering one. Builds no workers and makes no calls, so it is safe against a production host list.

### `doctor`

Worker availability with an install command for anything missing, the registered schemas and their fields, the active settings, and the two retry rules most likely to surprise someone reading a result file.

---

## Outcomes

| Outcome | Meaning | Retried? | Counts against the breaker? |
|:--|:--|:--|:--|
| `ok` | succeeded and validated | – | no (resets the failure run) |
| `invalid` | arrived, wrong shape | no | no |
| `failed` | attempted, did not satisfy | only `WorkerError` | only `WorkerError` |
| `rejected` | circuit open, never sent | no | no |

`rejected` versus `failed` is the load-bearing distinction: rejected work is safe to re-run once the dependency recovers, failed work needs somebody to look at it.

Against a local server returning a mix of 200s, a 404 and a bad payload:

```
total           6
ok              4
invalid         1
failed          1
rejected        0
attempts        6  (1.00 per item)
```

Amplification of exactly 1.00 is the check that the classification works — neither the 404 nor the schema violation was retried.

And with a host returning 503s, a threshold of 2 and concurrency 1:

```
total           8      host                     ok  inv  fail  rej  trips  state
failed          2      127.0.0.1:18771           0    0     2    6      1  open
rejected        6
attempts        4  (0.50 per item)
```

Fewer than one call per item, because six were refused before spending anything.

---

## Testing

```bash
pytest                          # 241 tests
pytest tests/unit -v
pytest tests/integration -v     # CLI end to end via CliRunner
pytest --cov=parallel_ops --cov-report=term-missing
```

Time is injected everywhere — `clock` and `sleep` are constructor arguments on the bucket, the breaker and the retry policy. Nothing in the suite sleeps, so timing assertions are exact rather than `0.19 < waited < 0.21`, and the whole thing runs in about a third of a second.

That decision paid for itself in a way I did not expect; see the first bug below.

The HTTP layer is tested against `httpx.MockTransport`, so status classification and body handling are covered without touching a network. The CLI tests stub worker construction for the same reason.

---

## Configuration

`PO_`-prefixed environment variables or a `.env` file. No list-typed fields, because Pydantic Settings parses those as JSON and `PO_HOSTS=a,b` produces a confusing decode error.

```bash
PO_CONCURRENCY=8
PO_RATE=10.0
PO_BURST=0                  # 0 means "derive from rate"

PO_FAILURE_THRESHOLD=5
PO_RESET_TIMEOUT=30.0
PO_SUCCESS_THRESHOLD=1

PO_ATTEMPTS=3
PO_BASE_DELAY=0.2
PO_MAX_DELAY=5.0
PO_JITTER=0.1

PO_TIMEOUT=10.0
PO_HEADLESS=true
PO_SCHEMA_NAME=catalog
PO_OUTPUT_DIR=output
PO_OUTPUT_FORMAT=csv
```

The defaults are deliberately timid. A tool whose entire job is to avoid hammering someone else's service should not ship with a rate of 100/s and 64-way concurrency, because the first run anybody does will be with the defaults.

---

## Tradeoffs and Decisions I Made Along the Way

**A deterministic clock found an infinite loop that a real clock would have hidden.** The bucket's `acquire` re-checked `self._tokens >= tokens` in a `while True` after each sleep. Tokens accrue as `elapsed * rate` on a float clock, so that exact comparison periodically landed a few parts in 1e13 short of the target. The loop then asked for a corrective sleep of about 5e-14 seconds — smaller than the float64 spacing at a clock value of 1000. The clock could not represent it, `elapsed` came back as zero, no tokens accrued, and the next iteration computed the same delay. The suite hung, indefinitely, on the thirteenth acquire.

A real `time.monotonic()` escapes this, because time passes whether or not you asked it to, which is exactly why it is worth writing up: the bug was invisible under a real clock and would have shipped as an occasional hot spin of event-loop round trips. The deterministic clock I had built for *exact assertions* turned out to be a correctness tool.

I fixed it by deleting the loop rather than widening the comparison. The lock is held for the whole method, so no other caller can take the tokens this one is waiting for — the wait is uncontended, and one computed sleep is therefore exact. The loop had been defending against a race the lock already prevented. The replacement is shorter, provably terminating, and there is a test that drives it with a clock which refuses to advance at all.

**A test caught the tool quietly filtering its own audit trail.** `write_json` carries a docstring promising the JSON report always contains every row even when `--ok-only` trims the CSV. It did not. `write_results` took one list of results and handed the same list to both writers, so the CLI's filtered rows reached both. Passing `--ok-only` silently dropped the failures out of the artifact whose entire purpose is to record them.

The fix was to make the complete set the required argument and filtering the optional narrowing, rather than the other way round. That inverts the failure mode: a caller who forgets now fails to filter, which is immediately visible, instead of dropping rows from an audit file, which is not. Worth noting that the docstring was right and the code was wrong — a comment describing an invariant is not the same as enforcing one.

**404s and schema errors do not count against the breaker, and that took a wrong version to see.** The first implementation counted every failure. A batch of six items with three-week-old product IDs then tripped a five-failure breaker and rejected everything after item five, so the run reported the catalog as down when the catalog was entirely healthy and the input file was stale.

A breaker answers one question: is this dependency unhealthy. A 404 is evidence of the opposite — it answered correctly and promptly. Schema errors are excluded for the same reason plus a sharper one: if upstream ships a breaking change, every item fails validation, and tripping on that converts the run into a wall of `rejected` rows where the field-level detail explaining *what* changed exists only for the first few items. That detail is the entire value of validating.

**One verdict per logical operation, not per attempt.** This was the longest bug to find. With three attempts and a five-failure threshold, two bad items produce six recorded failures and trip the circuit. The breaker ends up measuring the retry policy rather than the dependency, so tightening retries silently loosens the breaker — and those two settings are normally owned by different people. The retry loop now knows nothing about the breaker; composition happens in `executor.py`, in one readable place, rather than emerging from two classes calling each other.

**Half-open admits exactly one probe.** The obvious implementation flips to half-open when the timer expires and lets waiting work through, which hands the entire backlog to a dependency that has just come back up and knocks it over again. `_probe_in_flight` allows one request; everything else keeps being refused until that one reports back. There is a test that fires fifty concurrent `admit()` calls and asserts exactly one wins.

**The lock is held across the limiter's wait, costing throughput to buy fairness.** Waiters are served in arrival order. Releasing the lock while sleeping would let a late arrival take a token an earlier waiter was queued for, and on a long batch some unlucky item waits indefinitely. For a client trying to stay inside someone else's quota, predictable ordering is worth more than the last few percent of throughput.

**One token per item, not per attempt.** Debatable, and I went back and forth. As it stands a retrying batch can exceed the nominal rate, which is a real flaw. The alternative makes a partial outage throttle the healthy work queued behind it, and the breaker is already the mechanism for a sustained problem. I would revisit this if the rate limit were a hard contractual ceiling rather than a courtesy.

**The breaker cannot stop the first wave, and that is inherent.** With concurrency C, the first C items are all admitted before any of them has reported back, so a five-failure threshold cannot trip until at least C calls are in flight. A circuit breaker bounds *sustained* traffic to a failing dependency, not the initial burst. Lowering concurrency tightens the window. The honest framing is that the breaker bounds how long an outage costs, not whether the first wave lands.

**Playwright shares the event loop instead of a thread pool.** A page load takes hundreds of milliseconds and yields throughout, so API items keep flowing while it waits. Running the browser in a thread pool would mean two concurrency budgets and two places to reason about the rate limit, and the per-host guards would have to be made thread-safe on top of being async-safe.

**Everything is in memory and there is no checkpoint.** A 50,000-item batch holds 50,000 results, and an interrupted run starts over. AdaptiveFetch in this portfolio solves resumability and resource-adaptive batching; this project is about the guards, and adding a checkpoint store would have buried them. The two are complementary: AdaptiveFetch reacts to *local* pressure — memory, disk, RSS — while ParallelOps reacts to *remote* health. Neither subsumes the other, and I checked that before starting.

---

## Stack

| Concern | Choice |
|:--|:--|
| Language | Python 3.11+, asyncio |
| HTTP | httpx (async, one pooled client per batch) |
| Browser | Playwright, optional extra |
| Models + config | Pydantic, Pydantic Settings |
| CLI | Click |
| Output | stdlib csv/json, openpyxl |
| Tests | pytest, pytest-asyncio, httpx.MockTransport, CliRunner |
| Lint | ruff |

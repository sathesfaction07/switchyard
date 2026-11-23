#!/usr/bin/env bash
set -euo pipefail

BLUE='\033[0;34m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'; RED='\033[0;31m'; NC='\033[0m'
info() { echo -e "${BLUE}==>${NC} $1"; }
ok()   { echo -e "${GREEN} ok ${NC} $1"; }
warn() { echo -e "${YELLOW}warn${NC} $1"; }
fail() { echo -e "${RED}fail${NC} $1"; exit 1; }

WITH_BROWSER=0
WITH_EXCEL=0
for arg in "$@"; do
    case "$arg" in
        --browser) WITH_BROWSER=1 ;;
        --excel)   WITH_EXCEL=1 ;;
        --all)     WITH_BROWSER=1; WITH_EXCEL=1 ;;
        -h|--help)
            cat <<'USAGE'
Usage: bash setup.sh [options]

  --browser   also install Playwright and chromium, for browser work items
  --excel     also install openpyxl, for .xlsx output
  --all       both
  -h          this message

With no options you get a fully working install for API work items. The browser
worker is optional and its absence is reported by `switchyard doctor` with the
command needed to add it.
USAGE
            exit 0 ;;
        *) fail "unknown option: $arg (try --help)" ;;
    esac
done

info "Checking Python"
command -v python3 >/dev/null 2>&1 || {
    echo "python3 was not found. Install it first:"
    echo "  macOS:         brew install python@3.12"
    echo "  Ubuntu/Debian: sudo apt install python3 python3-venv"
    echo "  RHEL/Fedora:   sudo dnf install python3"
    fail "python3 missing"
}

PY_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
if [ "$(python3 -c 'import sys; print(1 if sys.version_info >= (3,11) else 0)')" != "1" ]; then
    echo "Python 3.11 or newer is required. Found $PY_VER."
    echo "  macOS:         brew install python@3.12"
    echo "  Ubuntu/Debian: sudo add-apt-repository ppa:deadsnakes/ppa && sudo apt install python3.12 python3.12-venv"
    fail "python $PY_VER too old"
fi
ok "Python $PY_VER"

info "Creating virtual environment"
[ -d .venv ] || python3 -m venv .venv || fail "could not create .venv (Debian/Ubuntu: sudo apt install python3-venv)"
ok ".venv ready"

EXTRAS="dev"
[ "$WITH_BROWSER" = "1" ] && EXTRAS="$EXTRAS,browser"
[ "$WITH_EXCEL" = "1" ] && EXTRAS="$EXTRAS,excel"

info "Installing dependencies [$EXTRAS]"
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -e ".[$EXTRAS]" || fail "pip install failed"
ok "dependencies installed"

if [ "$WITH_BROWSER" = "1" ]; then
    info "Installing chromium for Playwright"
    .venv/bin/playwright install chromium || warn "chromium install failed; the api worker still works"
fi

info "Verifying imports"
.venv/bin/python - <<'PYCHECK' || fail "core imports failed"
import click, httpx                                        # noqa: F401
from pydantic_settings import BaseSettings                  # noqa: F401
from parallel_ops.core.executor import execute_item         # noqa: F401
from parallel_ops.core.scheduler import Scheduler           # noqa: F401
from parallel_ops.resilience.breaker import GuardRegistry   # noqa: F401
from parallel_ops.resilience.limiter import TokenBucket     # noqa: F401
from parallel_ops.workers import availability               # noqa: F401
PYCHECK
ok "imports resolve"

info "Reporting worker availability"
.venv/bin/python - <<'PYCHECK'
from parallel_ops.workers import availability

for name, (ready, reason) in sorted(availability().items()):
    if ready:
        print(f"     {name:<10} available")
    else:
        print(f"     {name:<10} unavailable  {reason}")
PYCHECK

info "Checking the limiter terminates on a clock that cannot advance"
.venv/bin/python - <<'PYCHECK' || fail "the limiter did not terminate"
import asyncio

from parallel_ops.resilience.limiter import TokenBucket


class Clock:
    def __init__(self, start=1_000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


async def main():
    # The regression this guards: acquire used to loop, re-checking an exact float
    # comparison. A sub-ulp corrective delay left the clock unable to move and it spun
    # forever. 30 sequential acquires used to hang on the thirteenth.
    clock = Clock()
    slept = []

    async def sleep(seconds):
        slept.append(seconds)
        clock.advance(seconds)

    bucket = TokenBucket(10, burst=10, clock=clock, sleep=sleep)
    for _ in range(30):
        await asyncio.wait_for(bucket.acquire(), timeout=2.0)

    assert bucket.granted == 30, bucket.granted
    assert abs(sum(slept) - 2.0) < 1e-6, sum(slept)

    # And a clock that refuses to move at all must still not spin.
    frozen_calls = []

    async def frozen(seconds):
        frozen_calls.append(seconds)
        if len(frozen_calls) > 20:
            raise AssertionError("spun on a frozen clock")

    stubborn = TokenBucket(10, burst=1, clock=Clock(), sleep=frozen)
    await stubborn.acquire()
    await asyncio.wait_for(stubborn.acquire(), timeout=2.0)
    assert len(frozen_calls) == 1, frozen_calls

    print(f"     30 acquires at 10/s waited {sum(slept):.2f}s, one sleep per wait")


asyncio.run(main())
PYCHECK
ok "limiter terminates"

info "Checking the guards actually fire"
.venv/bin/python - <<'PYCHECK' || fail "the guards did not behave"
import asyncio

from parallel_ops.core.executor import ExecutionPolicy, execute_item
from parallel_ops.core.schema import CatalogRecord
from parallel_ops.models import BreakerState, Outcome, WorkerKind, WorkItem
from parallel_ops.resilience.breaker import CircuitBreaker
from parallel_ops.resilience.errors import PermanentError, WorkerError
from parallel_ops.resilience.limiter import TokenBucket
from parallel_ops.resilience.retry import RetryPolicy
from parallel_ops.workers.base import Worker


class Scripted(Worker):
    kind = WorkerKind.API

    def __init__(self, error=None, payload=None):
        self.error = error
        self.payload = payload
        self.calls = 0

    async def execute(self, item):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return dict(self.payload or {
            "product_id": item.item_id, "status": "active", "region": "us",
        })


class Clock:
    def __init__(self):
        self.now = 1_000.0

    def __call__(self):
        return self.now


async def sleep(_seconds):
    return None


async def main():
    clock = Clock()
    policy = ExecutionPolicy(
        retry=RetryPolicy(attempts=3, base_delay=0.01, jitter=0.0),
        schema=CatalogRecord,
    )
    item = WorkItem(item_id="S-1", host="https://example.invalid", path="items/1")

    def guards(threshold=5):
        return (
            CircuitBreaker(failure_threshold=threshold, clock=clock),
            TokenBucket(1_000, clock=clock, sleep=sleep),
        )

    # 1. a clean item validates
    breaker, limiter = guards()
    result = await execute_item(item, Scripted(), breaker, limiter, policy, sleep=sleep)
    assert result.outcome is Outcome.OK, result.error

    # 2. a bad shape is INVALID, not FAILED, and is not retried
    breaker, limiter = guards()
    worker = Scripted(payload={"product_id": "S-1", "status": "ACTIVE", "region": "us"})
    result = await execute_item(item, worker, breaker, limiter, policy, sleep=sleep)
    assert result.outcome is Outcome.INVALID, result.outcome
    assert worker.calls == 1, f"a schema error was retried {worker.calls} times"
    assert breaker.failures == 0, "a contract problem counted against the dependency"

    # 3. three attempts must record exactly one verdict
    breaker, limiter = guards()
    worker = Scripted(error=WorkerError("HTTP 503"))
    result = await execute_item(item, worker, breaker, limiter, policy, sleep=sleep)
    assert worker.calls == 3, worker.calls
    assert breaker.failures == 1, f"breaker heard {breaker.failures} verdicts, not 1"

    # 4. six 404s must leave a five-failure breaker closed
    breaker, limiter = guards(threshold=5)
    for n in range(6):
        outcome = await execute_item(
            WorkItem(item_id=f"M-{n}", host="https://example.invalid"),
            Scripted(error=PermanentError("HTTP 404")),
            breaker, limiter, policy, sleep=sleep,
        )
        assert outcome.outcome is Outcome.FAILED
    assert breaker.state is BreakerState.CLOSED, "stale input reported as an outage"
    assert breaker.trips == 0

    # 5. an open circuit must cost no token and make no call
    breaker, limiter = guards(threshold=1)
    await breaker.record_failure()
    worker = Scripted()
    result = await execute_item(item, worker, breaker, limiter, policy, sleep=sleep)
    assert result.outcome is Outcome.REJECTED
    assert worker.calls == 0
    assert limiter.granted == 0, "a token was spent on a call that never happened"

    print("     ok / invalid / one-verdict / 404s-stay-closed / rejected-spends-nothing")


asyncio.run(main())
PYCHECK
ok "guards verified"

info "Checking the audit report is never filtered"
.venv/bin/python - <<'PYCHECK' || fail "the audit report was filtered"
import json
import shutil
import tempfile
from pathlib import Path

from parallel_ops.io import write_results
from parallel_ops.models import BatchReport, Outcome, WorkerKind, WorkResult

work = Path(tempfile.mkdtemp(prefix="switchyard-verify-"))
try:
    def row(item_id, outcome):
        return WorkResult(
            item_id=item_id, host="https://example.invalid",
            kind=WorkerKind.API, outcome=outcome,
        )

    everything = [
        row("A-1", Outcome.OK),
        row("A-2", Outcome.FAILED),
        row("A-3", Outcome.INVALID),
    ]
    clean = [r for r in everything if r.outcome is Outcome.OK]

    write_results(
        everything,
        BatchReport(total=3, ok=1, failed=1, invalid=1),
        work,
        "csv",
        emit=clean,
    )

    body = (work / "results.csv").read_text(encoding="utf-8")
    report = json.loads((work / "full_report.json").read_text(encoding="utf-8"))

    assert "A-2" not in body, "emit did not narrow the csv"
    assert len(report["results"]) == 3, (
        f"the report kept only {len(report['results'])} of 3 rows"
    )
    print("     csv trimmed to 1 row, report kept all 3")
finally:
    shutil.rmtree(work, ignore_errors=True)
PYCHECK
ok "audit report intact"

info "Running the test suite"
.venv/bin/python -m pytest -q || fail "tests failed"
ok "tests pass"

cat <<'DONE'

Setup complete.

  source .venv/bin/activate

  switchyard doctor                          # what can this machine run
  switchyard plan data/sample_items.csv      # what would a run do; calls nothing
  switchyard run data/sample_items.csv -o out/

Defaults are deliberately timid: 10 requests/s per host, 8 concurrent, 3 attempts.
Raise them once you know what the dependency tolerates.
DONE

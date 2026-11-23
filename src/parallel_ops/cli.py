from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import click

from . import __version__
from .config import Settings
from .core.pipeline import build_policy, run_batch
from .core.scheduler import plan_summary, worst_state
from .core.schema import SCHEMAS
from .io import load_items, write_results
from .models import BatchReport, Outcome, WorkResult
from .resilience.errors import ParallelOpsError
from .workers import availability


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    for noisy in ("httpx", "httpcore", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def _settings(**overrides) -> Settings:
    return Settings(**{k: v for k, v in overrides.items() if v is not None})


def _installed_version() -> str:
    """The installed distribution's version, or the source tree's when running uninstalled
    (`click.version_option(package_name=...)` raises in that case instead of answering)."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("parallel-ops")
    except PackageNotFoundError:
        return __version__


@click.group()
@click.version_option(version=_installed_version())
def main() -> None:
    """Concurrent batch operations behind a shared rate limiter and circuit breaker."""


@main.command()
@click.argument("input_path", type=click.Path(exists=True, path_type=Path))
@click.option("--output", "-o", type=click.Path(path_type=Path), help="Directory for results.")
@click.option("--format", "-f", "fmt", type=click.Choice(["csv", "json", "excel"]))
@click.option("--concurrency", "-c", type=int, help="Max items in flight. Default 8.")
@click.option("--rate", type=float, help="Requests per second per host. Default 10.")
@click.option("--burst", type=float, help="Bucket capacity. Default equals rate.")
@click.option("--attempts", type=int, help="Total tries per item. Default 3.")
@click.option("--failure-threshold", type=int, help="Failures before a circuit opens. Default 5.")
@click.option("--reset-timeout", type=float, help="Seconds before a probe is allowed. Default 30.")
@click.option(
    "--schema",
    "schema_name",
    type=click.Choice([*sorted(SCHEMAS), "none"]),
    help="Response schema to validate against, or none.",
)
@click.option("--ok-only", is_flag=True, help="Emit only ok rows. The report keeps everything.")
@click.option("--progress/--no-progress", default=True, help="Stream each result as it lands.")
@click.option("--log-level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]))
def run(
    input_path: Path,
    output: Path | None,
    fmt: str | None,
    concurrency: int | None,
    rate: float | None,
    burst: float | None,
    attempts: int | None,
    failure_threshold: int | None,
    reset_timeout: float | None,
    schema_name: str | None,
    ok_only: bool,
    progress: bool,
    log_level: str | None,
) -> None:
    """Run a batch of work items from a CSV or JSON file."""
    settings = _settings(
        output_dir=output,
        output_format=fmt,
        concurrency=concurrency,
        rate=rate,
        burst=burst,
        attempts=attempts,
        failure_threshold=failure_threshold,
        reset_timeout=reset_timeout,
        schema_name=schema_name,
        log_level=log_level,
    )
    _configure_logging(settings.log_level)

    try:
        items = load_items(input_path)
    except (OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    if not items:
        raise click.ClickException(f"no usable work items in {input_path}")

    hook = _progress_hook(len(items)) if progress else None

    try:
        results, report = asyncio.run(run_batch(settings, items, on_result=hook))
    except ParallelOpsError as exc:
        raise click.ClickException(str(exc)) from exc

    if progress:
        click.echo("")

    _print_report(report)

    # `results` is passed whole so the JSON report keeps every row; `emit` only narrows
    # the CSV or spreadsheet.
    emit = [r for r in results if r.outcome is Outcome.OK] if ok_only else None
    try:
        written = write_results(
            results,
            report,
            settings.output_dir,
            settings.output_format,
            emit=emit,
        )
    except (ImportError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo("")
    for path in written:
        click.echo(f"wrote {path}")

    if report.ok != report.total:
        sys.exit(1)


def _progress_hook(total: int):
    """
    Stream each result as it completes.

    Progress is per result rather than a percentage bar because the useful signal on a
    guarded batch is *which* outcome is accumulating. A run quietly filling up with
    `rejected` is a very different situation from one filling up with `invalid`, and a
    bar at 60% looks identical in both.
    """
    seen = {"n": 0}

    def hook(result: WorkResult) -> None:
        seen["n"] += 1
        marker = {
            Outcome.OK: click.style("ok", fg="green"),
            Outcome.INVALID: click.style("invalid", fg="yellow"),
            Outcome.FAILED: click.style("failed", fg="red"),
            Outcome.REJECTED: click.style("rejected", fg="white", dim=True),
        }[result.outcome]
        detail = f"  {result.error}" if result.error else ""
        click.echo(f"[{seen['n']:>4}/{total}] {result.item_id:<12} {marker}{detail[:96]}")

    return hook


def _print_report(report: BatchReport) -> None:
    click.echo(f"{'total':<16}{report.total}")
    click.echo(f"{'ok':<16}{report.ok}")
    click.echo(f"{'invalid':<16}{report.invalid}")
    click.echo(f"{'failed':<16}{report.failed}")
    click.echo(f"{'rejected':<16}{report.rejected}")
    click.echo(f"{'attempts':<16}{report.attempts}  ({report.amplification:.2f} per item)")
    click.echo(f"{'limiter wait':<16}{report.waited_s:.2f}s")
    click.echo(f"{'elapsed':<16}{report.duration_s:.2f}s")

    if len(report.hosts) > 1 or any(h.trips for h in report.hosts):
        click.echo("")
        click.echo(f"  {'host':<34}{'ok':>5}{'inv':>5}{'fail':>6}{'rej':>5}{'trips':>7}  state")
        for host in report.hosts:
            state = host.final_state.value
            colour = {"closed": "green", "half_open": "yellow", "open": "red"}[state]
            click.echo(
                f"  {host.host[:33]:<34}{host.ok:>5}{host.invalid:>5}"
                f"{host.failed:>6}{host.rejected:>5}{host.trips:>7}  "
                + click.style(state, fg=colour)
            )


@main.command()
@click.argument("input_path", type=click.Path(exists=True, path_type=Path))
@click.option("--concurrency", "-c", type=int)
@click.option("--rate", type=float)
@click.option("--attempts", type=int)
@click.option("--schema", "schema_name", type=click.Choice([*sorted(SCHEMAS), "none"]))
def plan(
    input_path: Path,
    concurrency: int | None,
    rate: float | None,
    attempts: int | None,
    schema_name: str | None,
) -> None:
    """
    Show what a run would do, and what the settings imply, without calling anything.

    The floor it prints is the honest lower bound: items divided by the per-host rate.
    Seeing "at least 50s" before starting a 500-item run at 10/s is the difference
    between choosing a rate and discovering one.
    """
    settings = _settings(
        concurrency=concurrency, rate=rate, attempts=attempts, schema_name=schema_name
    )

    try:
        items = load_items(input_path)
    except (OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc

    summary = plan_summary(items)
    policy = build_policy(settings)

    click.echo(f"{'items':<16}{summary['total']}")
    click.echo(f"{'hosts':<16}{summary['hosts']}")
    for kind, count in summary["by_kind"].items():  # type: ignore[union-attr]
        click.echo(f"  {kind:<14}{count}")

    click.echo("")
    click.echo(f"{'concurrency':<16}{settings.concurrency}")
    click.echo(f"{'rate':<16}{settings.rate}/s per host (burst {policy_burst(settings)})")
    click.echo(
        f"{'attempts':<16}{settings.attempts} (base {settings.base_delay}s, "
        f"max {settings.max_delay}s, jitter {settings.jitter})"
    )
    click.echo(
        f"{'breaker':<16}{settings.failure_threshold} failures, "
        f"{settings.reset_timeout}s reset, {settings.success_threshold} to close"
    )
    click.echo(f"{'schema':<16}{policy.schema.__name__ if policy.schema else 'none'}")

    click.echo("")
    click.echo("per host:")
    for host, kinds in summary["by_host"].items():  # type: ignore[union-attr]
        breakdown = ", ".join(f"{count} {kind}" for kind, count in kinds.items())
        floor = sum(kinds.values()) / settings.rate
        click.echo(f"  {host[:44]:<46}{breakdown:<22}>= {floor:.1f}s at {settings.rate}/s")

    worst = max(
        (sum(kinds.values()) for kinds in summary["by_host"].values()),  # type: ignore[union-attr]
        default=0,
    )
    click.echo("")
    click.echo(f"floor for the busiest host: {worst / settings.rate:.1f}s")
    click.echo("(retries and browser page loads add to this; the limiter sets the floor)")


def policy_burst(settings: Settings) -> str:
    burst = settings.effective_burst
    return f"{burst:g}" if burst is not None else f"{max(1.0, settings.rate):g}"


@main.command()
def doctor() -> None:
    """Report which workers this machine can run, and the active settings."""
    settings = Settings()

    click.echo("workers")
    for name, (ok, reason) in sorted(availability().items()):
        if ok:
            click.echo(f"  {name:<10}" + click.style("available", fg="green"))
        else:
            click.echo(f"  {name:<10}" + click.style("unavailable", fg="yellow") + f"  {reason}")

    click.echo("")
    click.echo("schemas")
    for name, model in sorted(SCHEMAS.items()):
        fields = ", ".join(model.model_fields)
        click.echo(f"  {name:<10}{fields}")
    click.echo(f"  {'none':<10}validation disabled")

    click.echo("")
    click.echo("settings")
    click.echo(f"  {'concurrency':<20}{settings.concurrency}")
    click.echo(f"  {'rate':<20}{settings.rate}/s per host")
    click.echo(f"  {'attempts':<20}{settings.attempts}")
    click.echo(f"  {'failure_threshold':<20}{settings.failure_threshold}")
    click.echo(f"  {'reset_timeout':<20}{settings.reset_timeout}s")
    click.echo(f"  {'schema':<20}{settings.schema_name}")
    click.echo(f"  {'output_format':<20}{settings.output_format}")

    click.echo("")
    click.echo("Nothing here is retried blindly: 404s and schema mismatches fail once,")
    click.echo("and only timeouts and 5xx count toward opening a circuit.")


__all__ = ["main", "worst_state"]

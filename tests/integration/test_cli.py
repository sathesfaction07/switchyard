from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from parallel_ops.cli import main
from parallel_ops.models import WorkerKind
from parallel_ops.resilience.errors import PermanentError, WorkerError

from ..conftest import AlwaysFailWorker, AlwaysOkWorker


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def items_csv(tmp_path):
    path = tmp_path / "items.csv"
    path.write_text(
        "item_id,host,path,region\n"
        "T-1,https://catalog.example.com,items/SKU-1,us\n"
        "T-2,https://catalog.example.com,items/SKU-2,us\n"
        "T-3,https://catalog.example.com,items/SKU-3,eu\n",
        encoding="utf-8",
    )
    return path


def stub_workers(monkeypatch, worker):
    """
    Replace worker construction so the CLI wiring can be exercised offline.

    The CLI is what is under test here, not httpx. `test_workers.py` covers the HTTP
    layer against a mock transport.
    """
    monkeypatch.setattr(
        "parallel_ops.core.pipeline.build_workers",
        lambda settings, kinds: {kind: worker for kind in kinds},
    )


class TestDoctor:
    def test_reports_worker_availability(self, runner):
        result = runner.invoke(main, ["doctor"])
        assert result.exit_code == 0
        assert "workers" in result.output
        assert "api" in result.output
        assert "browser" in result.output

    def test_lists_the_schemas_and_their_fields(self, runner):
        result = runner.invoke(main, ["doctor"])
        assert "catalog" in result.output
        assert "product_id" in result.output
        assert "validation disabled" in result.output

    def test_shows_the_active_settings(self, runner):
        result = runner.invoke(main, ["doctor"])
        for label in ("concurrency", "rate", "attempts", "failure_threshold"):
            assert label in result.output

    def test_explains_the_retry_policy_in_words(self, runner):
        """
        The two rules most likely to surprise someone reading a result file, stated where
        they will be seen before a run rather than after it.
        """
        result = runner.invoke(main, ["doctor"])
        assert "404" in result.output
        assert "circuit" in result.output.lower()

    def test_needs_no_input_file_and_no_network(self, runner):
        assert runner.invoke(main, ["doctor"]).exit_code == 0


class TestPlan:
    def test_counts_items_hosts_and_kinds(self, runner, items_csv):
        result = runner.invoke(main, ["plan", str(items_csv)])
        assert result.exit_code == 0
        assert "items" in result.output
        assert "3" in result.output

    def test_shows_the_resolved_policy(self, runner, items_csv):
        result = runner.invoke(main, ["plan", str(items_csv), "--rate", "5", "--attempts", "4"])
        assert "5" in result.output
        assert "4" in result.output
        assert "CatalogRecord" in result.output

    def test_none_schema_is_shown_as_none(self, runner, items_csv):
        result = runner.invoke(main, ["plan", str(items_csv), "--schema", "none"])
        assert "none" in result.output

    def test_prints_a_floor_for_the_busiest_host(self, runner, items_csv):
        """
        Seeing "at least 50s" before starting a 500-item run at 10/s is the difference
        between choosing a rate and discovering one.
        """
        result = runner.invoke(main, ["plan", str(items_csv), "--rate", "1"])
        assert "floor for the busiest host" in result.output
        assert "3.0s" in result.output

    def test_the_floor_scales_with_the_rate(self, runner, items_csv):
        slow = runner.invoke(main, ["plan", str(items_csv), "--rate", "1"])
        fast = runner.invoke(main, ["plan", str(items_csv), "--rate", "10"])
        assert "3.0s" in slow.output
        assert "0.3s" in fast.output

    def test_breaks_the_plan_down_per_host(self, runner, tmp_path):
        path = tmp_path / "mixed.csv"
        path.write_text(
            "item_id,host,kind\n"
            "T-1,https://a.example.com,api\n"
            "T-2,https://a.example.com,browser\n"
            "T-3,https://b.example.com,api\n",
            encoding="utf-8",
        )

        result = runner.invoke(main, ["plan", str(path)])

        assert "a.example.com" in result.output
        assert "b.example.com" in result.output
        assert "browser" in result.output

    def test_calls_nothing(self, runner, items_csv, monkeypatch):
        """`plan` must be safe to run against a production host list."""

        def explode(*args, **kwargs):
            raise AssertionError("plan built a worker")

        monkeypatch.setattr("parallel_ops.core.pipeline.build_workers", explode)
        assert runner.invoke(main, ["plan", str(items_csv)]).exit_code == 0

    def test_an_unsupported_file_type_is_a_clean_error(self, runner, tmp_path):
        path = tmp_path / "items.parquet"
        path.write_text("x", encoding="utf-8")
        result = runner.invoke(main, ["plan", str(path)])
        assert result.exit_code != 0
        assert "use .csv or .json" in result.output


class TestRun:
    def test_a_clean_run_exits_zero_and_writes_both_files(
        self, runner, items_csv, tmp_path, monkeypatch
    ):
        stub_workers(monkeypatch, AlwaysOkWorker())
        out = tmp_path / "out"

        result = runner.invoke(main, ["run", str(items_csv), "-o", str(out), "--no-progress"])

        assert result.exit_code == 0, result.output
        assert (out / "results.csv").exists()
        assert (out / "full_report.json").exists()

    def test_the_report_counts_every_item(self, runner, items_csv, tmp_path, monkeypatch):
        stub_workers(monkeypatch, AlwaysOkWorker())
        out = tmp_path / "out"

        runner.invoke(main, ["run", str(items_csv), "-o", str(out), "--no-progress"])
        payload = json.loads((out / "full_report.json").read_text(encoding="utf-8"))

        assert payload["summary"]["total"] == 3
        assert payload["summary"]["ok"] == 3
        assert len(payload["results"]) == 3

    def test_a_failing_run_exits_non_zero(self, runner, items_csv, tmp_path, monkeypatch):
        """
        A batch that did not fully succeed must not look successful to a shell script or
        a scheduler.
        """
        stub_workers(monkeypatch, AlwaysFailWorker(PermanentError("HTTP 404")))
        out = tmp_path / "out"

        result = runner.invoke(
            main,
            ["run", str(items_csv), "-o", str(out), "--no-progress", "--attempts", "1"],
        )

        assert result.exit_code == 1
        assert "failed" in result.output

    def test_ok_only_trims_the_csv_but_never_the_report(self, runner, tmp_path, monkeypatch):
        """
        The JSON report is the audit artifact. A summary that silently omits failures is
        how a batch gets signed off as clean.
        """

        class Mixed(AlwaysOkWorker):
            async def execute(self, item):
                self.call_count += 1
                if item.item_id == "T-2":
                    raise PermanentError("HTTP 404")
                return {"product_id": item.item_id, "status": "active", "region": "us"}

        path = tmp_path / "items.csv"
        path.write_text(
            "item_id,host\n"
            "T-1,https://catalog.example.com\n"
            "T-2,https://catalog.example.com\n"
            "T-3,https://catalog.example.com\n",
            encoding="utf-8",
        )
        stub_workers(monkeypatch, Mixed())
        out = tmp_path / "out"

        runner.invoke(
            main,
            [
                "run",
                str(path),
                "-o",
                str(out),
                "--no-progress",
                "--ok-only",
                "--attempts",
                "1",
            ],
        )

        csv_body = (out / "results.csv").read_text(encoding="utf-8")
        report = json.loads((out / "full_report.json").read_text(encoding="utf-8"))

        assert "T-2" not in csv_body, "--ok-only should have dropped the failed row"
        assert len(report["results"]) == 3, "the report must keep every row"
        assert any(r["item_id"] == "T-2" for r in report["results"])

    def test_progress_streams_one_line_per_item(self, runner, items_csv, tmp_path, monkeypatch):
        stub_workers(monkeypatch, AlwaysOkWorker())
        result = runner.invoke(
            main, ["run", str(items_csv), "-o", str(tmp_path / "out"), "--progress"]
        )
        for item_id in ("T-1", "T-2", "T-3"):
            assert item_id in result.output

    def test_json_format_writes_only_the_report(self, runner, items_csv, tmp_path, monkeypatch):
        stub_workers(monkeypatch, AlwaysOkWorker())
        out = tmp_path / "out"

        runner.invoke(main, ["run", str(items_csv), "-o", str(out), "-f", "json", "--no-progress"])

        assert (out / "full_report.json").exists()
        assert not (out / "results.csv").exists()

    def test_extras_from_the_input_reach_the_worker(self, runner, items_csv, tmp_path, monkeypatch):
        """The `region` column in the fixture has to survive loading."""
        seen: list[str] = []

        class Recording(AlwaysOkWorker):
            async def execute(self, item):
                seen.append(item.extras.get("region", ""))
                return {"product_id": item.item_id, "status": "active", "region": "us"}

        stub_workers(monkeypatch, Recording())
        runner.invoke(main, ["run", str(items_csv), "-o", str(tmp_path / "out"), "--no-progress"])

        assert sorted(seen) == ["eu", "us", "us"]

    def test_amplification_is_reported(self, runner, items_csv, tmp_path, monkeypatch):
        """High amplification next to rising rejections is the signal worth seeing."""
        stub_workers(monkeypatch, AlwaysOkWorker())
        result = runner.invoke(
            main, ["run", str(items_csv), "-o", str(tmp_path / "out"), "--no-progress"]
        )
        assert "per item" in result.output

    def test_a_breaker_trip_is_visible_in_the_output(self, runner, tmp_path, monkeypatch):
        path = tmp_path / "items.csv"
        path.write_text(
            "item_id,host\n" + "".join(f"T-{n},https://catalog.example.com\n" for n in range(1, 9)),
            encoding="utf-8",
        )
        stub_workers(monkeypatch, AlwaysFailWorker(WorkerError("HTTP 503")))

        result = runner.invoke(
            main,
            [
                "run",
                str(path),
                "-o",
                str(tmp_path / "out"),
                "--no-progress",
                "--attempts",
                "1",
                "--failure-threshold",
                "2",
                "--concurrency",
                "1",
            ],
        )

        assert result.exit_code == 1
        assert "rejected" in result.output

    def test_a_file_with_no_usable_items_is_a_clean_error(self, runner, tmp_path):
        path = tmp_path / "items.csv"
        path.write_text("item_id,host\nT-1,\n", encoding="utf-8")

        result = runner.invoke(main, ["run", str(path), "--no-progress"])

        assert result.exit_code != 0
        assert "no usable work items" in result.output

    def test_a_missing_input_file_is_rejected_by_the_argument_parser(self, runner, tmp_path):
        result = runner.invoke(main, ["run", str(tmp_path / "nope.csv")])
        assert result.exit_code != 0

    def test_an_unknown_schema_is_rejected_before_anything_runs(self, runner, items_csv):
        result = runner.invoke(main, ["run", str(items_csv), "--schema", "catalogue"])
        assert result.exit_code != 0


class TestCliSurface:
    def test_help_lists_every_command(self, runner):
        output = runner.invoke(main, ["--help"]).output
        for command in ("run", "plan", "doctor"):
            assert command in output

    def test_version_is_reported(self, runner):
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "1.0.0" in result.output

    @pytest.mark.parametrize("command", ["run", "plan", "doctor"])
    def test_every_command_has_help_text(self, runner, command):
        result = runner.invoke(main, [command, "--help"])
        assert result.exit_code == 0
        assert len(result.output.strip()) > 40

    def test_a_worker_kind_with_no_worker_is_reported_per_item(self, runner, tmp_path, monkeypatch):
        """A browser row in an API-only environment fails its own row, not the batch."""
        path = tmp_path / "items.csv"
        path.write_text(
            "item_id,host,kind\n"
            "T-1,https://catalog.example.com,api\n"
            "T-2,https://catalog.example.com,browser\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(
            "parallel_ops.core.pipeline.build_workers",
            lambda settings, kinds: {WorkerKind.API: AlwaysOkWorker()},
        )

        result = runner.invoke(
            main, ["run", str(path), "-o", str(tmp_path / "out"), "--no-progress"]
        )

        assert result.exit_code == 1
        assert "1" in result.output

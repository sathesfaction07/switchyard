from __future__ import annotations

import csv
import json

import pytest

from parallel_ops.io import load_items, write_csv, write_excel, write_json, write_results
from parallel_ops.models import (
    ROW_FIELDS,
    BatchReport,
    HostReport,
    Outcome,
    WorkerKind,
    WorkItem,
    WorkResult,
    as_row,
)


def result(**kwargs) -> WorkResult:
    defaults = {
        "item_id": "T-1",
        "host": "https://catalog.example.com",
        "kind": WorkerKind.API,
        "outcome": Outcome.OK,
    }
    return WorkResult(**{**defaults, **kwargs})


class TestWorkItemTargets:
    def test_host_and_path_are_joined(self):
        item = WorkItem(item_id="1", host="https://h.example.com", path="items/1")
        assert item.target() == "https://h.example.com/items/1"

    def test_a_missing_path_yields_the_bare_host(self):
        assert WorkItem(item_id="1", host="https://h.example.com").target() == (
            "https://h.example.com"
        )

    @pytest.mark.parametrize(
        ("host", "path"),
        [
            ("https://h.example.com/", "items/1"),
            ("https://h.example.com", "/items/1"),
            ("https://h.example.com/", "/items/1"),
        ],
    )
    def test_slashes_never_double_up(self, host, path):
        item = WorkItem(item_id="1", host=host, path=path)
        assert item.target() == "https://h.example.com/items/1"


class TestLoadItems:
    def test_reads_a_csv(self, tmp_path):
        path = tmp_path / "items.csv"
        path.write_text(
            "item_id,host,path\nT-1,https://a.example.com,items/1\n"
            "T-2,https://b.example.com,items/2\n",
            encoding="utf-8",
        )

        items = load_items(path)

        assert [i.item_id for i in items] == ["T-1", "T-2"]
        assert items[0].host == "https://a.example.com"

    def test_reads_a_json_list(self, tmp_path):
        path = tmp_path / "items.json"
        path.write_text(
            json.dumps([{"item_id": "T-1", "host": "https://a.example.com"}]), encoding="utf-8"
        )
        assert len(load_items(path)) == 1

    def test_reads_a_json_object_with_an_items_key(self, tmp_path):
        path = tmp_path / "items.json"
        path.write_text(
            json.dumps({"items": [{"item_id": "T-1", "host": "https://a.example.com"}]}),
            encoding="utf-8",
        )
        assert len(load_items(path)) == 1

    def test_kind_defaults_to_api(self, tmp_path):
        path = tmp_path / "items.csv"
        path.write_text("item_id,host\nT-1,https://a.example.com\n", encoding="utf-8")
        assert load_items(path)[0].kind is WorkerKind.API

    def test_kind_is_read_case_insensitively(self, tmp_path):
        path = tmp_path / "items.csv"
        path.write_text("item_id,host,kind\nT-1,https://a.example.com,BROWSER\n", encoding="utf-8")
        assert load_items(path)[0].kind is WorkerKind.BROWSER

    def test_an_unknown_kind_falls_back_rather_than_aborting(self, tmp_path):
        """
        One typo in a 5,000-line file must not stop the run before anything happens.
        It becomes an ordinary api item, and if that is wrong the row says so.
        """
        path = tmp_path / "items.csv"
        path.write_text("item_id,host,kind\nT-1,https://a.example.com,carrier\n", encoding="utf-8")
        assert load_items(path)[0].kind is WorkerKind.API

    def test_unknown_columns_are_carried_through_as_extras(self, tmp_path):
        """
        A `region` column in the input has to survive to the output, or correlating the
        two means a spreadsheet join that somebody will get wrong.
        """
        path = tmp_path / "items.csv"
        path.write_text(
            "item_id,host,region,owner\nT-1,https://a.example.com,eu,alex\n", encoding="utf-8"
        )

        extras = load_items(path)[0].extras

        assert extras == {"region": "eu", "owner": "alex"}

    def test_empty_extras_are_dropped(self, tmp_path):
        path = tmp_path / "items.csv"
        path.write_text("item_id,host,region\nT-1,https://a.example.com,\n", encoding="utf-8")
        assert load_items(path)[0].extras == {}

    def test_whitespace_is_trimmed(self, tmp_path):
        path = tmp_path / "items.csv"
        path.write_text("item_id,host\n  T-1  ,  https://a.example.com  \n", encoding="utf-8")
        item = load_items(path)[0]
        assert item.item_id == "T-1"
        assert item.host == "https://a.example.com"

    def test_rows_without_a_host_are_skipped(self, tmp_path):
        """A host is the key for both guards, so a row without one cannot be scheduled."""
        path = tmp_path / "items.csv"
        path.write_text(
            "item_id,host\nT-1,https://a.example.com\nT-2,\nT-3,https://c.example.com\n",
            encoding="utf-8",
        )
        assert [i.item_id for i in load_items(path)] == ["T-1", "T-3"]

    def test_a_missing_item_id_is_generated_from_the_row_number(self, tmp_path):
        path = tmp_path / "items.csv"
        path.write_text("item_id,host\n,https://a.example.com\n", encoding="utf-8")
        assert load_items(path)[0].item_id == "row-1"

    def test_a_bom_does_not_corrupt_the_first_column(self, tmp_path):
        """Excel writes a BOM by default, and it would otherwise appear in the header."""
        path = tmp_path / "items.csv"
        path.write_text("\ufeffitem_id,host\nT-1,https://a.example.com\n", encoding="utf-8")
        assert load_items(path)[0].item_id == "T-1"

    def test_a_missing_file_says_so(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="input not found"):
            load_items(tmp_path / "nope.csv")

    def test_an_unsupported_extension_names_what_is_accepted(self, tmp_path):
        path = tmp_path / "items.parquet"
        path.write_text("x", encoding="utf-8")
        with pytest.raises(ValueError, match="use .csv or .json"):
            load_items(path)

    def test_an_empty_csv_yields_no_items(self, tmp_path):
        path = tmp_path / "items.csv"
        path.write_text("item_id,host\n", encoding="utf-8")
        assert load_items(path) == []

    def test_non_dict_json_entries_are_skipped(self, tmp_path):
        path = tmp_path / "items.json"
        path.write_text(
            json.dumps(["nonsense", {"item_id": "T-1", "host": "https://a.example.com"}]),
            encoding="utf-8",
        )
        assert len(load_items(path)) == 1


class TestRowShape:
    def test_as_row_covers_every_declared_field(self):
        assert set(as_row(result())) == set(ROW_FIELDS)

    def test_enums_are_written_as_their_values(self):
        row = as_row(result(outcome=Outcome.REJECTED))
        assert row["outcome"] == "rejected"
        assert row["kind"] == "api"
        assert row["breaker_state"] == "closed"

    def test_floats_are_rounded_for_readability(self):
        row = as_row(result(waited_s=0.123456, duration_s=1.987654))
        assert row["waited_s"] == 0.123
        assert row["duration_s"] == 1.988

    def test_a_missing_data_payload_becomes_an_empty_string(self):
        """`None` in a CSV cell reads as the literal word None, which looks like data."""
        assert as_row(result(data=None))["data"] == ""


class TestReportMaths:
    def test_amplification_of_an_empty_report_is_zero(self):
        assert BatchReport().amplification == 0.0

    def test_amplification_is_attempts_per_item(self):
        assert BatchReport(total=4, attempts=10).amplification == pytest.approx(2.5)

    def test_a_host_report_defaults_to_closed(self):
        assert HostReport(host="h").final_state.value == "closed"

    def test_succeeded_is_true_only_for_ok(self):
        assert result(outcome=Outcome.OK).succeeded is True
        for other in (Outcome.INVALID, Outcome.FAILED, Outcome.REJECTED):
            assert result(outcome=other).succeeded is False


class TestWriters:
    def test_csv_has_a_stable_column_order(self, tmp_path):
        path = tmp_path / "out" / "results.csv"
        write_csv([result()], path)

        with path.open(encoding="utf-8") as handle:
            assert next(csv.reader(handle)) == list(ROW_FIELDS)

    def test_csv_creates_missing_parent_directories(self, tmp_path):
        path = tmp_path / "deep" / "nested" / "results.csv"
        write_csv([result()], path)
        assert path.exists()

    def test_csv_of_nothing_still_writes_a_header(self, tmp_path):
        """An empty file with no header looks like a crash rather than an empty result."""
        path = tmp_path / "results.csv"
        write_csv([], path)
        assert path.read_text(encoding="utf-8").strip() == ",".join(ROW_FIELDS)

    def test_json_report_carries_summary_amplification_and_rows(self, tmp_path):
        path = tmp_path / "report.json"
        report = BatchReport(total=2, ok=1, failed=1, attempts=4)

        write_json([result(), result(item_id="T-2", outcome=Outcome.FAILED)], report, path)
        payload = json.loads(path.read_text(encoding="utf-8"))

        assert payload["summary"]["total"] == 2
        assert payload["amplification"] == 2.0
        assert len(payload["results"]) == 2

    def test_excel_writes_a_sheet(self, tmp_path):
        openpyxl = pytest.importorskip("openpyxl")
        path = tmp_path / "results.xlsx"

        write_excel([result(), result(item_id="T-2", outcome=Outcome.FAILED)], path)
        book = openpyxl.load_workbook(path)

        assert book.active.title == "results"
        assert book.active.max_row == 3

    def test_excel_freezes_the_header(self, tmp_path):
        openpyxl = pytest.importorskip("openpyxl")
        path = tmp_path / "results.xlsx"
        write_excel([result()], path)
        assert openpyxl.load_workbook(path).active.freeze_panes == "A2"


class TestWriteResults:
    def test_csv_mode_writes_the_csv_and_the_report(self, tmp_path):
        written = write_results([result()], BatchReport(total=1, ok=1), tmp_path, "csv")
        assert [p.name for p in written] == ["results.csv", "full_report.json"]

    def test_json_mode_writes_only_the_report(self, tmp_path):
        written = write_results([result()], BatchReport(total=1), tmp_path, "json")
        assert [p.name for p in written] == ["full_report.json"]

    def test_the_report_is_written_for_every_format(self, tmp_path):
        """
        The JSON report is the audit artifact. A summary that can be omitted is how a
        batch gets signed off as clean.
        """
        for fmt in ("csv", "json"):
            target = tmp_path / fmt
            written = write_results([result()], BatchReport(total=1), target, fmt)
            assert any(p.name == "full_report.json" for p in written)

    def test_an_unknown_format_names_the_valid_ones(self, tmp_path):
        with pytest.raises(ValueError, match="use csv, json or excel"):
            write_results([result()], BatchReport(), tmp_path, "yaml")

    def test_emit_narrows_the_csv_without_touching_the_report(self, tmp_path):
        """
        Regression cover. `write_results` used to take one list and hand it to both
        writers, so `--ok-only` silently filtered the audit report too -- contradicting
        the guarantee stated in `write_json`'s own docstring.
        """
        everything = [
            result(item_id="T-1"),
            result(item_id="T-2", outcome=Outcome.FAILED, error="WorkerError: HTTP 503"),
            result(item_id="T-3"),
        ]
        ok_rows = [r for r in everything if r.outcome is Outcome.OK]

        write_results(
            everything, BatchReport(total=3, ok=2, failed=1), tmp_path, "csv", emit=ok_rows
        )

        csv_body = (tmp_path / "results.csv").read_text(encoding="utf-8")
        report = json.loads((tmp_path / "full_report.json").read_text(encoding="utf-8"))

        assert "T-2" not in csv_body
        assert [r["item_id"] for r in report["results"]] == ["T-1", "T-2", "T-3"]

    def test_omitting_emit_writes_everything_to_both(self, tmp_path):
        """
        The safe default. A caller who forgets `emit` fails to filter, which is visible;
        the old shape's failure mode was dropping rows from the audit trail, which is not.
        """
        everything = [result(item_id="T-1"), result(item_id="T-2", outcome=Outcome.FAILED)]

        write_results(everything, BatchReport(total=2, ok=1, failed=1), tmp_path, "csv")

        csv_body = (tmp_path / "results.csv").read_text(encoding="utf-8")
        report = json.loads((tmp_path / "full_report.json").read_text(encoding="utf-8"))

        assert "T-2" in csv_body
        assert len(report["results"]) == 2

    def test_emit_also_narrows_excel(self, tmp_path):
        openpyxl = pytest.importorskip("openpyxl")
        everything = [result(item_id="T-1"), result(item_id="T-2", outcome=Outcome.FAILED)]

        write_results(
            everything,
            BatchReport(total=2, ok=1, failed=1),
            tmp_path,
            "excel",
            emit=[everything[0]],
        )

        book = openpyxl.load_workbook(tmp_path / "results.xlsx")
        assert book.active.max_row == 2, "header plus one emitted row"
        report = json.loads((tmp_path / "full_report.json").read_text(encoding="utf-8"))
        assert len(report["results"]) == 2

    def test_the_output_directory_is_created(self, tmp_path):
        target = tmp_path / "brand" / "new"
        write_results([result()], BatchReport(total=1), target, "csv")
        assert (target / "results.csv").exists()

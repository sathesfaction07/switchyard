from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Sequence

from .models import ROW_FIELDS, BatchReport, WorkerKind, WorkItem, WorkResult, as_row

KNOWN_ITEM_FIELDS = frozenset({"item_id", "host", "kind", "path"})


def load_items(path: Path) -> list[WorkItem]:
    """
    Read work items from CSV or JSON.

    Columns outside `KNOWN_ITEM_FIELDS` are collected into `extras` and carried through
    to the output, so a `region` or `owner` column in the input survives the run and lands
    next to the result. Without that, correlating output back to input means a spreadsheet
    join that somebody will get wrong.

    An unrecognised `kind` is not an error here. It becomes a clean failed row later,
    which keeps one typo in a 5,000-line file from aborting before anything runs.
    """
    if not path.exists():
        raise FileNotFoundError(f"input not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".json":
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            raw = raw.get("items", [])
        rows = list(raw)
    elif suffix == ".csv":
        with path.open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
    else:
        raise ValueError(f"unsupported input type '{suffix}'; use .csv or .json")

    items: list[WorkItem] = []
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        cleaned = {
            str(key).strip(): ("" if value is None else str(value).strip())
            for key, value in row.items()
            if key is not None
        }
        item_id = cleaned.get("item_id") or f"row-{index}"
        host = cleaned.get("host", "")
        if not host:
            continue

        kind_raw = (cleaned.get("kind") or "api").lower()
        kind = WorkerKind.API
        for candidate in WorkerKind:
            if candidate.value == kind_raw:
                kind = candidate
                break

        extras = {k: v for k, v in cleaned.items() if k not in KNOWN_ITEM_FIELDS and v}

        items.append(
            WorkItem(
                item_id=item_id,
                host=host,
                kind=kind,
                path=cleaned.get("path", ""),
                extras=extras,
            )
        )

    return items


def write_csv(results: Sequence[WorkResult], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(ROW_FIELDS))
        writer.writeheader()
        for result in results:
            writer.writerow(as_row(result))


def write_json(results: Sequence[WorkResult], report: BatchReport, path: Path) -> None:
    """
    Write the full report, always including every row.

    The JSON report is the audit artifact and it never gets filtered, even when the CSV
    is trimmed with `--ok-only`. A summary that silently omits the failures is how a batch
    gets signed off as clean.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "summary": report.model_dump(),
        "amplification": round(report.amplification, 3),
        "results": [result.model_dump() for result in results],
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def write_excel(results: Sequence[WorkResult], path: Path) -> None:
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Font, PatternFill
    except ImportError as exc:
        raise ImportError(
            "Excel output needs openpyxl - run: pip install 'parallel-ops[excel]'"
        ) from exc

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "results"

    sheet.append(list(ROW_FIELDS))
    for cell in sheet[1]:
        cell.font = Font(bold=True)

    colours = {
        "ok": "C6EFCE",
        "invalid": "FFEB9C",
        "failed": "FFC7CE",
        "rejected": "D9D9D9",
    }
    outcome_column = ROW_FIELDS.index("outcome") + 1

    for result in results:
        row = as_row(result)
        sheet.append([row[field] for field in ROW_FIELDS])
        colour = colours.get(row["outcome"])
        if colour:
            sheet.cell(row=sheet.max_row, column=outcome_column).fill = PatternFill(
                start_color=colour, end_color=colour, fill_type="solid"
            )

    widths = {
        "item_id": 14,
        "host": 30,
        "kind": 9,
        "outcome": 10,
        "attempts": 9,
        "waited_s": 10,
        "duration_s": 11,
        "breaker_state": 14,
        "error": 60,
        "data": 40,
    }
    for index, field in enumerate(ROW_FIELDS, start=1):
        sheet.column_dimensions[sheet.cell(row=1, column=index).column_letter].width = widths.get(
            field, 14
        )

    error_column = ROW_FIELDS.index("error") + 1
    for row_index in range(2, sheet.max_row + 1):
        sheet.cell(row=row_index, column=error_column).alignment = Alignment(wrap_text=True)

    sheet.freeze_panes = "A2"
    workbook.save(path)


def write_results(
    results: Sequence[WorkResult],
    report: BatchReport,
    directory: Path,
    fmt: str,
    *,
    emit: Sequence[WorkResult] | None = None,
) -> list[Path]:
    """
    Write the chosen format plus the always-complete JSON report.

    `results` is the full set and is what the JSON report always receives. `emit`
    optionally narrows what the tabular output contains, for `--ok-only`.

    The arguments are this way round deliberately. The obvious signature takes one list
    and writes it to both, which is what this function used to do -- and it silently
    broke the guarantee in `write_json`'s own docstring, because the caller passed the
    already-filtered rows. Making the complete set the required argument means the worst
    a forgetful caller can do is fail to filter, rather than quietly drop the failures
    out of the audit trail.
    """
    directory.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    rows = results if emit is None else emit

    fmt = (fmt or "csv").lower()
    if fmt == "csv":
        target = directory / "results.csv"
        write_csv(rows, target)
        written.append(target)
    elif fmt == "excel":
        target = directory / "results.xlsx"
        write_excel(rows, target)
        written.append(target)
    elif fmt == "json":
        pass
    else:
        raise ValueError(f"unsupported format '{fmt}'; use csv, json or excel")

    report_path = directory / "full_report.json"
    write_json(results, report, report_path)
    written.append(report_path)
    return written

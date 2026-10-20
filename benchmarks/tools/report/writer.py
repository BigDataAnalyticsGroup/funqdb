#
#    This is funqDB, a query processing library and system built around FDM and FQL.
#
#    Copyright (C) 2026 Prof. Dr. Jens Dittrich, Saarland University
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#

import html
import json
import os
import tempfile
from dataclasses import asdict, fields
from pathlib import Path

from benchmarks.tools.report.contracts import ReportCase, ReportDefinition
from benchmarks.tools.shared.page import render_page, script_json

_TEMPLATE = Path(__file__).parent / "report.html"


def write_report(
    definition: ReportDefinition, cases: list[ReportCase], output_dir: Path
) -> None:
    """Write `<name>.json` and `<name>.html` into `output_dir`; a `ValueError` writes nothing."""
    definition.validate()

    for case in cases:
        _reject_foreign(definition, case)
        case.validate()

    _reject_repeated_ids(cases)

    record = {
        "definition": definition,
        "cases": [_case_record(case) for case in cases],
    }
    record_json = json.dumps(record, indent=4, default=asdict)

    case_views = [case.get_view() for case in cases]
    case_view_dicts = [asdict(case_view) for case_view in case_views]
    case_views_json = json.dumps(case_view_dicts)
    # For `JSON.parse`: an object literal makes a "__proto__" key the prototype.
    report_json = script_json(case_views_json)
    report_name = html.escape(definition.name)
    page = render_page(
        _TEMPLATE,
        definition.stages,
        REPORT_NAME=report_name,
        REPORT_JSON=report_json,
    )

    json_path = output_dir / f"{definition.name}.json"
    html_path = output_dir / f"{definition.name}.html"
    _write_pair(json_path, record_json, html_path, page)


def _reject_foreign(definition: ReportDefinition, case: ReportCase) -> None:
    if case.definition != definition:
        raise ValueError(f"{case.id}: the case has another definition than the report")


def _reject_repeated_ids(cases: list[ReportCase]) -> None:
    ids = [case.id for case in cases]
    repeated = sorted({case_id for case_id in ids if ids.count(case_id) > 1})
    if repeated:
        raise ValueError(f"case ids {repeated} repeat")


def _case_record(case: ReportCase) -> dict:
    """The fields of `case` without its definition, which the record holds once."""
    return {
        field.name: getattr(case, field.name)
        for field in fields(case)
        if field.name != "definition"
    }


def _write_pair(json_path: Path, json_text: str, html_path: Path, page: str) -> None:
    """Write both files via temporary files; a failed second replace keeps the older page."""
    json_bytes = json_text.encode("utf-8")
    html_bytes = page.encode("utf-8")
    temps: list[Path] = []
    try:
        json_temp = _write_temp(json_path, json_bytes, temps)
        html_temp = _write_temp(html_path, html_bytes, temps)
        os.replace(json_temp, json_path)
        os.replace(html_temp, html_path)
    finally:
        for temp in temps:
            temp.unlink(missing_ok=True)


def _write_temp(target: Path, data: bytes, temps: list[Path]) -> Path:
    """A new temporary file beside `target` with `data`, added to `temps` before the write."""
    handle, name = tempfile.mkstemp(
        dir=target.parent, prefix=f".{target.name}.", suffix=".tmp"
    )
    temp = Path(name)
    temps.append(temp)
    with os.fdopen(handle, "wb") as file:
        file.write(data)
    return temp

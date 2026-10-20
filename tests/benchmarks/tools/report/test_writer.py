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

"""Tests of `benchmarks/tools/report/writer.py`."""

import json
import os
import re
import tempfile
from dataclasses import asdict, replace

import pytest

from benchmarks.tools.report import write_report
from tests.benchmarks.tools.report.sample_report import sample_cases, sample_definition


def report_literal(html: str) -> str:
    """The string literal the writer embedded as ``const REPORT = JSON.parse(...);``."""
    match = re.search(r"const REPORT = JSON\.parse\((.*?)\);\n", html, re.DOTALL)
    return match.group(1)


def read_view(html: str) -> list:
    """The page data the writer embedded, decoded."""
    literal = report_literal(html)
    view_json = json.loads(literal)
    return json.loads(view_json)


def read_files(directory) -> dict[str, str]:
    """The contents of all files in `directory`, by name, hidden temporary files included."""
    return {path.name: path.read_text(encoding="utf-8") for path in directory.iterdir()}


def write_sample_report(directory) -> dict[str, str]:
    """Write the sample report into `directory` and return its files by name."""
    definition = sample_definition()
    cases = sample_cases(definition)
    write_report(definition, cases, directory)
    return read_files(directory)


def fail_on_call(number, function):
    """`function`, but the call with this `number` (from 1) raises an `OSError`."""
    calls = []

    def wrapper(*args, **kwargs):
        calls.append(args)
        if len(calls) == number:
            raise OSError("disk full")
        return function(*args, **kwargs)

    return wrapper


def test_write_report_rejects_a_case_of_another_definition(tmp_path):
    """A case built from another definition would not fit the columns of the page."""
    definition = sample_definition()  # the definition of the report
    other = replace(definition, name="Other")  # a definition that differs in its name
    cases = sample_cases(definition)  # two fitting cases
    foreign = other.create_empty_case("foreign")  # and one of the other definition
    cases.append(foreign)  # in the same list

    with pytest.raises(ValueError, match="foreign"):  # names the offending case
        write_report(definition, cases, tmp_path)  # validates, then refuses

    assert list(tmp_path.iterdir()) == []  # neither file was written


def test_write_report_rejects_repeated_case_ids(tmp_path):
    """Two cases with one id could not be told apart in the JSON or on the page."""
    definition = sample_definition()  # the definition of the report
    cases = sample_cases(definition)  # two cases with distinct ids
    twin = definition.create_empty_case(cases[0].id)  # a third with the first's id
    cases.append(twin)  # in the same list

    with pytest.raises(ValueError, match="filter.tag_literal"):  # names the id
        write_report(definition, cases, tmp_path)  # validates, then refuses

    assert list(tmp_path.iterdir()) == []  # neither file was written


def test_write_report_rejects_a_case_changed_to_not_fit(tmp_path):
    """A case that fitted at construction but lost an intermediate later is refused."""
    definition = sample_definition()  # the definition of the report
    cases = sample_cases(definition)  # two fitting cases
    del cases[1].intermediates["raw_sql"]  # the second case no longer fits

    with pytest.raises(ValueError, match="join.movie_companies"):  # names the case
        write_report(definition, cases, tmp_path)  # validates again, then refuses

    assert list(tmp_path.iterdir()) == []  # neither file was written


def test_write_report_rejects_a_definition_changed_to_repeat_stage_keys(tmp_path):
    """A definition that was valid at construction but repeats a stage key later is refused."""
    definition = sample_definition()  # four distinct stages
    definition.stages.append(definition.stages[0])  # a fifth repeating the first, later

    with pytest.raises(ValueError, match="repeat"):  # refused
        write_report(definition, [], tmp_path)  # validates again, then refuses

    assert list(tmp_path.iterdir()) == []  # neither file was written


def test_write_report_keeps_the_existing_report_when_rendering_fails(
    tmp_path, monkeypatch
):
    """Both files are built before either is written, so a rendering error changes no file."""
    old_files = write_sample_report(tmp_path)  # an older report in the directory
    definition = sample_definition()  # the same definition
    cases = sample_cases(definition)[:1]  # a new run with one case

    def fail(*args, **kwargs):  # a stand-in for an error while rendering
        raise RuntimeError("render failed")  # raised after the JSON text was built

    monkeypatch.setattr("benchmarks.tools.report.writer.render_page", fail)  # break it
    with pytest.raises(RuntimeError):  # the error reaches the caller
        write_report(definition, cases, tmp_path)  # the new run

    assert read_files(tmp_path) == old_files  # the old JSON and page, unchanged


def test_write_report_keeps_the_existing_report_when_a_temporary_file_fails(
    tmp_path, monkeypatch
):
    """When the second temporary file cannot be made, no file of an older report
    changes and the first temporary file is removed."""
    old_files = write_sample_report(tmp_path)  # an older report in the directory
    definition = sample_definition()  # the same definition
    cases = sample_cases(definition)[:1]  # a new run with one case
    mkstemp = fail_on_call(2, tempfile.mkstemp)  # the temp file of the page fails

    monkeypatch.setattr(tempfile, "mkstemp", mkstemp)  # the writer now uses it
    with pytest.raises(OSError):  # the error reaches the caller
        write_report(definition, cases, tmp_path)  # the new run

    assert read_files(tmp_path) == old_files  # the old report, and no temp file


def test_write_report_keeps_the_existing_report_when_the_first_replace_fails(
    tmp_path, monkeypatch
):
    """When the JSON cannot replace the old JSON, nothing changes and both
    temporary files are removed."""
    old_files = write_sample_report(tmp_path)  # an older report in the directory
    definition = sample_definition()  # the same definition
    cases = sample_cases(definition)[:1]  # a new run with one case
    replace_file = fail_on_call(1, os.replace)  # the replace of the JSON fails

    monkeypatch.setattr(os, "replace", replace_file)  # the writer now uses it
    with pytest.raises(OSError):  # the error reaches the caller
        write_report(definition, cases, tmp_path)  # the new run

    assert read_files(tmp_path) == old_files  # the old report, and no temp file


def test_write_report_keeps_the_older_page_when_the_second_replace_fails(
    tmp_path, monkeypatch
):
    """When the page cannot replace the old page (e.g. a locked file on Windows),
    the new JSON stays beside the old page, as `_write_pair` documents."""
    old_files = write_sample_report(tmp_path)  # an older report in the directory
    definition = sample_definition()  # the same definition
    cases = sample_cases(definition)[:1]  # a new run with one case
    replace_file = fail_on_call(2, os.replace)  # the replace of the page fails

    monkeypatch.setattr(os, "replace", replace_file)  # the writer now uses it
    with pytest.raises(OSError):  # the error reaches the caller
        write_report(definition, cases, tmp_path)  # the new run

    files = read_files(tmp_path)  # what the directory holds now
    record = json.loads(files["Backward-Sample.json"])  # the JSON in the directory
    assert set(files) == set(old_files)  # the two report files, no temp file
    assert files["Backward-Sample.html"] == old_files["Backward-Sample.html"]  # old
    assert len(record["cases"]) == 1  # new: one case


def test_write_report_writes_an_empty_report_without_cases(tmp_path):
    """A run without any case still gives a valid report with no rows."""
    definition = sample_definition()  # the definition of the report

    write_report(definition, [], tmp_path)  # no cases at all

    files = read_files(tmp_path)  # the written files
    record = json.loads(files["Backward-Sample.json"])  # the JSON, decoded
    view = read_view(files["Backward-Sample.html"])  # the page data, decoded
    assert record["cases"] == []  # an empty list of records
    assert view == []  # and a page with no case


def test_write_report_json_holds_the_definition_once_and_the_raw_cases(tmp_path):
    """The JSON holds the definition, then each case in order without its definition."""
    definition = sample_definition()  # the definition of the report
    cases = sample_cases(definition)  # the passing and the failing case
    write_report(definition, cases, tmp_path)  # write both files

    json_text = (tmp_path / "Backward-Sample.json").read_text(encoding="utf-8")  # file
    record = json.loads(json_text)  # decoded

    expected_cases = [asdict(case) for case in cases]  # every field of every case
    for expected_case in expected_cases:  # the record leaves out the definition
        del expected_case["definition"]  # because it holds it once
    assert record["definition"] == asdict(definition)  # once, enums as values
    assert record["cases"] == expected_cases  # every other field, in case order

    assert record["definition"]["stages"][0]["key"] == "fql_parsing"  # stage, by hand
    assert record["definition"]["output"]["language"] == "sql"  # the enum's value
    assert record["cases"][1]["id"] == "join.movie_companies"  # the second case
    assert record["cases"][1]["settings"] == {"join_style": "explicit"}  # raw setting
    assert "definition" not in record["cases"][0]  # not repeated in a case


def test_write_report_page_holds_the_view_of_each_case(tmp_path):
    """The page gets the view of each case, in case order."""
    definition = sample_definition()  # the definition of the report
    cases = sample_cases(definition)  # the passing and the failing case
    write_report(definition, cases, tmp_path)  # write both files

    html = (tmp_path / "Backward-Sample.html").read_text(encoding="utf-8")  # the page
    case_views = [case.get_view() for case in cases]  # the views as the cases give
    expected = [asdict(case_view) for case_view in case_views]  # as plain data

    assert read_view(html) == expected  # exactly that data reaches the page

    view = read_view(html)  # the page data, decoded
    assert view[1]["id"] == "join.movie_companies"  # the second case, in order
    assert view[1]["passed"] is False  # its last stage failed
    assert view[1]["output"]["header"] == "Generated SQL"  # the header of its block
    assert view[1]["intermediates"][1]["header"] == "Raw SQL"  # in definition order
    assert view[0]["output_diff"] == []  # the passing case matches its expectation

    assert not re.search(r"__[A-Z_]+__", html)  # and no placeholder is left
    assert "<title>Backward-Sample</title>" in html  # the name became the title


def test_write_report_escapes_the_report_name_in_the_page(tmp_path):
    """A name with markup characters shows as text in the title and heading."""
    definition = sample_definition()  # the sample definition
    named = replace(definition, name="A&B")  # "&" is markup in HTML
    cases = sample_cases(named)  # cases of that definition

    write_report(named, cases, tmp_path)  # write it
    html = (tmp_path / "A&B.html").read_text(encoding="utf-8")  # the file keeps it

    assert "<title>A&amp;B</title>" in html  # escaped in the title
    assert "<h1>A&amp;B</h1>" in html  # and in the heading


@pytest.mark.parametrize(
    "hostile",
    [
        "SELECT '</script>'",  # would close the page's script
    ],
)
def test_write_report_keeps_script_markup_in_the_code_as_data(tmp_path, hostile):
    """Code with script-like markup stays data instead of altering the page's script."""
    definition = sample_definition()  # the definition of the report
    case = definition.create_empty_case("case")  # one case
    case.output = hostile  # with hostile output

    write_report(definition, [case], tmp_path)  # write it
    html = (tmp_path / "Backward-Sample.html").read_text(encoding="utf-8")  # page

    script_data = report_literal(html)  # exactly what sits inside the page's script
    assert "<" not in script_data  # every "<" is escaped, so no markup survives
    assert read_view(html)[0]["output"]["code"] == hostile  # same, decoded

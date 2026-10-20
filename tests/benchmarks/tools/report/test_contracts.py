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

"""Tests of `benchmarks/tools/report/contracts.py`."""

from dataclasses import replace

import pytest

from benchmarks.tools.report import CodeLanguage, StageColumn
from benchmarks.tools.report.contracts import CodeBlockView
from tests.benchmarks.tools.report.sample_report import sample_cases, sample_definition


@pytest.mark.parametrize(
    "name",
    [
        "",  # no file name at all
        "..",  # the parent directory
        "sub/Report",  # a path, not a name
        "../Report",  # a path escaping the output directory
    ],
)
def test_report_definition_init_rejects_a_name_that_is_not_a_file_name(name):
    """The name becomes the file names, so it must be a plain file name."""
    definition = sample_definition()  # a valid definition

    with pytest.raises(ValueError, match="not a plain file name"):  # refused at once
        replace(definition, name=name)  # `replace` runs the constructor


def test_report_definition_init_rejects_repeated_stage_keys():
    """Two columns fed by one status would show the same outcome twice."""
    definition = sample_definition()  # four distinct stages
    repeated = definition.stages + [definition.stages[0]]  # a fifth repeats the first

    with pytest.raises(ValueError, match="repeat"):  # refused at once
        replace(definition, stages=repeated)  # `replace` runs the constructor


def test_report_definition_create_empty_case_fits_the_definition():
    """The empty case has every stage unmeasured, every block empty and nothing else set."""
    definition = sample_definition()  # four stages, two intermediates

    case = definition.create_empty_case("empty")  # the case to fill

    assert case.definition is definition  # it belongs to this definition
    assert case.id == "empty"  # under the given id
    assert case.statuses == {
        "fql_parsing": None,
        "ir_building": None,
        "sql_rendering": None,
        "result_match": None,
    }  # one status for each stage, none measured yet
    assert case.intermediates == {"ir": "", "raw_sql": ""}  # one slot for each block
    assert (case.input, case.output) == ("", "")  # empty code, not None
    assert case.expected_output is None  # nothing expected yet
    assert (case.sql_result, case.fql_result, case.error) == (None, None, None)  # unset
    assert case.limitation is None  # no gap documented
    assert case.settings == {}  # no setting differs from the default
    assert not case.passed  # nothing measured is not a pass


@pytest.mark.parametrize(
    "statuses",
    [
        {"fql_parsing": True},  # stages missing
        {
            "fql_parsing": True,
            "ir_building": True,
            "sql_rendering": True,
            "result_match": True,
            "extra": True,
        },  # a stage the definition does not have
        {
            "fql_parsing": True,
            "ir_building": True,
            "sql_rendering": True,
            "renamed": True,
        },  # the right number of stages, but one with another key
    ],
)
def test_report_case_init_rejects_statuses_not_matching_the_stages(statuses):
    """A case must report exactly the stages of its definition."""
    case = sample_definition().create_empty_case("broken")  # a fitting case

    with pytest.raises(ValueError, match="broken: statuses"):  # names case and field
        replace(case, statuses=statuses)  # `replace` runs the constructor


@pytest.mark.parametrize(
    "intermediates",
    [
        {"ir": ""},  # one intermediate short
        {"ir": "", "raw_sql": "", "extra": ""},  # one intermediate too many
        {"ir": "", "renamed": ""},  # the right number, but one with another key
    ],
)
def test_report_case_init_rejects_intermediates_not_matching_the_definition(
    intermediates,
):
    """A case must hold the code of exactly the intermediates of its definition."""
    case = sample_definition().create_empty_case("broken")  # a fitting case

    with pytest.raises(ValueError, match="broken: intermediates"):  # names the case
        replace(case, intermediates=intermediates)  # `replace` runs the constructor


@pytest.mark.parametrize(
    "statuses, error, passed",
    [
        ({}, None, False),  # no stages at all is not a pass
        ({"a": None, "b": None}, None, False),  # nothing measured is not a pass
        ({"a": True, "b": True}, "crash", False),  # an error fails the case
        ({"a": True, "b": False}, None, False),  # one failed stage fails the case
        ({"a": True, "b": None}, None, True),  # an unmeasured stage does not count
        ({"a": True, "b": True}, None, True),  # all stages pass, no error
    ],
)
def test_report_case_passed_requires_a_measured_stage_no_failure_and_no_error(
    statuses, error, passed
):
    """A case passes only with a measured stage, no failed stage and no error."""
    stages = [StageColumn(key, key) for key in statuses]  # one stage for each status
    definition = sample_definition()  # the sample definition
    staged = replace(definition, stages=stages)  # with just these stages
    case = staged.create_empty_case("case")  # every stage unmeasured

    case.statuses.update(statuses)  # the combination under test
    case.error = error  # with or without an error

    assert case.passed is passed  # the expected outcome for this combination


def test_report_case_get_view_rejects_a_case_changed_to_not_fit():
    """A case that lost an intermediate after construction gives no view."""
    case = sample_definition().create_empty_case("broken")  # a fitting case
    del case.intermediates["raw_sql"]  # no longer fits its definition

    with pytest.raises(ValueError, match="broken: intermediates"):  # not a KeyError
        case.get_view()  # validates first


@pytest.mark.parametrize(
    "output, expected_output",
    [
        ("SELECT a FROM t", None),  # nothing expected
        ("SELECT a FROM t", "select a from t"),  # only the keyword case differs
        ("SELECT 'abc", "SELECT 'abc\n"),  # unparseable, so kept verbatim, plus "\n"
    ],
)
def test_report_case_get_view_has_no_diff_without_a_differing_expectation(
    output, expected_output
):
    """A diff of only agreeing lines would show nothing, so the view has none."""
    case = sample_definition().create_empty_case("case")  # a case with SQL output
    case.output = output  # the actual output
    case.expected_output = expected_output  # and what was expected, if anything

    view = case.get_view()  # the view the page gets

    assert view.output_diff == []  # nothing to show


def test_report_case_get_view_diffs_a_differing_expected_output():
    """An expected output that differs is shown as pretty-printed ndiff lines."""
    definition = sample_definition()  # the sample definition
    case = sample_cases(definition)[1]  # the failing case: JOIN vs comma

    diff = case.get_view().output_diff  # its diff

    assert "- FROM movie_companies AS mc, title AS t" in diff  # expected, dropped
    assert "+ FROM movie_companies AS mc" in diff  # rendered, added
    assert "  SELECT" in diff  # an agreeing line, kept as context
    assert not [line for line in diff if line.startswith("?")]  # no intraline hints


def test_report_case_get_view_shows_each_block_in_its_language():
    """Each block carries its header, its language, its raw code and its pretty code."""
    definition = sample_definition()  # the sample definition
    case = sample_cases(definition)[0]  # the passing case
    input_pretty = CodeLanguage.PYTHON.pretty(case.input)  # the input as Python
    ir_pretty = CodeLanguage.PYTHON.pretty(case.intermediates["ir"])  # as Python
    raw_sql_pretty = CodeLanguage.SQL.pretty(case.intermediates["raw_sql"])  # as SQL
    output_pretty = CodeLanguage.SQL.pretty(case.output)  # the output as SQL

    view = case.get_view()  # the view the page gets

    assert view.input == CodeBlockView(
        "FQL", "python", case.input, input_pretty
    )  # the input with its header, language, raw and pretty code
    assert view.intermediates == [
        CodeBlockView("FQL IR", "python", case.intermediates["ir"], ir_pretty),
        CodeBlockView("Raw SQL", "sql", case.intermediates["raw_sql"], raw_sql_pretty),
    ]  # each intermediate in the language of its own block
    assert view.output == CodeBlockView(
        "Generated SQL", "sql", case.output, output_pretty
    )  # the output, pretty-printed as SQL
    assert "\n" in view.output.code_pretty  # and pretty-printing did change something


def test_report_case_get_view_orders_intermediates_by_the_definition():
    """The page shows the intermediates in the order of the definition, not of the case."""
    case = sample_definition().create_empty_case("case")  # "ir" before "raw_sql"
    case.intermediates = {"raw_sql": "", "ir": ""}  # the same keys, the other order

    headers = [block.header for block in case.get_view().intermediates]  # page order

    assert headers == ["FQL IR", "Raw SQL"]  # the order of the definition


def test_report_case_get_view_is_a_snapshot_of_the_case():
    """The view copies the statuses and settings, so it cannot contradict itself."""
    definition = sample_definition()  # the sample definition
    case = sample_cases(definition)[1]  # the failing case
    view = case.get_view()  # the snapshot

    case.statuses["result_match"] = True  # the case passes its last stage later
    case.settings["join_style"] = "implicit"  # and a setting changes later

    assert view.statuses["result_match"] is False  # the status as built
    assert view.settings == {"join_style": "explicit"}  # the settings as built

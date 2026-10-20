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

"""A backward-style (FQL → SQL) sample report, shared by the report tests and a
manual browser check: `python -m tests.benchmarks.tools.report.sample_report <dir>`."""

import sys
from pathlib import Path

from benchmarks.tools.report import (
    CodeBlockDefinition,
    CodeLanguage,
    ReportCase,
    ReportDefinition,
    ReportLimitation,
    StageColumn,
    write_report,
)


def sample_definition() -> ReportDefinition:
    """Four stages, FQL in, SQL out, one intermediate of each language."""
    return ReportDefinition(
        name="Backward-Sample",
        stages=[
            StageColumn("fql_parsing", "FQL Parsing"),
            StageColumn("ir_building", "IR Building"),
            StageColumn("sql_rendering", "SQL Rendering"),
            StageColumn("result_match", "Result Match"),
        ],
        input=CodeBlockDefinition("FQL", CodeLanguage.PYTHON),
        output=CodeBlockDefinition("Generated SQL", CodeLanguage.SQL),
        intermediates={
            "ir": CodeBlockDefinition("FQL IR", CodeLanguage.PYTHON),
            "raw_sql": CodeBlockDefinition("Raw SQL", CodeLanguage.SQL),
        },
    )


def sample_cases(definition: ReportDefinition) -> list[ReportCase]:
    """A passing case whose output holds a tag-like literal, and a case failing
    the last stage with a differing expected output, a limitation and settings."""
    passing = definition.create_empty_case("filter.tag_literal")
    passing.statuses["fql_parsing"] = True
    passing.statuses["ir_building"] = True
    passing.statuses["sql_rendering"] = True
    passing.statuses["result_match"] = True
    passing.input = "dbf.title.filter(lambda t: t.title == '<b>x</b>').project('title')"
    passing.intermediates["ir"] = (
        "ir.project(ir.filter(ir.relation('title'), \"title = '<b>x</b>'\"), 'title')"
    )
    passing.intermediates["raw_sql"] = (
        "select title from title t where t.title = '<b>x</b>'"
    )
    passing.output = "SELECT t.title FROM title AS t WHERE t.title = '<b>x</b>'"
    passing.expected_output = passing.output
    passing.sql_result = "('<b>x</b>',)"
    passing.fql_result = "('<b>x</b>',)"

    failing = definition.create_empty_case("join.movie_companies")
    failing.statuses["fql_parsing"] = True
    failing.statuses["ir_building"] = True
    failing.statuses["sql_rendering"] = True
    failing.statuses["result_match"] = False
    failing.input = "dbf.movie_companies.join('movie_id', dbf.title).project('title')"
    failing.intermediates["ir"] = (
        "ir.project(ir.join(ir.relation('movie_companies'), 'movie_id', "
        "ir.relation('title')), 'title')"
    )
    failing.intermediates["raw_sql"] = (
        "select title from movie_companies mc join title t on mc.movie_id = t.id"
    )
    failing.output = "SELECT t.title FROM movie_companies AS mc JOIN title AS t ON mc.movie_id = t.id"
    failing.expected_output = (
        "SELECT t.title FROM movie_companies AS mc, title AS t WHERE mc.movie_id = t.id"
    )
    failing.sql_result = "('A',)\n('B',)"
    failing.fql_result = "('A',)"
    failing.error = "ResultMismatch: SQL and FQL results differ."
    failing.limitation = ReportLimitation(
        name="DUPLICATE_ELIMINATION",
        category="fql",
        description="FQL relations are sets, so duplicate SQL rows collapse.",
    )
    failing.settings["join_style"] = "explicit"

    return [passing, failing]


if __name__ == "__main__":  # pragma: no cover - manual browser check
    output_dir = Path(sys.argv[1])
    output_dir.mkdir(parents=True, exist_ok=True)
    definition = sample_definition()
    cases = sample_cases(definition)
    write_report(definition, cases, output_dir)

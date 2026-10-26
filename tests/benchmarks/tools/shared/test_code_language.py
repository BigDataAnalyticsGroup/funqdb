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

"""`CodeLanguage.pretty` formats code for display, verbatim when the formatter fails."""

import black
import pytest
import sqlglot

from benchmarks.tools.shared.code_language import CodeLanguage


def test_code_language_value_is_the_highlighter_tag():
    """The value names the language for the page's syntax highlighter."""
    assert CodeLanguage.SQL.value == "sql"  # Prism's SQL grammar
    assert CodeLanguage.PYTHON.value == "python"  # Prism's Python grammar


@pytest.mark.parametrize("language", list(CodeLanguage))
def test_code_language_pretty_keeps_empty_code_empty(language):
    """An absent block's empty text is not turned into anything else."""
    assert language.pretty("") == ""  # nothing in, nothing out, for each language


@pytest.mark.parametrize(
    "broken",
    [
        "dbf.((",  # black raises InvalidInput on this
        "\x00",  # black raises a SyntaxError on a null byte
    ],
)
def test_code_language_pretty_returns_unparseable_python_verbatim(broken):
    """Code black cannot parse is shown as given rather than failing the page."""
    assert CodeLanguage.PYTHON.pretty(broken) == broken  # unchanged


@pytest.mark.parametrize(
    "broken",
    [
        "SELECT FROM WHERE (",  # sqlglot raises a ParseError on this
        "SELECT 'abc",  # and a TokenError on an unterminated string
    ],
)
def test_code_language_pretty_returns_unparseable_sql_verbatim(broken):
    """SQL sqlglot cannot parse is shown as given rather than failing the page."""
    assert CodeLanguage.SQL.pretty(broken) == broken  # unchanged


@pytest.mark.parametrize(
    "statementless",
    [
        "-- only a comment",  # sqlglot parses this to no statement
        ";",  # and an empty statement likewise
    ],
)
def test_code_language_pretty_returns_sql_without_statements_verbatim(statementless):
    """SQL holding no statement is shown as given instead of as an empty block."""
    assert CodeLanguage.SQL.pretty(statementless) == statementless  # not erased


def test_code_language_pretty_returns_python_verbatim_on_any_formatter_error(
    monkeypatch,
):
    """Any error of black, not only a parse error, keeps the code as given."""

    def fail(*args, **kwargs):  # a stand-in for a bug inside black
        raise RuntimeError("formatter bug")  # not a parse error

    monkeypatch.setattr(black, "format_str", fail)  # black now always fails
    code = "dbf.title.project('title')"  # valid Python

    assert CodeLanguage.PYTHON.pretty(code) == code  # shown verbatim, no exception


def test_code_language_pretty_returns_sql_verbatim_on_a_generation_error(monkeypatch):
    """An error while sqlglot generates the SQL, after parsing, keeps the code."""

    def fail(*args, **kwargs):  # a stand-in for a bug in sqlglot's generator
        raise RuntimeError("generator bug")  # raised after a successful parse

    monkeypatch.setattr(sqlglot.exp.Expression, "sql", fail)  # generation fails
    code = "SELECT a FROM t"  # valid SQL that parses

    assert CodeLanguage.SQL.pretty(code) == code  # shown verbatim, no exception


def test_code_language_pretty_formats_python_with_black():
    """A long one-line FQL chain is split over several lines."""
    fql = "dbf.title.filter(lambda t: t.production_year > 2000).project('title', 'production_year', 'kind_id', 'imdb_index')"  # one line, too long for black

    pretty = CodeLanguage.PYTHON.pretty(fql)  # format it

    expected = (
        "dbf.title.filter(lambda t: t.production_year > 2000).project(\n"
        '    "title", "production_year", "kind_id", "imdb_index"\n'
        ")"
    )  # black's layout, without the trailing newline black adds
    assert pretty == expected  # wrapped exactly as black wraps it


def test_code_language_pretty_formats_sql_with_sqlglot():
    """A one-line query is split into one clause per line."""
    pretty = CodeLanguage.SQL.pretty("SELECT a FROM t WHERE b = 1")  # format it

    assert pretty == "SELECT\n  a\nFROM t\nWHERE\n  b = 1"  # sqlglot's pretty layout


def test_code_language_pretty_keeps_pretty_sql_unchanged():
    """Formatting already formatted SQL changes nothing."""
    pretty = "SELECT\n  a\nFROM t\nWHERE\n  b = 1"  # sqlglot's own layout

    assert CodeLanguage.SQL.pretty(pretty) == pretty  # idempotent


def test_code_language_pretty_joins_sql_statements_with_semicolons():
    """A script keeps its statements apart, each formatted on its own."""
    pretty = CodeLanguage.SQL.pretty("SELECT a FROM t; SELECT b FROM u")  # two

    assert pretty == "SELECT\n  a\nFROM t;\nSELECT\n  b\nFROM u"  # ";\n" between

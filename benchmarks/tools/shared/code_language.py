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

from enum import StrEnum

import black
import sqlglot


class CodeLanguage(StrEnum):
    """A language code is shown in; the value is the page highlighter's tag."""

    SQL = "sql"
    PYTHON = "python"

    def pretty(self, code: str) -> str:
        """`code` pretty-printed, verbatim when the formatter fails for any reason."""
        if self is CodeLanguage.PYTHON:
            return _pretty_python(code)
        return _pretty_sql(code)


def _pretty_python(code: str) -> str:
    try:
        formatted = black.format_str(code, mode=black.Mode())
    except Exception:
        return code
    return formatted.strip()


def _pretty_sql(code: str) -> str:
    try:
        asts = sqlglot.parse(code)
        pretty = ";\n".join(ast.sql(pretty=True) for ast in asts if ast is not None)
    except Exception:
        return code
    # sqlglot parses comment-only SQL to no statement at all
    return pretty or code

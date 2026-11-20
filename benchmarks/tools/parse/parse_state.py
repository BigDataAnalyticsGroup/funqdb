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


"""What the parser knows about a query beyond its IR."""

from dataclasses import dataclass

from benchmarks.tools.ir.query import Query
from benchmarks.tools.parse.cte import CteDefinition


@dataclass(frozen=True)
class ParseState:
    """A query's IR while it is parsed, with its visible CTEs and reserved aliases."""

    query: Query
    ctes: dict[str, CteDefinition]
    reserved: frozenset[str]

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

"""Helpers that parse SQL into the IR for the parser tests."""

import sqlglot

from benchmarks.datasets.schema import Schema
from benchmarks.tools.ir.query import Query
from benchmarks.tools.parse.cte import CteDefinition
from benchmarks.tools.parse.query_parser import (
    parse_query,
    where_conjuncts,
)

# plain tables only: no foreign keys, keys or SQL columns
EMPTY_SCHEMA = Schema(references={}, primary_keys={}, columns={})


def query_from_sql(sql: str, schema: Schema = EMPTY_SCHEMA) -> Query:
    """The IR of the unoptimized query `sql`, parsed against `schema`."""
    ast = sqlglot.parse_one(sql)  # the AST as written: no optimizer rule touched it
    return parse_query(ast, schema)  # the IR, or the rejection a caller tests


def parse_cte(sql: str, schema: Schema = EMPTY_SCHEMA) -> CteDefinition:
    """The CTE the query `sql` defines, its WHERE conjuncts as its conditions."""
    query = query_from_sql(sql, schema)  # the CTE's query as IR
    ast = sqlglot.parse_one(sql)  # parsed again: the conditions are SQL, not IR
    conditions = where_conjuncts(ast)  # the WHERE's top-level conjuncts
    return CteDefinition(query, tuple(conditions))  # what a reading query inlines

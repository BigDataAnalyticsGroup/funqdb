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

"""Helpers that transpile SQL for a test and run both sides on a dataset."""

import sqlglot
from sqlglot import exp
from sqlglot.optimizer import optimize

from benchmarks.datasets.dataset import LoadedDataset
from benchmarks.datasets.schema import Schema
from benchmarks.tools.ir.query import Query
from benchmarks.tools.ir.statements import Statement
from benchmarks.tools.parse.sql_baseline import reject_non_baseline_sql
from benchmarks.tools.parse.statement_parser import parse_statement
from benchmarks.tools.render.config import Config
from benchmarks.tools.render.statement_renderer import StatementRenderer
from benchmarks.tools.sql.optimization import (
    OPTIMIZER_DIALECT,
    OPTIMIZER_RULES,
    factor_common_conjuncts,
    quoted_schema,
)


def fql_of(sql: str, schema: Schema, config: Config = Config()) -> str:
    """The FQL that the statement `sql` renders to, parsed against `schema`."""
    statement = _statement_of(sql, schema)  # the IR, or the rejection a test expects
    return StatementRenderer(config).render_statement(statement)  # in `config`'s style


def check_fql_reads_as_sql(
    sql: str, dataset: LoadedDataset, config: Config = Config()
) -> None:
    """Raises `AssertionError` when the rendered query reads other rows than DuckDB."""
    schema = dataset.schema()  # the columns as they stand now, also after a write
    statement = _statement_of(sql, schema)  # the IR, which also says the row order
    fql = StatementRenderer(config).render_statement(statement)  # the FQL to run

    fql_result = dataset.execute_fql(fql, config.dbf_name)  # the FDM's rows
    sql_result = dataset.execute_sql(sql)  # DuckDB's rows, the reference

    ordered = isinstance(statement, Query) and statement.preserves_order  # ORDER BY
    sql_result.check_matches(fql_result, ordered)  # aligns the columns by name


def run_on_both_sides(
    sql: str, dataset: LoadedDataset, config: Config = Config()
) -> None:
    """Runs the write statement `sql` in DuckDB and its rendered FQL in the FDM."""
    schema = dataset.schema()  # read before the write changes it
    fql = fql_of(sql, schema, config)  # the write as FQL statements
    dataset.execute_sql(sql)  # DuckDB's side
    dataset.execute_fql(fql, config.dbf_name)  # the FDM's side


def _statement_of(sql: str, schema: Schema) -> Statement:
    """The IR of `sql`; a SELECT goes through the transpiler's optimizer first."""
    ast = sqlglot.parse_one(sql)  # the AST as written
    reject_non_baseline_sql(ast)  # SQL outside the baseline never reaches the parser
    # a write statement skips the optimizer, as `parse_statement` expects
    if isinstance(ast, exp.Select):
        factored = factor_common_conjuncts(ast)  # common conjuncts moved out of an OR
        columns = quoted_schema(schema.columns)  # each name read as written
        ast = optimize(  # qualified and typed, with the transpiler's rules
            factored,
            schema=columns,
            dialect=OPTIMIZER_DIALECT,
            rules=OPTIMIZER_RULES,
        )
    return parse_statement(ast, schema)  # the IR of the statement

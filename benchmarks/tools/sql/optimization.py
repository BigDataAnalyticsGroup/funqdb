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

"""The rewrites of a statement before the parser reads it: sqlglot's optimizer with the
type checks as an extra rule, and the factoring of shared conjuncts."""

from sqlglot import Expr, exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.optimizer import optimizer
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.simplify import simplify
from sqlglot.schema import Schema

from benchmarks.datasets.dataset import quote_identifier
from benchmarks.datasets.dataset_types import SqlSchema
from benchmarks.tools.parse.node_args import sql_conjuncts
from benchmarks.tools.parse.type_checks import reject_ill_typed_sql


def quoted_schema(columns: SqlSchema) -> SqlSchema:
    """`columns` with each table and column name quoted, so that sqlglot reads a
    name as written, quotes included; names still match case-insensitively."""
    return {
        quote_identifier(table): {
            quote_identifier(column): type_ for column, type_ in table_columns.items()
        }
        for table, table_columns in columns.items()
    }


def ill_typed_sql_rule(expression: Expr, schema: Schema, dialect: DialectType) -> Expr:
    """Optimizer rule raising for what `reject_ill_typed_sql` rejects;
    it types a copy and returns `expression` unchanged."""
    typed = annotate_types(expression.copy(), schema=schema, dialect=dialect)
    reject_ill_typed_sql(typed, dialect)
    return expression


# the reference database DuckDB, whose dialect resolves names case-insensitively
OPTIMIZER_DIALECT = "duckdb"
# unnest_subqueries turns EXISTS and IN into joins that do not translate;
# optimize_joins raises a Cycle error when a subquery reuses an outer alias
_KEPT_RULES = tuple(
    rule
    for rule in optimizer.RULES
    if rule not in (optimizer.unnest_subqueries, optimizer.optimize_joins)
)
_AFTER_QUALIFY = _KEPT_RULES.index(qualify) + 1
OPTIMIZER_RULES = (
    *_KEPT_RULES[:_AFTER_QUALIFY],
    ill_typed_sql_rule,
    *_KEPT_RULES[_AFTER_QUALIFY:],
)


def factor_common_conjuncts(ast: Expr) -> Expr:
    """`ast` with the conjuncts that every branch of a disjunction shares, by their SQL
    text after `simplify`, lifted out as the first branch writes them. Columns are
    compared as written: `t.a` and an unqualified `a` do not match, as qualification
    comes later (see TODO.md). `_simplified_sql` names what the matching risks."""
    factored = ast.copy()
    disjunctions = [
        node
        for node in factored.walk()
        if isinstance(node, exp.Or) and not isinstance(node.parent, exp.Or)
    ]
    # innermost first, so an outer disjunction compares branches already factored
    for disjunction in reversed(disjunctions):
        replacement = _factor_disjunction(disjunction)
        replaced = disjunction.replace(replacement)
        if disjunction is factored:
            factored = replaced
    return factored


def _factor_disjunction(disjunction: exp.Or) -> Expr:
    """`disjunction` with its shared conjuncts lifted out, if it has any; else unchanged.
    When it lifts conjuncts, a conjunct repeated within a branch is kept once."""
    branches = [_conjuncts_by_sql(branch) for branch in disjunction.flatten()]
    shared_sql = _shared_sql(branches)
    if not shared_sql:
        return disjunction

    first = branches[0]
    shared = [first[text] for text in shared_sql]
    remainders = [
        [conjunct for text, conjunct in branch.items() if text not in shared_sql]
        for branch in branches
    ]
    # if a branch has only shared conjuncts, it becomes just the shared conjunction: (a and b) or a is a
    if any(not remainder for remainder in remainders):
        return exp.and_(*shared)

    alternatives = exp.or_(*(exp.and_(*remainder) for remainder in remainders))
    return exp.and_(*shared, alternatives)


def _conjuncts_by_sql(branch: Expr) -> dict[str, Expr]:
    """The conjuncts of `branch`, once each, by their simplified SQL text; of two
    that read alike, the one written first is kept."""
    by_sql: dict[str, Expr] = {}
    for conjunct in sql_conjuncts(branch):
        by_sql.setdefault(_simplified_sql(conjunct), conjunct)
    return by_sql


def _simplified_sql(conjunct: Expr) -> str:
    """The SQL text of `conjunct` as `simplify` writes it, so that `b = a` reads as `a = b`.

    Any two conjuncts that `simplify` writes alike match, e.g. also `x = 1 + 1` and
    `x = 2`. The match is only as sound as `simplify`, which does not know the types:
    it folds `x + 0.1 = 0.3` into `x = 0.2`, although for a DOUBLE `x` the two tests
    differ, as 0.3 - 0.1 is not exactly 0.2 in floating point. The pipeline's optimizer,
    which runs afterwards, makes that rewrite anyway. A later spelling that matches, in
    the same or a later branch, is dropped before `ill_typed_sql_rule` sees it, though,
    so it escapes the type checks, e.g. a sub-day INTERVAL shift that `simplify` folds
    into a CAST (see TODO.md)."""
    # simplify rewrites the tree it gets; the original keeps its spelling for lifting
    simplified = simplify(conjunct.copy())
    return simplified.sql()


def _shared_sql(branches: list[dict[str, Expr]]) -> list[str]:
    """The texts of the conjuncts every branch has, in the first branch's order."""
    first, *rest = branches
    return [text for text in first if all(text in branch for branch in rest)]

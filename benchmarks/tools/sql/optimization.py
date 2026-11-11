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
from sqlglot.schema import Schema

from benchmarks.tools.parse.node_args import sql_conjuncts
from benchmarks.tools.parse.type_checks import reject_ill_typed_sql


def ill_typed_sql_rule(expression: Expr, schema: Schema, dialect: DialectType) -> Expr:
    """Optimizer rule raising for what `reject_ill_typed_sql` rejects;
    it types a copy and returns `expression` unchanged."""
    typed = annotate_types(expression.copy(), schema=schema, dialect=dialect)
    reject_ill_typed_sql(typed)
    return expression


# the dialect the optimizer resolves and types names in: case-insensitive, as DuckDB
OPTIMIZER_DIALECT = "sqlite"
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
    """`ast` with the conjuncts that every branch of a disjunction shares, by SQL text, lifted out."""
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
    """`disjunction` with its shared conjuncts lifted out, if it has any; else unchanged."""
    branches = [sql_conjuncts(branch) for branch in disjunction.flatten()]
    shared = _shared_conjuncts(branches)
    if not shared:
        return disjunction

    shared_sql = {conjunct.sql() for conjunct in shared}
    remainders = [
        [conjunct for conjunct in branch if conjunct.sql() not in shared_sql]
        for branch in branches
    ]
    # if a branch has only shared conjuncts, it becomes just the shared conjunction: (a and b) or a is a
    if any(not remainder for remainder in remainders):
        return exp.and_(*shared)

    alternatives = exp.or_(*(exp.and_(*remainder) for remainder in remainders))
    return exp.and_(*shared, alternatives)


def _shared_conjuncts(branches: list[list[Expr]]) -> list[Expr]:
    """The conjuncts every branch of a disjunction has, once each, in the first branch's order."""
    first, *rest = branches
    first_by_sql = {conjunct.sql(): conjunct for conjunct in first}
    rest_sql = [{conjunct.sql() for conjunct in branch} for branch in rest]
    return [
        conjunct
        for text, conjunct in first_by_sql.items()
        if all(text in branch for branch in rest_sql)
    ]

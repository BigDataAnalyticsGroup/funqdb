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

"""The rewrites before the parser: factoring shared conjuncts out of a disjunction,
the type checks as an optimizer rule, and the optimizer rules the pipeline keeps."""

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.optimizer.optimizer import optimize
from sqlglot.optimizer.qualify import qualify
from sqlglot.schema import MappingSchema

from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.sql.optimization import (
    OPTIMIZER_DIALECT,
    OPTIMIZER_RULES,
    factor_common_conjuncts,
    ill_typed_sql_rule,
)


def factored_where(predicate: str) -> str:
    """`predicate` as a WHERE clause, factored and rendered back to SQL."""
    ast = sqlglot.parse_one(f"SELECT 1 FROM t WHERE {predicate}")  # one WHERE clause
    factored = factor_common_conjuncts(ast)  # the member under test
    return factored.args["where"].this.sql()  # the predicate alone, as SQL text


def test_factor_common_conjuncts_leaves_a_disjunction_without_a_shared_conjunct_alone():
    """With nothing in common there is nothing to lift, so the predicate is
    returned unchanged."""
    assert (  # branches share no conjunct
        factored_where("(a AND x) OR (b AND y)") == "(a AND x) OR (b AND y)"
    )


def test_factor_common_conjuncts_lifts_a_conjunct_shared_by_both_branches():
    """The conjunct both branches carry moves in front of the disjunction."""
    assert factored_where("(a AND x) OR (a AND y)") == "a AND (x OR y)"  # a is shared


def test_factor_common_conjuncts_collapses_a_branch_of_only_shared_conjuncts():
    """`(a AND x) OR a` is `a`: the bare branch is implied by the other, so the
    disjunction collapses to the shared conjuncts alone."""
    assert factored_where("(a AND x) OR a") == "a"  # absorption law


def test_factor_common_conjuncts_matches_conjuncts_in_any_order():
    """Conjuncts are matched structurally, not positionally: the branches are the
    same predicate written in either order, so both lift and nothing remains."""
    assert factored_where("(a AND x) OR (x AND a)") == "a AND x"  # same set, reordered


def test_factor_common_conjuncts_keeps_a_conjunct_missing_from_one_branch_in_place():
    """Only the intersection of the branches lifts: `b` is in two of three and stays."""
    assert (  # only a is shared; b must not be lifted out
        factored_where("(a AND b AND x) OR (a AND b AND y) OR (a AND z)")
        == "a AND ((b AND x) OR (b AND y) OR z)"
    )


def test_factor_common_conjuncts_lifts_every_shared_conjunct_across_all_branches():
    """Every shared conjunct lifts, leaving each branch only what is unique to it."""
    assert (  # a and b are in all three branches, x/y/z in one each
        factored_where("(a AND b AND x) OR (a AND b AND y) OR (a AND b AND z)")
        == "a AND b AND (x OR y OR z)"
    )


def test_factor_common_conjuncts_factors_a_disjunction_nested_under_a_conjunction():
    """Factoring reaches disjunctions anywhere in the tree, not just at the root
    of the WHERE clause."""
    assert (  # the inner Or is factored while the outer z is untouched
        factored_where("z AND ((a AND x) OR (a AND y))") == "z AND (a AND (x OR y))"
    )


def test_factor_common_conjuncts_factors_a_disjunction_inside_a_factored_one():
    """The inner disjunction is factored before the outer one moves it into a new node."""
    assert (  # a lifts out of the outer Or, b out of the inner one
        factored_where("(a AND ((b AND x) OR (b AND y))) OR (a AND z)")
        == "a AND ((b AND (x OR y)) OR z)"
    )


def test_factor_common_conjuncts_factors_a_disjunction_at_the_root():
    """A disjunction handed in directly is factored and returned as the new root."""
    ast = sqlglot.parse_one("(a AND x) OR (a AND y)")  # no statement around it

    factored = factor_common_conjuncts(ast)  # the member under test

    assert factored.sql() == "a AND (x OR y)"  # the replacement, not the old root


def test_factor_common_conjuncts_keeps_literals_that_differ_in_case_apart():
    """sqlglot's equality ignores the case of N'...', but the values differ."""
    assert (  # nothing shared: lifting the name would lose the rows of 'foo'
        factored_where("(t.name = N'Foo' AND t.x = 1) OR (t.name = N'foo' AND t.y = 2)")
        == "(t.name = N'Foo' AND t.x = 1) OR (t.name = N'foo' AND t.y = 2)"
    )


def test_ill_typed_sql_rule_rejects_what_a_later_rule_would_coerce():
    """It runs after `qualify`, which gives `d` its table and type, and before
    `canonicalize`, which would read this text as a date."""
    ast = sqlglot.parse_one("SELECT t.id FROM t WHERE d = '2020-01-01'")  # unqualified
    columns = {"t": {"id": "INT", "d": "DATE"}}  # d is typed DATE

    with pytest.raises(UnsupportedSql) as raised:  # as the pipeline optimizes it
        optimize(
            ast, schema=columns, dialect=OPTIMIZER_DIALECT, rules=OPTIMIZER_RULES
        )  # raises

    assert raised.value.limitation is Limitation.DIALECT_SPECIFIC  # the catalog entry


def test_ill_typed_sql_rule_rejects_a_sub_day_shift_a_later_rule_would_fold():
    """`pushdown_predicates` folds such an INTERVAL into a literal, so it must run first."""
    ast = sqlglot.parse_one(
        "SELECT t.id FROM t WHERE DATE '1995-03-01' + INTERVAL '1' HOUR < t.d"
    )  # a constant shift
    columns = {"t": {"id": "INT", "d": "DATE"}}  # d is typed DATE

    with pytest.raises(UnsupportedSql) as raised:  # as the pipeline optimizes it
        optimize(
            ast, schema=columns, dialect=OPTIMIZER_DIALECT, rules=OPTIMIZER_RULES
        )  # raises

    assert raised.value.limitation is Limitation.DIALECT_SPECIFIC  # the catalog entry


def test_ill_typed_sql_rule_leaves_the_statement_unchanged():
    """The check types a copy, so the optimizer's later rules see the statement as
    `qualify` left it: the same object, still untyped."""
    ast = sqlglot.parse_one(
        "SELECT t.id FROM t WHERE t.d + INTERVAL '1' DAY < DATE '1995-03-01'"
    )  # whole days shift a date exactly, so the check passes
    schema = MappingSchema({"t": {"id": "INT", "d": "DATE"}})  # d is typed DATE
    qualified = qualify(ast, schema=schema, dialect=OPTIMIZER_DIALECT)  # as rule 1

    checked = ill_typed_sql_rule(qualified, schema, OPTIMIZER_DIALECT)  # no raise
    column = checked.find(exp.Column)  # any column of the statement

    assert checked is qualified  # handed on unchanged
    assert column.type is None  # the types went onto the copy only


def test_optimizer_rules_keep_a_subquery_and_the_join_order_without_two_rules():
    """Without `optimize_joins`, which would raise a `Cycle error` here, and without
    `unnest_subqueries`, which would turn the IN into a join."""
    ast = sqlglot.parse_one(
        "SELECT a.id FROM a JOIN b ON a.id = b.id AND b.x IN (SELECT d.x FROM d) "
        "JOIN d ON b.id = d.id"
    )  # the subquery's alias d is also the outer table joined after b
    columns = {name: {"id": "INT", "x": "INT"} for name in ("a", "b", "d")}  # typed

    optimized = optimize(
        ast, schema=columns, dialect=OPTIMIZER_DIALECT, rules=OPTIMIZER_RULES
    )  # as the pipeline optimizes a query; no Cycle error
    joined = [join.alias_or_name for join in optimized.args["joins"]]  # in order
    subquery_holder = optimized.find(exp.In).find_ancestor(exp.Join)  # its ON

    assert joined == ["b", "d"]  # both outer joins kept, as declared
    assert subquery_holder.alias_or_name == "b"  # the IN stays in b's ON


def test_optimizer_rules_keep_a_star_of_a_table_without_columns():
    """sqlglot 30.21 and later raise here, so pyproject.toml pins 30.20."""
    ast = sqlglot.parse_one("SELECT * FROM t")  # no SQL columns known for t

    optimized = optimize(
        ast, schema={}, dialect=OPTIMIZER_DIALECT, rules=OPTIMIZER_RULES
    )  # raises "requires star-free selections" from 30.21 on

    assert optimized.find(exp.Star) is not None  # the star stays unexpanded

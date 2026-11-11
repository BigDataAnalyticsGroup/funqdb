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

"""How SQL names a sqlglot node's arguments, its conjuncts, and its sign."""

import sqlglot

from benchmarks.tools.parse.node_args import (
    arg_names,
    is_negative_literal,
    sql_conjuncts,
)


def test_arg_names_spells_the_keys_as_sql_does():
    """The trailing underscore of sqlglot's keys is dropped, as in `from_`."""
    spelled = arg_names(["distinct", "from_"])  # sqlglot's keys

    assert spelled == "distinct, from"  # as SQL spells them


def test_is_negative_literal_is_false_without_a_node():
    """An unset argument, such as SUBSTRING's missing length, is no literal."""
    assert not is_negative_literal(None)  # nothing to read


def test_is_negative_literal_is_false_for_a_negated_column():
    """Only a literal under the minus sign counts."""
    negated = sqlglot.parse_one("-a")  # a minus sign in front of a column

    assert not is_negative_literal(negated)  # no literal


def test_is_negative_literal_is_false_for_a_positive_literal():
    """A literal without a minus sign is not negative."""
    literal = sqlglot.parse_one("1")  # no Neg around it

    assert not is_negative_literal(literal)  # no sign


def test_is_negative_literal_is_true_for_a_signed_number():
    """sqlglot reads `-0.1` as a minus sign in front of the literal `0.1`."""
    negated = sqlglot.parse_one("-0.1")  # Neg(Literal)

    assert is_negative_literal(negated)  # seen through the sign


def test_is_negative_literal_is_true_for_a_literal_in_parentheses():
    """`-(1)` is the same number as `-1`."""
    negated = sqlglot.parse_one("-(1)")  # Neg(Paren(Literal))

    assert is_negative_literal(negated)  # seen through the parentheses


def test_sql_conjuncts_of_a_conjunction_are_its_operands():
    """A nested AND flattens, also through parentheses, so each conjunct stands alone."""
    condition = sqlglot.parse_one("a AND (b) AND c")  # three operands
    conjuncts = sql_conjuncts(condition)  # the member under test

    assert [conjunct.sql() for conjunct in conjuncts] == ["a", "b", "c"]  # in order


def test_sql_conjuncts_of_any_other_condition_is_the_condition():
    """A disjunction is one conjunct."""
    condition = sqlglot.parse_one("a OR b")  # no top-level AND

    assert sql_conjuncts(condition) == [condition]  # itself

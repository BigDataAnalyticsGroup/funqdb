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


"""The checks of what SQL's types make FQL read otherwise without an error, on a
statement as written and typed. Other ill-typed SQL is not looked for."""

from sqlglot import Expr, exp

from benchmarks.tools.ir.expressions import IntervalUnit
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.parse.node_args import is_negative_literal

DATETIME_TYPES = frozenset(
    {
        exp.DataType.Type.TIMESTAMP,
        exp.DataType.Type.TIMESTAMPNTZ,
        exp.DataType.Type.DATETIME,
    }
)
_DATE_TYPES = frozenset({exp.DataType.Type.DATE})
_FLOAT_TYPES = frozenset(exp.DataType.FLOAT_TYPES | {exp.DataType.Type.UDOUBLE})
_DECIMAL_TYPES = frozenset(exp.DataType.REAL_TYPES - _FLOAT_TYPES)
_WHOLE_NUMBER_TYPES = frozenset(exp.DataType.INTEGER_TYPES)
_INTERVAL_TYPES = frozenset({exp.DataType.Type.INTERVAL})
_OTHER_TEMPORAL_TYPES = exp.DataType.TEMPORAL_TYPES - _DATE_TYPES - DATETIME_TYPES
# Python never equates a date with a datetime or a time, so each is its own family
_COMPARABLE_FAMILIES = (
    frozenset(exp.DataType.NUMERIC_TYPES),
    frozenset(exp.DataType.TEXT_TYPES),
    _DATE_TYPES,
    DATETIME_TYPES,
    frozenset({exp.DataType.Type.BOOLEAN}),
    _INTERVAL_TYPES,
    *(frozenset({temporal}) for temporal in _OTHER_TEMPORAL_TYPES),
)


def reject_ill_typed_sql(typed_ast: Expr) -> None:
    """Rejects a value FQL would read otherwise than SQL; runs before the optimizer hides it."""
    for node in typed_ast.walk():
        _check_node(node)


def has_other_type(node: Expr, *expected: exp.DataType.Type) -> bool:
    """True when `node`'s annotated SQL type is known and none of `expected`; an unknown type is trusted."""
    if node.type is None or node.is_type(exp.DataType.Type.UNKNOWN):
        return False
    return not node.is_type(*expected)


def _reject_incomparable(value: Expr, compared: Expr) -> None:
    """Rejects comparing two type families, or a decimal literal with a float."""
    left = _compared_operand(value)
    right = _compared_operand(compared)
    for family in _COMPARABLE_FAMILIES:
        if left.is_type(*family) and has_other_type(right, *family):
            raise UnsupportedSql(
                Limitation.DIALECT_SPECIFIC,
                f"Unsupported comparison of a value of type {left.type.sql()} with one "
                f"of type {right.type.sql()}: ISO SQL compares only values of one type "
                "family, and Python finds them unequal.",
            )

    if _compares_decimal_with_float(left, right) or _compares_decimal_with_float(
        right, left
    ):
        raise UnsupportedSql(
            Limitation.LITERAL_TYPE_COERCION,
            f"Unsupported comparison of {left.sql()} with {right.sql()}: SQL converts "
            "the literal to the column's type, while Python compares the exact values.",
        )


def _compares_decimal_with_float(column: Expr, value: Expr) -> bool:
    """True when the literal `value`, signed or not, is a Decimal against a float `column`, or the reverse."""
    literal = value.this.unnest() if is_negative_literal(value) else value
    if not isinstance(column, exp.Column) or not isinstance(literal, exp.Literal):
        return False
    if literal.is_string or literal.is_int:
        return False

    is_float_literal = "e" in literal.this.lower()
    other_types = _DECIMAL_TYPES if is_float_literal else _FLOAT_TYPES
    return column.is_type(*other_types)


def _compared_operand(node: Expr) -> Expr:
    """`node` without parentheses; for a subquery, the column it selects, as its SELECT has no type."""
    if isinstance(node, exp.Subquery) and isinstance(node.this, exp.Select):
        return node.this.selects[0].unalias()
    return node.unnest()


def _check_comparison(comparison: Expr) -> None:
    """Rejects a comparison of incomparable sides."""
    _reject_incomparable(comparison.this, comparison.expression)


def _check_between(between: exp.Between) -> None:
    """Rejects a BETWEEN whose value is incomparable with a bound."""
    _reject_incomparable(between.this, between.args["low"])
    _reject_incomparable(between.this, between.args["high"])


def _check_in(membership: exp.In) -> None:
    """Rejects an IN whose value is incomparable with an option or the subquery's column."""
    query = membership.args.get("query")
    options = membership.expressions if query is None else [query]
    for option in options:
        _reject_incomparable(membership.this, option)


def _check_case(case: exp.Case) -> None:
    """Rejects a CASE whose base value is incomparable with a WHEN value."""
    if case.this is None:
        return

    for branch in case.args.get("ifs", []):
        _reject_incomparable(case.this, branch.this)


def _check_division(division: exp.Div) -> None:
    """Rejects a divided interval: Python drops the fraction of its shift, SQL does not."""
    if division.this.is_type(*_INTERVAL_TYPES):
        raise UnsupportedSql(
            Limitation.INEXACT_INTERVAL,
            f"Unsupported {division.sql()}: Python drops the fraction of the interval.",
        )


def _check_multiplication(product: exp.Mul) -> None:
    """Rejects an interval scaled by a value of no integer type, as Python drops the fraction."""
    if product.this.is_type(*_INTERVAL_TYPES):
        factor = product.expression
    elif product.expression.is_type(*_INTERVAL_TYPES):
        factor = product.this
    else:
        return

    if has_other_type(factor, *_WHOLE_NUMBER_TYPES):
        raise UnsupportedSql(
            Limitation.INEXACT_INTERVAL,
            f"Unsupported {product.sql()}: Python drops the fraction of the interval.",
        )


def _check_cast(cast: exp.Cast) -> None:
    """Rejects a cast to a date or datetime of anything but a text literal or a value of that type."""
    target = cast.to.this
    operand = cast.this
    is_temporal_target = target in _DATE_TYPES or target in DATETIME_TYPES
    is_literal = isinstance(operand, exp.Literal) and operand.is_string
    if is_temporal_target and not is_literal and has_other_type(operand, target):
        raise UnsupportedSql(
            Limitation.TEMPORAL_CAST,
            f"Unsupported CAST of a value of type {operand.type.sql()} to "
            f"{target.value}: only a value already of that type passes through.",
        )


def _check_interval(interval: exp.Interval) -> None:
    """Admits a sub-day INTERVAL only as the direct shift of a datetime: elsewhere FQL
    could shift a date by it, which Python drops without an error."""
    unit = interval.unit
    if isinstance(unit, exp.IntervalSpan):
        return
    if not IntervalUnit[unit.name.upper()].is_sub_day:
        return

    parent = interval.parent
    is_shift = isinstance(parent, (exp.Add, exp.Sub))
    shifted = None
    if is_shift:
        shifted = parent.expression if parent.this is interval else parent.this
    if not is_shift or has_other_type(shifted, *DATETIME_TYPES):
        raise UnsupportedSql(
            Limitation.INEXACT_INTERVAL,
            f"Unsupported {interval.sql()}: a sub-day interval passes only as the direct "
            "shift of a datetime, as Python drops it from a date.",
        )


def _check_node(node: Expr) -> None:
    """Runs the check of `node`'s kind, if it has one."""
    match node:
        case exp.EQ() | exp.NEQ() | exp.LT() | exp.LTE() | exp.GT() | exp.GTE():
            _check_comparison(node)
        case exp.Between():
            _check_between(node)
        case exp.In():
            _check_in(node)
        case exp.Case():
            _check_case(node)
        case exp.Div():
            _check_division(node)
        case exp.Mul():
            _check_multiplication(node)
        case exp.Cast():
            _check_cast(node)
        case exp.Interval():
            _check_interval(node)

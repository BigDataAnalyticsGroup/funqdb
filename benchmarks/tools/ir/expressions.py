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

"""The IR of a value or condition: `Expression` and its subtypes."""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from datetime import timedelta
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Callable, Iterator

from dateutil.relativedelta import relativedelta

from benchmarks.datasets.dataset_types import Reference
from benchmarks.tools.ir.paths import Path

if TYPE_CHECKING:
    from benchmarks.tools.ir.query import Query


class Expression:
    """A node of a SQL condition or value."""

    def children(self) -> tuple[Expression, ...]:
        """The expressions directly inside this one, in field order; a subquery is not among them."""
        children: list[Expression] = []
        for field in fields(self):
            value = getattr(self, field.name)
            children.extend(_expressions_in(value))
        return tuple(children)

    def conjuncts(self) -> list[Expression]:
        """The operands of this `and`-chain; this expression alone if it is none."""
        return [self]

    def map_paths(self, mapping: Callable[[Path], Path]) -> Expression:
        """A copy with `mapping` applied to every path in it; a subquery keeps its own."""
        changes = {}
        for field in fields(self):
            value = getattr(self, field.name)
            changes[field.name] = _mapped(value, mapping)
        return replace(self, **changes)

    def paths(self) -> list[Path]:
        """Every path in this expression that `map_paths` rebases."""
        return [
            expression.path
            for expression in self.nested_expressions()
            if isinstance(expression, Column) and expression.path is not None
        ]

    def nested_expressions(self) -> Iterator[Expression]:
        """This expression and every expression inside it, subqueries excluded."""
        yield self
        for child in self.children():
            yield from child.nested_expressions()

    def unparenthesized(self) -> Expression:
        """This expression without the parentheses around it."""
        return self

    def is_null_literal(self) -> bool:
        """True when this is SQL's `NULL` literal, also in parentheses."""
        return False

    def is_comparison(self) -> bool:
        """True when this compares values, which is false over an unmapped attribute."""
        return False

    def null_tested_column(self) -> Column | None:
        """The column if this is `column IS NULL`; `None` otherwise."""
        return None

    def not_null_tested_column(self) -> Column | None:
        """The column if this is `column IS NOT NULL`; `None` otherwise."""
        return None

    def compared_columns(self, replaced: dict[Expression, Column]) -> Iterator[Column]:
        """The columns the compared values read, `replaced` as its column; not CASE results or subqueries."""
        for operand in self.children():
            yield from operand._operand_columns(replaced)

    def _operand_columns(self, replaced: dict[Expression, Column]) -> Iterator[Column]:
        """The columns this operand reads; none below a node that tests its own operands."""
        if self in replaced:
            yield replaced[self]
            return

        self_testing = (CaseWhenExpression, AggregateExpression, IsNullExpression)
        if self.is_comparison() or isinstance(self, self_testing):
            return

        for child in self.children():
            yield from child._operand_columns(replaced)


# `is` / `is not` compare two references by the tuple they lead to
UNEQUAL_SYMBOLS = frozenset({"!=", "is not"})
_COMPARISON_SYMBOLS = frozenset({"==", "<", "<=", ">", ">=", "is", *UNEQUAL_SYMBOLS})


@dataclass(frozen=True)
class Literal(Expression):
    """A constant; `None` is SQL's `NULL`."""

    value: object

    def is_null_literal(self) -> bool:
        return self.value is None


@dataclass(frozen=True)
class UnaryExpression(Expression):
    """The prefix operator `op` applied to `value`."""

    op: str
    value: Expression

    def not_null_tested_column(self) -> Column | None:
        if self.op != "not":
            return None

        return self.value.null_tested_column()


@dataclass(frozen=True)
class BinaryExpression(Expression):
    """`left op right` for the binary operator `op`."""

    op: str
    left: Expression
    right: Expression

    def conjuncts(self) -> list[Expression]:
        if self.op != "and":
            return [self]

        return self.left.conjuncts() + self.right.conjuncts()

    def is_comparison(self) -> bool:
        return self.op in _COMPARISON_SYMBOLS


@dataclass(frozen=True)
class BetweenExpression(Expression):
    """`value BETWEEN lower AND upper`."""

    value: Expression
    lower: Expression
    upper: Expression

    def is_comparison(self) -> bool:
        return True


@dataclass(frozen=True)
class InExpression(Expression):
    """`value IN (options)`."""

    value: Expression
    options: tuple[Expression, ...]

    def is_comparison(self) -> bool:
        return True

    def compared_columns(self, replaced: dict[Expression, Column]) -> Iterator[Column]:
        """The options are not guarded."""
        return self.value._operand_columns(replaced)


@dataclass(frozen=True)
class InSubqueryExpression(Expression):
    """`value IN (subquery)`."""

    value: Expression
    subquery: Query

    def is_comparison(self) -> bool:
        return True


@dataclass(frozen=True)
class ExistsExpression(Expression):
    """`EXISTS (subquery)`."""

    subquery: Query


@dataclass(frozen=True)
class SubqueryExpression(Expression):
    """A scalar subquery used as a value."""

    subquery: Query


@dataclass(frozen=True)
class LikeExpression(Expression):
    """`value LIKE pattern`."""

    value: Expression
    pattern: str

    def is_comparison(self) -> bool:
        return True


@dataclass(frozen=True)
class IsNullExpression(Expression):
    """`value IS NULL`."""

    value: Expression

    def null_tested_column(self) -> Column | None:
        return self.value if isinstance(self.value, Column) else None


@dataclass(frozen=True)
class IsBooleanExpression(Expression):
    """`value IS TRUE` or `value IS FALSE`, as `expected` says."""

    value: Expression
    expected: bool

    def is_comparison(self) -> bool:
        return True


@dataclass(frozen=True)
class ParenExpression(Expression):
    """`value` in parentheses."""

    value: Expression

    def conjuncts(self) -> list[Expression]:
        inner_conjuncts = self.value.conjuncts()
        if len(inner_conjuncts) == 1:
            return [self]

        return inner_conjuncts

    def unparenthesized(self) -> Expression:
        return self.value.unparenthesized()

    def is_null_literal(self) -> bool:
        return self.value.is_null_literal()

    def null_tested_column(self) -> Column | None:
        return self.value.null_tested_column()


@dataclass(frozen=True)
class CaseBranch:
    """`WHEN condition THEN result`."""

    condition: Expression
    result: Expression


@dataclass(frozen=True)
class CaseWhenExpression(Expression):
    """`CASE` over `branches`, with an optional `default` as `ELSE`."""

    branches: tuple[CaseBranch, ...]
    default: Expression | None = None


@dataclass(frozen=True)
class SubstringExpression(Expression):
    """`SUBSTRING(value FROM start [FOR length])`."""

    value: Expression
    start: int
    length: int | None = None


class ExtractPart(Enum):
    """A supported ISO SQL-92 ``EXTRACT`` field; its value is the attribute read off the stored date."""

    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"


# the parts that a DATE value does not hold
TIME_OF_DAY_PARTS = frozenset(
    {ExtractPart.HOUR, ExtractPart.MINUTE, ExtractPart.SECOND}
)


class IntervalUnit(Enum):
    """A supported ISO SQL-92 ``INTERVAL`` unit; ``count`` of it renders as ``delta(keyword=count)``."""

    def __init__(self, delta: type, keyword: str) -> None:
        """Stores the type of the shift and its keyword argument."""
        self.delta = delta
        self.keyword = keyword

    SECOND = (timedelta, "seconds")
    MINUTE = (timedelta, "minutes")
    HOUR = (timedelta, "hours")
    DAY = (timedelta, "days")
    MONTH = (relativedelta, "months")
    YEAR = (relativedelta, "years")

    @property
    def is_sub_day(self) -> bool:
        """True when the unit is smaller than a day."""
        return self.delta is timedelta and self is not IntervalUnit.DAY


@dataclass(frozen=True)
class ExtractExpression(Expression):
    """`EXTRACT(part FROM value)`."""

    part: ExtractPart
    value: Expression


@dataclass(frozen=True)
class CastExpression(Expression):
    """A ``CAST`` to ``DOUBLE``, the one numeric target supported."""

    value: Expression


@dataclass(frozen=True)
class IntervalExpression(Expression):
    """`INTERVAL count unit`."""

    unit: IntervalUnit
    count: int | Decimal


@dataclass(frozen=True)
class Column(Expression):
    """The attribute `name` at the end of `path`; without a path the output `name`,
    with an empty `name` the tuple at the end of `path` (see `tuple_at`)."""

    path: Path | None
    name: str
    is_reference_column: bool = False

    @classmethod
    def tuple_at(cls, path: Path) -> Column:
        """The tuple at the end of `path` itself, as a column without a name."""
        return cls(path, "")

    def _operand_columns(self, replaced: dict[Expression, Column]) -> Iterator[Column]:
        """This column, or its replacement."""
        yield replaced.get(self, self)


@dataclass(frozen=True)
class ReferencedTupleExpression(Expression):
    """The tuple keyed `key` that a written foreign key along `reference` refers to."""

    reference: Reference
    key: Literal


@dataclass(frozen=True)
class TupleIdentityExpression(Expression):
    """True when the reference `left` reads leads to the same tuple as `right`."""

    left: Column
    right: Column


@dataclass(frozen=True)
class AggregateExpression(Expression):
    """An aggregate `function` over `argument` (`None` for no argument), optionally `distinct`."""

    function: str
    argument: Column | None
    distinct: bool = False


def conjunction(first: Expression, *rest: Expression) -> Expression:
    """The conditions as one, all of them holding at once: ``((a and b) and c)``."""
    combined = first
    for operand in rest:
        combined = BinaryExpression("and", left=combined, right=operand)

    return combined


def _mapped(value: object, mapping: Callable[[Path], Path]) -> object:
    """The field value `value` with `mapping` applied to every path in it; a subquery stays."""
    if isinstance(value, Expression):
        return value.map_paths(mapping)
    if isinstance(value, Path):
        return mapping(value)
    if isinstance(value, CaseBranch):
        condition = _mapped(value.condition, mapping)
        result = _mapped(value.result, mapping)
        return CaseBranch(condition, result)
    if isinstance(value, tuple):
        return tuple(_mapped(item, mapping) for item in value)
    return value


def _expressions_in(value: object) -> list[Expression]:
    """The expressions that the field value `value` holds directly; a subquery is none."""
    if isinstance(value, Expression):
        return [value]
    if isinstance(value, CaseBranch):
        return [value.condition, value.result]
    if isinstance(value, tuple):
        return [expression for item in value for expression in _expressions_in(item)]
    return []

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

"""The STRUCTURED predicate form: a filter condition as `fql.predicates` objects
(`Eq`, `And`, `Like`, ...)."""

from dataclasses import dataclass

from benchmarks.tools.ir.expressions import (
    BetweenExpression,
    BinaryExpression,
    Column,
    Expression,
    InExpression,
    IsBooleanExpression,
    LikeExpression,
    Literal,
    ParenExpression,
    UnaryExpression,
)
from benchmarks.tools.ir.query import Query
from benchmarks.tools.render.fql_names import FDM_LOOKUP_SEPARATOR, SHADOWED_NAMES
from benchmarks.tools.render.fragments import (
    group_aware_segments,
    join_path,
    render_literal,
)
from benchmarks.tools.render.like import LikePattern, LikeShape
from fql.predicates import (
    And,
    Eq,
    Exists,
    Gt,
    Gte,
    In,
    Like,
    Lt,
    Lte,
    Missing,
    Not,
    Or,
    Ref,
)

_COMPARISON_CLASSES: dict[str, type] = {
    "==": Eq,
    "!=": Eq,
    "<": Lt,
    "<=": Lte,
    ">": Gt,
    ">=": Gte,
}
_FLIPPED_COMPARISONS = {
    "==": "==",
    "!=": "!=",
    "<": ">",
    "<=": ">=",
    ">": "<",
    ">=": "<=",
}


@dataclass(frozen=True)
class StructuredPredicates:
    """Renders conditions on the tuples of `query` as structured predicates: on its
    groups when `post_group`, and on join tuples when `joined`."""

    query: Query
    post_group: bool
    joined: bool

    def render(self, expression: Expression) -> str | None:
        """`expression` as structured predicates; `None` where it has none, so the
        caller renders a lambda for it instead."""
        if isinstance(expression, ParenExpression):
            return self.render(expression.value)

        mapped_column = expression.not_null_tested_column()
        if mapped_column is not None:
            return self._definedness(Exists, mapped_column)

        unmapped_column = expression.null_tested_column()
        if unmapped_column is not None:
            return self._definedness(Missing, unmapped_column)

        if isinstance(expression, UnaryExpression) and expression.op == "not":
            negated = self.render(expression.value)
            return _call(Not, [negated])

        if isinstance(expression, BinaryExpression) and expression.op == "and":
            rendered = [self.render(conjunct) for conjunct in expression.conjuncts()]
            return _call(And, rendered)

        if isinstance(expression, BinaryExpression) and expression.op == "or":
            left = self.render(expression.left)
            right = self.render(expression.right)
            return _call(Or, [left, right])

        if (
            isinstance(expression, BinaryExpression)
            and expression.op in _COMPARISON_CLASSES
        ):
            return self._comparison(expression.op, expression.left, expression.right)

        if isinstance(expression, BetweenExpression):
            lower = self._comparison(">=", expression.value, expression.lower)
            upper = self._comparison("<=", expression.value, expression.upper)
            return _call(And, [lower, upper])

        if isinstance(expression, InExpression):
            return self._membership(expression)

        if isinstance(expression, IsBooleanExpression):
            expected = Literal(expression.expected)
            return self._comparison("==", expression.value, expected)

        if isinstance(expression, LikeExpression):
            return self._like(expression)

        return None

    def _definedness(self, predicate: type, column: Column) -> str | None:
        """`predicate` over the attribute of `column`; `None` where it has none."""
        attribute = self._attribute(column)
        return None if attribute is None else f"{predicate.__name__}({attribute!r})"

    def _membership(self, expression: InExpression) -> str | None:
        """An `In` over literal options; `None` for other options."""
        attribute = self._attribute(expression.value)
        if attribute is None or not all(
            isinstance(option, Literal) for option in expression.options
        ):
            return None

        options = ", ".join(
            render_literal(option.value) for option in expression.options
        )
        return f"{In.__name__}({attribute!r}, [{options}])"

    def _comparison(
        self, operator: str, left: Expression, right: Expression
    ) -> str | None:
        """The two sides in the order the predicate classes take them: attribute
        first, so a literal on the left flips the comparison."""
        attribute = self._attribute(left)
        if attribute is None:
            attribute = self._attribute(right)
            right = left
            operator = _FLIPPED_COMPARISONS[operator]

        value = self._operand(right)
        if attribute is None or value is None:
            return None

        rendered = f"{_COMPARISON_CLASSES[operator].__name__}({attribute!r}, {value})"
        return rendered if operator != "!=" else f"{Not.__name__}({rendered})"

    def _operand(self, expression: Expression) -> str | None:
        """The right-hand side of a comparison: a literal, or a reference to another
        attribute."""
        if isinstance(expression, Literal):
            return render_literal(expression.value)

        attribute = self._attribute(expression)
        return None if attribute is None else f"{Ref.__name__}({attribute!r})"

    def _like(self, expression: LikeExpression) -> str | None:
        """`None` for a pattern that needs a regular expression: `Like` silently
        matches nothing for it."""
        attribute = self._attribute(expression.value)
        like = LikePattern(expression.pattern)
        if attribute is None or like.shape is LikeShape.REGEX:
            return None

        pattern = render_literal(expression.pattern)
        return f"{Like.__name__}({attribute!r}, {pattern})"

    def _attribute(self, expression: Expression) -> str | None:
        """The path of a column of this query as a predicate attribute; `None` otherwise."""
        if (
            not isinstance(expression, Column)
            or self.query.owner_of(expression) is not self.query
        ):
            return None

        segments = group_aware_segments(
            expression, self.query, self.post_group, self.joined
        )
        if any(segment in SHADOWED_NAMES for segment in segments):
            return None

        return join_path(segments, FDM_LOOKUP_SEPARATOR)


def _call(predicate: type, arguments: list[str | None]) -> str | None:
    """`predicate` over `arguments`; `None` when an argument has no structured form."""
    if any(argument is None for argument in arguments):
        return None

    return f"{predicate.__name__}({', '.join(arguments)})"

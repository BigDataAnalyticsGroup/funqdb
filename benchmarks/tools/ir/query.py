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

"""The IR of a query: `Query`, `Output`, `OrderKey`, `Limit`, `GeneratedColumns`."""

from __future__ import annotations

from dataclasses import dataclass, field

from benchmarks.tools.ir.expressions import Column, Expression, conjunction
from benchmarks.tools.ir.join_tree import Join, JoinTree
from benchmarks.tools.ir.statements import Statement


@dataclass
class GeneratedColumns:
    """The columns made for expressions, each named with `prefix` and a running number."""

    prefix: str
    by_expression: dict[Expression, Column] = field(default_factory=dict)
    _idx: int = 0

    def column_for(self, expression: Expression, name: str | None = None) -> Column:
        """The column holding `expression`; made on first use, as `name` or a generated name."""
        if expression not in self.by_expression:
            if name is None:
                name = f"{self.prefix}{self._idx}"
                self._idx += 1
            self.by_expression[expression] = Column(None, name)
        return self.by_expression[expression]


@dataclass(frozen=True)
class Output:
    """One SELECT-list entry: `column`, shown in the result as `name`."""

    column: Column
    name: str


@dataclass(frozen=True)
class OrderKey:
    """One ORDER BY key: `expression`, descending or ascending."""

    expression: Expression
    descending: bool


@dataclass(frozen=True)
class Limit:
    """LIMIT `count` rows, after skipping `offset` rows if given."""

    count: int
    offset: int | None = None


# eq=False keeps the identity hash, so a frozen expression that holds a subquery stays hashable
@dataclass(eq=False)
class Query(Statement):
    """A SELECT: its join tree, condition, outputs, grouping, ordering and limit."""

    tree: JoinTree
    enclosing: Query | None = None
    # the conjuncts of ON and WHERE that are no join equality
    stated_condition: Expression | None = None
    outputs: list[Output] = field(default_factory=list)
    aggregates: GeneratedColumns = field(
        default_factory=lambda: GeneratedColumns("_aggregate_")
    )
    computed_columns: GeneratedColumns = field(
        default_factory=lambda: GeneratedColumns("_computed_")
    )
    aggregate_computed_columns: GeneratedColumns = field(
        default_factory=lambda: GeneratedColumns("_aggregate_computed_")
    )
    group_keys: list[Column] = field(default_factory=list)
    having_condition: Expression | None = None
    # SELECT DISTINCT on a grouped query: its groups are grouped once more by output
    regroups: bool = False
    order_keys: list[OrderKey] = field(default_factory=list)
    limit: Limit | None = None

    def __post_init__(self) -> None:
        """Rejects a tree whose depth is not the nesting of this query."""
        nesting = 0 if self.enclosing is None else self.enclosing.depth + 1
        if self.tree.depth != nesting:
            raise ValueError("The tree's depth is not the query's nesting.")

    @property
    def condition(self) -> Expression | None:
        """The tree's identities, then `stated_condition`; `None` if neither holds anything."""
        conditions = list(self.tree.identities)
        if self.stated_condition is not None:
            conditions.append(self.stated_condition)
        if not conditions:
            return None

        return conjunction(*conditions)

    @property
    def join(self) -> Join | None:
        """The join of this query's roots with its condition split over it; `None` for one root."""
        return self.tree.join(self.condition)

    @property
    def preserves_order(self) -> bool:
        """True when the result's row order is part of its meaning, as an ORDER BY fixes it."""
        return bool(self.order_keys)

    @property
    def depth(self) -> int:
        """How many queries this one is nested in; 0 for the outermost."""
        return self.tree.depth

    @property
    def projected_outputs(self) -> list[Output]:
        """The outputs whose column no earlier output selects; `project` reads these."""
        first_outputs = self._first_output_by_column()
        return list(first_outputs.values())

    @property
    def copy_sources(self) -> dict[Output, Output]:
        """Per output that selects an earlier output's column again, that earlier output."""
        first_outputs = self._first_output_by_column()
        return {
            output: first_outputs[output.column]
            for output in self.outputs
            if output is not first_outputs[output.column]
        }

    def lookup_output_column(self, column: Column) -> Column:
        """The column of the output an unqualified `column` names; any other `column` unchanged."""
        if column.path is not None:
            return column

        named = next(
            (output for output in self.outputs if output.name == column.name), None
        )
        return column if named is None else named.column

    def owner_of(self, column: Column) -> Query:
        """The query whose alias `column` reads: this one unless correlated; the outermost if none is as deep."""
        if column.path is None:
            return self

        query = self
        while query.depth != column.path.depth and query.enclosing is not None:
            query = query.enclosing
        return query

    @property
    def inlinable(self) -> bool:
        """True when a query that reads this one as a CTE can read its one table with its condition instead,
        as this query only filters that table under its own column names."""
        has_one_alias = len(self.tree.aliases) == 1
        keeps_names = all(output.name == output.column.name for output in self.outputs)
        return (
            has_one_alias
            and keeps_names
            and not self.group_keys
            and not self.aggregates.by_expression
            and not self.computed_columns.by_expression
            and not self.order_keys
            and self.limit is None
        )

    @property
    def is_grouped(self) -> bool:
        """True when the query has group keys: GROUP BY, or the outputs of a SELECT DISTINCT."""
        return bool(self.group_keys)

    @property
    def is_ungrouped_aggregation(self) -> bool:
        """True when the query aggregates all its rows into one tuple of scalars."""
        return bool(self.aggregates.by_expression) and not self.is_grouped

    @property
    def is_single_value_aggregation(self) -> bool:
        """True when the query renders to one scalar: an ungrouped aggregation with one output."""
        return self.is_ungrouped_aggregation and len(self.outputs) == 1

    @property
    def single_value_column(self) -> Column | None:
        """The column of a single-value aggregation's one output; `None` for any other query."""
        if not self.is_single_value_aggregation:
            return None

        (output,) = self.outputs
        return output.column

    def _first_output_by_column(self) -> dict[Column, Output]:
        """Per selected column, the first output that selects it."""
        first_outputs: dict[Column, Output] = {}
        for output in self.outputs:
            first_outputs.setdefault(output.column, output)
        return first_outputs

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

"""Renders a statement as FQL: a query as an expression, a write as Python statements."""

from benchmarks.datasets import PrimaryKey
from benchmarks.tools.ir.expressions import Expression, ReferencedTupleExpression
from benchmarks.tools.ir.query import Query
from benchmarks.tools.ir.statements import (
    CreateTable,
    Delete,
    DropTable,
    Insert,
    Statement,
    Update,
)
from benchmarks.tools.render.config import Config
from benchmarks.tools.render.fql_names import (
    ITEM_NAME,
    KEY_NAME,
    NEW_VALUES_NAME,
    VALUE_NAME,
    reject_unaddressable_name,
)
from benchmarks.tools.render.fragments import render_tuple_literal
from benchmarks.tools.render.query_renderer import QueryRenderer, ValueBase
from fdm.attribute_functions import RF, TF

_INDENT = "    "


class StatementRenderer:
    """Renders a `Statement` as FQL in the style of a `Config`."""

    def __init__(self, config: Config) -> None:
        self._query_renderer = QueryRenderer(config)

    def render_statement(self, statement: Statement) -> str:
        """`statement` as FQL: an expression for a query, statements for a write."""
        if isinstance(statement, Query):
            return self._query_renderer.render_query(statement)

        if isinstance(statement, Insert):
            return self._render_insert(statement)

        if isinstance(statement, Delete):
            return self._render_delete(statement)

        if isinstance(statement, Update):
            return self._render_update(statement)

        if isinstance(statement, CreateTable):
            return self._render_create_table(statement)

        if isinstance(statement, DropTable):
            return self._render_drop_table(statement)

        raise ValueError(f"Unsupported statement: {type(statement).__name__}.")

    def _render_insert(self, statement: Insert) -> str:
        """One assignment per inserted row; a NULL column stays unmapped."""
        relation = self._query_renderer.render_table(statement.written_table)

        for column in statement.columns:
            reject_unaddressable_name(column)

        lines = []
        for row in statement.rows:
            written = {
                column: expression
                for column, expression in zip(statement.columns, row)
                if not expression.is_null_literal()  # a NULL column drops the attribute
            }
            attributes = ", ".join(
                self._render_attribute(column, expression)
                for column, expression in written.items()
            )
            key = self._render_key(written, statement.primary_key)

            lines.append(
                f"{relation}[{key}] = {TF.__name__}({{{attributes}}}, frozen=False)"
            )

        return "\n".join(lines)

    def _render_delete(self, statement: Delete) -> str:
        """A loop that deletes every selected tuple."""
        query = statement.query
        relation = self._query_renderer.render_table(statement.written_table)
        selected = self._query_renderer.render_filter(
            query, relation, query.condition, False, False
        )
        return (
            f"for {KEY_NAME} in list({selected}.keys()):\n"
            f"{_INDENT}del {relation}[{KEY_NAME}]"
        )

    def _render_update(self, statement: Update) -> str:
        """`SET column = NULL` unmaps the attribute; a value that is
        NULL only at run time (a CASE without ELSE) is stored as `None`."""
        query = statement.query
        relation = self._query_renderer.render_table(statement.written_table)
        selected = self._query_renderer.render_filter(
            query, relation, query.condition, False, False
        )
        assigned = []
        unmapped = []
        for assignment in statement.assignments:
            if assignment.value.is_null_literal():
                unmapped.append(assignment.attribute)
            else:
                assigned.append((assignment.attribute, assignment.value))
        removals = "".join(
            f"\n{_INDENT}if {column!r} in {VALUE_NAME}:\n"
            f"{_INDENT * 2}del {self._query_renderer.render_access(VALUE_NAME, [column])}"
            for column in unmapped
        )
        if not assigned:
            return f"for {VALUE_NAME} in list({selected}.values()):{removals}"

        value_base = ValueBase(VALUE_NAME, False)
        targets = ", ".join(
            self._query_renderer.render_access(VALUE_NAME, [column])
            for column, _ in assigned
        )
        right_hand_sides = ", ".join(
            self._query_renderer.render_expression(expression, query, value_base, False)
            for _, expression in assigned
        )
        # SQL evaluates every SET on the rows as they were before the UPDATE, so all
        # right-hand sides are computed for every selected row before any is assigned
        return (
            f"for {VALUE_NAME}, {NEW_VALUES_NAME} in "
            f"[({VALUE_NAME}, [{right_hand_sides}]) for {VALUE_NAME} in {selected}.values()]:\n"
            f"{_INDENT}[{targets}] = {NEW_VALUES_NAME}{removals}"
        )

    def _render_attribute(self, column: str, expression: Expression) -> str:
        """The attribute that `expression` writes under `column`, as a dict entry."""
        name = _attribute_name(column, expression)
        value = self._render_value(expression)
        return f"{name!r}: {value}"

    def _render_value(self, expression: Expression) -> str:
        """`expression` as a written value, which reads no tuple."""
        value_base = ValueBase(ITEM_NAME, False)
        return self._query_renderer.render_expression(
            expression, None, value_base, False
        )

    def _render_key(
        self, written: dict[str, Expression], primary_key: PrimaryKey
    ) -> str:
        """The key of an inserted tuple; a foreign key in it keys by its raw value."""
        key_values = []
        for column in primary_key.columns:
            expression = written[column]
            if isinstance(expression, ReferencedTupleExpression):
                expression = expression.key
            rendered = self._render_value(expression)
            key_values.append(rendered)
        return render_tuple_literal(key_values)

    def _render_create_table(self, statement: CreateTable) -> str:
        """An assignment of an empty relation to the table."""
        return f"{self._query_renderer.render_table(statement.table)} = {RF.__name__}({{}}, frozen=False)"

    def _render_drop_table(self, statement: DropTable) -> str:
        """A deletion of the table from the database."""
        return f"del {self._query_renderer.render_table(statement.table)}"


def _attribute_name(column: str, value: Expression) -> str:
    """The attribute a written value is stored under: a foreign key as its reference."""
    if isinstance(value, ReferencedTupleExpression):
        return value.reference.ref_key
    return column

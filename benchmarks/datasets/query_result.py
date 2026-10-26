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

"""A query's result as SQL and FQL results are compared: rows under named columns."""

from __future__ import annotations

from typing import Any, Iterable

from fdm.attribute_functions import RF, TF, DictionaryAttributeFunction
from fql.operators.APIs import Operator


class FQLExecutionFailed(Exception):
    """An FQL string failed to run or its result to be read; the engine error is
    its cause."""


class NotTabular(Exception):
    """An FQL result that cannot be read as rows under named columns."""


class ReferenceValue(NotTabular):
    """A result attribute holds a tuple or a relation, not a value."""

    def __init__(self, attribute: str) -> None:
        super().__init__(
            f"the result attribute {attribute!r} holds a tuple or relation, not a value"
        )
        self.attribute = attribute


class NotATable(NotTabular):
    """The result is an attribute function other than a tuple or a relation."""


class NotExecuted(NotTabular):
    """The result is an operator that was never applied to its input."""


class ResultMismatch(AssertionError):
    """Two results that do not hold the same rows."""


class QueryResult:
    """Rows under named columns; values compare with Python's `==`. A result
    `from_statement` is the empty result of a statement that gives no rows (DDL,
    DML, or FQL that is not one expression): it has nothing to compare."""

    def __init__(
        self,
        columns: tuple[str, ...],
        rows: Iterable[tuple],
        *,
        from_statement: bool = False,
    ) -> None:
        self.columns = columns
        self.rows = tuple(rows)
        self.from_statement = from_statement
        for row in self.rows:
            _check_width(row, columns)

    @classmethod
    def of_fql(cls, value: Any) -> QueryResult:
        """`value` as rows: a tuple is one row, a relation one per tuple, anything
        else one value in a column named ``""``. The columns are every attribute of
        any row, in first-seen order; a row that does not map one reads it as
        `None`. Reads every value before checking for references."""
        if isinstance(value, Operator):
            raise NotExecuted(f"the result is an unexecuted {type(value).__name__}")
        if not isinstance(value, DictionaryAttributeFunction):
            return cls(("",), [(value,)])

        tuples = _tuples_of(value)
        rows = [_evaluated(tf) for tf in tuples]
        for row in rows:
            _check_values(row)
        attributes = dict.fromkeys(column for row in rows for column in row)
        columns = tuple(attributes)
        return cls(
            columns, [tuple(row.get(column) for column in columns) for row in rows]
        )

    def check_matches(self, other: QueryResult, ordered: bool = False) -> None:
        """Raise `ResultMismatch` unless `other` holds the same rows, as a multiset
        unless `ordered`. Two empty results match; a column `other` lacks reads as
        `None` (SQL's NULL) and a column `self` lacks is a mismatch. A result
        `from_statement`, on either side, never matches."""
        if self.from_statement or other.from_statement:
            raise ResultMismatch("a statement without rows has no result to compare")
        if not self.rows and not other.rows:
            return
        if not other._aligns_with(self):
            raise ResultMismatch(
                f"the columns {list(other.columns)} do not align with "
                f"{list(self.columns)}"
            )

        other_rows = other._rows_in(self.columns)
        if ordered:
            _check_same_order(self.rows, other_rows)
        else:
            _check_same_multiset(self.rows, other_rows)

    def _aligns_with(self, other: QueryResult) -> bool:
        """True when these rows can be put in the column order of `other`."""
        # a scalar's column is unnamed, so it aligns with any single column by position
        if len(self.columns) == 1 and len(other.columns) == 1:
            return (
                "" in (self.columns[0], other.columns[0])
                or self.columns == other.columns
            )
        if _has_duplicates(self.columns) or _has_duplicates(other.columns):
            return False
        return set(self.columns) <= set(other.columns)

    def _rows_in(self, columns: tuple[str, ...]) -> tuple[tuple, ...]:
        """These rows in the order of `columns`, a missing column as `None`."""
        if len(self.columns) == 1 and len(columns) == 1:
            return self.rows

        rows = [dict(zip(self.columns, row)) for row in self.rows]
        return tuple(tuple(row.get(column) for column in columns) for row in rows)


def describe_error(exc: Exception) -> str:
    """`exc` as its type name and, when it has one, its message."""
    message = str(exc)
    if not message:
        return type(exc).__name__
    return f"{type(exc).__name__}: {message}"


def _check_same_order(rows: tuple[tuple, ...], other_rows: tuple[tuple, ...]) -> None:
    """Raise `ResultMismatch` unless both row sequences match in order."""
    if rows != other_rows:
        raise ResultMismatch("the rows differ, or their order does")


def _check_same_multiset(
    rows: tuple[tuple, ...], other_rows: tuple[tuple, ...]
) -> None:
    """Raise `ResultMismatch` unless both row sequences match in any order."""
    unmatched = list(other_rows)
    for row in rows:
        if row not in unmatched:
            raise ResultMismatch("the rows differ")

        unmatched.remove(row)

    if unmatched:
        raise ResultMismatch("the rows differ")


def _tuples_of(value: Any) -> list[TF]:
    """The tuples of `value`: itself if a tuple, its tuples if a relation."""
    if isinstance(value, TF):
        return [value]
    if isinstance(value, RF):
        return _tuples_in(value)

    raise NotATable(
        f"the result is a {type(value).__name__}, not a tuple or a relation"
    )


def _tuples_in(relation: RF) -> list[TF]:
    """The tuples of `relation`."""
    try:
        return [item.value for item in relation]
    except Exception as exc:
        raise FQLExecutionFailed(
            f"the result relation cannot be read: {describe_error(exc)}"
        ) from exc


def _evaluated(tf: TF) -> dict[str, Any]:
    """The attributes of `tf` with their values read."""
    attributes = _attributes_of(tf)
    return {attribute: _read(tf, attribute) for attribute in attributes}


def _attributes_of(tf: TF) -> list[str]:
    """The attribute names of `tf`."""
    try:
        return list(tf.keys())
    except Exception as exc:
        raise FQLExecutionFailed(
            f"the attributes of a result tuple cannot be read: {describe_error(exc)}"
        ) from exc


def _read(tf: TF, attribute: str) -> Any:
    """The value of `attribute` in `tf`."""
    try:
        return tf[attribute]
    except Exception as exc:
        raise FQLExecutionFailed(
            f"the result attribute {attribute!r} cannot be evaluated: "
            f"{describe_error(exc)}"
        ) from exc


def _check_values(row: dict[str, Any]) -> None:
    """Raise `ReferenceValue` when a value of `row` is a reference."""
    for attribute, value in row.items():
        if isinstance(value, DictionaryAttributeFunction):
            raise ReferenceValue(attribute)


def _check_width(row: tuple, columns: tuple[str, ...]) -> None:
    """Raise `ValueError` unless `row` has one value per column."""
    if len(row) != len(columns):
        raise ValueError(f"the row {row!r} does not have the columns {columns!r}")


def _has_duplicates(columns: tuple[str, ...]) -> bool:
    """True when a name occurs twice in `columns`."""
    return len(set(columns)) < len(columns)

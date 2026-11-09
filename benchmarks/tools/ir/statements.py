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

"""The IR of a statement: `Statement` and its subtypes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from benchmarks.datasets.schema import PrimaryKey
from benchmarks.tools.ir.expressions import Expression

if TYPE_CHECKING:
    from benchmarks.tools.ir.query import Query


class Statement:
    """A SQL statement."""


@dataclass(frozen=True)
class Insert(Statement):
    """`INSERT ... VALUES`; each row aligns with `columns`, the table's declared columns
    (the listed ones if none are declared), an unlisted one holding `Literal(None)`."""

    written_table: str
    primary_key: PrimaryKey
    columns: tuple[str, ...]
    rows: tuple[tuple[Expression, ...], ...]

    def __post_init__(self) -> None:
        """Rejects a row that does not hold one value per column."""
        if any(len(row) != len(self.columns) for row in self.rows):
            raise ValueError("Every inserted row holds one value per column.")


@dataclass
class Delete(Statement):
    """`DELETE FROM table [WHERE ...]`."""

    query: Query

    def __post_init__(self) -> None:
        """Rejects a query that does not read exactly one root."""
        _reject_several_roots(self.query)

    @property
    def written_table(self) -> str:
        """The table this statement writes."""
        return _only_table(self.query)


@dataclass(frozen=True)
class Assignment:
    """`attribute = value` in the SET clause of an `UPDATE`."""

    attribute: str
    value: Expression


@dataclass
class Update(Statement):
    """`UPDATE table SET ... [WHERE ...]`."""

    query: Query
    assignments: tuple[Assignment, ...]

    def __post_init__(self) -> None:
        """Rejects a query that does not read exactly one root."""
        _reject_several_roots(self.query)

    @property
    def written_table(self) -> str:
        """The table this statement writes."""
        return _only_table(self.query)


@dataclass
class CreateTable(Statement):
    """`CREATE TABLE table (...)`."""

    table: str
    primary_key: PrimaryKey


@dataclass
class DropTable(Statement):
    """`DROP TABLE table`."""

    table: str


def _reject_several_roots(query: Query) -> None:
    """Rejects a query that does not read exactly one root."""
    if len(query.tree.roots) != 1:
        raise ValueError("A write statement reads exactly one root.")


def _only_table(query: Query) -> str:
    """The table of the one root `query` reads."""
    (root,) = query.tree.roots
    table = query.tree.table_of(root)
    # a root is an alias of this tree, so it has a table
    assert table is not None
    return table

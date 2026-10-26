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
"""The keys, references and columns a statement is parsed against."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from benchmarks.datasets.dataset_types import (
    DatasetPrimaryKey,
    References,
    SqlSchema,
)


@dataclass(frozen=True)
class PrimaryKey:
    """The columns a relation stores its tuples under, in key order."""

    columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("A primary key names at least one column.")

    @classmethod
    def of(cls, key: DatasetPrimaryKey) -> PrimaryKey:
        """`key` as a dataset declares it: a column name, or a tuple of them."""
        return cls((key,) if isinstance(key, str) else tuple(key))

    def key_of(self, row: dict[str, Any]) -> Any:
        """The key `row` is stored under: its one key value for a single-column key,
        the tuple of its key values for a composite one."""
        values = tuple(row[column] for column in self.columns)
        return values[0] if len(values) == 1 else values


@dataclass(frozen=True)
class Schema:
    """The dataset's tables as a statement is parsed against them; `columns` is
    empty for a dataset without a reference database."""

    references: References
    primary_keys: dict[str, PrimaryKey]
    columns: SqlSchema

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
"""The shared type vocabulary of the dataset package."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Reference:
    """A reference whose attribute is named `reference_name`, which
    refers to the tuples of `target_table`."""

    reference_name: str
    target_table: str


# per table, per foreign key column, the reference it is swizzled into
References = dict[str, dict[str, Reference]]
# per table, per column, the SQL type
SqlSchema = dict[str, dict[str, str]]
# per table, its rows as column-to-value dicts
DatasetRowData = dict[str, list[dict[str, Any]]]
# the column of a single-column key, the columns of a composite one
DatasetPrimaryKey = str | tuple[str, ...]

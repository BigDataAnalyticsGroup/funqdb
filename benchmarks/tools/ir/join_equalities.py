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

"""The equalities a query states between its aliases, as the join tree reads them."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Edge:
    """The equality of a foreign key of `source` with the key of `target`, as its reference `ref_key`."""

    source: str
    ref_key: str
    target: str


@dataclass(frozen=True)
class ReferenceRead:
    """The reference `ref_key` read off `alias`."""

    alias: str
    ref_key: str


@dataclass(frozen=True)
class ForeignKeyEquality:
    """The stated equality of two foreign keys into `target_table`."""

    target_table: str
    left: ReferenceRead
    right: ReferenceRead


# an equality between aliases, as the parser reads it off a condition
JoinEquality = Edge | ForeignKeyEquality

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

"""`Path`: how a query reaches the tuple of an alias."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Path:
    """The path from the tuple of the root alias `root` along `ref_keys` to the tuple of `alias`,
    in the query `depth` levels deep."""

    depth: int
    alias: str
    root: str
    ref_keys: tuple[str, ...]

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

"""Names that the transpiler makes up."""

import unicodedata
from collections.abc import Set

from benchmarks.datasets import is_python_name


def fresh_name(base: str, taken: Set[str]) -> str:
    """`base` with the smallest suffix `_<n>` that makes it a name not in `taken`."""
    number = 0
    while f"{base}_{number}" in taken:
        number += 1
    return f"{base}_{number}"


def as_python_name(text: str) -> str:
    """`text` with each character Python does not allow in a name as `_`,
    and `_` in front of a leading digit or a keyword."""
    normalized = unicodedata.normalize("NFKC", text)
    characters = [
        character if f"_{character}".isidentifier() else "_" for character in normalized
    ]
    name = "".join(characters)
    if is_python_name(name):
        return name

    return f"_{name}"

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

"""SQL LIKE patterns, as both predicate forms render them."""

import re
from dataclasses import dataclass
from enum import Enum


class LikeShape(Enum):
    """The shape of a SQL LIKE pattern; only `REGEX` needs a regular expression."""

    EXACT = "exact"
    PREFIX = "prefix"
    SUFFIX = "suffix"
    CONTAINS = "contains"
    REGEX = "regex"


@dataclass(frozen=True)
class LikePattern:
    """A SQL LIKE `pattern`, where `%` matches any characters and `_` one character."""

    pattern: str

    @property
    def shape(self) -> LikeShape:
        """The simplest test that matches the values this pattern matches."""
        if "_" in self.pattern:
            return LikeShape.REGEX

        wildcards = self.pattern.count("%")
        if wildcards == 0:
            return LikeShape.EXACT
        if wildcards == 1 and self.pattern.endswith("%"):
            return LikeShape.PREFIX
        if wildcards == 1 and self.pattern.startswith("%"):
            return LikeShape.SUFFIX
        if (
            wildcards == 2
            and self.pattern.startswith("%")
            and self.pattern.endswith("%")
        ):
            return LikeShape.CONTAINS

        return LikeShape.REGEX

    @property
    def regex(self) -> str:
        """The regular expression that `re.fullmatch` reads as SQL reads the pattern."""
        parts: list[str] = ["(?s)"]
        for char in self.pattern:
            if char == "%":
                parts.append(".*")
            elif char == "_":
                parts.append(".")
            else:
                parts.append(re.escape(char))

        return "".join(parts)

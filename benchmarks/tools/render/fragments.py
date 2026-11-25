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

"""The FQL fragments both predicate forms render: attribute paths and literals."""

from datetime import date, datetime
from decimal import Decimal

from benchmarks.tools.ir.expressions import Column
from benchmarks.tools.ir.query import Query
from benchmarks.tools.render.fql_names import (
    GROUP_KEY_SEPARATOR,
    reject_unaddressable_name,
)


def group_aware_segments(
    column: Column, query: Query, post_group: bool, joined: bool
) -> list[str]:
    """The attribute path reading `column`. Once `query` is grouped, a group key
    is the single attribute `key_to_value` reintroduced it under."""
    if post_group and column in query.group_keys:
        group_key = render_group_key_attribute(column, joined)
        return [group_key]

    return column.segments(joined)


def render_group_key_attribute(column: Column, joined: bool) -> str:
    """The attribute that `key_to_value` reintroduces the group key `column` under."""
    return render_path_argument(column, GROUP_KEY_SEPARATOR, joined)


def render_path_argument(column: Column, separator: str, joined: bool) -> str:
    """The path of `column` as one FDM argument, its names joined by `separator`."""
    segments = column.segments(joined)
    return join_path(segments, separator)


def join_path(segments: list[str], separator: str) -> str:
    """`segments` as one FDM path argument; raises for a segment that cannot be
    addressed inside one (see `reject_unaddressable_name`)."""
    for segment in segments:
        reject_unaddressable_name(segment)
    return separator.join(segments)


def render_tuple_literal(items: list[str]) -> str:
    """One item as itself, several as a parenthesised Python tuple."""
    if len(items) == 1:
        return items[0]

    return f"({', '.join(items)})"


def render_literal(value: object) -> str:
    """`value` as a Python literal that the FQL sandbox evaluates."""
    # datetime is a subclass of date, so it must be checked first.
    if isinstance(value, datetime):
        return (
            f"{datetime.__name__}({value.year}, {value.month}, {value.day}, "
            f"{value.hour}, {value.minute}, {value.second}, {value.microsecond})"
        )

    if isinstance(value, date):
        return f"{date.__name__}({value.year}, {value.month}, {value.day})"

    if isinstance(value, Decimal):
        return f"{Decimal.__name__}({str(value)!r})"

    if isinstance(value, str):
        return repr(value)

    return str(value)

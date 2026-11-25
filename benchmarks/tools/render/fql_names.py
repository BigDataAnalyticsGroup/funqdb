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

"""The names rendered FQL binds and the separators FDM reads inside a name."""

from benchmarks.tools.limitations import Limitation, UnsupportedSql
from fdm.attribute_functions import DictionaryAttributeFunction

# the member names of an attribute function, which attribute access finds first
SHADOWED_NAMES = frozenset(
    name for name in dir(DictionaryAttributeFunction) if not name.startswith("_")
)
FDM_LOOKUP_SEPARATOR = "__"  # used by grouping and aggregation as separator
PROJECTION_SEPARATOR = "."  # used by project as separator
GROUP_KEY_SEPARATOR = "_"  # used by key_to_value as separator when reintroducing
_FDM_PATH_SEPARATORS = (FDM_LOOKUP_SEPARATOR, PROJECTION_SEPARATOR)

# The names rendered lambdas and loops bind. A nested binding carries its nesting
# level as a number suffix (see `query_renderer._nested_name`).
# an item of a relation: lambda i: i.value.x
ITEM_NAME = "i"
# a tuple: lambda tf: tf.x
TUPLE_NAME = "tf"
NESTED_NAMES = (ITEM_NAME, TUPLE_NAME)
# The names below are never nested.
# the values a DISTINCT aggregate reads
VALUES_NAME = "values"
# an item of an IN subquery's result, read only by the selected column
ELEMENT_NAME = "x"
# a DELETE loop's key
KEY_NAME = "key"
# an UPDATE loop's tuple
VALUE_NAME = "value"
# an UPDATE loop's assigned values
NEW_VALUES_NAME = "new"
# the names that hide the database where they are bound
UNNESTED_NAMES = (ELEMENT_NAME, KEY_NAME, VALUE_NAME, NEW_VALUES_NAME)


def reject_unaddressable_name(name: str) -> None:
    """Raises `UnsupportedSql` for a name that contains `__` or `.`, which FDM reads
    as a path separator."""
    for separator in _FDM_PATH_SEPARATORS:
        if separator in name:
            raise UnsupportedSql(
                Limitation.PATH_SEPARATOR_NAME,
                f"Unsupported name {name!r}: FDM reads {separator!r} inside a key as a path separator, "
                "so this attribute cannot be addressed.",
            )

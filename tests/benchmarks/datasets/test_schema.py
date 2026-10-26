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

"""The schema facts a statement is parsed against: primary keys."""

import pytest

from benchmarks.datasets import PrimaryKey


def test_PrimaryKey_init_without_columns_raises_ValueError():
    """No relation stores its tuples under nothing, so an empty key is rejected."""
    with pytest.raises(ValueError):  # a key over no column is invalid
        PrimaryKey(())  # the constructor enforces at least one column


def test_PrimaryKey_of_normalises_name_and_tuple_to_columns():
    """A key name and a tuple of key names give the same `PrimaryKey` shape."""
    assert PrimaryKey.of("id").columns == ("id",)  # a bare name is one column
    assert PrimaryKey.of(("a", "b")).columns == ("a", "b")  # a tuple keeps its order
    assert PrimaryKey.of("id") == PrimaryKey.of(("id",))  # two spellings, one key


def test_PrimaryKey_key_of_returns_value_for_one_column_and_tuple_for_several():
    """A single-column key gives the bare value, a composite key a tuple."""
    row = {"id": 7, "line": 2, "note": "x"}  # one row, keyed either way

    assert PrimaryKey(("id",)).key_of(row) == 7  # not the 1-tuple (7,)
    assert PrimaryKey(("id", "line")).key_of(row) == (7, 2)  # in key order
    assert PrimaryKey(("line", "id")).key_of(row) == (2, 7)  # order matters

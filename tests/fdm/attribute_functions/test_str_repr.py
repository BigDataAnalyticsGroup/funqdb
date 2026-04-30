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

import pytest

from fdm.attribute_functions import DictionaryAttributeFunction, TF
from fql.util import ReadOnlyError


def test_print_and_str(capsys):
    """Verify print() in flat/non-flat mode and __str__ for nested and plain values."""
    inner: TF = TF({"x": 1})
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={"nested": inner, "plain": 42}
    )

    daf.print(flat=False)
    captured = capsys.readouterr()
    assert "nested:" in captured.out
    assert "x: 1" in captured.out
    assert "plain: 42" in captured.out

    daf.print(flat=True)
    captured = capsys.readouterr()
    assert "nested:" in captured.out
    assert "plain: 42" in captured.out

    s: str = str(daf)
    assert "nested:" in s
    assert "plain: 42" in s


def test_repr():
    """Verify that __repr__ returns the class itself (used for debugger display)."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={"a": 1})
    assert daf.__repr__() == DictionaryAttributeFunction


def test_get_lineage_and_add_lineage():
    """Verify get_lineage() returns the lineage and add_lineage() appends to it."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={}, lineage=["origin"]
    )
    assert daf.get_lineage() == ["origin"]

    daf.add_lineage("step1")
    assert daf.get_lineage() == ["origin", "step1"]


def test_frozen_add_lineage():
    """Verify that adding lineage to a frozen DAF raises ReadOnlyError."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={}, frozen=True)

    with pytest.raises(ReadOnlyError):
        daf.add_lineage("nope")

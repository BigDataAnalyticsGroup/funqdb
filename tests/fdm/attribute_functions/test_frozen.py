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

from fdm.attribute_functions import DictionaryAttributeFunction
from fql.util import ReadOnlyError


def test_frozen_add_remove_attribute_function_constraint():
    """Verify that adding or removing an AF-constraint on a frozen DAF raises ReadOnlyError."""
    from fql.predicates.constraints import max_count

    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={"a": 1}, frozen=True
    )
    c: max_count = max_count(10)

    with pytest.raises(ReadOnlyError):
        daf.add_attribute_function_constraint(c)

    with pytest.raises(ReadOnlyError):
        daf.remove_attribute_function_constraint(c)


def test_frozen_remove_values_constraint():
    """Verify that removing a values-constraint on a frozen DAF raises ReadOnlyError."""
    from fql.predicates.constraints import max_count

    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={"a": 1}, frozen=True
    )
    c: max_count = max_count(10)

    with pytest.raises(ReadOnlyError):
        daf.remove_values_constraint(c)


def test_frozen_add_remove_observer():
    """Verify that adding or removing an observer on a frozen DAF raises ReadOnlyError."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={"a": 1}, frozen=True
    )

    with pytest.raises(ReadOnlyError):
        daf.add_observer(daf)

    with pytest.raises(ReadOnlyError):
        daf.remove_observer(daf)


def test_frozen_property():
    """Verify that freeze() and unfreeze() correctly toggle the frozen state."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={"a": 1})
    assert daf.__dict__["frozen"] is False
    daf.freeze()
    assert daf.__dict__["frozen"] is True
    daf.unfreeze()
    assert daf.__dict__["frozen"] is False


def test_constraint_violation_rollback_new_key():
    """Verify that inserting a key that violates an AF-constraint is rolled back (key removed)."""
    from fql.predicates.constraints import attribute_name_equivalence
    from fql.util import ConstraintViolationError

    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={"a": 1})
    daf.add_attribute_function_constraint(attribute_name_equivalence({"a"}))

    with pytest.raises(ConstraintViolationError):
        daf["b"] = 2

    assert "b" not in daf
    assert len(daf) == 1


def test_frozen_delitem():
    """Verify that deleting an item from a frozen DAF raises ReadOnlyError."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={"a": 1}, frozen=True
    )

    with pytest.raises(ReadOnlyError):
        del daf["a"]


def test_delitem_nonexistent():
    """Verify that deleting a non-existent key raises AttributeError."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={"a": 1})

    with pytest.raises(AttributeError):
        del daf["nonexistent"]

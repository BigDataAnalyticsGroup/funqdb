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

from fdm.attribute_functions import DictionaryAttributeFunction, DBF, RF
from tests.lib import _create_testdata


def test_DictionaryAttributeFunction():
    daf = DictionaryAttributeFunction(data={"x": 0, "y": 0, 42: "answer"})

    # accessing a non-existing attribute raises AttributeError
    with pytest.raises(AttributeError):
        assert daf.z == 0

    # check existing attributes:
    assert "x" in daf
    assert "y" in daf
    assert "z" not in daf

    # int attributes
    assert 3 not in daf
    assert 42 in daf
    assert daf[42] == "answer"
    assert daf.x == 0
    assert daf.y == 0

    assert daf.uuid == 0

    # check assigning to an existing attribute:
    daf.x = 42
    assert daf.x == 42

    daf[42] = "a new answer"
    assert daf[42] == "a new answer"

    # create/assign to a new attribute:
    daf.z = 100

    assert daf.z == 100
    assert "z" in daf
    assert len(daf) == 4

    daf[43] = "another answer"
    assert daf[43] == "another answer"
    assert 43 in daf

    daf[42.42] = "float answer"
    assert daf[42.42] == "float answer"
    assert 42.42 in daf

    # delete an attribute:
    del daf.z
    assert "z" not in daf
    assert len(daf) == 5

    # delete int and float attributes
    del daf[42]
    assert 42 not in daf
    assert len(daf) == 4

    del daf[42.42]
    assert 42.42 not in daf
    assert len(daf) == 3


def test_underscore_syntax():
    db: DBF = _create_testdata(frozen=False)
    users: RF = db.users

    assert users[1]["department__name"] == "Dev"
    assert users[2]["department__name"] == "Dev"
    assert users[3]["department__name"] == "Consulting"
    assert users[3]["department__name"] == "Consulting"
    # multiple "__" should also work:
    assert db["departments__d1__name"] == "Dev"
    assert db("departments__d1__name") == "Dev"

    # dot syntax combined with underscore syntax:
    assert users[1].department__name == "Dev"
    assert users[2].department__name == "Dev"
    assert users[3].department__name == "Consulting"

    # vs good old dot syntax:
    assert users[1].department.name == "Dev"
    assert users[2].department.name == "Dev"
    assert users[3].department.name == "Consulting"


def test_eq_different_type():
    """Verify that comparing a DAF with a non-DAF object returns False."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={"a": 1})
    assert daf != "not a DAF"


def test_copy():
    """Verify that copy() creates a new DAF with a distinct UUID but identical data."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(
        data={"a": 1, "b": 2}
    )
    original_uuid: int = daf.uuid
    daf_copy: DictionaryAttributeFunction = daf.copy()

    assert daf_copy.uuid != original_uuid
    assert daf_copy["a"] == 1
    assert daf_copy["b"] == 2

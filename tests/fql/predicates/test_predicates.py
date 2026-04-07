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

from fdm.attribute_functions import DictionaryAttributeFunction
from fql.predicates.constraints import in_subset, attribute_name_equivalence, max_count
from fql.util import ChangeEvent


def test_predicates():
    i = in_subset({"a", "b", "c"})
    assert i("a") is True
    assert i("d") is False
    assert i("b") is True
    assert i("c") is True


def test_attribute_name_equivalence():
    """Verify that attribute_name_equivalence checks whether keys match the expected set exactly."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={"name": "Alice", "age": 30})
    constraint: attribute_name_equivalence = attribute_name_equivalence({"name", "age"})

    assert constraint(daf, ChangeEvent.UPDATE) is True

    constraint_wrong: attribute_name_equivalence = attribute_name_equivalence({"name", "yob"})
    assert constraint_wrong(daf, ChangeEvent.UPDATE) is False


def test_max_count():
    """Verify that max_count checks whether the number of entries stays within the limit."""
    daf: DictionaryAttributeFunction = DictionaryAttributeFunction(data={"a": 1, "b": 2})
    c: max_count = max_count(3)
    assert c(daf) is True

    c2: max_count = max_count(1)
    assert c2(daf) is False

    c3: max_count = max_count(2)
    assert c3(daf) is True

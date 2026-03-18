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


from abc import abstractmethod, ABC

from fdm.API import AttributeFunction
from fql.util import ChangeEvent


class in_subset:
    def __init__(self, whitelist: set[str]):
        self.whitelist = whitelist

    def __call__(self, *args, **kwargs) -> bool:
        return args[0] in self.whitelist


class AttributeFunctionConstraint(ABC):
    """Marks an attribute function constraint, i.e. a constraint that must hold for the entire attribute function not
    just one particular item."""

    @abstractmethod
    def __call__(
        self, attribute_function: AttributeFunction, event: ChangeEvent
    ) -> bool:
        """Evaluates whether the given attribute_function fulfills the constraint."""
        ...


class attribute_name_equivalence(AttributeFunctionConstraint):
    """Predicate that checks if the foreign_objects in a given attribute_function match a given set."""

    def __init__(self, attribute_names: set[str]):
        self.attribute_names = attribute_names

    def __call__(
        self, attribute_function: AttributeFunction, event: ChangeEvent
    ) -> bool:
        assert isinstance(attribute_function, AttributeFunction)
        return self.attribute_names == {item.key for item in attribute_function}


class max_count(AttributeFunctionConstraint):
    """Predicate that checks if the number of entries in a given item is below a maximum."""

    def __init__(self, max_count_limit: int):
        self.max_count_limit = max_count_limit

    def __call__(self, af: AttributeFunction) -> bool:
        return len(af) <= self.max_count_limit

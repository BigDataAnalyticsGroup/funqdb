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


from typing import Type

from fdm.API import AttributeFunction
from fdm.attribute_functions import DictionaryAttributeFunction
from fql.predicates.constraints import AttributeFunctionConstraint
from store.store import Store


class Schema[Key](DictionaryAttributeFunction[Key, Type], AttributeFunctionConstraint):
    """A schema is an attribute function that defines the expected keys and their types for items in a relation."""

    def __init__(
        self,
        data=None,
        frozen=False,
        observe_items: bool = False,
        lineage: list[str] = None,
        store: Store = None,
    ):
        """Initialize a Schema with the given data and properties.
        @param data: A dictionary mapping keys to their expected types.
        @param frozen: Whether the schema is frozen (i.e., cannot be modified).
        @param observe_items: Whether to observe items for changes (not implemented).
        @param lineage: A list of strings representing the lineage of this schema (not implemented).
        @param store: A Store instance for managing this schema (not implemented).
        """
        super().__init__(
            data=data,
            frozen=frozen,
            observe_items=observe_items,
            lineage=lineage,
            store=store,
        )

    def __call__(self, attribute_function: AttributeFunction) -> bool:
        """Evaluates whether the given attribute_function fulfills the schema."""
        assert isinstance(attribute_function, AttributeFunction)

        # check if all keys in the schema are present in the attribute function and their types are compatible
        for item in attribute_function:
            if item.key not in self:
                return False
            if not isinstance(item.value, self[item.key]):
                return False
        return True

    def __hash__(self):
        """Compute the hash of the Schema based on its items.
        @return: The hash value of the Schema.
        """
        return AttributeFunction.__hash__(self)


class ForeignKeyConstraint[Key](AttributeFunctionConstraint):
    """A foreign key constraint is an attribute function constraint that a given value of an attribute function must
    be mapped to by another attribute function (the parent). This is used to express foreign key constraints between
    relations. This class is from the point of view of the referrer, i.e., the relation that has the foreign key
    reference to another relation."""

    def __init__(self, key: Key, parent_attribute_function: AttributeFunction):
        self.key = key
        self.parent_attribute_function = parent_attribute_function

    def __call__(self, attribute_function: AttributeFunction) -> bool:
        assert isinstance(attribute_function, AttributeFunction)

        # check whether the value mapped to by attribute_function[self.key] is available in the parent attribute
        # function, i.e., whether there is an item in the parent attribute function that maps to this value
        # O(n) find, TODO: replace by indexed version
        # maybe extend AFs to generally index on their values
        value_to_find = attribute_function[self.key]
        return (
            len(
                self.parent_attribute_function.where(lambda i: i.value == value_to_find)
            )
            > 0
        )


class ReverseForeignKeyConstraint[Key](AttributeFunctionConstraint):
    """This is the reverse of a foreign key constraint, i.e., it is from the point of view of the referenced relation."""

    def __init__(self, key: Key, child_attribute_function: AttributeFunction):
        self.key = key
        self.child_attribute_function = child_attribute_function

    def __call__(self, attribute_function: AttributeFunction) -> bool:
        assert isinstance(attribute_function, AttributeFunction)
        return (
            len(
                self.child_attribute_function.where(
                    lambda i: i.value[self.key] == attribute_function
                )
            )
            == 0
        )

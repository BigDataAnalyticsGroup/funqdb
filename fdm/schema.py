from typing import Type

from fdm.API import AttributeFunction
from fdm.attribute_functions import DictionaryAttributeFunction
from fql.predicates.constraints import ItemConstraint, AttributeFunctionConstraint


class Schema[Key](DictionaryAttributeFunction[Key, Type], AttributeFunctionConstraint):
    """A schema is an attribute function that defines the expected keys and their types for items in a relation."""

    def __init__(self):
        super().__init__()

    def __call__(self, attribute_function: AttributeFunction) -> bool:
        """Evaluates whether the given attribute_function fulfills the schema."""
        assert isinstance(attribute_function, AttributeFunction)

        # check all keys in the schema are present in the attribute function and their types are compatible
        for item in attribute_function:
            if item.key not in self:
                return False
            if not issubclass(type(item.value), self[item.key]):
                return False
        return True

    def __hash__(self):
        """Compute the hash of the Schema based on its items.
        @return: The hash value of the Schema.
        """
        return AttributeFunction.__hash__(self)


# TODO: write test

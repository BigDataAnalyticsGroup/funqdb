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


# TODO: write test

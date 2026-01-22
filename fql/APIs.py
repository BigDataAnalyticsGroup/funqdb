from abc import ABC, abstractmethod
from dataclasses import dataclass


class PureFunction[INPUT, OUTPUT](ABC):
    """An abstract mapping_function."""

    @abstractmethod
    def __call__(self, *args, **kwargs) -> OUTPUT:
        """Make the object callable.
        @param arg: The argument for the call.
        @return: The result of the call.
        """
        pass


class AttributeFunction[Key, Value](PureFunction):
    """An abstract base class representing a callable object that can also manage its attributes."""

    def __init__(self):
        super().__init__()

    def __getattr__(self, name: str) -> Value:
        """Make the object callable through .-syntax.
        Redirects to __getitem__ for actual retrieval.
        @param name: Name of the attribute being accessed. Notice that the 'name' parameter is of type 'Any' to allow
        flexibility in attribute types.
        @return: The value of the requested attribute.
        """
        return self.__getitem__(name)

    def __setattr__(self, name: str, value: Value):
        """Customize attribute assignment. Note that the 'name' parameter is of type 'Any' to allow flexibility in
        attribute types. Redirects to __setitem__ for actual storage."""
        self.__setitem__(name, value)

    def __delattr__(self, name):
        """Customize attribute deletion. Redirects to __delitem__ for actual deletion."""
        self.__delitem__(name)

    def __getitem__(self, key: Key) -> Value:
        """Make the object callable through []-syntax."""
        pass

    def __call__(self, *args, **kwargs) -> "AttributeFunction":
        """Make the object callable through () syntax.
        @return: The result of the call.
        """
        return self.__getitem__(*args, **kwargs)

    def update(self, name, value):
        """Update or add an attribute. In addition to setting the attribute, this method returns the object itself,
        allowing for method chaining.
        @param name: The name of the attribute to update or add.
        @param value: The new value for the attribute.
        """
        self.__setitem__(name, value)
        return self

    def __eq__(self, other: "AttributeFunction") -> bool:
        """Check equality between two AttributeFunction instances based on their items.
        @param other: The other AttributeFunction instance to compare with.
        @return: True if both instances have the same items, False otherwise.
        """
        pass


@dataclass
class Item[Key, Value]:
    """A simple key-value pair (aka item) representation."""

    key: Key
    value: Value

    def __eq__(self, other: "Item") -> bool:
        """Check equality between two DictionaryAttributeFunction instances based on their items.
        @param other: The other DictionaryAttributeFunction instance to compare with.
        @return: True if both instances have the same items, False otherwise.
        """
        return self.key == other.key and self.value == other.value

    def __hash__(self) -> int:
        """Compute the hash of the Item based on its key and value.
        @return: The hash value of the Item.
        """
        return hash(self.key)

from abc import ABC, abstractmethod


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

    def __eq__(self, other: "AttributeFunction") -> bool:
        """Check equality between two AttributeFunction instances based on their items.
        @param other: The other AttributeFunction instance to compare with.
        @return: True if both instances have the same items, False otherwise.
        """
        pass

    def update(self, other: "AttributeFunction") -> "AttributeFunction":
        """Update the current AttributeFunction with another one.
        @param other: The other AttributeFunction to update from.
        @return: The updated AttributeFunction.
        """
        pass

    @property
    @abstractmethod
    def frozen(self) -> bool:
        pass

    def freeze(self):
        """Make the AttributeFunction read-only."""
        pass

    def unfreeze(self):
        """Make the AttributeFunction writable."""
        pass

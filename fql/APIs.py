from abc import ABC, abstractmethod
from typing import Any, Callable


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


class AttributeFunctionWrapper[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    PureFunction[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """A base class for wrapping AttributeFunction instances."""

    def __init__(self, wrapped: AttributeFunction):
        super().__init__()
        self.__dict__["_wrapped"] = wrapped

    def __call__(self, *args, **kwargs) -> OUTPUT_AttributeFunction:
        """Make the object callable.
        @param arg: The argument for the call.
        @return: The result of the call.
        """
        return self._wrapped(*args, **kwargs)

    def __getattr__(self, name: str) -> OUTPUT_AttributeFunction:
        """Delegate attribute access to the wrapped AttributeFunction.
        @param name: Name of the attribute being accessed.
        @return: The value of the requested attribute from the wrapped AttributeFunction.
        """
        return self._wrapped.__getattr__(name)

    def __setattr__(self, name: str, value: OUTPUT_AttributeFunction):
        """Delegate attribute assignment to the wrapped AttributeFunction.
        @param name: Name of the attribute being assigned.
        @param value: The value to assign to the attribute.
        """
        self._wrapped.__setattr__(name, value)

    def __delattr__(self, name):
        """Delegate attribute deletion to the wrapped AttributeFunction.
        @param name: Name of the attribute being deleted.
        """
        self._wrapped.__delattr__(name)

    def __getitem__(self, key: Any) -> OUTPUT_AttributeFunction:
        """Delegate item access to the wrapped AttributeFunction.
        @param key: The key of the item being accessed.
        @return: The value of the requested item from the wrapped AttributeFunction.
        """
        return self._wrapped.__getitem__(key)

    def __setitem__(self, key: Any, value: OUTPUT_AttributeFunction):
        """Delegate item assignment to the wrapped AttributeFunction.
        @param key: The key of the item being assigned.
        @param value: The value to assign to the item.
        """
        self._wrapped.__setitem__(key, value)

    def __delitem__(self, key):
        """Delegate item deletion to the wrapped AttributeFunction.
        @param key: The key of the item being deleted.
        """
        self._wrapped.__delitem__(key)

    def __eq__(self, other: AttributeFunction) -> bool:
        """Check equality between two wrapped AttributeFunction instances based on their items.
        @param other: The other AttributeFunction instance to compare with.
        @return: True if both wrapped instances have the same items, False otherwise.
        """
        return self._wrapped.__eq__(other)

    @property
    def frozen(self) -> bool:
        return self._wrapped.frozen

    def freeze(self):
        self._wrapped.freeze()

    def unfreeze(self):
        self._wrapped.unfreeze()


class ConstrainedAttributeFunction[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    AttributeFunctionWrapper[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """A class for AttributeFunctions with constraints."""

    def __init__(self, wrapped, constraints: set[Callable[[Any], bool]]):
        super().__init__(wrapped)
        self.__dict__["_constraints"] = constraints

    def __setitem__(self, key: Any, value: OUTPUT_AttributeFunction):
        """Delegate item assignment to the wrapped AttributeFunction.
        @param key: The key of the item being assigned.
        @param value: The value to assign to the item.
        """
        for constraint in self._constraints:
            if not constraint(value):
                raise ValueError(
                    f"Value '{value}' does not satisfy constraint '{constraint}'."
                )

        self._wrapped.__setitem__(key, value)

# good article:
# https://realpython.com/python-magic-methods/
import logging
import uuid
from abc import abstractmethod, ABC

from fdm.util import Explainable

logger = logging.Logger(__name__)


class PureFunction[INPUT, OUTPUT](ABC):
    """An abstract mapping_function."""

    @abstractmethod
    def __call__(self, *args, **kwargs) -> OUTPUT:
        """Make the object callable.
        @param arg: The argument for the call.
        @return: The result of the call.
        """
        ...


class AttributeFunction[Key, Value](PureFunction, Explainable):
    """An abstract base class representing a callable object that can also manage its attributes."""

    global_uuid: int = 0

    def __init__(self):
        super().__init__()
        self.__dict__["_uuid"] = AttributeFunction.global_uuid
        AttributeFunction.global_uuid += 1

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

    @abstractmethod
    def __getitem__(self, key: Key) -> Value:
        """Make the object callable through []-syntax."""
        ...

    @abstractmethod
    def __setitem__(self, key: Key, value: Value):
        """Customize item assignment. This must be used for non-str-type keys.
        @param key: The key of the item being assigned.
        @param value: The value to assign to the item.
        """
        ...

    def __call__(self, *args, **kwargs) -> "AttributeFunction":
        """Make the object callable through () syntax.
        @return: The result of the call.
        """
        return self.__getitem__(*args, **kwargs)

    @abstractmethod
    def __eq__(self, other: "AttributeFunction") -> bool:
        """Check equality between two AttributeFunction instances based on their items.
        @param other: The other AttributeFunction instance to compare with.
        @return: True if both instances have the same items, False otherwise.
        """
        ...

    @abstractmethod
    def update(self, other: "AttributeFunction") -> "AttributeFunction":
        """Update the current AttributeFunction with another one.
        @param other: The other AttributeFunction to update from.
        @return: The updated AttributeFunction.
        """
        ...

    @abstractmethod
    def __len__(self) -> int:
        """Return the number of items in the AttributeFunction.
        @return: The number of items.
        """
        ...

    @property
    @abstractmethod
    def frozen(self) -> bool: ...

    @property
    def uuid(self) -> int:
        """Get the UUID of this AttributeFunction.
        @return: The UUID.
        """
        return self.__dict__["_uuid"]

    @abstractmethod
    def freeze(self):
        """Make the AttributeFunction read-only."""
        ...

    @abstractmethod
    def unfreeze(self):
        """Make the AttributeFunction writable."""
        ...

    @abstractmethod
    def get_lineage(self) -> list[str]:
        """Get the lineage of this AttributeFunction.
        @return: A list representing the lineage.
        """
        ...

    def add_lineage(self, entry: str):
        """Add an entry to the lineage of this AttributeFunction.
        @param entry: The lineage entry to add.
        """
        ...

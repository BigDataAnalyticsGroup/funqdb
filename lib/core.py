# good article:
# https://realpython.com/python-magic-methods/
from abc import ABC
from typing import Any, Iterator


class AttributeFunction(ABC):
    """An abstract base class representing a callable object that can also manage its attributes."""

    def __init__(self):
        pass

    def __getattr__(self, name: str):
        """Customize attribute access. Redirects to __getitem__ for actual retrieval.
        @param name: Name of the attribute being accessed. Notice that the 'name' parameter is of type 'Any' to allow
        flexibility in attribute types.
        @return: The value of the requested attribute.
        """
        return self.__getitem__(name)

    def __setattr__(self, name: str, value: Any):
        """Customize attribute assignment. Note that the 'name' parameter is of type 'Any' to allow flexibility in
        attribute types. Redirects to __setitem__ for actual storage."""
        self.__setitem__(name, value)

    def __delattr__(self, name: Any):
        """Customize attribute deletion. Note that the 'name' parameter is of type 'Any' to allow flexibility in
        attribute types."""
        pass


class DictionaryAttributeFunction(AttributeFunction):
    """An AttributeFunction that uses a dictionary to store its attributes."""

    def __init__(self, data=None):
        super().__init__()
        if data is None:
            data = {}
        self.__dict__[f"data"] = data or {}

    def __getitem__(self, name: Any) -> Any:
        """Customize item access. This must be used for non-str-type keys.
        @param key: The key of the item being accessed.
        @return: The value of the requested item.
        """
        if name in self.__dict__["data"]:
            return self.__dict__["data"][name]
        else:
            raise AttributeError

    def __setitem__(self, name: Any, value: Any):
        """Customize item assignment. This must be used for non-str-type keys.
        @param key: The key of the item being assigned.
        @param value: The value to assign to the item.
        """
        self.__dict__["data"][name] = value

    def __delattr__(self, name):
        if name in self.__dict__["data"]:
            del self.__dict__["data"][name]
        else:
            raise AttributeError

    def __contains__(self, item):
        return item in self.__dict__["data"]

    def __len__(self):
        return len(self.__dict__["data"])


class TF(DictionaryAttributeFunction):
    """A dictionary-based attribute function that behaves like a tuple."""
    pass


class RF(DictionaryAttributeFunction):
    """A dictionary-based attribute function that behaves like a relation."""
    pass


class DBF(DictionaryAttributeFunction):
    """A dictionary-based attribute function that behaves like a database."""
    pass

# good article:
# https://realpython.com/python-magic-methods/
from abc import ABC, abstractmethod
from typing import ItemsView


class AttributeFunction[Key, Value](ABC):
    """An abstract base class representing a callable object that can also manage its attributes."""

    def __init__(self):
        super().__init__()

    def __getattr__(self, name: str) -> Value:
        """Customize attribute access. Redirects to __getitem__ for actual retrieval.
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


class DictionaryItem[Key, Value]:
    """A simple key-value pair class."""

    def __init__(self, key: Key, value: Value):
        self.key = key
        self.value = value

class DictionaryAttributeFunction[Key, Value](AttributeFunction[Key, Value]):
    """An AttributeFunction that uses a dictionary to store its attributes."""

    def __init__(self, data=None):
        super().__init__()
        if data is None:
            data = {}
        self.__dict__[f"data"] = data or {}

    def __getitem__(self, key: Key) -> Value:
        """Customize item access. This must be used for non-str-type keys.
        @param key: The key of the item being accessed.
        @return: The value of the requested item.
        """
        if key in self.__dict__["data"]:
            return self.__dict__["data"][key]
        else:
            raise AttributeError

    def __setitem__(self, key: Key, value: Value):
        """Customize item assignment. This must be used for non-str-type keys.
        @param key: The key of the item being assigned.
        @param value: The value to assign to the item.
        """
        self.__dict__["data"][key] = value

    def __delitem__(self, key):
        """Customize item deletion. This must be used for non-str-type keys."""
        if key in self.__dict__["data"]:
            del self.__dict__["data"][key]
        else:
            raise AttributeError

    def __contains__(self, item):
        return item in self.__dict__["data"]

    def __len__(self):
        return len(self.__dict__["data"])

    def __iter__(self):
        def mapper(item):
            return DictionaryItem(item[0], item[1])

        return map(mapper,self.__dict__["data"].items())


class TF[Key, Value](DictionaryAttributeFunction[Key, Value]):
    """A dictionary-based attribute mapping_function that behaves like a tuple."""
    pass


class RF[Key](DictionaryAttributeFunction[Key, TF]):
    """A dictionary-based attribute mapping_function that behaves like a relation."""
    pass


class DBF[Key](DictionaryAttributeFunction[Key, RF]):
    """A dictionary-based attribute mapping_function that behaves like a database."""
    pass

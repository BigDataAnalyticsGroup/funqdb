# good article:
# https://realpython.com/python-magic-methods/
from abc import ABC
from typing import Any, Iterator


class AttributeFunction(ABC):
    """An abstract base class representing a callable object that can also manage its attributes."""

    def __init__(self):
        pass

    def __getattr__(self, name: Any):
        """Customize attribute access.
        @param name: Name of the attribute being accessed. Notice that the 'name' parameter is of type 'Any' to allow
        flexibility in attribute types.
        @return: The value of the requested attribute.
        """
        pass

    def __setattr__(self, name: Any, value: Any):
        """Customize attribute assignment. Note that the 'name' parameter is of type 'Any' to allow flexibility in
        attribute types."""
        pass

    def __delattr__(self, name: Any):
        """Customize attribute deletion. Note that the 'name' parameter is of type 'Any' to allow flexibility in
        attribute types."""
        pass


class DictionaryAttributeFunction(AttributeFunction):
    """An AttributeFunction that uses a dictionary to store its attributes."""

    def __init__(self):
        super().__init__()
        self.__dict__[f"data"] = {"x": 0, "y": 0}

    def __getattr__(self, name: str):
        if name in self.__dict__["data"]:
            return self.__dict__["data"][name]
        else:
            raise AttributeError

    def __setattr__(self, name, value):
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

# good article:
# https://realpython.com/python-magic-methods/
from abc import ABC
from typing import Any

class B:
    def __init__(self):
        self.a = "I exist!"


class AttributeFunction(object):
    """An abstract base class representing a callable object that can also manage its attributes."""
    def __init__(self):
        pass

    def __getattribute__(self, name: Any):
        """Customize attribute access.
        @param name: Name of the attribute being accessed. Notice that the 'name' parameter is of type 'Any' to allow
        flexibility in attribute types.
        @return: The value of the requested attribute.
        """
        pass

    def __setattr__(self, name: Any, value:Any):
        """Customize attribute assignment. Note that the 'name' parameter is of type 'Any' to allow flexibility in
        attribute types."""
        pass

class DictionaryAttributeFunction(object):
    """An AttributeFunction representing its attributes using a dictionary."""

    def __init__(self):
        self.bla = 42


    def __get__(self, name: Any):
        """Customize attribute access.
        @param name: Name of the attribute being accessed. Notice that the 'name' parameter is of type 'Any' to allow
        flexibility in attribute types.
        @return: The value of the requested attribute.
        """
        print("get called")
        pass

class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __getattr__(self, name: str):
        print("get called")
        return self.__dict__[f"_{name}"]

    def __setattr__(self, name, value):
        print("set called")
        self.__dict__[f"_{name}"] = float(value)

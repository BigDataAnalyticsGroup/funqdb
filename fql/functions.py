# good article:
# https://realpython.com/python-magic-methods/
import json

from fql.APIs import AttributeFunction
from fql.util import ReadOnlyError, Item


class DictionaryAttributeFunction[Key, Value](AttributeFunction[Key, Value]):
    """An AttributeFunction that uses a dictionary to store its attributes."""

    def __init__(self, data=None, frozen=False):
        self.__dict__["data"] = data or {}
        self.__dict__["frozen"] = frozen
        super().__init__()

    def freeze(self):
        """Make the AttributeFunction read-only."""
        self.__dict__["frozen"] = True

    def unfreeze(self):
        """Make the AttributeFunction writable."""
        self.__dict__["frozen"] = False

    def frozen(self) -> bool:
        """Check if the AttributeFunction is read-only.
        @return: True if the AttributeFunction is read-only, False otherwise.
        """
        return self.__dict__["frozen"]

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
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"Write attempt to attribute '{key}'. This DictionaryAttributeFunction is read-only."
            )
        self.__dict__["data"][key] = value

    def __delitem__(self, key):
        """Customize item deletion. This must be used for non-str-type keys."""
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"Delete attempt to attribute '{key}'. This DictionaryAttributeFunction is read-only."
            )

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
            return Item(item[0], item[1])

        return map(mapper, self.__dict__["data"].items())

    def __eq__(self, other: "DictionaryAttributeFunction") -> bool:
        """Check equality between two DictionaryAttributeFunction instances based on their items.
        @param other: The other DictionaryAttributeFunction instance to compare with.
        @return: True if both instances have the same items, False otherwise.
        """
        if not isinstance(other, DictionaryAttributeFunction):
            return False

        self_items: set[Item] = {item for item in self}
        other_items: set[Item] = {item for item in other}
        return self_items == other_items

    def update(self, AttributeFunction: "AttributeFunction[Key, Value]"):
        """Update the current AttributeFunction with another one.
        @param AttributeFunction: The AttributeFunction to update with.
        """
        for item in AttributeFunction:
            self.__setitem__(item.key, item.value)

    def print(self, flat=False, recursion_depth: int = 0):
        """Print representation of the current AttributeFunction."""
        prefix: str = "    " * recursion_depth
        for key, value in self.__dict__["data"].items():
            if isinstance(value, AttributeFunction):
                if flat:
                    print(prefix + f"{key}: {value}")
                else:
                    print(prefix + f"{key}:")
                    value.print(flat=flat, recursion_depth=recursion_depth + 1)
            else:
                print(prefix + f"{key}: {value}")

    def _my_str_(self, flat=False, recursion_depth: int = 0):
        ret: str = ""
        prefix: str = "    " * recursion_depth
        for key, value in self.__dict__["data"].items():
            if isinstance(value, AttributeFunction):
                if flat:
                    ret += prefix + f"{key}: {value.__repr__()}\n"
                else:
                    ret += prefix + f"{key}:\n"
                    ret += value._my_str_(
                        flat=flat, recursion_depth=recursion_depth + 1
                    )
            else:
                ret += prefix + f"{key}: {value}\n"
        return ret

    def __str__(self):
        """String representation of the current AttributeFunction."""
        return self._my_str_(flat=True)


class TF[Key, Value](DictionaryAttributeFunction[Key, Value]):
    """A dictionary-based attribute transformation_function that behaves like a tuple."""

    pass


class RF[Key](DictionaryAttributeFunction[Key, TF]):
    """A dictionary-based attribute transformation_function that behaves like a relation."""

    pass


class DBF[Key](DictionaryAttributeFunction[Key, RF]):
    """A dictionary-based attribute transformation_function that behaves like a database."""

    pass

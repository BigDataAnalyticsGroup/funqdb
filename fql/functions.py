# good article:
# https://realpython.com/python-magic-methods/
from fql.APIs import AttributeFunction, Item
from fql.util import ReadOnlyError


class DictionaryAttributeFunction[Key, Value](AttributeFunction[Key, Value]):
    """An AttributeFunction that uses a dictionary to store its attributes."""

    def __init__(self, data=None, read_only=False):
        self.__dict__["data"] = data or {}
        self.__dict__["read_only"] = read_only
        super().__init__()

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
        if self.__dict__["read_only"]:
            raise ReadOnlyError(
                f"Write attempt to attribute '{key}'. This DictionaryAttributeFunction is read-only."
            )
        self.__dict__["data"][key] = value

    def __delitem__(self, key):
        """Customize item deletion. This must be used for non-str-type keys."""
        if self.__dict__["read_only"]:
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


class TF[Key, Value](DictionaryAttributeFunction[Key, Value]):
    """A dictionary-based attribute transformation_function that behaves like a tuple."""

    pass


class RF[Key](DictionaryAttributeFunction[Key, TF]):
    """A dictionary-based attribute transformation_function that behaves like a relation."""

    pass


class DBF[Key](DictionaryAttributeFunction[Key, RF]):
    """A dictionary-based attribute transformation_function that behaves like a database."""

    pass

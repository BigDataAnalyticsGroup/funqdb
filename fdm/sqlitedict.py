from abc import abstractmethod

from fdm.API import AttributeFunction
from fdm.util import Observer, Observable
from fql.util import Item


class SQLLiteDictAttributeFunction[Key, Value](
    AttributeFunction[Key, Value], Observable, Observer
):
    """A dictionary-like AttributeFunction backed by an SQLite dict key/value-store."""

    @property
    def frozen(self) -> bool:
        return self.__dict__["frozen"]

    def freeze(self):
        """Make the AttributeFunction read-only."""
        self.__dict__["frozen"] = True

    def unfreeze(self):
        """Make the AttributeFunction writable."""
        self.__dict__["frozen"] = False

    def __init__(self, sqlite_file_name: str, frozen: bool = False):
        AttributeFunction.__init__(self)
        Observable.__init__(self)
        Observer.__init__(self)
        self.__dict__["frozen"] = frozen
        self.__dict__["sqlite_file_name"] = sqlite_file_name

    def __getitem__(self, key: Key) -> Value:
        """Make the object callable through []-syntax."""
        # TODO: fetch from sqlite dict
        ...

    def __eq__(self, other: "AttributeFunction") -> bool:
        """Check equality between two AttributeFunction instances based on their items.
        @param other: The other AttributeFunction instance to compare with.
        @return: True if both instances have the same items, False otherwise.
        """
        # TODO: implement equality check
        ...

    def update(self, other: "AttributeFunction") -> "AttributeFunction":
        """Update the current AttributeFunction with another one.
        @param other: The other AttributeFunction to update from.
        @return: The updated AttributeFunction.
        """
        # TODO: implement update method
        ...

    def __len__(self) -> int:
        """Return the number of items in the AttributeFunction.
        @return: The number of items.
        """
        # TODO: implement length method
        ...

    def get_lineage(self) -> list[str]:
        pass

    def receive_notification(self, observable: "Observable", item: Item):
        pass

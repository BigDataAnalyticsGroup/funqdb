import atexit
import uuid

from sqlitedict import SqliteDict

from fdm.API import AttributeFunction
from fdm.util import Observer, Observable
from fql.util import Item, ReadOnlyError


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

    def __init__(
        self, sqlite_file_name: str, frozen: bool = False, tablename: str = "bla"
    ):
        AttributeFunction.__init__(self)
        Observable.__init__(self)
        Observer.__init__(self)
        self.__dict__["frozen"] = frozen
        self.__dict__["sqlite_file_name"] = sqlite_file_name
        self.__dict__["sqllitedict"] = SqliteDict(
            sqlite_file_name, tablename=tablename, autocommit=True
        )
        # assign a uuid:
        self.__dict__["_uuid"] = uuid.uuid4()

        # register to be called at exit:
        atexit.register(self.cleanup)

    def cleanup(self):
        self.__dict__["sqllitedict"].close()

    def __getitem__(self, key: Key) -> Value:
        """Make the object callable through []-syntax."""
        value: Value = None
        try:
            value = self.__dict__["sqllitedict"][key]
        except KeyError as e:
            raise AttributeError(
                f"Key '{key}' not found in SQLLiteDictAttributeFunction."
            ) from e
        return value

    def __setitem__(self, key: Key, value: Value):
        """Customize item assignment. This must be used for non-str-type keys.
        @param key: The key of the item being assigned.
        @param value: The value to assign to the item.
        """
        # check if frozen:
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"Write attempt to attribute '{key}'. This DictionaryAttributeFunction is read-only."
            )
        self.__dict__["sqllitedict"][key] = value

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


class TF_SQLLite[Key, Value](SQLLiteDictAttributeFunction[Key, Value]):
    """A SQL-lite based attribute function that behaves like a tuple."""

    ...


class RF_SQLLite[Key](SQLLiteDictAttributeFunction[Key, TF_SQLLite]):
    """A SQL-lite based attribute function that behaves like a relation."""

    ...


class DBF_SQLLite[Key](SQLLiteDictAttributeFunction[Key, RF_SQLLite]):
    """A SQL-lite based attribute function that behaves like a database."""

    ...

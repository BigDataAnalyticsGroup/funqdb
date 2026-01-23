from dataclasses import dataclass


class ReadOnlyError(Exception):
    """Exception raised when attempting to modify a read-only object."""

    pass


@dataclass(frozen=True)
class Item[Key, Value]:
    """A simple key-value pair (aka item) representation."""

    key: Key
    value: Value

    def __eq__(self, other: "Item") -> bool:
        """Check equality between two DictionaryAttributeFunction instances based on their items.
        @param other: The other DictionaryAttributeFunction instance to compare with.
        @return: True if both instances have the same items, False otherwise.
        """
        return self.key == other.key and self.value == other.value

    def __hash__(self) -> int:
        """Compute the hash of the Item based on its key and value.
        @return: The hash value of the Item.
        """
        # TODO: what about the value?
        return hash(self.key)

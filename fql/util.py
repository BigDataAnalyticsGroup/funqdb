from dataclasses import dataclass


class ReadOnlyError(Exception):
    """Exception raised when attempting to modify a read-only object."""

    pass


class ConstraintViolationError(Exception):
    """Exception raised when a constraint is violated."""

    pass


class ConstraintViolationErrorFromOutside(Exception):
    """Exception raised when a constraint is violated from outside the object."""

    def __init__(self, specific_message: str):
        message: str = (
            "This change and the constraint check and violation was triggered by notification from an"
            "observed value, i.e., the change did not originate from a direct assignment to this "
            "AttributeFunction"
        )
        super().__init__(specific_message + "\n" + message)


class KeyDeletedSentinel:
    """A sentinel value indicating that a key has been deleted."""

    pass


@dataclass(frozen=True)
class Item[Key, Value]:
    """A simple key-value pair (aka item) representation of an entry in an DictionaryAttributeFunction."""

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

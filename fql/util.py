from dataclasses import dataclass


class ReadOnlyError(Exception):
    """Exception raised when attempting to modify a read-only object."""

    ...


class ConstraintViolationError(Exception):
    """Exception raised when a constraint is violated."""

    def __init__(self, specific_message: str):
        message: str = (
            "BACKGROUND:\nThis change and the constraint check and violation was triggered while trying to assign a new value to a\n"
            "key. This assignment violated a constraint defined on this AttributeFunction.\n"
            "\nAs a result, the change has been rolled back automatically to restore a consistent state w.r.t. the\n"
            "constraints. You do NOT need to manually revert the change.\n"
        )
        super().__init__(specific_message + "\n" + message)


class ConstraintViolationErrorFromOutside(Exception):
    """Exception raised when a constraint is violated from outside the object."""

    def __init__(self, specific_message: str):
        message: str = (
            "BACKGROUND:\nThis change and the constraint check and violation was triggered by notification from an\n"
            "observed value, i.e., the change did not originate from a direct assignment to this\n"
            "AttributeFunction.\nAs a result, NO rollback of the change was performed automatically as that\n"
            "would be unsafe in this context. You need to manually revert the change to restore a\n"
            "consistent state w.r.t. the constraints.\n"
        )
        super().__init__(specific_message + "\n" + message)


class KeyDeletedSentinel:
    """A sentinel value indicating that a key has been deleted."""

    ...


@dataclass(frozen=True)
class Item[Key, Value]:
    """A simple key-value pair (aka item) representation of an entry in an DictionaryAttributeFunction."""

    key: Key
    __value: Value  # make value "private" to enforce access via property

    @property
    def value(self):
        return self.__value

    def __init__(self, key: Key, value: Value):
        object.__setattr__(self, "key", key)
        object.__setattr__(self, "_Item__value", value)

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

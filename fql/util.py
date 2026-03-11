#
#    This is funqDB, a query processing library and system built around FDM and FQL.
#
#    Copyright (C) 2026 Prof. Dr. Jens Dittrich, Saarland University
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#
from abc import ABC
from enum import Enum


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


class Item[Key, Value]:
    """A simple key-value pair (aka item) representation of an entry in an DictionaryAttributeFunction."""

    def __init__(self, key: Key, value: Value, frozen=True):
        """Initialize an Item with a key and a value.
        @param key: The key of the item.
        @param value: The value of the item.
        """
        self.key: Key = key
        self._value: Value = value
        self.frozen: bool = frozen

    @property
    def value(self) -> Value:
        """Get the value of the Item.
        @return: The value of the Item.
        """
        return self._value

    @value.setter
    def value(self, value: Value):
        """Set the value of the Item.
        @param value: The new value to set.
        """
        if self.frozen:
            raise ReadOnlyError("Cannot modify value of a frozen Item instance.")
        self._value = value

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

    def __getstate__(self):
        """Define the state of this to be serialized (pickled) to disk, network, etc."""

        from fdm.API import AttributeFunction

        # if value is an AF, store its UUID instead:
        _value_to_pickle: Value | int = (
            self.value.uuid if isinstance(self.value, AttributeFunction) else self.value
        )
        # _value_to_pickle: Value | int = self.value

        return {"key": self.key, "_value": _value_to_pickle}


class ChangeEvent(str, Enum):
    """represents the type of change operation being executed."""

    UPDATE = "update"
    INSERT = "insert"
    DELETE = "delete"

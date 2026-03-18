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


from abc import ABC, abstractmethod

from fdm.API import AttributeFunction
from fql.util import Item, ChangeEvent


class Observer(ABC):
    """An observer that can be notified of changes."""

    @abstractmethod
    def receive_notification(
        self, observable: "Observable", item: Item, event: ChangeEvent
    ):
        """Notify the observer of a change.
        @param item: The item that has changed.
        """
        ...


class Observable(ABC):
    """An observable that can be observed by observers."""

    def add_observer(self, observer: Observer):
        """Add an observer to the observable.
        @param observer: The observer to add.
        """
        ...

    def remove_observer(self, observer: Observer):
        """Remove an observer from the observable.
        @param observer: The observer to remove.
        """
        ...

    def notify_observers(self, item: Item, event: ChangeEvent):
        """Notify all observers of a change.
        @param item: The item that has changed.
        """
        ...


class Explainable:
    """An abstract base class for explainable objects."""

    def explain(self) -> str:
        """Provide an explanation of the object.
        @return: A string explanation of the object.
        """
        return "The explanation is 42."


class CompositeForeignObject:
    """A composite foreign object represents a relationship between multiple AFs. This replaces the traditional
    composite key in a relational database."""

    def __init__(self, foreign_objects: list[AttributeFunction]):
        self.foreign_objects: list[AttributeFunction] = foreign_objects

    def __hash__(self):
        """The hash of the MKey is based on the UUIDs of the user and customer, as those are immutable and unique
        identifiers for the respective TFs. This allows us to use MKey instances as foreign_objects in a dictionary (or RF) to
        represent relationships between users and customers."""
        return hash(tuple([k.uuid for k in self.foreign_objects]))

    def __eq__(self, other_foreign_objects: list[AttributeFunction]):
        """Two MKey instances are considered equal if they have the same user and customer UUIDs. This ensures that
        the relationship is correctly identified based on the involved TFs, regardless of whether the same MKey
        instance is used or different instances with the same user and customer are created.
        """
        return self.foreign_objects == other_foreign_objects

    def __contains__(self, foreign_object: AttributeFunction):
        """Check if a given AttributeFunction is part of the CompositeForeignObject."""
        return foreign_object in self.foreign_objects

    def subkey(self, index: int) -> AttributeFunction:
        """Get the subkey at the given index."""
        return self.foreign_objects[index]

    def __len__(self) -> int:
        """Get the number of subkeys in the CompositeForeignObject."""
        return len(self.foreign_objects)

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

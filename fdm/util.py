from abc import ABC, abstractmethod

from fql.util import Item


class Observer(ABC):
    """An observer that can be notified of changes."""

    @abstractmethod
    def receive_notification(self, observable: "Observable", item: Item):
        """Notify the observer of a change.
        @param item: The item that has changed.
        """
        pass


class Observable(ABC):
    """An observable that can be observed by observers."""

    def add_observer(self, observer: Observer):
        """Add an observer to the observable.
        @param observer: The observer to add.
        """
        pass

    def remove_observer(self, observer: Observer):
        """Remove an observer from the observable.
        @param observer: The observer to remove.
        """
        pass

    def notify_observers(self, item: Item):
        """Notify all observers of a change.
        @param item: The item that has changed.
        """
        pass

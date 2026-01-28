# good article:
# https://realpython.com/python-magic-methods/
import inspect
import logging
from abc import ABC, abstractmethod

logger = logging.Logger(__name__)

from fql.APIs import AttributeFunction
from fql.util import ReadOnlyError, Item, ConstraintViolationError, KeyDeletedSentinel


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


class DictionaryAttributeFunction[Key, Value](
    AttributeFunction[Key, Value], Observable, Observer
):
    """An AttributeFunction that uses a dictionary to store its attributes."""

    def __init__(self, data=None, frozen=False, observe_items: bool = False):
        self.__dict__["data"] = data or dict()
        self.__dict__["frozen"] = frozen
        self.__dict__["self_constraints"] = set()
        self.__dict__["items_constraints"] = set()
        self.__dict__["observe_items"] = observe_items
        self.__dict__["observers"] = list()

        if observe_items:
            # register self as observer at all Observable values:
            for value in self.__dict__["data"].values():
                if isinstance(value, Observable):
                    value.add_observer(self)

        super().__init__()

    def add_self_constraint(self, constraint):
        """Add a self-constraint to this AttributeFunction.
        @param constraint: A callable that takes a value and returns True if the value satisfies the constraint, False otherwise.
        """
        self.__dict__["self_constraints"].add(constraint)

    def remove_self_constraint(self, constraint):
        """Remove a self-constraint from the AttributeFunction.
        @param constraint: The constraint to remove.
        """
        self.__dict__["self_constraints"].remove(constraint)

    def add_items_constraint(self, constraint):
        """Add an item-constraint to this AttributeFunction, i.e., this constraint must be fulfilled for all items.
        @param constraint: A callable that takes an Item and returns True if the item satisfies the constraint, False otherwise.
        """
        self.__dict__["items_constraints"].add(constraint)

    def remove_items_constraint(self, constraint):
        """Remove an item-constraint from the AttributeFunction.
        @param constraint: The constraint to remove.
        """
        self.__dict__["items_constraints"].remove(constraint)

    def add_observer(self, observer: Observer):
        """Add an observer to the AttributeFunction.
        @param observer: The observer to add.
        """
        self.__dict__["observers"].append(observer)

    def notify_observers(self, item: Item):
        """Notify all observers of a change.
        @param item: The item that has changed.
        """
        for observer in self.__dict__["observers"]:
            observer.receive_notification(self, item)

    def remove_observer(self, observer):
        """Remove an observer from the AttributeFunction.
        @param observer: The observer to remove.
        """
        self.__dict__["observers"].remove(observer)

    def freeze(self):
        """Make the AttributeFunction read-only."""
        self.__dict__["frozen"] = True

    def unfreeze(self):
        """Make the AttributeFunction writable."""
        self.__dict__["frozen"] = False

    def frozen(self) -> bool:
        """Check if the AttributeFunction is read-only.
        @return: True if the AttributeFunction is read-only, False otherwise.
        """
        return self.__dict__["frozen"]

    def __getitem__(self, key: Key) -> Value:
        """Customize item access. This must be used for non-str-type keys.
        @param key: The key of the item being accessed.
        @return: The value of the requested item.
        """
        if key in self.__dict__["data"]:
            return self.__dict__["data"][key]
        else:
            raise AttributeError

    def _check_items_constraints(self, item: Item):
        """Check all constraints on a given item.
        @param item: The item to check.
        """
        for constraint in self.__dict__["items_constraints"]:
            if not constraint(item):
                raise ConstraintViolationError(
                    f"Value '{item.value}' does not satisfy constraint:\n'{inspect.getsource(constraint.__call__)}'.\n"
                    f"for key '{item.key}' and value '{item.value}'."
                )

    def _check_self_constraints(self):
        """Check all self-constraints on the current AttributeFunction."""
        for constraint in self.__dict__["self_constraints"]:
            if not constraint(self):
                raise ConstraintViolationError(
                    f"AttributeFunction'{self}' does not satisfy constraint:\n'{inspect.getsource(constraint.__call__)}'."
                )

    def receive_notification(self, observable: "Observable", item: Item):
        """Notify the AttributeFunction of a change in an observed value.
        @param item: The item that has changed.
        """
        # when a value changes, we need to notify our observers about the change:
        # TODO: wrong: should call with the item that changed, not with the observable (the value in this case):
        # so we have to find all items pointing to that TP, which can mbe multiple ones!
        # brute force search:
        # TODO: maybe we should keep an inverted index for that?
        for item in self:
            if item.value == observable:
                self._check_items_constraints(item)

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

        # check constraints on the item and on self:
        item: Item = Item(key, value)

        # unroll logic:
        key_existed_before: bool = key in self.__dict__["data"]
        _old_value: Value = self.__dict__["data"][key] if key_existed_before else None

        self.__dict__["data"][key] = value

        try:
            self._check_items_constraints(item)
            self._check_self_constraints()
        except ConstraintViolationError as e:
            # rollback change:
            if key_existed_before:
                self.__dict__["data"][key] = _old_value
            else:
                del self.__dict__["data"][key]
            raise e

        # notify observers about the change:
        self.notify_observers(item)

    def __delitem__(self, key):
        """Customize item deletion. This must be used for non-str-type keys."""
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"Delete attempt to attribute '{key}'. This DictionaryAttributeFunction is read-only."
            )

        if key in self.__dict__["data"]:
            # unroll logic:
            # keep ref to old value:
            _old_value: Value = self.__dict__["data"][key]

            try:
                del self.__dict__["data"][key]
                self._check_self_constraints()
            except ConstraintViolationError as e:
                # rollback change:
                self.__dict__["data"][key] = _old_value
                raise e
            # notify observers about the change:
            self.notify_observers(Item(key, KeyDeletedSentinel))

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

    def keys(self) -> iter:
        """Get the keys of the AttributeFunction.
        @return: An iterable of the keys.
        """
        return iter(self.__dict__["data"].keys())

    def values(self):
        """Get the values of the AttributeFunction.
        @return: An iterable of the values.
        """
        return iter(self.__dict__["data"].values())

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

    def update(self, AttributeFunction: "AttributeFunction[Key, Value]"):
        """Update the current AttributeFunction with another one.
        @param AttributeFunction: The AttributeFunction to update with.
        """
        for item in AttributeFunction:
            if item.key in self:
                logger.warning(
                    f"key '{item.key}' already exists and will be overwritten."
                )
            self.__setitem__(item.key, item.value)

    def print(self, flat=False, recursion_depth: int = 0):
        """Print representation of the current AttributeFunction."""
        prefix: str = "    " * recursion_depth
        for key, value in self.__dict__["data"].items():
            if isinstance(value, AttributeFunction):
                if flat:
                    print(prefix + f"{key}: {value}")
                else:
                    print(prefix + f"{key}:")
                    value.print(flat=flat, recursion_depth=recursion_depth + 1)
            else:
                print(prefix + f"{key}: {value}")

    def _my_str_(self, flat=False, recursion_depth: int = 0):
        ret: str = ""
        prefix: str = "    " * recursion_depth
        for key, value in self.__dict__["data"].items():
            if isinstance(value, AttributeFunction):
                if flat:
                    ret += prefix + f"{key}: {value.__repr__() if value else "NONE"}\n"
                else:
                    ret += prefix + f"{key}:\n"
                    ret += value._my_str_(
                        flat=flat, recursion_depth=recursion_depth + 1
                    )
            else:
                ret += prefix + f"{key}: {value}\n"
        return ret

    def __str__(self):
        """String representation of the current AttributeFunction."""
        return self._my_str_(flat=True)

    def __repr__(self):
        """String representation of the current AttributeFunction used fo dev purposes (including the debugger)."""
        return self.__class__


class TF[Key, Value](DictionaryAttributeFunction[Key, Value]):
    """A dictionary-based attribute transformation_function that behaves like a tuple."""

    pass


class RF[Key](DictionaryAttributeFunction[Key, TF]):
    """A dictionary-based attribute transformation_function that behaves like a relation."""

    pass


class DBF[Key](DictionaryAttributeFunction[Key, RF]):
    """A dictionary-based attribute transformation_function that behaves like a database."""

    pass

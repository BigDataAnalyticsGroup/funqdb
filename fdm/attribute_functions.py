import inspect
from typing import Generator, Iterable

from fdm.API import AttributeFunction, logger, AttributeFunctionSentinel
from fdm.util import Observable, Observer
from fql.operators.filters import filter_items
from fql.predicates.constraints import AttributeFunctionConstraint
from fql.util import (
    Item,
    ConstraintViolationErrorFromOutside,
    ConstraintViolationError,
    ReadOnlyError,
    KeyDeletedSentinel,
)

from store.store import Store


class DictionaryAttributeFunction[Key, Value](
    AttributeFunction[Key, Value], Observable, Observer
):
    """An AttributeFunction that uses a dictionary to store its attributes."""

    def __init__(
        self,
        data=None,
        frozen=False,
        observe_items: bool = False,
        lineage: list[str] = None,
        store: Store = None,
    ):
        self.__dict__["data"] = data or dict()
        self.__dict__["frozen"] = frozen
        self.__dict__["af_constraint"] = set()
        self.__dict__["values_constraints"] = set()
        self.__dict__["observe_items"] = observe_items
        self.__dict__["observers"] = list()
        # how this attribute function was derived:
        self.__dict__["lineage"] = [] if lineage is None else lineage
        self.__dict__["store"] = store

        if observe_items:
            # register self as observer at all Observable values:
            for value in self.__dict__["data"].values():
                if isinstance(value, Observable):
                    value.add_observer(self)

        super().__init__()

    def add_attribute_function_constraint(
        self, constraint: AttributeFunctionConstraint
    ):
        """Add an attribute function-constraint to this AttributeFunction.
        @param constraint: A callable that takes a value and returns True if the value satisfies the constraint, False otherwise.
        """
        # check if frozen:
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"attempt to add a function constraint. This DictionaryAttributeFunction is read-only."
            )

        self.__dict__["af_constraint"].add(constraint)

    def remove_attribute_function_constraint(self, constraint):
        """Remove an attribute function-constraint from the AttributeFunction.
        @param constraint: The constraint to remove.
        """

        # check if frozen:
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"attempt to remove a function constraint. This DictionaryAttributeFunction is read-only."
            )

        self.__dict__["af_constraint"].remove(constraint)

    def add_values_constraint(self, constraint: AttributeFunctionConstraint):
        """Add a values-constraint to this AttributeFunction, i.e., this constraint must be fulfilled for all values.
        @param constraint: A callable that takes an attribute function and returns True if the attribute function
        satisfies the constraint, False otherwise.
        """
        # check if frozen:
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"attempt to add a values constraint. This DictionaryAttributeFunction is read-only."
            )

        self.__dict__["values_constraints"].add(constraint)

    def remove_values_constraint(self, constraint):
        """Remove a values-constraint from the AttributeFunction.
        @param constraint: The constraint to remove.
        """
        # check if frozen:
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"attempt to remove a values constraint. This DictionaryAttributeFunction is read-only."
            )

        self.__dict__["values_constraints"].remove(constraint)

    def add_observer(self, observer: Observer):
        """Add an observer to the AttributeFunction.
        @param observer: The observer to add.
        """
        # check if frozen:
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"attempt to add an observer. This DictionaryAttributeFunction is read-only."
            )

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
        # check if frozen:
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"attempt to remove an observer. This DictionaryAttributeFunction is read-only."
            )

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
            value: Value | AttributeFunctionSentinel = self.__dict__["data"][key]

            # if we have a reference to a store and the value is an AttributeFunctionSentinel, we load the actual
            # AttributeFunction instance from the store:
            if self.__dict__["store"] is not None and isinstance(
                value, AttributeFunctionSentinel
            ):
                # load the AttributeFunction from the store:
                value: AttributeFunction = self.__dict__["store"].get(value.id)
                # update the data dict with the loaded AttributeFunction for future accesses:
                self.__dict__["data"][key] = value

            return value
        else:
            raise AttributeError

    def _check_value_constraints(
        self, value: Value, triggered_by_notification: bool = False
    ):
        """Check all constraints on a given item.
        @param item: The item to check.
        @param triggered_by_notification: Whether the check was triggered by a notification from an observed value.
        """
        for constraint in self.__dict__["values_constraints"]:
            if not constraint(value):
                message: str = (
                    f"Value '{value}' does not satisfy constraint:\n'{inspect.getsource(constraint.__call__)}'.\n "
                )
                if triggered_by_notification:
                    raise ConstraintViolationErrorFromOutside(message)
                raise ConstraintViolationError(message)

    def _check_attribute_function_constraints(
        self, triggered_by_notification: bool = False
    ):
        """Check all attribute function constraints on the current AttributeFunction."""
        for constraint in self.__dict__["af_constraint"]:
            if not constraint(self):
                message: str = (
                    f"AttributeFunction'{self}' does not satisfy constraint:\n'{inspect.getsource(constraint.__call__)}'."
                )
                if triggered_by_notification:
                    raise ConstraintViolationErrorFromOutside(message)
                raise ConstraintViolationError(message)

    def receive_notification(
        self, observable: "Observable", item_from_observable: Item
    ):
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
                self._check_value_constraints(
                    item.value, triggered_by_notification=True
                )
        # TODO: do we need to notify recursively here?
        # self.notify_observers(item)
        self._check_attribute_function_constraints(triggered_by_notification=True)

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
            # TODO: maybe rename to value_constraints?
            self._check_value_constraints(item.value)
            self._check_attribute_function_constraints()
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
                self._check_attribute_function_constraints()
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

    def keys(self) -> Generator:
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

    def update(self, other: "AttributeFunction[Key, Value]"):
        """Update the current AttributeFunction with another one.
        @param AttributeFunction: The AttributeFunction to update with.
        """
        for item in other:
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

    def my_str(self, flat=False, recursion_depth: int = 0):
        ret: str = ""
        prefix: str = "    " * recursion_depth
        for key, value in self.__dict__["data"].items():
            if isinstance(value, AttributeFunction):
                if flat:
                    ret += prefix + f"{key}: {value.__repr__() if value else "NONE"}\n"
                else:
                    ret += prefix + f"{key}:\n"
                    ret += value.my_str(flat=flat, recursion_depth=recursion_depth + 1)
            else:
                ret += prefix + f"{key}: {value}\n"
        return ret

    def __str__(self):
        """String representation of the current AttributeFunction."""
        return self.my_str(flat=True)

    def __repr__(self):
        """String representation of the current AttributeFunction used fo dev purposes (including the debugger)."""
        return self.__class__

    def get_lineage(self) -> list[str]:
        """Get the lineage of this AttributeFunction.
        @return: A list representing the lineage.
        """
        return self.__dict__["lineage"]

    def add_lineage(self, entry: str):
        """Add an entry to the lineage of this AttributeFunction."""

        # check if frozen:
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"attempt to add lineage. This DictionaryAttributeFunction is read-only."
            )

        self.__dict__["lineage"].append(entry)

    def __getstate__(self):
        """This method defines what data gets saved when the object is pickled."""
        # handle values of type AttributeFunction, i.e. store their UUIDs instead

        # create a physical copy of the data dict, otherwise we would modify the original data dict when
        # replacing AttributeFunctions with their UUIDs:
        state_dict = self.__dict__.copy()
        state_dict["data"] = state_dict["data"].copy()
        state_dict["observers"] = state_dict["observers"].copy()

        # We also need to handle the store reference, otherwise we would try to pickle the whole store,
        #  which is not what we want, see Store class for that
        state_dict["store"] = None
        for key, value in state_dict["data"].items():
            if isinstance(value, AttributeFunction):
                # replace the AttributeFunction with a AttributeFunctionSentinel:
                state_dict["data"][key] = AttributeFunctionSentinel(value.uuid)

        # replace the AttributeFunction with a AttributeFunctionSentinel:
        state_dict["observers"] = [
            AttributeFunctionSentinel(f.uuid) for f in state_dict["observers"]
        ]

        return state_dict

    def __setstate__(self, state):
        """This method defines how to restore the object when unpickling.
        TODO: Custom unpickling logic to restore observers."""
        self.__dict__.update(state)


class TF[Key, Value](DictionaryAttributeFunction[Key, Value]):
    """A dictionary-based attribute function that behaves like a tuple."""

    ...


class RF[Key](DictionaryAttributeFunction[Key, TF]):
    """A dictionary-based attribute function that behaves like a relation."""

    ...


class DBF[Key](DictionaryAttributeFunction[Key, RF]):
    """A dictionary-based attribute function that behaves like a database."""

    ...


class SDBF[Key](DictionaryAttributeFunction[Key, DBF]):
    """A dictionary-based attribute function that behaves like a set of databases."""

    ...


class CompositeKey:
    """A composite key"""

    def __init__(self, keys: list[AttributeFunction]):
        self.keys: list[AttributeFunction] = keys

    def __hash__(self):
        """The hash of the MKey is based on the UUIDs of the user and customer, as those are immutable and unique
        identifiers for the respective TFs. This allows us to use MKey instances as keys in a dictionary (or RF) to
        represent relationships between users and customers."""
        return hash(tuple([k.uuid for k in self.keys]))

    def __eq__(self, other_keys: list[AttributeFunction]):
        """Two MKey instances are considered equal if they have the same user and customer UUIDs. This ensures that
        the relationship is correctly identified based on the involved TFs, regardless of whether the same MKey
        instance is used or different instances with the same user and customer are created.
        """
        return self.keys == other_keys

    def __contains__(self, key: AttributeFunction):
        """Check if a given AttributeFunction is part of the CompositeKey."""
        return key in self.keys

    def subkey(self, index: int) -> AttributeFunction:
        """Get the subkey at the given index."""
        return self.keys[index]

    def __len__(self) -> int:
        """Get the number of subkeys in the CompositeKey."""
        return len(self.keys)


class RSF[Value](DictionaryAttributeFunction[CompositeKey, Value]):
    """A dictionary-based attribute function that behaves like a relationship."""

    def related_values(
        self, subkey_index: int, subkey: AttributeFunction
    ) -> Iterable[AttributeFunction]:
        """Get the related values for a given subkey value at a given subkey index."""

        return map(
            lambda item: item.key.subkey(
                subkey_index
            ),  # extract the related subkey value at the given index from the composite key of the item
            filter(
                lambda item: item.key.subkey(subkey_index)
                == subkey,  # filter items based on the subkey value at the given index
                self,  # iterate on all items of this attribute function
            ),
        )


class Tensor[Value](DictionaryAttributeFunction[CompositeKey, Value]):
    """A tensor is simply a dictionary function with a composite key. It may have additional methods for tensor-specific
    operations, but for now it is just a subclass of DictionaryAttributeFunction with a different key type.
    """

    def __init__(self, dimensions: list[int]):
        """Initialize the TensorKey with the given dimensions.
        @param dimensions: A list of int representing the number of elements of each dimension of the tensor.
        """
        super().__init__()
        assert len(dimensions) > 0, "Tensor must have at least one dimension."
        self.dimensions = dimensions

    def rank(self):
        """Get the rank of the tensor, which is the number of dimensions."""
        return len(self.dimensions)

    def __add__(self, other):
        """Add another tensor to this tensor. This is a simple element-wise addition, i.e., we add the values of the
        two tensors for each key and return a new tensor with the same keys and the added values. We assume that
        the two tensors have the same keys and dimensions, but we do not check this for simplicity.
        """
        assert (
            self.dimensions == other.dimensions
        ), "Cannot add tensors with different dimensions."
        result = Tensor(self.dimensions)
        for key in self.keys():
            result[key] = self[key] + other[key]
        return result

    def __sub__(self, other):
        """Subtract another tensor from this tensor. This is a simple element-wise subtraction, i.e., we subtract the values of the
        two tensors for each key and return a new tensor with the same keys and the subtracted values. We assume that
        the two tensors have the same keys and dimensions, but we do not check this for simplicity.
        """
        assert (
            self.dimensions == other.dimensions
        ), "Cannot add tensors with different dimensions."
        result = Tensor(self.dimensions)
        for key in self.keys():
            result[key] = self[key] - other[key]
        return result

    def __mul__(self, other):
        """Multiply this tensor with another tensor. This is a simple element-wise multiplication, i.e., we multiply the values of the
        two tensors for each key and return a new tensor with the same keys and the multiplied values. We assume that
        the two tensors have the same keys and dimensions, but we do not check this for simplicity.
        """
        assert (
            self.dimensions == other.dimensions
        ), "Cannot add tensors with different dimensions."
        result = Tensor(self.dimensions)
        for key in self.keys():
            result[key] = self[key] * other[key]
        return result

    def __matmul__(self, other):
        """Perform matrix multiplication between this tensor and another tensor. This is a simple implementation of
        matrix multiplication, where we multiply the values of the two tensors according to the rules of matrix
        multiplication and return a new tensor with the resulting values. We assume that the two tensors have
        compatible dimensions for matrix multiplication, but we do not check this for simplicity.
        """

        raise NotImplementedError

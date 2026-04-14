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


import inspect
import random
from copy import copy
from typing import Generator, Iterable, Callable, Any

from fdm.API import AttributeFunction, logger, AttributeFunctionSentinel
from fdm.util import Observable, Observer
from fql.predicates.constraints import AttributeFunctionConstraint
from fql.predicates.predicates import Predicate
from fql.util import (
    Item,
    ConstraintViolationErrorFromOutside,
    ConstraintViolationError,
    ReadOnlyError,
    KeyDeletedSentinel,
    ChangeEvent,
)

from store.store import Store


class CompositeForeignObject:
    """A composite foreign object represents a relationship between multiple AFs. This replaces the traditional
    composite key in a relational database."""

    def __init__(self, *foreign_objects: AttributeFunction):
        self.foreign_objects: tuple[AttributeFunction, ...] = foreign_objects

    def __hash__(self):
        """Hash based on the UUIDs of the constituent AFs, which are immutable and unique.
        This makes CompositeForeignObject instances usable as dictionary keys (and therefore
        as keys in RFs and RSFs)."""
        return hash(tuple(k.uuid for k in self.foreign_objects))

    def __eq__(self, other: object) -> bool:
        """Two CompositeForeignObject instances are equal if they reference the same AFs
        (compared by identity/UUID). This ensures the relationship is correctly identified
        regardless of whether the same instance or a new one with the same components is used.
        """
        if isinstance(other, CompositeForeignObject):
            return self.foreign_objects == other.foreign_objects
        return NotImplemented

    def __contains__(self, foreign_object: AttributeFunction):
        """Check if a given AttributeFunction is part of the CompositeForeignObject."""
        return foreign_object in self.foreign_objects

    def subkey(self, index: int) -> AttributeFunction:
        """Get the subkey at the given index."""
        return self.foreign_objects[index]

    def __len__(self) -> int:
        """Get the number of subkeys in the CompositeForeignObject."""
        return len(self.foreign_objects)


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
        schema: dict | None = None,
        computed: dict[str, Callable] | None = None,
        default: Callable[[Any], Any] | None = None,
    ):
        """Initialize a DictionaryAttributeFunction.
        @param data: A dictionary of stored key-value pairs.
        @param frozen: Whether the AF is read-only after construction.
        @param observe_items: Whether to register as observer at Observable values.
        @param lineage: Provenance information for this AF.
        @param store: A Store instance for lazy loading of AF references.
        @param schema: Optional schema dict — see _apply_schema() for details.
        @param computed: Optional dict mapping attribute names to callables.
            Each callable receives this AF as its argument and is evaluated on every
            access (not cached). Computed attributes are indistinguishable from stored
            attributes when accessed via [], ., (), in, len, or iteration.
            They are read-only (cannot be overwritten or deleted), ephemeral (stripped
            on pickling), and must not overlap with stored keys.
        @param default: Optional fallback callable(key) -> value for keys not found
            in data or computed. Enables computed relations (paper Sec 2.6): an RF
            with a default function can generate TFs on the fly for any key.
            The default covers a potentially infinite domain — it is not reflected
            in len(), iteration, keys(), or values(). Ephemeral (stripped on pickling).
        """
        self.__dict__["data"] = data or dict()
        self.__dict__["frozen"] = False  # start unfrozen to allow schema setup
        self.__dict__["af_constraints"] = set()
        self.__dict__["values_constraints"] = set()
        self.__dict__["observe_items"] = observe_items
        self.__dict__["observers"] = list()
        # how this attribute function was derived:
        self.__dict__["lineage"] = [] if lineage is None else lineage
        self.__dict__["store"] = store
        # Computed attributes: key -> callable(self), evaluated lazily on every access.
        # Stored separately from data to avoid the Tensor bug pattern (metadata
        # contaminating the data dict and breaking iteration/arithmetic).
        self.__dict__["computed"] = computed or {}
        # Default fallback: callable(key) -> value for any unstored key.
        # Enables computed relations with potentially infinite domains.
        self.__dict__["default"] = default
        # A key must be either stored or computed, never both — reject overlaps
        # to prevent trapped-value states where a stored value becomes unmodifiable:
        overlap = set(self.__dict__["data"]) & set(self.__dict__["computed"])
        if overlap:
            raise ValueError(
                f"Keys {overlap} exist in both data and computed. "
                f"A key must be either stored or computed, not both."
            )

        if observe_items:
            # register self as observer at all Observable values:
            for value in self.__dict__["data"].values():
                if isinstance(value, Observable):
                    value.add_observer(self)

        super().__init__()

        if schema is not None:
            self._apply_schema(schema)

        # apply the requested frozen state after schema setup:
        self.__dict__["frozen"] = frozen

    def _apply_schema(self, schema: dict) -> None:
        """Apply a schema dict: type values become a Schema constraint,
        AF-instance values additionally set up foreign references via .references()."""
        from fdm.schema import Schema as SchemaConstraint

        schema_types: dict = {}
        for key, value in schema.items():
            if isinstance(value, AttributeFunction):
                schema_types[key] = AttributeFunction
                self.references(key, value)
            else:
                schema_types[key] = value
        self.add_values_constraint(SchemaConstraint(schema_types))

    def add_computed(self, key: str, func: Callable[..., Any]) -> None:
        """Add a computed attribute that is evaluated on every access (not cached).

        The callable receives this AF instance as its sole argument at *access time*,
        not at definition time. This means the computed value reflects the current state
        of stored attributes, e.g.::

            t = TF({"age": 20})
            t.add_computed("salary", lambda tf: 1000 * tf["age"])
            t.salary        # → 20000
            t["age"] = 30
            t.salary        # → 30000  (recomputed)

        @param key: The attribute name. Must not collide with an existing stored key.
        @param func: A callable(self) -> value, evaluated on every access.
        @raises ReadOnlyError: If the AF is frozen.
        @raises ValueError: If key already exists as a stored attribute.
        """
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"Attempt to add computed attribute '{key}'. "
                f"This DictionaryAttributeFunction is read-only."
            )
        if key in self.__dict__["data"]:
            raise ValueError(
                f"Key '{key}' already exists as a stored attribute. "
                f"A key must be either stored or computed, not both."
            )
        self.__dict__["computed"][key] = func

    def add_default(self, func: Callable[[Any], Any]) -> None:
        """Set a default fallback function for keys not in data or computed.

        The callable receives the requested key as its sole argument and returns
        the value. This enables computed relations (paper Sec 2.6): an RF with a
        default can generate TFs on the fly for any key::

            users = RF({1: TF({"name": "Alice"})})
            users.add_default(lambda key: TF({"name": f"User-{key}"}))
            users[999]  # → TF({"name": "User-999"})

        The default covers a potentially infinite domain — it is not reflected
        in len(), iteration, keys(), or values().

        @param func: A callable(key) -> value.
        @raises ReadOnlyError: If the AF is frozen.
        """
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"Attempt to set default function. "
                f"This DictionaryAttributeFunction is read-only."
            )
        self.__dict__["default"] = func

    def copy(self) -> "DictionaryAttributeFunction":
        """Create a copy of this AttributeFunction with a new UUID, i.e. this functions as a copy constructor for
        creating a new AttributeFunction based on an existing one, but with a new identity.
        """
        new_copy: DictionaryAttributeFunction = copy(self)
        new_copy._assign_uuid()
        return new_copy

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

        self.__dict__["af_constraints"].add(constraint)

    def remove_attribute_function_constraint(self, constraint):
        """Remove an attribute function-constraint from the AttributeFunction.
        @param constraint: The constraint to remove.
        """

        # check if frozen:
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"attempt to remove a function constraint. This DictionaryAttributeFunction is read-only."
            )

        self.__dict__["af_constraints"].remove(constraint)

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

    def references(self, key: Key, parent_attribute_function: AttributeFunction):
        """Convenience method to express a foreign value constraint, i.e., the value of the item with the given key
        mapped to by the given parent attribute function. Adds constraints to both the parent and the child attribute function.
        """
        from fdm.schema import ForeignValueConstraint

        self.add_values_constraint(
            ForeignValueConstraint(key, parent_attribute_function)
        )
        from fdm.schema import ReverseForeignObjectConstraint

        parent_attribute_function.add_values_constraint(
            ReverseForeignObjectConstraint(key, self)
        )
        return self

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

    def notify_observers(self, item: Item, event: ChangeEvent):
        """Notify all observers of a change.
        @param item: The item that has changed.
        """
        for observer in self.__dict__["observers"]:
            observer.receive_notification(self, item, event)

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

    def __getitem__(self, key: Key) -> Any:
        """Customize item access. This must be used for non-str-type keys.
        TODO: discuss whether we really want to have this
        Supports __-syntax for accessing sub-items of values, e.g., if we have an item with key "department" and value
        being a TF with an item with key "name", we can access the name of the department with "department__name".

        @param key: The key of the item being accessed.
        @return: The value of the requested item.
        """
        key_suffix: str | None = None
        if type(key) is str:
            if "__" in key:
                key_suffix: str | None = key.split("__")[1:]
                key: str = key.split("__")[0]
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

            # if the suffix is not empty, we try to access the sub-item of the value with the suffix as key, otherwise
            # we return the value itself:
            return (
                value.__getitem__("__".join(key_suffix))
                if (key_suffix and len(key_suffix) > 0)
                else value
            )
        elif key in self.__dict__["computed"]:
            # Computed attribute: call the stored function with self as argument.
            # The value is recomputed on every access (no caching), so it always
            # reflects the current state of stored attributes.
            value = self.__dict__["computed"][key](self)
            return (
                value.__getitem__("__".join(key_suffix))
                if (key_suffix and len(key_suffix) > 0)
                else value
            )
        elif self.__dict__["default"] is not None:
            # Default fallback: generate a value for any unstored key.
            # The callable receives the key as argument (not self), enabling
            # computed relations with potentially infinite domains (paper Sec 2.6).
            value = self.__dict__["default"](key)
            return (
                value.__getitem__("__".join(key_suffix))
                if (key_suffix and len(key_suffix) > 0)
                else value
            )
        else:
            raise AttributeError

    def _check_value_constraints(
        self, value: Value, event: ChangeEvent, triggered_by_notification: bool = False
    ):
        """Check all constraints on a given item.
        @param item: The item to check.
        @param triggered_by_notification: Whether the check was triggered by a notification from an observed value.
        """
        for constraint in self.__dict__["values_constraints"]:
            if not constraint(value, event):
                message: str = (
                    f"Value '{value}' does not satisfy constraint:\n'{inspect.getsource(constraint.__call__)}'.\n "
                )
                if triggered_by_notification:
                    raise ConstraintViolationErrorFromOutside(message)
                raise ConstraintViolationError(message)

    def _check_attribute_function_constraints(
        self, event: ChangeEvent, triggered_by_notification: bool = False
    ):
        """Check all attribute function constraints on the current AttributeFunction."""
        for constraint in self.__dict__["af_constraints"]:
            if not constraint(self, event):
                message: str = (
                    f"AttributeFunction'{self}' does not satisfy constraint:\n'{inspect.getsource(constraint.__call__)}'."
                )
                if triggered_by_notification:
                    raise ConstraintViolationErrorFromOutside(message)
                raise ConstraintViolationError(message)

    def receive_notification(
        self, observable: "Observable", item_from_observable: Item, event: ChangeEvent
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
                    item.value, event, triggered_by_notification=True
                )
        # TODO: do we need to notify recursively here?
        # self.notify_observers(item)
        self._check_attribute_function_constraints(
            event, triggered_by_notification=True
        )

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
        # Computed attributes are derived values — overwriting them with a stored
        # value would silently break the computation contract. Raise instead.
        if key in self.__dict__["computed"]:
            raise ReadOnlyError(
                f"Write attempt to computed attribute '{key}'. "
                f"Computed attributes cannot be overwritten."
            )

        # check constraints on the item and on self:
        item: Item = Item(key, value)

        # unroll logic:
        key_existed_before: bool = key in self.__dict__["data"]
        _old_value: Value = self.__dict__["data"][key] if key_existed_before else None

        self.__dict__["data"][key] = value

        try:
            self._check_value_constraints(item.value, ChangeEvent.UPDATE)
            self._check_attribute_function_constraints(ChangeEvent.UPDATE)
        except ConstraintViolationError as e:
            # rollback change:
            if key_existed_before:
                self.__dict__["data"][key] = _old_value
            else:
                del self.__dict__["data"][key]
            raise e

        # notify observers about the change:
        self.notify_observers(item, ChangeEvent.UPDATE)

    def __delitem__(self, key):
        """Customize item deletion. This must be used for non-str-type keys."""
        if self.__dict__["frozen"]:
            raise ReadOnlyError(
                f"Delete attempt to attribute '{key}'. This DictionaryAttributeFunction is read-only."
            )
        # Computed attributes are definitions, not stored data — deleting them
        # through the regular item API would be confusing. Raise instead.
        if key in self.__dict__["computed"]:
            raise ReadOnlyError(
                f"Delete attempt to computed attribute '{key}'. "
                f"Computed attributes cannot be deleted."
            )

        if key in self.__dict__["data"]:
            # unroll logic:
            # keep ref to old value:
            _old_value: Value = self.__dict__["data"][key]

            try:
                del self.__dict__["data"][key]
                self._check_attribute_function_constraints(ChangeEvent.DELETE)
                self._check_value_constraints(_old_value, ChangeEvent.DELETE)
            except ConstraintViolationError as e:
                # rollback change:
                self.__dict__["data"][key] = _old_value
                raise e
            # notify observers about the change:
            self.notify_observers(Item(key, KeyDeletedSentinel), ChangeEvent.DELETE)

        else:
            raise AttributeError

    def __contains__(self, item):
        """Check if a key exists in this AF — includes both stored and computed attributes."""
        return item in self.__dict__["data"] or item in self.__dict__["computed"]

    def __len__(self):
        """Return the total number of attributes (stored + computed).
        The constructor ensures no overlap between stored and computed keys,
        so a simple addition suffices."""
        return len(self.__dict__["data"]) + len(self.__dict__["computed"])

    def __iter__(self):
        """Iterate over all items (stored and computed) as Item(key, value) objects.
        Computed values are evaluated on each iteration. Stored items are yielded
        first, then computed items — making computed attributes indistinguishable
        from stored ones when consumed by operators, filters, or schema validation."""

        def mapper(item):
            return Item(item[0], item[1])

        yield from map(mapper, self.__dict__["data"].items())
        # Yield computed attributes — each function is called with self to produce
        # the current value. Since the constructor rejects key overlaps, no
        # shadowing check is needed here.
        for key, func in self.__dict__["computed"].items():
            yield Item(key, func(self))

    def keys(self) -> Generator:
        """Get the keys of the AttributeFunction (stored and computed).
        @return: An iterable of the keys.
        """
        yield from self.__dict__["data"].keys()
        yield from self.__dict__["computed"].keys()

    def values(self):
        """Get the values of the AttributeFunction (stored and computed).
        Computed values are evaluated on each call.
        @return: An iterable of the values.
        """
        yield from self.__dict__["data"].values()
        for func in self.__dict__["computed"].values():
            yield func(self)

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
        """Update the current AttributeFunction with the stored items of another one.
        Computed attributes of the source are skipped — they are not materialized.
        @param other: The AttributeFunction to update with.
        """
        # iterate only over stored items to avoid materializing computed attributes:
        for key, value in other.__dict__["data"].items():
            if key in self:
                logger.warning(f"key '{key}' already exists and will be overwritten.")
            self.__setitem__(key, value)

    def print(self, flat=False, recursion_depth: int = 0):
        """Print representation of the current AttributeFunction."""
        prefix: str = "    " * recursion_depth
        for item in self:
            if isinstance(item.value, AttributeFunction):
                if flat:
                    print(prefix + f"{item.key}: {item.value}")
                else:
                    print(prefix + f"{item.key}:")
                    item.value.print(flat=flat, recursion_depth=recursion_depth + 1)
            else:
                print(prefix + f"{item.key}: {item.value}")

    def my_str(self, flat=False, recursion_depth: int = 0):
        ret: str = ""
        prefix: str = "    " * recursion_depth
        for item in self:
            if isinstance(item.value, AttributeFunction):
                if flat:
                    ret += (
                        prefix
                        + f"{item.key}: {item.value.__repr__() if item.value else "NONE"}\n"
                    )
                else:
                    ret += prefix + f"{item.key}:\n"
                    ret += item.value.my_str(
                        flat=flat, recursion_depth=recursion_depth + 1
                    )
            else:
                ret += prefix + f"{item.key}: {item.value}\n"
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
        # Computed attributes contain lambdas/closures which cannot be pickled.
        # They are ephemeral by design — after deserialization, the caller must
        # re-attach computed definitions via add_computed() or the constructor.
        state_dict["computed"] = {}
        # Default fallback is also a lambda — strip it for the same reason:
        state_dict["default"] = None
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

    # Django ORM-style lookup operators for use in where() kwargs.
    # If the last __-segment of a kwarg key matches a key in this dict,
    # it is treated as a lookup operator; otherwise the entire key is
    # treated as a field path with an implicit "exact" lookup.
    _LOOKUPS: dict[str, Callable] = {
        "exact": lambda attr, val: attr == val,
        "lt": lambda attr, val: attr < val,
        "lte": lambda attr, val: attr <= val,
        "gt": lambda attr, val: attr > val,
        "gte": lambda attr, val: attr >= val,
        "in": lambda attr, val: attr in val,
        "contains": lambda attr, val: val in attr,
        "icontains": lambda attr, val: val.lower() in attr.lower(),
        "startswith": lambda attr, val: attr.startswith(val),
        "endswith": lambda attr, val: attr.endswith(val),
        "isnull": lambda attr, val: (attr is None) == val,
        "range": lambda attr, val: val[0] <= attr <= val[1],
    }

    @staticmethod
    def _parse_lookup(key: str) -> tuple[str, str]:
        """Parse a kwarg key into (field_path, lookup_name).
        If the last __-segment is a known lookup, split it off;
        otherwise treat the whole key as a field path with implicit 'exact'.
        Examples:
            'name'              -> ('name', 'exact')
            'department__name'  -> ('department__name', 'exact')   # traversal only
            'salary__gte'       -> ('salary', 'gte')               # lookup
            'department__name__gte' -> ('department__name', 'gte') # traversal + lookup
        """
        if "__" in key:
            field_path, last_segment = key.rsplit("__", 1)
            if last_segment in DictionaryAttributeFunction._LOOKUPS:
                return field_path, last_segment
        return key, "exact"

    def where(
        self, predicate: Callable[..., Any] | Predicate = None, **kwargs
    ) -> "DictionaryAttributeFunction":
        """Filter the items of this DictionaryAttributeFunction based on the given conditions.
        @param predicate: A callable or structured ``Predicate``. Plain callables receive an
            ``Item`` and return True/False. Structured ``Predicate`` instances are automatically
            applied to ``item.value`` (the TF value), consistent with ``filter_values`` semantics.
        @param kwargs: Keyword arguments for filtering conditions, phrased directly against the value of this attribute
        function. Supports Django ORM-style lookups: field__lt, field__lte, field__gt, field__gte, field__in,
        field__contains, field__icontains, field__startswith, field__endswith, field__isnull, field__range.
        Plain field=value is equivalent to field__exact=value.

        @return: A new DictionaryAttributeFunction instance containing only the items that satisfy the filtering conditions.
        """
        result: DictionaryAttributeFunction = type(
            self
        )()  # create result instance of the same type as self

        # TODO: shouldn't this be a copy constructor instead?
        # the following triggers errors in some unit tests,
        # result: DictionaryAttributeFunction = self.copy()
        # result.unfreeze()

        # TODO: delegate to FQL operator

        assert predicate is None or callable(predicate)
        assert kwargs is None or dict

        # Structured predicates operate on values (filter_values semantics),
        # while plain callables receive the full Item.
        is_structured: bool = isinstance(predicate, Predicate)

        item: Item
        # loop over entries of self:
        for item in self:
            # if predicate exists, evaluate it:
            if predicate is not None:
                if is_structured:
                    if not predicate(item.value):
                        continue
                elif not predicate(item):
                    continue

            # if kwargs conditions exist, evaluate them (Django ORM-style conjunct):
            match: bool = True
            for key, value in kwargs.items():
                field_path, lookup_name = self._parse_lookup(key)
                lookup_fn = self._LOOKUPS[lookup_name]
                if not (
                    hasattr(item.value, field_path)
                    and lookup_fn(getattr(item.value, field_path), value)
                ):
                    match = False
                    break

            if match:
                result[item.key] = item.value

        return result

    def project(self, *keys) -> "AttributeFunction":
        """Project the values of this DictionaryAttributeFunction based on the given keys.
        @param keys: the keys to project to, i.e., the keys of the items to include in the result.

        @return: A new DictionaryAttributeFunction instance containing only the keys specified.
        """
        result: DictionaryAttributeFunction = type(
            self
        )()  # create result instance of the same type as self

        assert len(keys) >= 1, "At least one key must be provided for projection."

        # TODO: make operator, via new transform_values operator?
        # what about inserting multiple items into an AF?
        def project(input_DAF: DictionaryAttributeFunction, keys):
            output_DAF: DictionaryAttributeFunction = type(input_DAF)()
            # Copy only stored items that are in the projection set:
            for key, value in input_DAF.__dict__["data"].items():
                if key in keys:
                    output_DAF[key] = value
            # Carry over computed definitions for projected keys — the lambda
            # itself is preserved, not its current value, so the computation
            # stays live on the projected AF. Note: if the lambda references
            # a key that was excluded from the projection, it will raise
            # AttributeError on access (same as SQL dropping a dependency column).
            for key, func in input_DAF.__dict__["computed"].items():
                if key in keys:
                    output_DAF.__dict__["computed"][key] = func
            return output_DAF

        outer_item: Item
        # loop over entries of self:
        for outer_item in self:
            # if key exists, add it to the projection result:
            result[outer_item.key] = project(outer_item.value, keys)

        return result

    def rename(self, **kwargs) -> "DictionaryAttributeFunction":
        """Rename keys in the values of this DictionaryAttributeFunction.
        Analogous to the rename operator ρ in relational algebra.

        The kwargs map old key names to new key names. Keys not mentioned in kwargs are kept as-is.
        Returns a new AF of the same type — the original is not modified.

        This operates one level deep: it renames keys inside each *value* of self, not the keys of
        self itself. For example, on an RF it renames attributes inside each TF, not the tuple keys.

        Example:
            users.rename(name="first_name", yob="birth_year")
            # Before: RF({1: TF({"name": "Alice", "yob": 1990}), ...})
            # After:  RF({1: TF({"first_name": "Alice", "birth_year": 1990}), ...})

        @param kwargs: old_key=new_key pairs, e.g. rename(name="first_name").
        @return: A new DictionaryAttributeFunction where each value has its keys renamed accordingly.
        """
        assert len(kwargs) >= 1, "At least one rename mapping must be provided."

        # create a new AF of the same type (e.g. RF → RF):
        result: DictionaryAttributeFunction = type(self)()

        outer_item: Item
        for outer_item in self:
            # create a new value AF of the same type as the original value (e.g. TF → TF):
            renamed: DictionaryAttributeFunction = type(outer_item.value)()
            # Copy stored items with potentially renamed keys:
            for key, value in outer_item.value.__dict__["data"].items():
                new_key = kwargs.get(key, key)
                renamed[new_key] = value
            # Carry over computed definitions under their (potentially renamed) keys.
            # The lambda itself is preserved — only the key changes, the computation
            # stays live. The lambda still references the original attribute names
            # internally (e.g. tf["age"]), which is correct since rename() only
            # changes the external key, not how other attributes are accessed.
            for key, func in outer_item.value.__dict__["computed"].items():
                new_key = kwargs.get(key, key)
                renamed.__dict__["computed"][new_key] = func
            # preserve the outer key (e.g. the tuple ID in an RF):
            result[outer_item.key] = renamed

        return result

    def random_item(self) -> Any:
        """Get a random item from the AttributeFunction.
        @return: A random item.
        """
        assert (
            len(self) > 0
        ), "Cannot get a random item from an empty AttributeFunction."

        # TODO: could be more efficient
        return random.choice(list(self))


# --- AF Type Hierarchy ---
#
# The following classes form the core type hierarchy of the Functional Data Model (FDM).
# Each level nests the level below as its values, creating a uniform recursive structure:
#
#   SDBF  →  DBF  →  RF  →  TF  →  scalar values
#
# Every level is a DictionaryAttributeFunction, meaning the same operations (where, project,
# rename, filter, etc.) work uniformly at any level. This is a key difference to the relational
# model where tuples, relations, and databases are distinct concepts with different operations.
#
# In addition, RSF (Relationship Function) models N:M relationships using CompositeForeignObjects
# as keys, and Tensor extends DAF with multi-dimensional composite keys and element-wise arithmetic.


class TF[Key, Value](DictionaryAttributeFunction[Key, Value]):
    """A tuple function: maps attribute names to scalar values.
    Analogous to a single row/tuple in the relational model, but as a first-class function.
    Example: TF({"name": "Alice", "yob": 1990, "department": <another AF>})
    """

    ...


class RF[Key](DictionaryAttributeFunction[Key, TF]):
    """A relation function: maps keys (e.g. surrogate IDs) to TFs.
    Analogous to a table/relation in the relational model.
    Example: RF({1: TF({"name": "Alice", ...}), 2: TF({"name": "Bob", ...})})
    """

    ...


class DBF[Key](DictionaryAttributeFunction[Key, RF]):
    """A database function: maps relation names to RFs.
    Analogous to a database/schema in the relational model.
    Example: DBF({"users": RF({...}), "departments": RF({...})})
    """

    ...


class SDBF[Key](DictionaryAttributeFunction[Key, DBF]):
    """A set-of-databases function: maps database names to DBFs.
    Has no direct analogue in the relational model — enables cross-database queries
    that are not possible (or at least very awkward) in SQL.
    """

    ...


class RSF[Value](DictionaryAttributeFunction[CompositeForeignObject, Value]):
    """A relationship function: models N:M relationships between AFs.
    Uses CompositeForeignObject as keys to represent multi-way associations.
    Each key is a composite of references to the participating AFs,
    and the value holds the relationship's own attributes (e.g. a date).

    Example: meetings[CompositeForeignObject(user1, customer1)] = TF({"date": "2025-01-01"})
    """

    def related_values(
        self,
        match_index: int,
        subkey: AttributeFunction,
        return_index: int,
    ) -> Iterable[AttributeFunction]:
        """Get all related AFs at one subkey position that are paired with a specific AF at another position.
        @param match_index: Which position in the composite key to match against (0-based).
        @param subkey: The AF to match against at position match_index.
        @param return_index: Which position in the composite key to return (0-based).
        @return: An iterable of AFs from the matched composite keys at position return_index.

        Example: Given meetings[CompositeForeignObject(user, customer)] = ...,
                 meetings.related_values(0, user1, 1) returns all customers that have a meeting with user1.
        """

        return map(
            # extract the related subkey value at the return index from the composite key:
            lambda item: item.key.subkey(return_index),
            filter(
                # keep only items where the composite key matches the given subkey at the match index:
                lambda item: item.key.subkey(match_index) == subkey,
                self,
            ),
        )


class Tensor[Value](DictionaryAttributeFunction[CompositeForeignObject, Value]):
    """A tensor: a multi-dimensional AF with composite keys representing coordinates.
    Supports element-wise arithmetic (+, -, *) and is parameterized by its dimensions.

    Note: dimensions are stored via __setattr__ which places them in the data dict alongside
    actual tensor entries. This means keys() includes "dimensions" — a known limitation.
    """

    def __init__(self, dimensions: list[int]):
        """Initialize the Tensor with the given dimensions.
        @param dimensions: Shape of the tensor, e.g. [3, 4] for a 3x4 matrix.
        """
        super().__init__()
        assert len(dimensions) > 0, "Tensor must have at least one dimension."
        self.dimensions = dimensions

    def rank(self) -> int:
        """Get the rank (number of dimensions) of this tensor.
        A vector has rank 1, a matrix rank 2, etc.
        """
        return len(self.dimensions)

    def __add__(self, other: "Tensor") -> "Tensor":
        """Element-wise addition. Returns a new Tensor with the summed values."""
        assert (
            self.dimensions == other.dimensions
        ), "Cannot add tensors with different dimensions."
        result = Tensor(self.dimensions)
        for key in self.keys():
            result[key] = self[key] + other[key]
        return result

    def __sub__(self, other: "Tensor") -> "Tensor":
        """Element-wise subtraction. Returns a new Tensor with the differences."""
        assert (
            self.dimensions == other.dimensions
        ), "Cannot subtract tensors with different dimensions."
        result = Tensor(self.dimensions)
        for key in self.keys():
            result[key] = self[key] - other[key]
        return result

    def __mul__(self, other: "Tensor") -> "Tensor":
        """Element-wise multiplication (Hadamard product). Returns a new Tensor."""
        assert (
            self.dimensions == other.dimensions
        ), "Cannot multiply tensors with different dimensions."
        result = Tensor(self.dimensions)
        for key in self.keys():
            result[key] = self[key] * other[key]
        return result

    def __matmul__(self, other: "Tensor") -> "Tensor":
        """Matrix multiplication (not yet implemented)."""

        raise NotImplementedError

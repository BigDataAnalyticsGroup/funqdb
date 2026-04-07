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

import logging
from abc import abstractmethod, ABC
from typing import Generator, Callable, Any

from fdm.util import Explainable

logger = logging.Logger(__name__)


class PureFunction[INPUT, OUTPUT](ABC):
    """An abstract mapping_function."""

    @abstractmethod
    def __call__(self, *args, **kwargs) -> OUTPUT:
        """Make the object callable.
        @param arg: The argument for the call.
        @return: The result of the call.
        """
        ...


class AttributeFunctionSentinel:
    """Uses this as entry in AttributeFunctions as sentinel for lazy loading from store.
    So when loading an AttributeFunction from the store, we can have
    AttributeFunctionSentinel instances in place of actual AttributeFunction instances.
    This should be checked when accessing values in AttributeFunctions.
    At that point, the actual AttributeFunction can be fetched from the store.
    The AttributeFunctionSentinel instance is then replaced with the actual AttributeFunction instance.

    Note: the "inverse" problem also exists: when evicting an AttributeFunction from memory,
    we need to replace actual AttributeFunction instances with AttributeFunctionSentinel instances.
    This is not implemented yet.

    Maybe some sort of (weak) ref counting is required to know when to evict an AttributeFunction from memory.

    """

    def __init__(self, id: int):
        self._id = id

    @property
    def id(self):
        return self._id


class AttributeFunction[Key, Value](PureFunction, Explainable):
    """An abstract base class representing a callable object that can also manage its attributes."""

    # global UUID counter for all AttributeFunction instances
    # this is required for serialization and storage in the Store, as well as for references between AttributeFunctions
    # note: this will break if we first create instances in memory and then load additional instances from the store,
    # as the global_uuid will be reset to 0. This calls for a more robust UUID generation strategy, such as using the
    # uuid module to generate unique identifiers.
    global_uuid: int = 0

    def _assign_uuid(self):
        """Assign a unique UUID to this instance."""
        self.__dict__["_uuid"] = AttributeFunction.global_uuid

        # increase the global UUID counter for the next instance:
        AttributeFunction.global_uuid += 1

    @property
    def _uuid(self) -> int:
        """Return the UUID of this instance."""
        return self.__dict__["_uuid"]

    def __init__(self):
        super().__init__()
        self._assign_uuid()

    def __getattr__(self, name: str) -> Value:
        """Make the object callable through .-syntax.
        Redirects to __getitem__ for actual retrieval.
        @param name: Name of the attribute being accessed. Notice that the 'name' parameter is of type 'Any' to allow
        flexibility in attribute types.
        @return: The value of the requested attribute.
        """
        return self.__getitem__(name)

    def __setattr__(self, name: str, value: Value):
        """Customize attribute assignment. Note that the 'name' parameter is of type 'Any' to allow flexibility in
        attribute types. Redirects to __setitem__ for actual storage."""
        self.__setitem__(name, value)

    def __delattr__(self, name):
        """Customize attribute deletion. Redirects to __delitem__ for actual deletion."""
        self.__delitem__(name)

    @abstractmethod
    def __getitem__(self, key: Key) -> Any:
        """Make the object callable through []-syntax."""
        ...

    @abstractmethod
    def __setitem__(self, key: Key, value: Value):
        """Customize item assignment. This must be used for non-str-type keys.
        @param key: The key of the item being assigned.
        @param value: The value to assign to the item.
        """
        ...

    def __call__(self, *args, **kwargs) -> Any:
        """Make the object callable through () syntax.
        @return: The result of the call.
        """
        # not sure about this one: looks also weird in the code
        # if len(kwargs) > 0:
        # TODO:
        # allow kwargs to carry filter conditions in order to enable easy filters,
        #  then return a new(!) filtered instance
        # create a new instance of the same type as self and return it:
        #    ret: AttributeFunction = type(self)()
        # filter the items of self based on the kwargs and enter them into ret:
        # TODO
        # evaluate filter conditions based on kwargs and filter items of self accordingly, then enter them into ret

        #    return ret
        return self.__getitem__(*args)

    @abstractmethod
    def __eq__(self, other: "AttributeFunction") -> bool:
        """Check equality between two AttributeFunction instances based on their items.
        @param other: The other AttributeFunction instance to compare with.
        @return: True if both instances have the same items, False otherwise.
        """
        ...

    @abstractmethod
    def update(self, other: "AttributeFunction") -> "AttributeFunction":
        """Update the current AttributeFunction with another one.
        @param other: The other AttributeFunction to update from.
        @return: The updated AttributeFunction.
        """
        ...

    @abstractmethod
    def __len__(self) -> int:
        """Return the number of items in the AttributeFunction.
        @return: The number of items.
        """
        ...

    @property
    @abstractmethod
    def frozen(self) -> bool: ...

    @property
    def uuid(self) -> int:
        """Get the UUID of this AttributeFunction.
        @return: The UUID.
        """
        return self.__dict__["_uuid"]

    @abstractmethod
    def freeze(self):
        """Make the AttributeFunction read-only."""
        ...

    @abstractmethod
    def unfreeze(self):
        """Make the AttributeFunction writable."""
        ...

    @abstractmethod
    def get_lineage(self) -> list[str]:
        """Get the lineage of this AttributeFunction.
        @return: A list representing the lineage.
        """
        ...

    def add_lineage(self, entry: str):
        """Add an entry to the lineage of this AttributeFunction.
        @param entry: The lineage entry to add.
        """
        ...

    def __hash__(self):
        """Compute the hash of the AttributeFunction based on its UUID.
        @return: The hash value of the AttributeFunction.
        """
        return hash(self.uuid)

    @abstractmethod
    def keys(self) -> Generator:
        """Get the keys of the AttributeFunction.
        @return: An iterable of the keys.
        """
        ...

    @abstractmethod
    def values(self):
        """Get the values of the AttributeFunction.
        @return: An iterable of the values.
        """
        ...

    @abstractmethod
    def where(
        self, predicate: Callable[..., Any] = None, **kwargs
    ) -> "AttributeFunction":
        """Filter the items of this AttributeFunction based on the given conditions.
        @param predicate: A callable defined on the values of this attribute function and returns True if the item
        should be included in the result, False otherwise.

        @param kwargs: Keyword arguments for filtering conditions, phrased directly against the value of this attribute
        function.
        @return: A new AttributeFunction instance containing only the items that satisfy the filtering conditions.
        """

        ...

    def 𝛔(self, predicate: Callable[..., Any] = None, **kwargs) -> "AttributeFunction":
        """Rel algebra style naming for where."""
        return self.where(predicate, **kwargs)

    @abstractmethod
    def project(self, *keys) -> "AttributeFunction":
        """Project the AttributeFunction to the specified keys.
        @param keys: The keys to project to.
        @return: A new AttributeFunction instance containing only the specified keys.
        """
        ...

    def π(self, *keys) -> "AttributeFunction":
        """Rel algebra style naming for project."""
        return self.project(*keys)

    @abstractmethod
    def rename(self, **kwargs) -> "AttributeFunction":
        """Rename keys in the values of this AttributeFunction.
        @param kwargs: Mapping of old_key=new_key pairs, e.g. rename(name="first_name").
        @return: A new AttributeFunction with renamed keys in each value.
        """
        ...

    def ρ(self, **kwargs) -> "AttributeFunction":
        """Rel algebra style naming for rename."""
        return self.rename(**kwargs)

    def top(
        self, k: int, key: Callable[..., Any]
    ) -> "AttributeFunction":
        """Return the k items with the smallest key values (top-k ascending).
        @param k: Number of items to keep.
        @param key: A function mapping an Item to a comparable value for ranking.
        @return: A new AttributeFunction containing the k smallest items.
        """
        from fql.operators.subsets import subset

        return subset(self, ranking_key=key, k=k).result

    def bottom(
        self, k: int, key: Callable[..., Any]
    ) -> "AttributeFunction":
        """Return the k items with the largest key values (top-k descending).
        @param k: Number of items to keep.
        @param key: A function mapping an Item to a comparable value for ranking.
        @return: A new AttributeFunction containing the k largest items.
        """
        from fql.operators.subsets import subset

        return subset(self, ranking_key=key, k=k, reverse=True).result

    @abstractmethod
    def random_item(self) -> Any:
        """Get a random item from the AttributeFunction.
        @return: A random item.
        """
        ...

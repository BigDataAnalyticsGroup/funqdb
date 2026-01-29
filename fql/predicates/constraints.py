from abc import abstractmethod, ABC

from fdm.functions import AttributeFunction
from fql.util import Item


class in_subset:
    def __init__(self, whitelist: set[str]):
        self.whitelist = whitelist

    def __call__(self, *args, **kwargs) -> bool:
        return args[0] in self.whitelist


class AttributeFunctionConstraint(ABC):
    """Marks an attribute function constraint."""

    @abstractmethod
    def __call__(self, attribute_function: AttributeFunction) -> bool:
        """Evaluates whether the given attribute_function fulfills the constraint."""
        pass


class ItemConstraint(ABC):
    """Marks an item constraint."""

    @abstractmethod
    def __call__(self, item: Item) -> bool:
        """Evaluates whether the given item fulfills the constraint."""
        pass


class attribute_name_equivalence(AttributeFunctionConstraint):
    """Predicate that checks if the keys in a given attribute_function match a given set."""

    def __init__(self, attribute_names: set[str]):
        self.attribute_names = attribute_names

    def __call__(self, attribute_function: AttributeFunction) -> bool:
        assert isinstance(attribute_function, AttributeFunction)
        return self.attribute_names == {item.key for item in attribute_function}


class attribute_name_equivalence_item(ItemConstraint):
    """Predicate that checks if the keys in a given item match a given set."""

    def __init__(self, attribute_names: set[str]):
        self.wrapped = attribute_name_equivalence(attribute_names=attribute_names)

    def __call__(self, item: Item) -> bool:
        assert type(item) is Item
        return self.wrapped(item.value)


class max_count(ItemConstraint):
    """Predicate that checks if the number of entries in a given item is below a maximum."""

    def __init__(self, max_count: int):
        self.max_count = max_count

    def __call__(self, af: AttributeFunction) -> bool:
        return len(af) <= self.max_count

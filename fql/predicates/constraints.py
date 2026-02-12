from abc import abstractmethod, ABC

from fdm.API import AttributeFunction
from fql.util import Item


class in_subset:
    def __init__(self, whitelist: set[str]):
        self.whitelist = whitelist

    def __call__(self, *args, **kwargs) -> bool:
        return args[0] in self.whitelist


class AttributeFunctionConstraint(ABC):
    """Marks an attribute function constraint, i.e. a constraint that must hold for the entire attribute function not
    just one particular item."""

    @abstractmethod
    def __call__(self, attribute_function: AttributeFunction) -> bool:
        """Evaluates whether the given attribute_function fulfills the constraint."""
        ...


class attribute_name_equivalence(AttributeFunctionConstraint):
    """Predicate that checks if the keys in a given attribute_function match a given set."""

    def __init__(self, attribute_names: set[str]):
        self.attribute_names = attribute_names

    def __call__(self, attribute_function: AttributeFunction) -> bool:
        assert isinstance(attribute_function, AttributeFunction)
        return self.attribute_names == {item.key for item in attribute_function}


class max_count(AttributeFunctionConstraint):
    """Predicate that checks if the number of entries in a given item is below a maximum."""

    def __init__(self, max_count_limit: int):
        self.max_count_limit = max_count_limit

    def __call__(self, af: AttributeFunction) -> bool:
        return len(af) <= self.max_count_limit

import inspect
from typing import Callable, Any, Iterable

from fql.operators.APIs import Operator
from fql.util import Item


import logging

logger = logging.Logger(__name__)


class filter_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that filters the values found in the input instance. In contrast to standard filter operations
    returning a set or list of the filtered items, this operator stays in the data model and returns an Attribute
    Function with the qualifying elements as its output.
    """

    def __init__(
        self,
        filter_predicate: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the filter_items operator.
        @param filter_predicate: A predicate that takes an Item and returns True if the item should be kept, False otherwise.
        @param output_factory: This factory function will be used to create the output instance.
        """

        self.filter_predicate = filter_predicate
        self.output_factory = output_factory

    def __call__(
        self, input_function: INPUT_AttributeFunction = None, explain: bool = False
    ) -> OUTPUT_AttributeFunction:

        print(inspect.signature(input_function))

        if explain:
            return f"filter_items operator with predicate {self.filter_predicate} applied to {input_function}"

        assert input_function is not None
        # get the mapped items:
        mapped_items: Iterable[Item] = filter(self.filter_predicate, input_function)

        output_function = input_function
        if self.output_factory is not None:
            output_function = self.output_factory(None)
            output_function.unfreeze()
        else:
            logger.warning(
                "No output function factory provided; modifying input function in place. This is not recommended as it"
                " may have side effects on the input."
            )

        # (1.) we need to materialize the items first to avoid modifying while iterating
        buffer = {item.key: item.value for item in mapped_items if item is not None}

        # (2.) enter values in output_function:
        for key, value in buffer.items():
            output_function[key] = value

        output_function.freeze()

        return output_function


class filter_items_complement[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    filter_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Computes the complement of the filter_items operator."""

    def __init__(
        self,
        filter_predicate: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the filter_items_complement operator.
        @param filter_predicate: A predicate that takes an Item and returns True if the item should be filtered out,
        False otherwise.
        @param output_factory: This factory function will be used to create the output instance.
        """
        super().__init__(filter_predicate, output_factory)

    def __call__(
        self, input_function: INPUT_AttributeFunction
    ) -> OUTPUT_AttributeFunction:
        """Call the filter_items operator with the negated predicate."""
        super.__call__(lambda x: not self.filter_predicate)

from abc import ABC
from typing import Callable, Any, Iterable

from fql.APIs import Item, PureFunction


class Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    PureFunction[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Signature for an operator that transforms inputs to outputs."""

    pass


class MapInstance[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that maps an input instance to an output instance."""

    def __init__(self, mapping_function: Callable[..., Any]):
        self.mapping_function = mapping_function

    def __call__(
        self, input_function: INPUT_AttributeFunction
    ) -> OUTPUT_AttributeFunction:
        """Make the object callable.
        @return: The result of the call.
        """
        return self.mapping_function(input_function)


class TransformValues[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that transforms the input instance by mapping its values.
    The modified input instance will be returned as the output."""

    def __init__(
        self,
        transformation_function: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the TransformValues operator.
        @param transformation_function: A function that takes an Item and returns a transformed Item or None
        @param output_factory: If set, this factory function will be used to create the output instance.
        """

        self.mapping_function = transformation_function
        self.output_factory = output_factory

    def __call__(
        self, input_function: INPUT_AttributeFunction
    ) -> OUTPUT_AttributeFunction:
        # get the mapped items:
        mapped_items: Iterable[Item] = map(self.mapping_function, input_function)

        output_function: INPUT_AttributeFunction = input_function
        if self.output_factory is not None:
            output_function = self.output_factory()

        # modify the input_function:
        # (1.) we need to materialize the items first to avoid modifying while iterating
        buffer = {item.key: item.value for item in mapped_items if item is not None}

        # (2.) replace the values in the input_function, i.e. modify the input function
        for key, value in buffer.items():
            output_function[key] = value

        return output_function

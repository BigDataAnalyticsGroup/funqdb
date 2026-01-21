from abc import abstractmethod, ABC
from typing import Callable, Any


class Function[INPUT, OUTPUT](ABC):
    """An abstract mappin_function."""

    @abstractmethod
    def __call__(self, *args, **kwargs) -> OUTPUT:
        """Make the object callable.
        @param arg: The argument for the call.
        @return: The result of the call.
        """
        pass


class Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Function[INPUT_AttributeFunction, OUTPUT_AttributeFunction]):
    """Signature for an operator that transforms inputs to outputs."""
    pass


class Map[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]):
    """An operator that maps inputs to outputs."""

    def __call__(self, mapping_function: Callable[[Any], Any], input_function: INPUT_AttributeFunction) -> OUTPUT_AttributeFunction:
        """Make the object callable.
        @param args: Positional arguments for the call.
        @param kwargs: Keyword arguments for the call.
        @return: The result of the call.
        """
        return map(mapping_function, input_function)

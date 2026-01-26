from typing import Callable, Any, Iterable

from fql.functions import RF, DBF
from fql.operators.APIs import Operator
from fql.util import Item

import logging

logger = logging.Logger(__name__)


class map_instance[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
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


class transform_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that transforms the input instance by mapping its values.
    The modified input instance will be returned as the output."""

    def __init__(
        self,
        transformation_function: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the transform_items operator.
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

        output_function = input_function
        if self.output_factory is not None:
            output_function = self.output_factory(None)
            output_function.unfreeze()
        else:
            logger.warning(
                "No output function factory provided; modifying input function in place. This is not recommended as it"
                " may have sideeffect on the input."
            )

        # (1.) we need to materialize the items first to avoid modifying while iterating
        buffer = {item.key: item.value for item in mapped_items if item is not None}

        # (2.) enter values in output_function:
        for key, value in buffer.items():
            output_function[key] = value

        output_function.freeze()

        return output_function


class partition(Operator[RF, DBF]):
    """Partition an input RF into a DBF with its partitions as RFs."""

    # TODO: do we even need any factories here as the output types are fixed anyhow?

    def __init__(
        self,
        partitioning_function: Callable[[Item], Any],
    ):
        self.partitioning_function = partitioning_function

    def __call__(self, input_function: RF) -> DBF:
        output_function: DBF = DBF(frozen=False)
        item: Item
        for item in input_function:
            partition_key = self.partitioning_function(item)
            if partition_key not in output_function:
                output_function[partition_key] = RF(frozen=False)
            output_function[partition_key][item.key] = item.value

        # freeze all RFs in the output DBF
        for item in output_function:
            item.value.freeze()

        output_function.freeze()
        return output_function


class group_by_aggregate(Operator[RF, RF]):
    """Group an input RF by a grouping function and aggregate the groups using an aggregation function."""

    def __init__(
        self,
        grouping_function: Callable[[Item], Any],
        aggregation_function: Callable[[RF], Any],
    ):
        self.grouping_function = grouping_function
        self.aggregation_function = aggregation_function

    def __call__(self, input_function: RF) -> RF:
        return transform_items[DBF, RF](
            transformation_function=self.aggregation_function,
            output_factory=lambda _: RF(),
        )(partition(partitioning_function=self.grouping_function)(input_function))

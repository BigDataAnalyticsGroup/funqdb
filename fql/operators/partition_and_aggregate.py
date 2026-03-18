from typing import Callable, Any

from fdm.attribute_functions import RF, DBF
from fql.operators.APIs import Operator
from fql.operators.aggregates import aggregate
from fql.operators.partition import group_by, partition
from fql.operators.transforms import transform_items
from fql.util import Item


class group_by_aggregate(Operator[RF, RF]):
    """Group an input RF by the equality of the given keys (the values mapped to by those keys) and aggregate the
    groups using the specified aggregation functions."""

    def __init__(self, *aggregate_keys, **aggregates):
        self.aggregate_keys = aggregate_keys
        self.aggregates = aggregates

    def __call__(self, input_function: RF) -> RF:
        # partition the input RF into a DBF with one RF per partition:
        group_by_result: DBF = group_by(*self.aggregate_keys)(input_function)

        aggregation_result = transform_items[DBF, RF](
            transformation_function=lambda item: Item(
                item.key, aggregate(**self.aggregates)(item.value)
            ),
            output_factory=lambda _: RF(),
        )(group_by_result)

        # take partitions (a DBF of RFs) and return one TF with one aggregated TF per partition:
        return aggregation_result


class partition_by_aggregate(Operator[RF, RF]):
    """Group an input RF by a grouping function and aggregate the groups using an aggregation function."""

    # TODO: generic input and output AFs: this operator can also be used to partition an entire database and aggregate
    # those partitions

    def __init__(
        self,
        partitioning_function: Callable[[Item], Any],
        aggregation_function: Callable[[RF], Any],
    ):
        self.partitioning_function = partitioning_function
        self.aggregation_function = aggregation_function

    def __call__(self, input_function: RF) -> RF:

        return transform_items[DBF, RF](
            transformation_function=self.aggregation_function,
            output_factory=lambda _: RF(),
        )(partition(partitioning_function=self.partitioning_function)(input_function))

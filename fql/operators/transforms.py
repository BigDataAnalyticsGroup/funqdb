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


from typing import Callable, Any, Iterable

from fdm.attribute_functions import RF, DBF, TF
from fql.operators.APIs import Operator
from fql.util import Item

import logging

logger = logging.Logger(__name__)


class transform[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that transforms an input instance to an output instance."""

    def __init__(self, transformation_function: Callable[..., Any]):
        self.transformation_function = transformation_function

    def __call__(
        self, input_function: INPUT_AttributeFunction
    ) -> OUTPUT_AttributeFunction:
        """Make the object callable.
        @return: The result of the call.
        """
        return self.transformation_function(input_function)


class transform_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that transforms the input instance by mapping its items.
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
        # TODO: discuss, really needed?
        # TODO: shall we still support inplace modifications?
        buffer = {item.key: item.value for item in mapped_items if item is not None}

        # (2.) enter key,values in output_function:
        for key, value in buffer.items():
            output_function[key] = value

        output_function.freeze()

        return output_function


# TODO: do we need a transform_values operator?


class partition(Operator[RF, DBF]):
    """Partition an input RF into a DBF with its partitions as RFs.

    #TODO: generic type: this operator can also partition an entire database...

    @param partitioning_function: A function that takes an Item and returns the partition key for that item.
    @param output_factory: If set, this factory function will be used to create the output DBF instance. If not set,
    a new DBF instance will be created.
    """

    def __init__(
        self,
        partitioning_function: Callable[[Item], Any],
        output_factory: Callable[..., DBF] = None,
    ):
        self.partitioning_function = partitioning_function
        if output_factory is None:
            self.output_factory = lambda _: DBF(frozen=False)

    def __call__(self, input_function: RF) -> DBF:
        output_function: DBF = self.output_factory(None)
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


class group_by(partition):
    """Partitions an input RF into a DBF based on the equality of the given keys (the values mapped to by those keys).
    Thus, this operator simulates the traditional group-by in relational algebra and SQL. The partitioning function is
    automatically derived from the specified grouping keys (attributes)."""

    # TODO: generic type: this operator can also partition an entire database...

    def __init__(self, *aggregate_keys):
        super().__init__(
            # convert the grouping function to a partitioning function that returns a tuple of the grouping keys for
            # the specified attributes:
            lambda item: tuple(item.value[attribute] for attribute in aggregate_keys)
        )


class aggregate(Operator[RF, TF]):
    """Aggregate an input RF using the specified aggregation functions."""

    def __init__(self, **aggregates):
        self.aggregates = aggregates

    def __call__(self, input_function: RF) -> TF:
        output_function = TF(frozen=False)
        for key, value in self.aggregates.items():
            output_function[key] = value(input_function)
        output_function.freeze()
        return output_function


class 𝜞(aggregate):
    """Synonym for aggregate operator."""

    pass


class group_by_aggregate(Operator[RF, RF]):
    """Group an input RF by the equality of the given keys (the values mapped to by those keys) and aggregate the
    groups using the specified aggregation functions."""

    def __init__(self, *aggregate_keys, **aggregates):
        self.aggregate_keys = aggregate_keys
        self.aggregates = aggregates

    def __call__(self, input_function: RF) -> RF:
        # partition the input RF into a DBF with one RF per partition:
        group_by_result: DBF = group_by(self.aggregate_keys)(input_function)

        aggregation_result = transform_items[DBF, RF](
            transformation_function=lambda item: Item(
                item.key, aggregate(**self.aggregates)(item.value)
            ),
            output_factory=lambda _: RF(),
        )(group_by_result)

        # take partitions (a DBF of RFs) and return one TF with one aggregated TF per partition:
        return aggregation_result


def __init__(self, *aggregate_keys, **aggregates):
    pass


class partition_by_aggregate(Operator[RF, RF]):
    """Group an input RF by a grouping function and aggregate the groups using an aggregation function."""

    # TODO: generic input and putput AFs: this operator can also be used to partition an entire database and aggregate
    #  the partitions

    def __init__(
        self,
        partitioning_function: Callable[[Item], Any],
        aggregation_function: Callable[[RF], Any],
    ):
        self.partitioning_function = partitioning_function
        self.aggregation_function = aggregation_function

    def __call__(self, input_function: RF) -> RF:

        # TODO: maybe keep one function which calls transform with a lambda
        # and another one calling aggregate() and expecting aggregation functions?
        return transform_items[DBF, RF](
            transformation_function=self.aggregation_function,
            output_factory=lambda _: RF(),
        )(partition(partitioning_function=self.partitioning_function)(input_function))

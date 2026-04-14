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


from typing import Callable, Any

from fdm.attribute_functions import RF, DBF
from fql.operators.APIs import Operator, OperatorInput
from fql.operators.aggregates import aggregate
from fql.operators.partition import group_by, partition
from fql.operators.transforms import transform_items
from fql.util import Item


class group_by_aggregate(Operator[RF, RF]):
    """Group an input RF by the equality of the given keys (the values mapped to by those keys) and aggregate the
    groups using the specified aggregation functions."""

    def __init__(
        self, input_function: OperatorInput[RF], *aggregate_keys, **aggregates
    ):
        self.input_function = input_function
        self.aggregate_keys = aggregate_keys
        self.aggregates = aggregates

    def _compute(self) -> RF:
        input_function = self._resolve_input(self.input_function)
        # partition the input RF into a DBF with one RF per partition:
        group_by_result: DBF = group_by(input_function, *self.aggregate_keys).result

        aggregation_result = transform_items[DBF, RF](
            group_by_result,
            transformation_function=lambda item: Item(
                item.key, aggregate(item.value, **self.aggregates).result
            ),
            output_factory=lambda _: RF(),
        ).result

        # take partitions (a DBF of RFs) and return one TF with one aggregated TF per partition:
        return aggregation_result


class partition_by_aggregate(Operator[RF, RF]):
    """Group an input RF by a grouping function and aggregate the groups using an aggregation function."""

    # TODO: generic input and output AFs: this operator can also be used to partition an entire database and aggregate
    # those partitions

    def __init__(
        self,
        input_function: OperatorInput[RF],
        *,
        partitioning_function: Callable[[Item], Any],
        aggregation_function: Callable[[RF], Any],
    ):
        self.input_function = input_function
        self.partitioning_function = partitioning_function
        self.aggregation_function = aggregation_function

    def _compute(self) -> RF:
        input_function = self._resolve_input(self.input_function)

        return transform_items[DBF, RF](
            partition(input_function, partitioning_function=self.partitioning_function),
            transformation_function=self.aggregation_function,
            output_factory=lambda _: RF(),
        ).result

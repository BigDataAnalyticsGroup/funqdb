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
from fql.expressions import Expression
from fql.operators.APIs import Operator, OperatorInput
from fql.operators.aggregates import AggregationFunction, aggregate
from fql.operators.partition import group_by, partition
from fql.operators.transforms import transform_items
from fql.predicates.predicates import Predicate
from fql.util import Item


class group_by_aggregate(Operator[RF, RF]):
    """Group an input RF by the equality of the given keys (the values mapped to by those keys) and aggregate the
    groups using the specified aggregation functions. Grouping follows ``group_by``: tuples without a grouping
    attribute are dropped. The ``group_by`` is built lazily, so invalid keys raise ``ValueError`` only when the result
    is computed."""

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
    """Partition an input RF by a partitioning function (key rules of ``partition``) and aggregate each partition,
    either with keyword aggregates as in ``group_by_aggregate`` or with an ``aggregation_function`` (exactly one form,
    see ``__init__``). See ``docs/changes/010-structured-partition-aggregates/``."""

    # TODO: generic input and output AFs: this operator can also be used to partition an entire database and aggregate
    # those partitions

    def __init__(
        self,
        input_function: OperatorInput[RF],
        *,
        partitioning_function: Callable[[Item], Any] | Predicate,
        aggregation_function: Callable[[Item], Item] | None = None,
        **aggregates: AggregationFunction,
    ):
        """Initialize the partition_by_aggregate operator.

        @param input_function: The RF to partition.
        @param partitioning_function: Maps an item to its partition key (see ``partition``).
        @param aggregation_function: Callable mapping a partition ``Item(key, RF)`` to an ``Item``; mutually
            exclusive with ``aggregates``. Stays opaque in the plan.
        @param aggregates: Aggregation functions by result name, e.g. ``count=Count()``; each partition becomes one
            tuple of them (an aggregate over no mapped values is absent, see 004). With a structured partitioning key
            and built-in aggregates the plan contains no ``Opaque`` marker. An aggregate cannot be named
            ``input_function``, ``partitioning_function``, ``aggregation_function`` or ``self`` (Python raises
            ``TypeError``); a misspelt parameter name is taken as an aggregate.
        @raises ValueError: If neither or both forms are given.
        @raises TypeError: If ``aggregation_function`` is a structured object -- an expression such as ``Record``, a
            predicate or an aggregation function (built-in or user-built): it would look structured in the plan but
            fail at run time or yield empty tuples. Pass aggregations as keyword aggregates instead.
        """
        # Exactly one form: aggregation_function XOR keyword aggregates.
        if (aggregation_function is None) == (not aggregates):
            raise ValueError(
                "partition_by_aggregate needs exactly one of aggregation_function or "
                "keyword aggregates (e.g. count=Count())"
            )
        if isinstance(
            aggregation_function, (Expression, Predicate, AggregationFunction)
        ):
            raise TypeError(
                "aggregation_function must be a callable on a partition Item, got "
                f"{aggregation_function!r}; pass built-in aggregates as keywords, "
                "e.g. count=Count()"
            )
        self.input_function = input_function
        self.partitioning_function = partitioning_function
        self.aggregation_function = aggregation_function
        self.aggregates = aggregates

    def _compute(self) -> RF:
        input_function = self._resolve_input(self.input_function)

        return transform_items[DBF, RF](
            partition(input_function, partitioning_function=self.partitioning_function),
            transformation_function=(
                self.aggregation_function
                if self.aggregation_function is not None
                # keyword form: one aggregate tuple per partition, as in group_by_aggregate
                else lambda item: Item(
                    item.key, aggregate(item.value, **self.aggregates).result
                )
            ),
            output_factory=lambda _: RF(),
        ).result

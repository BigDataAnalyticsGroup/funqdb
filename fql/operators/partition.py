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


from typing import Callable, Any, Mapping

from fdm.attribute_functions import RF, DBF, CompositeForeignObject
from fql.expressions import UNDEFINED, Attr, Composite, _evaluate_on_item
from fql.predicates.predicates import Predicate
from fql.operators.APIs import Operator, OperatorInput
from fql.util import Item


class partition(Operator[RF, DBF]):
    """Partition an input RF into a DBF with its partitions as RFs.

    #TODO: generic type: this operator can also partition an entire database...

    @param input_function: The input RF to partition.
    @param partitioning_function: A function that takes an Item and returns the partition key for that item, or a
    structured key from ``fql.expressions`` (``Attr``, ``Key``, ``Composite``) or a ``Predicate``. A tuple whose
    expression key is ``UNDEFINED`` (attribute not mapped) is dropped. A ``Predicate`` is evaluated on the item's value
    and yields a bool, never ``UNDEFINED`` (two-valued logic of 004: a comparison, ``Like``, ``In`` or ``Exists`` over
    a missing attribute gives ``False``; ``Missing(...)`` or the negation of a comparison gives ``True``); this keeps
    ``partition(P) == {True: filter_values(P), False: filter_values(Not(P))}`` (empty partitions are absent). Like
    ``filter_values``, a predicate may still raise on a wrongly typed value.
    @param output_factory: If set, this factory function will be used to create the output DBF instance. If not set,
    a new DBF instance will be created.
    """

    def __init__(
        self,
        input_function: OperatorInput[RF],
        *,
        partitioning_function: Callable[[Item], Any] | Predicate,
        output_factory: Callable[..., DBF] = None,
    ):
        self.input_function = input_function
        self.partitioning_function = partitioning_function
        if output_factory is None:
            self.output_factory = lambda _: DBF(frozen=False)
        else:
            self.output_factory = output_factory

    def _compute(self) -> DBF:
        input_function = self._resolve_input(self.input_function)
        output_function: DBF = self.output_factory(None)
        item: Item
        for item in input_function:
            partition_key = _evaluate_on_item(self.partitioning_function, item)
            if partition_key is UNDEFINED:
                continue  # attribute not mapped -> tuple does not contribute
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
    automatically derived from the specified grouping keys (attributes): ``Attr(key)`` for one key (scalar partition
    keys), ``Composite(Attr(k1), ...)`` for several (tuple partition keys).

    A tuple that does not map a grouping attribute is dropped (FDM has no NULL group). Keys must be strings and are read
    as attribute paths (``.`` or ``__``); invalid keys raise ``ValueError`` here. Consequently a misspelt key, or values
    that are not attribute functions, give an empty result instead of an error."""

    # TODO: generic type: this operator can also partition an entire database...

    def __init__(self, input_function: OperatorInput[RF], *aggregate_keys: str):

        assert (
            len(aggregate_keys) > 0
        ), "At least one grouping key must be specified for group_by!"

        super().__init__(
            input_function,
            # one key -> scalar partition key; several -> tuple of the key values:
            partitioning_function=(
                Composite(*(Attr(attribute) for attribute in aggregate_keys))
                if len(aggregate_keys) > 1
                else Attr(aggregate_keys[0])
            ),
        )
        self.aggregate_keys: tuple[str, ...] = aggregate_keys

    def _plan_params(self) -> Mapping[str, Any]:
        """Hide the derived partitioning key; ``aggregate_keys`` carries the same
        information and reaches the plan via the default public-attribute scan."""
        params: dict[str, Any] = dict(super()._plan_params())
        del params["partitioning_function"]
        return params

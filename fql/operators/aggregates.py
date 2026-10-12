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

from fdm.attribute_functions import RF, TF
from fql.operators.APIs import Operator, OperatorInput
from fql.predicates.predicates import _resolve_attr_path, _UNDEFINED

# Sentinel returned by an aggregation over an empty bag of values. It signals to
# the ``aggregate`` operator that the aggregate is undefined, so no (key, value)
# mapping is written to the output TF -- absence of an input attribute propagates
# to absence in the result (FDM has no NULL). ``Count`` is the exception: it
# counts mappings, so zero is a real answer and it never returns ``_ABSENT``.
_ABSENT: Any = object()


class AggregationFunction:
    def __init__(
        self, aggregation_function: Callable[[Any], Any], attribute: str | None = None
    ):
        self.aggregation_function = aggregation_function
        self.attribute = attribute

    def _defined_values(self, rf: RF) -> list:
        """Collect the values the attribute function actually maps for this
        attribute, skipping tuples that have no mapping for it.

        Reuses the predicate path resolver, so ``attribute`` may be a nested
        path (e.g. ``"person_role__name"``) and the lookup is exception-free: an
        unmapped attribute (or a missing reference along the path) yields no
        value rather than raising. A stored ``None`` is a real value and is
        collected like any other.

        @param rf: The relation whose tuples are scanned.
        @return: The list of mapped values (may be empty).
        """
        values: list = []
        for i in rf:
            value: Any = _resolve_attr_path(i.value, self.attribute)
            if value is not _UNDEFINED:  # no mapping -> no element in the bag
                values.append(value)
        return values

    def __call__(self, rf: RF) -> Any:
        """Aggregate over the mapped values of the attribute.

        Note: direct callers (bypassing the ``aggregate`` operator) may receive
        the ``_ABSENT`` sentinel when no tuple maps the attribute.
        """
        if self.attribute is None:
            return self.aggregation_function(rf)
        data: list = self._defined_values(rf)
        if not data:  # empty bag -> aggregate undefined -> absent from output
            return _ABSENT
        return self.aggregation_function(data)


class Max(AggregationFunction):
    def __init__(self, attribute: str):
        super().__init__(max, attribute)


class Min(AggregationFunction):
    def __init__(self, attribute: str):
        super().__init__(min, attribute)


class Count(AggregationFunction):
    def __init__(self, attribute: str | None = None):
        super().__init__(len, attribute)

    def __call__(self, rf: RF) -> int:
        """Count tuples (``Count()``) or mapped values (``Count("attr")``).

        Unlike the other aggregates, Count never returns ``_ABSENT``: zero is a
        valid count. ``Count("attr")`` counts only tuples that map the attribute
        (like SQL ``COUNT(col)``); ``Count()`` counts all tuples (``COUNT(*)``).
        """
        if self.attribute is None:
            return len(rf)
        return len(self._defined_values(rf))


class Sum(AggregationFunction):
    def __init__(self, attribute: str):
        super().__init__(sum, attribute)


class Avg(AggregationFunction):
    def __init__(self, attribute: str):
        super().__init__(len, attribute)

    def __call__(self, rf: RF) -> Any:
        data: list = self._defined_values(rf)
        # here we draw the values only once, whereas if we used Sum and Count, we would draw the values twice, once
        # for Sum and once for Count. So this is more efficient.
        if not data:  # no mapped values -> average undefined -> absent from output
            return _ABSENT
        return sum(data) / len(data)


class Median(AggregationFunction):
    def __init__(self, attribute: str):
        super().__init__(len, attribute)

    def __call__(self, rf: RF) -> Any:
        data: list = sorted(self._defined_values(rf))
        n = len(data)
        if n == 0:  # no mapped values -> median undefined -> absent from output
            return _ABSENT
        elif n % 2 == 1:
            return data[n // 2]
        else:
            return (data[n // 2 - 1] + data[n // 2]) / 2


class Mean(Avg):
    """Synonym for Avg."""

    pass


class aggregate(Operator[RF, TF]):
    """Aggregate an input RF using the specified aggregation functions."""

    def __init__(self, input_function: OperatorInput[RF], **aggregates):
        self.input_function = input_function
        self.aggregates = aggregates

    def _compute(self) -> TF:
        input_function = self._resolve_input(self.input_function)
        output_function = TF(frozen=False)  # no freezing as this produces a simple TF
        for key, aggregation in self.aggregates.items():
            result: Any = aggregation(input_function)
            if result is not _ABSENT:  # undefined aggregate -> no mapping in output
                output_function[key] = result
        return output_function


class 𝜞(aggregate):
    """Synonym for aggregate operator."""

    pass

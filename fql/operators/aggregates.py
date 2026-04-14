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


class AggregationFunction:
    def __init__(self, aggregation_function: Callable[[Any], Any], attribute: str):
        self.aggregation_function = aggregation_function
        self.attribute = attribute

    def __call__(self, rf: RF) -> Any:
        return self.aggregation_function([i.value[self.attribute] for i in rf])


class Max(AggregationFunction):
    def __init__(self, attribute: str):
        super().__init__(max, attribute)


class Min(AggregationFunction):
    def __init__(self, attribute: str):
        super().__init__(min, attribute)


class Count(AggregationFunction):
    def __init__(self, attribute: str):
        super().__init__(len, attribute)


class Sum(AggregationFunction):
    def __init__(self, attribute: str):
        super().__init__(sum, attribute)


class Avg(AggregationFunction):
    def __init__(self, attribute: str):
        super().__init__(len, attribute)

    def __call__(self, rf: RF) -> Any:
        data: list = [i.value[self.attribute] for i in rf]
        # here we draw the values only once, whereas if we used Sum and Count, we would draw the values twice, once
        # for Sum and once for Count. So this is more efficient.
        return sum(data) / len(data) if len(data) > 0 else None


class Median(AggregationFunction):
    def __init__(self, attribute: str):
        super().__init__(len, attribute)

    def __call__(self, rf: RF) -> Any:
        data: list = sorted([i.value[self.attribute] for i in rf])
        n = len(data)
        if n == 0:
            raise ValueError("Cannot compute median of empty data")
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
        output_function = TF(frozen=False)
        for key, value in self.aggregates.items():
            output_function[key] = value(input_function)
        output_function.freeze()
        return output_function


class 𝜞(aggregate):
    """Synonym for aggregate operator."""

    pass

from typing import Callable, Any

from fdm.attribute_functions import RF


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

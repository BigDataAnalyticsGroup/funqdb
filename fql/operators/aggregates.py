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
    """Aggregates an RF to a single value, optionally over one attribute.

    The built-in subclasses (``Count``, ``Sum``, ...) also serialize to the
    plan IR via ``to_dict`` / ``from_dict``; user-built ones stay ``Opaque``.
    """

    #: Operator name in the plan IR (e.g. ``"sum"``). ``None`` for user-built
    #: aggregations, whose behaviour is an arbitrary callable.
    op_name: str | None = None

    def __init__(
        self, aggregation_function: Callable[[Any], Any], attribute: str | None = None
    ):
        self.aggregation_function = aggregation_function
        self.attribute = attribute

    def __repr__(self) -> str:
        """Readable form for ``explain()`` and ``Opaque`` markers.

        Built-in aggregations and their user subclasses render as ``Sum('a')``
        or ``Count()``: built-ins like ``Avg`` store only a placeholder function
        (``len``) and compute in ``__call__``, so showing it would mislead.
        ``AggregationFunction(my_fn, 'a')`` and user subclasses outside the
        built-ins additionally show the wrapped function before the attribute,
        since that function is what makes them opaque. Attributes are read
        defensively: a user subclass may skip ``super().__init__``.
        """
        arguments: list[str] = []
        # isinstance, not exact type (unlike ``is_serializable``): user
        # subclasses of built-ins also hide the placeholder function.
        if not isinstance(self, tuple(_SERIALIZABLE_AGGREGATIONS)):
            function: Any = getattr(self, "aggregation_function", None)
            if function is not None:
                arguments.append(getattr(function, "__name__", repr(function)))
        attribute: str | None = getattr(self, "attribute", None)
        if attribute is not None:
            arguments.append(repr(attribute))
        return f"{type(self).__name__}({', '.join(arguments)})"

    def is_serializable(self) -> bool:
        """Whether this aggregation can be written into the plan IR.

        True only for the exact built-in classes listed in
        ``_SERIALIZABLE_AGGREGATIONS`` (end of this module) with a valid
        attribute: a string, or ``None`` for ``Count()`` only. For those, op
        name and attribute fully describe the behaviour. User subclasses,
        ``AggregationFunction(custom_function, ...)`` and built-ins with an
        invalid attribute (e.g. ``Sum(None)``) return False.
        """
        if type(self) not in _SERIALIZABLE_AGGREGATIONS:
            return False
        # Same rule as ``from_dict``, so that every written plan can be read
        # back: a string attribute, or none at all for ``Count()``.
        return isinstance(self.attribute, str) or (
            self.attribute is None and type(self) is Count
        )

    def to_dict(self) -> dict:
        """Serialize this aggregation to a plain dict for the plan IR.

        @return: ``{"type": "aggregation", "op": ..., "attr": ...}``; ``attr`` is
            ``None`` for ``Count()``.
        @raises ValueError: If this is not a built-in aggregation or its
            attribute is invalid (see ``is_serializable``).
        """
        if not self.is_serializable():
            raise ValueError(
                f"{self!r} is not serializable: not a built-in aggregation,"
                " or invalid attribute"
            )
        return {"type": "aggregation", "op": self.op_name, "attr": self.attribute}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AggregationFunction":
        """Deserialize an aggregation from a dict produced by ``to_dict``.

        @param data: A dict with ``"type": "aggregation"``, ``"op"`` and ``"attr"``.
        @return: A new instance of the built-in class registered for ``"op"``.
        @raises ValueError: If ``"op"`` is missing or unknown, or ``"attr"`` is
            not a string. A ``None`` or missing ``"attr"`` is accepted for
            ``count`` only (``Count()`` counts all tuples).
        """
        op: Any = data.get("op")
        attribute: Any = data.get("attr")
        aggregation_cls: Callable[[str | None], AggregationFunction] | None = (
            _AGGREGATIONS_BY_OP.get(op) if isinstance(op, str) else None
        )
        if aggregation_cls is None:
            raise ValueError(f"Unknown aggregation op: {op!r}")
        if attribute is None and aggregation_cls is not Count:
            raise ValueError(f"Aggregation {op!r} requires an attribute")
        if attribute is not None and not isinstance(attribute, str):
            raise ValueError(f"Aggregation attribute must be a string: {attribute!r}")
        return aggregation_cls(attribute)

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
    op_name: str = "max"

    def __init__(self, attribute: str):
        super().__init__(max, attribute)


class Min(AggregationFunction):
    op_name: str = "min"

    def __init__(self, attribute: str):
        super().__init__(min, attribute)


class Count(AggregationFunction):
    op_name: str = "count"

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
    op_name: str = "sum"

    def __init__(self, attribute: str):
        super().__init__(sum, attribute)


class Avg(AggregationFunction):
    op_name: str = "avg"

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
    op_name: str = "median"

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
    """Synonym for Avg. Serializes to the plan IR as ``"avg"`` and therefore
    comes back as ``Avg``."""

    pass


#: Built-in aggregation classes that serialize to the plan IR. Exact types only:
#: a user subclass may override ``__call__``, so it stays Opaque.
_SERIALIZABLE_AGGREGATIONS: frozenset[type[AggregationFunction]] = frozenset(
    {Max, Min, Count, Sum, Avg, Median, Mean}
)

#: Maps an op name back to the class that restores it. ``Mean`` shares the op
#: name ``"avg"`` and therefore restores as ``Avg``.
_AGGREGATIONS_BY_OP: dict[str, Callable[[str | None], AggregationFunction]] = {
    "max": Max,
    "min": Min,
    "count": Count,
    "sum": Sum,
    "avg": Avg,
    "median": Median,
}


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

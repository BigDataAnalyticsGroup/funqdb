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


from collections.abc import Mapping
from typing import Callable, Any, Iterable

from fql.operators.APIs import Operator, OperatorInput
from fql.predicates.predicates import Predicate
from fql.util import Item


import logging

logger = logging.Logger(__name__)


class filter_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Logical filter operator filtering the items of an attribute function based on a given predicate."""

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        filter_predicate: Callable[..., Any],
        *,
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
        create_lineage=False,
    ):
        """Initialize the filter_items operator.
        @param input_function: The input attribute function to filter.
        @param filter_predicate: A predicate that takes an Item and returns True if the item should be kept, False otherwise.
        @param output_factory: This factory function will be used to create the output instance.
        @param create_lineage: If True, create lineage information (not yet implemented).
        """

        self.input_function = input_function
        self.filter_predicate = filter_predicate
        self.output_factory = output_factory
        self._create_lineage = create_lineage

    def _compute(self) -> OUTPUT_AttributeFunction:

        if self._create_lineage:
            raise NotImplementedError()

        input_function = self._resolve_input(self.input_function)
        assert input_function is not None

        # get the filtered items:
        mapped_items: Iterable[Item] = filter(self.filter_predicate, input_function)

        if self.output_factory is None:
            # use same type as input function if no output factory is provided:
            output_function = type(input_function)()
        else:
            output_function = self.output_factory(None)

        output_function.unfreeze()

        # (1.) we need to materialize the items first to avoid modifying while iterating
        buffer = {item.key: item.value for item in mapped_items if item is not None}

        # (2.) enter values in output_function:
        for key, value in buffer.items():
            output_function[key] = value

        output_function.freeze()
        return output_function


class filter_values[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    filter_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that filters the __values__ found in the input instance. Hence, the predicate may
    be phrased directly on the values of the items, e.g., lambda v: v.department.name == "Dev".
    This is a more intuitive way to filter items based on their values. The filter_items operator can be implemented in
    terms of this operator by using a predicate that takes an Item and applies the filter predicate to the value of the item.
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        filter_predicate: Callable[..., Any] | Predicate,
        *,
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        self._user_predicate = filter_predicate
        super().__init__(
            input_function,
            lambda i: filter_predicate(i.value),
            output_factory=output_factory,
        )

    def _plan_params(self) -> Mapping[str, Any]:
        """Expose the original user predicate instead of the Item-wrapping lambda."""
        params: dict[str, Any] = dict(super()._plan_params())
        params["filter_predicate"] = self._user_predicate
        return params


class filter_keys[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    filter_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that filters the __keys__ found in the input instance. Hence, the predicate may
    be phrased directly on the keys of the items, e.g., lambda k: k.startswith("user_").
    This is a more intuitive way to filter items based on their keys. The filter_items operator can be implemented in
    terms of this operator by using a predicate that takes an Item and applies the filter predicate to the key of the item.
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        filter_predicate: Callable[..., Any] | Predicate,
        *,
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        self._user_predicate = filter_predicate
        super().__init__(
            input_function,
            lambda i: filter_predicate(i.key),
            output_factory=output_factory,
        )

    def _plan_params(self) -> Mapping[str, Any]:
        """Expose the original user predicate instead of the Item-wrapping lambda."""
        params: dict[str, Any] = dict(super()._plan_params())
        params["filter_predicate"] = self._user_predicate
        return params


class filter_items_scan_complement[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    filter_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Computes the complement of the filter_values operator."""

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        filter_predicate: Callable[..., Any] | Predicate,
        *,
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the filter_items_scan_complement operator.
        @param input_function: The input attribute function to filter.
        @param filter_predicate: A predicate that takes an Item and returns True if the item should be filtered out,
        False otherwise.
        @param output_factory: This factory function will be used to create the output instance.
        """
        self._user_predicate = filter_predicate
        super().__init__(
            input_function,
            lambda x: not filter_predicate(x),
            output_factory=output_factory,
        )

    def _plan_params(self) -> Mapping[str, Any]:
        """Expose the original user predicate instead of the negation-wrapping lambda."""
        params: dict[str, Any] = dict(super()._plan_params())
        params["filter_predicate"] = self._user_predicate
        return params

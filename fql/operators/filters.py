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


import inspect
from typing import Callable, Any, Iterable

from fql.operators.APIs import Operator
from fql.util import Item


import logging

logger = logging.Logger(__name__)


class filter_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Logical filter operator filtering the items of an attribute function based on a given predicate."""

    def __init__(
        self,
        filter_predicate: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the filter_values operator.
        @param filter_predicate: A predicate that takes an Item and returns True if the item should be kept, False otherwise.
        @param output_factory: This factory function will be used to create the output instance.
        """

        self.filter_predicate = filter_predicate
        self.output_factory = output_factory

    def __call__(
        self, input_function: INPUT_AttributeFunction, create_lineage=False
    ) -> OUTPUT_AttributeFunction | str:

        if not create_lineage:
            assert input_function is not None
            # TODO: refactor to avoid code duplication with filter_values,
            # filter_values() and filter_key() should call this operator with the appropriate filter_predicate:
            return filter_values(self.filter_predicate, self.output_factory)(
                input_function
            )
        else:  # execute on db:
            # create lineage without executing anything
            output_function: OUTPUT_AttributeFunction = self.output_factory(None)
            output_function.__dict__[
                "lineage"
            ] += input_function.get_lineage()  # inherit lineage
            output_function.add_lineage(
                f"FILTER_ITEMS({inspect.getsource(self.filter_predicate).strip()})"
            )
            return output_function


class filter_values[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    filter_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that filters the __values__ found in the input instance and not the items. Hence, the predicate may
    be phrased directly on the values of the items, e.g., lambda v: v.department.name == "Dev".
    This is a more intuitive way to filter items based on their values. The filter_items operator can be implemented in
    terms of this operator by using a predicate that takes an Item and applies the filter predicate to the value of the item.
    """

    def __init__(
        self,
        filter_predicate: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        # wrap the filter_predicate to apply it to the value of the item:
        super().__init__(filter_predicate, output_factory)

        # goal:
        # super().__init__(lambda i: filter_predicate(i.v), output_factory)

    def explain(self) -> str:
        """Explains the filter."""
        return f"filter_values operator with predicate {self.filter_predicate}."

    def __call__(
        self, input_function: INPUT_AttributeFunction, create_lineage=False
    ) -> OUTPUT_AttributeFunction:

        assert input_function is not None
        # get the filtered items:
        mapped_items: Iterable[Item] = filter(self.filter_predicate, input_function)

        output_function = input_function
        if self.output_factory is not None:
            output_function = self.output_factory(None)
            output_function.unfreeze()
        else:
            logger.warning(
                "No output function factory provided; modifying input function in place. This is not recommended as it"
                " may have side effects on the input."
            )

        # (1.) we need to materialize the items first to avoid modifying while iterating
        buffer = {item.key: item.value for item in mapped_items if item is not None}

        # (2.) enter values in output_function:
        for key, value in buffer.items():
            output_function[key] = value

        output_function.freeze()

        return output_function


class filter_items_scan_complement[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    filter_values[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Computes the complement of the filter_values operator."""

    def __init__(
        self,
        filter_predicate: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the filter_items_scan_complement operator.
        @param filter_predicate: A predicate that takes an Item and returns True if the item should be filtered out,
        False otherwise.
        @param output_factory: This factory function will be used to create the output instance.
        """
        super().__init__(filter_predicate, output_factory)

    def __call__(
        self, input_function: INPUT_AttributeFunction, create_lineage=False
    ) -> OUTPUT_AttributeFunction:
        """Call the filter_values operator with the negated predicate."""
        super.__call__(lambda x: not self.filter_predicate)

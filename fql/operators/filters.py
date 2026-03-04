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
    """Logical filter operator"""

    def __init__(
        self,
        filter_predicate: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the filter_items_scan operator.
        @param filter_predicate: A predicate that takes an Item and returns True if the item should be kept, False otherwise.
        @param output_factory: This factory function will be used to create the output instance.
        """

        self.filter_predicate = filter_predicate
        self.output_factory = output_factory

    def __call__(
        self, input_function: INPUT_AttributeFunction, create_lineage=False
    ) -> OUTPUT_AttributeFunction | str:

        if not create_lineage:
            # standard python filter operation returning list of values
            assert input_function is not None
            return filter_items_scan(self.filter_predicate, self.output_factory)(
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


class filter_items_scan[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    filter_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that filters the values found in the input instance. In contrast to standard filter operations
    returning a set or list of the filtered items, this operator stays in the data model and returns an Attribute
    Function with the qualifying elements as its output.
    """

    def __init__(
        self,
        filter_predicate: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        super().__init__(filter_predicate, output_factory)

    def explain(self) -> str:
        """Explains the filter."""
        return f"filter_items_scan operator with predicate {self.filter_predicate}."

    def __call__(
        self, input_function: INPUT_AttributeFunction, create_lineage=False
    ) -> OUTPUT_AttributeFunction:

        assert input_function is not None
        # get the mapped items:
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
    filter_items_scan[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Computes the complement of the filter_items_scan operator."""

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
        """Call the filter_items_scan operator with the negated predicate."""
        super.__call__(lambda x: not self.filter_predicate)

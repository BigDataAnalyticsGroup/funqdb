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


from typing import Callable, Any, Iterable

from fql.operators.APIs import Operator
from fql.util import Item

import logging

logger = logging.Logger(__name__)


class transform[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that transforms an input instance to an output instance."""

    def __init__(self, input_function: INPUT_AttributeFunction, *, transformation_function: Callable[..., Any]):
        self.input_function = input_function
        self.transformation_function = transformation_function

    def _compute(self) -> OUTPUT_AttributeFunction:
        return self.transformation_function(self._resolve_input(self.input_function))


class transform_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that transforms the input instance by mapping its items.
    The modified input instance will be returned as the output."""

    def __init__(
        self,
        input_function: INPUT_AttributeFunction,
        *,
        transformation_function: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the transform_items operator.
        @param input_function: The input attribute function to transform.
        @param transformation_function: A function that takes an Item and returns a transformed Item or None
        @param output_factory: If set, this factory function will be used to create the output instance.
        """

        self.input_function = input_function
        self.mapping_function = transformation_function
        self.output_factory = output_factory

    def _compute(self) -> OUTPUT_AttributeFunction:
        input_function = self._resolve_input(self.input_function)

        # get the mapped items:
        mapped_items: Iterable[Item] = map(self.mapping_function, input_function)

        output_function = input_function
        if self.output_factory is not None:
            output_function = self.output_factory(None)
            output_function.unfreeze()
        else:
            logger.warning(
                "No output function factory provided; modifying input function in place. This is not recommended as it"
                " may have sideeffect on the input."
            )

        # (1.) we need to materialize the items first to avoid modifying while iterating
        # TODO: discuss, really needed?
        # TODO: shall we still support inplace modifications?
        buffer = {item.key: item.value for item in mapped_items if item is not None}

        # (2.) enter key,values in output_function:
        for key, value in buffer.items():
            output_function[key] = value

        output_function.freeze()

        return output_function


# TODO: do we need a transform_values operator?

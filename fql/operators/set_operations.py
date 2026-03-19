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

from typing import Callable

from fql.operators.APIs import Operator


class union[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Union n>=2 attribute functions."""

    def __init__(
        self,
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
        warn_about_duplicate_keys: bool = True,
    ):
        assert (
            output_factory is not None
        ), "An output factory must be provided for the generic union operator."

        self.output_factory = output_factory
        self.warn_about_duplicate_keys = warn_about_duplicate_keys

    def __call__(self, *args) -> OUTPUT_AttributeFunction:

        assert (
            len(args) >= 2
        ), "At least two input attribute functions must be provided for the union operator."

        # get result instance:
        output_function: OUTPUT_AttributeFunction = self.output_factory(None)

        for input_function in args:
            # add all items from the input function to the output function (overwriting existing items with the same key):
            for item in input_function:
                if self.warn_about_duplicate_keys:
                    if item.key in output_function:
                        print(
                            f"Warning: Duplicate key {item.key} found for input_function {input_function.uuid}. Overwriting existing value."
                        )
                        # TODO: what should be the semantics here? Conceptually, this could be done like the partition operator,
                        # for an incoming DBF return a DBF mapping from key to an RF with all duplicates, but this
                        # would be a very different operator than the union operator, which is more of a set operation.
                        # For now, we will just overwrite existing values and print a warning.
                output_function[item.key] = item.value

        return output_function


class V[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    union[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Alias for union operator. Set symbols are not possible for class names in Python, but we can use the letter V as
    a visual approximation of the union symbol ∪. It also resembles a logical OR which is in spirit what union is about.
    """

    pass


class intersect[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Intersect n>=2 attribute functions."""

    def __init__(
        self,
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        assert (
            output_factory is not None
        ), "An output factory must be provided for the generic intersect operator."

        self.output_factory = output_factory

    def __call__(self, *args) -> OUTPUT_AttributeFunction:

        assert (
            len(args) >= 2
        ), "At least two input attribute functions must be provided for the intersect operator."

        # get result instance:
        output_function: OUTPUT_AttributeFunction = self.output_factory(None)

        first: bool = True
        for input_function in args:
            # add all items from the input function to the output function (overwriting existing items with the same key):
            if first:
                # first af treated:
                first = False
                for item in input_function:
                    output_function[item.key] = item.value
            else:
                # second and subsequent afs treated: only keep items that are in the output function, but do not add new items:
                for item in output_function:
                    if item.key not in input_function:
                        del output_function[item.key]

                # LOL: this means that the output function will only contain keys that are in all input functions, but
                # the values will be from the first input function, which is a bit weird
        return output_function


class Ʌ[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    intersect[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Alias for intersect operator. Set symbols are not possible for class names in Python, but we can use the letter
    Ʌ as a visual approximation of the intersect symbol ∩. It also resembles a logical AND which is in spirit what
    intersect is about.
    """

    pass


class minus[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """minus n>=2 attribute functions."""

    def __init__(
        self,
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        assert (
            output_factory is not None
        ), "An output factory must be provided for the generic minus operator."

        self.output_factory = output_factory

    def __call__(self, *args) -> OUTPUT_AttributeFunction:

        assert (
            len(args) >= 2
        ), "At least two input attribute functions must be provided for the minus operator."

        # get result instance:
        output_function: OUTPUT_AttributeFunction = self.output_factory(None)

        first: bool = True
        for input_function in args:
            # add all items from the input function to the output function (overwriting existing items with the same key):
            if first:
                # first af treated:
                first = False
                for item in input_function:
                    output_function[item.key] = item.value
            else:
                # second and subsequent afs treated: only keep items that are in the output function, but do not add new items:
                for item in input_function:
                    if item.key in output_function:
                        del output_function[item.key]

                # LOL: this means that the output function will only contain keys that are in all input functions, but
                # the values will be from the first input function, which is a bit weird

        return output_function


class difference[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    minus[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Alias for minus operator. Except alias is not possible as it is a reserved keyword in Python."""

    pass

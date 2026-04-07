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

from fdm.API import AttributeFunction
from fql.operators.APIs import Operator
from fql.util import Item


class union[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Union n>=2 attribute functions based on the AF's keys.
    Input is a single DBF containing the relations to union."""

    def __init__(
        self,
        input_function: INPUT_AttributeFunction,
        *,
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
        warn_about_duplicate_keys: bool = True,
    ):
        assert (
            output_factory is not None
        ), "An output factory must be provided for the generic union operator."

        self.input_function = input_function
        self.output_factory = output_factory
        self.warn_about_duplicate_keys = warn_about_duplicate_keys

    def _compute(self) -> OUTPUT_AttributeFunction:
        input_dbf = self._resolve_input(self.input_function)

        assert (
            len(input_dbf) >= 2
        ), "At least two input attribute functions must be provided for the union operator."

        # get result instance:
        output_function: OUTPUT_AttributeFunction = self.output_factory(None)

        for relation in input_dbf:
            input_function = relation.value
            # add all items from the input function to the output function (overwriting existing items with the same key):
            item: Item
            for item in input_function:
                if self.warn_about_duplicate_keys:
                    if item.key in output_function:
                        print(
                            f"Warning: Duplicate key {item.key} found for input_function {input_function.uuid}. Overwriting existing value."
                        )
                        # TODO: what should be the semantics here? Conceptually, this could be done like the co-group
                        #  operator,
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


class cogroup[
    INPUT_AttributeFunction, OUTPUT_AttributeFunction, OUTPUT_AttributeFunction_Nested
](
    Operator[
        INPUT_AttributeFunction,
        OUTPUT_AttributeFunction,
    ]
):
    """Co-group n>=2 attribute functions based on the AF's keys. This can naturally be extended to a classical join
    operation if the grouping keys can be customized to reflect the attributes used in an equi join predicate.
    Input is a single DBF containing the relations to co-group.
    """

    def __init__(
        self,
        input_function: INPUT_AttributeFunction,
        *,
        output_factory: Callable[..., OUTPUT_AttributeFunction],
        output_factory_nested: Callable[..., OUTPUT_AttributeFunction_Nested],
    ):
        assert (
            output_factory is not None
        ), "An output factory must be provided for the generic cogroup operator."

        assert (
            output_factory_nested is not None
        ), "An output factory must be provided for the nested AFs in the output."

        self.input_function = input_function
        self.output_factory = output_factory
        self.output_factory_nested = output_factory_nested

    def _compute(self) -> OUTPUT_AttributeFunction:
        input_dbf = self._resolve_input(self.input_function)

        # TODO: revisit this assert as len(input_dbf)==1 corresponds to the standard grouping
        assert (
            len(input_dbf) >= 2
        ), "At least two input attribute functions must be provided for the cogroup operator."

        # get result instance:
        output_function: OUTPUT_AttributeFunction = self.output_factory(None)

        input_function: AttributeFunction
        for relation in input_dbf:
            input_function = relation.value
            # add a key/value mapping from the input function's key to a nested AF
            # that nested AF has a key/value-mapping from the input function's uudid to the input function's value for
            # that key
            item: Item
            for item in input_function:
                if item.key not in output_function:
                    output_function[item.key] = self.output_factory_nested(None)

                output_function[item.key][input_function.uuid] = item.value

        return output_function


class intersect[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Intersect n>=2 attribute functions based on the AF's keys.
    Input is a single DBF containing the relations to intersect."""

    def __init__(
        self,
        input_function: INPUT_AttributeFunction,
        *,
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        assert (
            output_factory is not None
        ), "An output factory must be provided for the generic intersect operator."

        self.input_function = input_function
        self.output_factory = output_factory

    def _compute(self) -> OUTPUT_AttributeFunction:
        input_dbf = self._resolve_input(self.input_function)

        assert (
            len(input_dbf) >= 2
        ), "At least two input attribute functions must be provided for the intersect operator."

        # get result instance:
        output_function: OUTPUT_AttributeFunction = self.output_factory(None)

        first: bool = True
        for relation in input_dbf:
            input_function = relation.value
            # add all items from the input function to the output function (overwriting existing items with the same key):
            if first:
                # first af treated:
                first = False
                item: Item
                for item in input_function:
                    output_function[item.key] = item.value
            else:
                # second and subsequent afs treated: only keep items whose key also appears in the current AF.
                # collect keys to delete first to avoid modifying the dict during iteration:
                keys_to_delete: list = [
                    item.key
                    for item in output_function
                    if item.key not in input_function
                ]
                for key in keys_to_delete:
                    del output_function[key]

        return output_function


class Ʌ[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    intersect[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Alias for intersect operator. Classical set symbols are not possible for class names in Python, but we can use
    the letter Ʌ as a visual approximation of the intersect symbol ∩. It also resembles a logical AND which is in
    spirit what intersect is about.
    """

    pass


class minus[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """minus n>=2 attribute functions based on the AF's keys.
    Input is a single DBF containing the relations to subtract."""

    def __init__(
        self,
        input_function: INPUT_AttributeFunction,
        *,
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        assert (
            output_factory is not None
        ), "An output factory must be provided for the generic minus operator."

        self.input_function = input_function
        self.output_factory = output_factory

    def _compute(self) -> OUTPUT_AttributeFunction:
        input_dbf = self._resolve_input(self.input_function)

        assert (
            len(input_dbf) >= 2
        ), "At least two input attribute functions must be provided for the minus operator."

        # get result instance:
        output_function: OUTPUT_AttributeFunction = self.output_factory(None)

        first: bool = True
        for relation in input_dbf:
            input_function = relation.value
            # add all items from the input function to the output function (overwriting existing items with the same key):
            if first:
                # first af treated:
                first = False
                item: Item
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

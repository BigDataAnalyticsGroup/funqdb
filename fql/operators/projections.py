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

from fql.operators.APIs import OperatorInput
from fql.operators.transforms import transform


class project[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    transform[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """A projection operator. Implemented as a transform operator that deletes all items from the input function that
    are not in the given set of attributes. Note that if you project to an attribute that does not exist in the input
    function, the output function will simply not contain that attribute, instead of throwing an error. This is because
    in FDM and FQL, missing attributes are simply not present in the attribute function, rather than being present with
    a null value.
    """

    def __init__(
        self, input_function: OperatorInput[INPUT_AttributeFunction], *attributes: str
    ):
        """Initialize the project operator with the given set of attributes to project to.
        @param input_function: The input attribute function to project.
        @param attributes: The set of attributes to project to.
        """
        super().__init__(
            input_function,
            # redirect the transformation function to the projection function:
            transformation_function=lambda af: af.project(*attributes),
        )

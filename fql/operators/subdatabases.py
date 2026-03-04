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

from fdm.attribute_functions import TF, RF, DBF
from fql.operators.APIs import Operator
from fql.operators.filters import filter_items_scan


class subdatabase[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Compute the subdatabase defined by the join predicate.
    Currently limited to nested loop joins. However, this implementation ALL join predicate as it treats these as black
    boxes (same effect as for traditional join operators). In order to be more efficient, we have to whitebox the
    join predicate and implement specialized join algorithms for typical predicates (e.g., equi-joins
    exploiting hash-joins or sort-merge-joins).

    Currently limited to a DB with two inputs only to simulate a standard SQL join operator
    """

    def __init__(
        self,
        join_predicate: Callable[..., Any],
        left: str | None = None,
        right: str | None = None,
        create_join_index: bool = False,
        keep_values_in_join_index: bool = False,
    ):
        self.join_predicate = join_predicate
        self.left = left
        self.right = right
        self.create_join_index = create_join_index
        self.keep_values_in_join_index = keep_values_in_join_index

    def __call__(
        self, input_function: INPUT_AttributeFunction
    ) -> OUTPUT_AttributeFunction:
        # brute force nested loop to start with,
        # TODO: optimize later to use standard DB subdatabase algorithms
        # TODO: implement typical join operators exploiting special predicates
        assert (
            len(input_function) == 2
        ), "Currently only two relations supported in subdatabase."

        left_RF: RF = input_function[self.left]
        right_RF: RF = input_function[self.right]

        left_qualifying_items = set()
        right_qualifying_items = set()

        join_index: RF | None = None
        # optional join index creation:
        if self.create_join_index:
            join_index = RF(frozen=False)

        # TODO: optimize nested loop join later
        no_results: int = 0
        for item_left in left_RF:
            for item_right in right_RF:
                if self.join_predicate(item_left, item_right):
                    # consider both elements as qualifying
                    # collect them in sets to avoid duplicates
                    left_qualifying_items.add(item_left.key)
                    right_qualifying_items.add(item_right.key)
                    if self.create_join_index:
                        d: dict = {
                            "left_key": item_left.key,
                            "right_key": item_right.key,
                        }
                        if self.keep_values_in_join_index:
                            d.update(
                                {
                                    "left_value": item_left.value,
                                    "right_value": item_right.value,
                                }
                            )
                        join_index[no_results] = TF(d)
                    no_results += 1
        if self.create_join_index:
            join_index.freeze()

        # create a reduced output database:
        output_DBF = DBF(frozen=False)

        # add reduced relations, delegated to filter_items_scan operator:
        # left relation:
        output_DBF[self.left] = filter_items_scan[RF, RF](
            lambda i: i.key in left_qualifying_items, lambda _: RF(frozen=False)
        )(left_RF)

        # right relation:
        output_DBF[self.right] = filter_items_scan[RF, RF](
            lambda i: i.key in right_qualifying_items, lambda _: RF(frozen=False)
        )(right_RF)

        # join index:
        if self.create_join_index:
            output_DBF["join_index"] = join_index

        output_DBF.freeze()

        return output_DBF

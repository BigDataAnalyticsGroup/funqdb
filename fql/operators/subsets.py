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

from fql.operators.APIs import Operator, OperatorInput
from fql.util import Item


class subset[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Compute a subset of an AF based on a global condition.

    In contrast to filter (which evaluates a predicate per item independently), subset needs access
    to ALL items to decide which ones survive. The canonical example is top-k: "give me the k items
    with the smallest/largest value" — this cannot be decided per item in isolation.

    Two modes of operation (mutually exclusive):

    1. Declarative top-k: provide ranking_key and k (and optionally reverse).
       Returns the k items with the smallest ranking_key values (or largest if reverse=True).

    2. Generic subset: provide subset_predicate, a function that receives the entire input AF
       and returns a new AF containing only the qualifying items. This covers arbitrary global
       conditions like "all items above the mean".
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        *,
        ranking_key: Callable[[Item], Any] = None,
        k: int = None,
        reverse: bool = False,
        subset_predicate: Callable[..., "OUTPUT_AttributeFunction"] = None,
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the subset operator.
        @param input_function: The input AF to compute a subset of.
        @param ranking_key: A function that maps an Item to a comparable value used for sorting.
        @param k: The number of items to keep (top-k). Required when ranking_key is provided.
        @param reverse: If True, keep the k largest instead of the k smallest. Only used with ranking_key.
        @param subset_predicate: A function that takes the entire input AF and returns a subset AF.
            Mutually exclusive with ranking_key/k.
        @param output_factory: Factory to create the output AF. If None, uses the same type as input.
        """
        assert (ranking_key is not None) != (
            subset_predicate is not None
        ), "Provide either ranking_key+k or subset_predicate, not both (and not neither)."

        if ranking_key is not None:
            assert k is not None and k >= 1, "k must be >= 1 when using ranking_key."

        self.input_function = input_function
        self.ranking_key = ranking_key
        self.k = k
        self.reverse = reverse
        self.subset_predicate = subset_predicate
        self.output_factory = output_factory

    def _compute(self) -> OUTPUT_AttributeFunction:
        input_function = self._resolve_input(self.input_function)
        assert input_function is not None

        if self.subset_predicate is not None:
            # generic mode: let the predicate compute the subset directly
            return self.subset_predicate(input_function)

        # declarative top-k mode:
        # sort all items by ranking_key, then keep the first k
        sorted_items: list[Item] = sorted(
            input_function, key=self.ranking_key, reverse=self.reverse
        )
        top_k_items: list[Item] = sorted_items[: self.k]

        # build output AF:
        if self.output_factory is None:
            output_function = type(input_function)()
        else:
            output_function = self.output_factory(None)

        output_function.unfreeze()
        for item in top_k_items:
            output_function[item.key] = item.value
        output_function.freeze()

        return output_function

from typing import Callable, Any

from fql.functions import DBF, RF, TF
from fql.operators.APIs import Operator
from fql.operators.subdatabases import subdatabase
from fql.util import Item


class join[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
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
    ):
        self.join_predicate = join_predicate
        self.left = left
        self.right = right

    def __call__(
        self, input_function: INPUT_AttributeFunction
    ) -> OUTPUT_AttributeFunction:
        # brute force nested loop to start with,
        # TODO: optimize later to use standard DB subdatabase algorithms
        # TODO: implement typical join operators exploiting special predicates
        reduced_DBF: DBF = subdatabase[DBF, DBF](
            lambda item_left, item_right: item_left.value.name == item_right.value.name,
            self.left,
            self.right,
            create_join_index=True,
            keep_values_in_join_index=True,
        )(input_function)

        join_index: RF = reduced_DBF.join_index
        result_RF: RF = RF(frozen=False)

        # flatten the joined relations into a single output relation:
        # whatever sense that makes is another question as the join index already contains the info
        # this is basically a from of tuple reconstruction
        item: Item
        no_results: int = 0
        for item in join_index:
            # get a new writable tf:
            result_TF = TF(frozen=False)
            # add entries from left and right value:
            result_TF.update(item.value.left_value)
            result_TF.update(item.value.right_value)
            # freeze tf and add to rf:
            result_TF.freeze()
            result_RF[no_results] = result_TF
            no_results += 1

        result_RF.freeze()

        return result_RF

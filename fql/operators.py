import logging
from typing import Callable, Any, Iterable

from fql.APIs import PureFunction, AttributeFunction
from fql.functions import RF, DBF, TF
from fql.util import Item

logger = logging.Logger(__name__)


class Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    PureFunction[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Signature for an operator that transforms inputs to outputs."""

    pass


class map_instance[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that maps an input instance to an output instance."""

    def __init__(self, mapping_function: Callable[..., Any]):
        self.mapping_function = mapping_function

    def __call__(
        self, input_function: INPUT_AttributeFunction
    ) -> OUTPUT_AttributeFunction:
        """Make the object callable.
        @return: The result of the call.
        """
        return self.mapping_function(input_function)


class transform_values[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """An operator that transforms the input instance by mapping its values.
    The modified input instance will be returned as the output."""

    def __init__(
        self,
        transformation_function: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the transform_values operator.
        @param transformation_function: A function that takes an Item and returns a transformed Item or None
        @param output_factory: If set, this factory function will be used to create the output instance.
        """

        self.mapping_function = transformation_function
        self.output_factory = output_factory

    def __call__(
        self, input_function: INPUT_AttributeFunction
    ) -> OUTPUT_AttributeFunction:
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
        buffer = {item.key: item.value for item in mapped_items if item is not None}

        # (2.) enter values in output_function:
        for key, value in buffer.items():
            output_function[key] = value

        output_function.freeze()

        return output_function


class filter_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
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
        """Initialize the filter_items operator.
        @param filter_predicate: A predicate that takes an Item and returns True if the item should be kept, False otherwise.
        @param output_factory: This factory function will be used to create the output instance.
        """

        self.filter_predicate = filter_predicate
        self.output_factory = output_factory

    def __call__(
        self, input_function: INPUT_AttributeFunction
    ) -> OUTPUT_AttributeFunction:
        # get the mapped items:
        mapped_items: Iterable[Item] = filter(self.filter_predicate, input_function)

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
        buffer = {item.key: item.value for item in mapped_items if item is not None}

        # (2.) enter values in output_function:
        for key, value in buffer.items():
            output_function[key] = value

        output_function.freeze()

        return output_function


class filter_items_complement[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    filter_items[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Computes the complement of the filter_items operator."""

    def __init__(
        self,
        filter_predicate: Callable[..., Any],
        output_factory: Callable[..., OUTPUT_AttributeFunction] = None,
    ):
        """Initialize the filter_items_complement operator.
        @param filter_predicate: A predicate that takes an Item and returns True if the item should be filtered out,
        False otherwise.
        @param output_factory: This factory function will be used to create the output instance.
        """
        super().__init__(filter_predicate, output_factory)

    def __call__(
        self, input_function: INPUT_AttributeFunction
    ) -> OUTPUT_AttributeFunction:
        """Call the filter_items operator with the negated predicate."""
        super.__call__(lambda x: not self.filter_predicate)


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
        output_DBF_factory: Callable[..., DBF] = None,
        output_RF_factory: Callable[..., RF] = None,
        left: str | None = None,
        right: str | None = None,
        create_join_index: bool = False,
        keep_values_in_join_index: bool = False,
    ):
        self.join_predicate = join_predicate
        self.output_DBF_factory = output_DBF_factory
        self.output_RF_factory = output_RF_factory
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
            join_index = self.output_RF_factory(None)
            join_index.unfreeze()

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
        output_DBF = self.output_DBF_factory(None)
        output_DBF.unfreeze()

        # add reduced relations, delegated to filter_items operator:
        # left relation:
        output_DBF[self.left] = filter_items[RF, RF](
            lambda i: i.key in left_qualifying_items,
            lambda _: self.output_RF_factory(None),
        )(left_RF)

        # right relation:
        output_DBF[self.right] = filter_items[RF, RF](
            lambda i: i.key in right_qualifying_items,
            lambda _: self.output_RF_factory(None),
        )(right_RF)

        # join index:
        if self.create_join_index:
            output_DBF["join_index"] = join_index

        output_DBF.freeze()

        return output_DBF


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
        output_RF_factory: Callable[..., RF] = None,
        left: str | None = None,
        right: str | None = None,
    ):
        self.join_predicate = join_predicate
        self.output_RF_factory = output_RF_factory
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
            lambda _: DBF(),
            lambda _: RF(),
            self.left,
            self.right,
            create_join_index=True,
            keep_values_in_join_index=True,
        )(input_function)

        join_index: RF = reduced_DBF.join_index
        result_RF: RF = self.output_RF_factory(None)

        # flatten the joined relations into a single output relation:
        # whatever sense that makes is another question as the join index already contains the info
        # this is basically a from of tuple reconstruction
        item: Item
        no_results: int = 0
        for item in join_index:
            result_TF = TF()
            result_TF.unfreeze()
            result_TF.update(item.value.left_value)
            result_TF.update(item.value.right_value)
            result_TF.freeze()
            result_RF[no_results] = result_TF
            no_results += 1

        return result_RF

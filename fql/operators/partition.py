from typing import Callable, Any

from fdm.attribute_functions import RF, DBF
from fql.operators.APIs import Operator
from fql.util import Item


class partition(Operator[RF, DBF]):
    """Partition an input RF into a DBF with its partitions as RFs.

    #TODO: generic type: this operator can also partition an entire database...

    @param partitioning_function: A function that takes an Item and returns the partition key for that item.
    @param output_factory: If set, this factory function will be used to create the output DBF instance. If not set,
    a new DBF instance will be created.
    """

    def __init__(
        self,
        partitioning_function: Callable[[Item], Any],
        output_factory: Callable[..., DBF] = None,
    ):
        self.partitioning_function = partitioning_function
        if output_factory is None:
            self.output_factory = lambda _: DBF(frozen=False)

    def __call__(self, input_function: RF) -> DBF:
        output_function: DBF = self.output_factory(None)
        item: Item
        for item in input_function:
            partition_key = self.partitioning_function(item)
            if partition_key not in output_function:
                output_function[partition_key] = RF(frozen=False)
            output_function[partition_key][item.key] = item.value

        # freeze all RFs in the output DBF
        for item in output_function:
            item.value.freeze()

        output_function.freeze()
        return output_function


class group_by(partition):
    """Partitions an input RF into a DBF based on the equality of the given keys (the values mapped to by those keys).
    Thus, this operator simulates the traditional group-by in relational algebra and SQL. The partitioning function is
    automatically derived from the specified grouping keys (attributes)."""

    # TODO: generic type: this operator can also partition an entire database...

    def __init__(self, *aggregate_keys):
        super().__init__(
            # convert the grouping function to a partitioning function that returns a tuple of the grouping keys for
            # the specified attributes:
            lambda item: tuple(item.value[attribute] for attribute in aggregate_keys)
        )

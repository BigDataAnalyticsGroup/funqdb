from fdm.attribute_functions import DBF, RF, TF
from fql.operators.aggregates import aggregate, Min, Max, Count
from fql.operators.partition import partition, group_by
from fql.operators.partition_and_aggregate import (
    partition_by_aggregate,
    group_by_aggregate,
)
from fql.operators.transforms import transform_items
from fql.util import Item
from tests.lib import _create_testdata


def test_partitioning_and_group_by_composed_partitioning_key():
    db: DBF = _create_testdata(frozen=True)
    customers: RF = db.customers

    # partition the users relation into two RFs: those name Tom and those not named Tom:
    for i in range(2):
        partitions: DBF | None = None
        if i == 0:
            # generic partitioning based on a partitioning function:
            partitions = partition(customers, partitioning_function=lambda i: (i.value.name, i.value.company)).result
        else:
            # explicit group by building partitions based on equality of multiple attributes:
            partitions = group_by(customers, "name", "company").result
        assert len(partitions) == 4
        assert type(partitions) == DBF

        tom_whatever_partition: RF = partitions[("Tom", "whatever gmbh")]
        assert type(tom_whatever_partition) == RF
        assert len(tom_whatever_partition) == 2

        john_whatever_partition: RF = partitions[("John", "whatever gmbh")]
        assert type(john_whatever_partition) == RF
        assert len(john_whatever_partition) == 1

        peter_ppmi_partition: RF = partitions[("Peter", "Peter, Paul, and Mary Inc.")]
        assert type(peter_ppmi_partition) == RF
        assert len(peter_ppmi_partition) == 1

        frank_masterhorst_partition: RF = partitions[("Frank", "Masterhorst")]
        assert type(frank_masterhorst_partition) == RF
        assert len(frank_masterhorst_partition) == 1


def test_partition_by_aggregate_stepwise():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # partition the users RF into a DBF with one RF per partition: one with name Tom and one not named Tom:
    # basically projects to the grouping key:
    partitions = partition(
        users, partitioning_function=lambda i: "Tom" if i.value.name == "Tom" else "not Tom"
    ).result

    # take partitions (a DBF of RFs) and return one RF with one aggregated TF per partition:
    # TODO: introduce a separate nest()-operation for this?
    aggregates = transform_items[DBF, RF](
        partitions,
        transformation_function=lambda item: Item(
            item.key, aggregate(item.value, min=Min("yob"), max=Max("yob")).result
        ),
        output_factory=lambda _: RF(),
    ).result

    assert len(aggregates) == 2
    assert "Tom" in aggregates
    assert "not Tom" in aggregates
    assert aggregates["Tom"].min == 1983
    assert aggregates["Tom"].max == 1983
    assert aggregates["not Tom"].min == 1972
    assert aggregates["not Tom"].max == 2003


def test_partition_by_aggregate_single_operator():
    rel: RF = _create_testdata(frozen=True).customers

    for i in range(2):
        aggregates: RF | None = None
        if i == 0:
            aggregates = partition_by_aggregate(
                rel,
                partitioning_function=lambda i: (
                    "Tom" if i.value.name == "Tom" else "not Tom"
                ),
                aggregation_function=lambda i: Item(
                    key=i.key, value=TF({"count": len(i.value)})
                ),
            ).result
        else:
            aggregates = partition_by_aggregate(
                rel,
                partitioning_function=lambda i: "Tom" if i.value.name == "Tom" else "not Tom",
                aggregation_function=lambda i: Item(key=i.key, value=TF({"count": len(i.value)})),
            ).result

        assert len(aggregates) == 2
        assert type(aggregates) == RF

        tom_aggregate: TF = aggregates["Tom"]
        assert type(tom_aggregate) == TF
        assert tom_aggregate.count == 2

        not_tom_aggregate: TF = aggregates["not Tom"]
        assert type(not_tom_aggregate) == TF
        assert not_tom_aggregate.count == 3


def test_group_by_aggregate_single_operator():
    rel: RF = _create_testdata(frozen=True).customers

    for i in range(2):
        aggregates: RF | None = None
        if True:
            aggregates = group_by_aggregate(
                rel,
                "name",
                count=Count("name"),
            ).result

        assert len(aggregates) == 4
        assert type(aggregates) == RF

        assert "Tom" in aggregates
        assert type(aggregates["Tom"]) == TF
        assert aggregates["Tom"].count == 2

        assert "John" in aggregates
        assert type(aggregates["John"]) == TF
        assert aggregates["John"].count == 1

        assert "Peter" in aggregates
        assert type(aggregates["Peter"]) == TF
        assert aggregates["Peter"].count == 1

        assert "Frank" in aggregates
        assert type(aggregates["Frank"]) == TF
        assert aggregates["Frank"].count == 1

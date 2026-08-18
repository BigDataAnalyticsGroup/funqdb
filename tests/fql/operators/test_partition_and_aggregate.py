import pytest

from fdm.attribute_functions import DBF, RF, TF
from fql.operators.aggregates import aggregate, Min, Max, Count
from fql.operators.partition import partition, group_by
from fql.operators.partition_and_aggregate import (
    partition_by_aggregate,
    group_by_aggregate,
)
from fql.operators.rank import rank_by
from fql.operators.transforms import transform_items, key_to_value
from fql.util import Item
from tests.lib import _create_testdata


def test_partitioning_and_group_by_composed_partitioning_key():
    db: DBF = _create_testdata(frozen=True)
    customers: RF = db.customers

    # partition the users relation into two RFs: those name Tom and those not named Tom:
    for i in range(2):
        if i == 0:
            # generic partitioning based on a partitioning function:
            partitions = partition(
                customers,
                partitioning_function=lambda i: (i.value.name, i.value.company),
            ).result
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
        users,
        partitioning_function=lambda i: "Tom" if i.value.name == "Tom" else "not Tom",
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

    aggregates = partition_by_aggregate(
        rel,
        partitioning_function=lambda i: ("Tom" if i.value.name == "Tom" else "not Tom"),
        aggregation_function=lambda i: Item(
            key=i.key, value=TF({"count": len(i.value)})
        ),
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


def test_group_by_dropping_aggregate_keys_with_projection():
    """After group_by the group name is the RF *key*, so value-level project() keeps it; key_to_value makes it
    droppable.
    Would fail if project() erroneously dropped RF domain keys, or if key_to_value+project could not turn the group
    name into a value attribute that project then removes."""
    rel: RF = _create_testdata(
        frozen=True
    ).customers  # 5 customers grouped by name below

    aggregated = group_by_aggregate(
        rel,
        "name",
        count=Count("name"),
    ).result  # RF keyed by group name -> TF{count}; the name is the group identity in the key

    projected = aggregated.project(
        "count"
    )  # project trims VALUE attributes, it does not touch the RF domain
    for name in ("Tom", "John", "Peter", "Frank"):  # every group name is an RF key
        assert (
            name in projected
        )  # ... so it survives value-level projection (the key is the group identity)

    # Supported way to make "name" a droppable *value* attribute: lift the key into the value, then project it away.
    lifted = key_to_value(
        aggregated, "name"
    ).result  # copy each group name into its value under "name"
    lifted_projected = lifted.project(
        "count"
    )  # keep only the "count" value attribute, dropping the value-level "name"
    for name in (
        "Tom",
        "John",
        "Peter",
        "Frank",
    ):  # the RF keys are never touched by project()
        assert (
            name in lifted_projected
        )  # ... so the group identity is still addressable by key
    assert (
        "count" in lifted_projected["Tom"]
    )  # the projected value retains the aggregate "count"
    assert (
        "name" not in lifted_projected["Tom"]
    )  # ... but the value-level "name" was dropped by the projection


def test_ordering_after_group_by_drops_aggregate_keys():
    """key_to_value carries the group identity into the value so it survives rank_by's re-keying to ℕ.
    Would fail if the lifted "name"/"count" were lost during ranking, or if the count==2 group were not Tom's.
    """
    rel: RF = _create_testdata(
        frozen=True
    ).customers  # 5 customers; names Tom(x2), John, Peter, Frank

    aggregated = group_by_aggregate(
        rel,
        "name",
        count=Count("name"),
    ).result  # RF keyed by group name -> TF{count}

    lifted = key_to_value(
        aggregated, "name"
    ).result  # lift the group name into the value before ranking discards keys
    ordered = rank_by(
        lifted, ranking_key=lambda tf: tf.value.count
    ).result  # re-key to ℕ, ordered ascending by count

    assert (
        len(ordered) == 4
    )  # all four groups survive ranking (guards the all() below against vacuous truth)
    # rank_by intentionally replaces the key domain with ℕ, so identity must be checked in the VALUE, not the key:
    attrs_retained = all(  # each ranked value should carry both the lifted name and the aggregate count
        "count" in item.value and "name" in item.value for item in ordered
    )
    assert attrs_retained  # both attributes travel along inside the value through the re-keying
    tom_value = next(
        item.value for item in ordered if item.value.count == 2
    )  # the only group with count 2 is the two customers named Tom
    assert (
        tom_value.name == "Tom"
    )  # its identity survived ranking because it was lifted into the value first

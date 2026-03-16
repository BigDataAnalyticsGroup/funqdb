from fdm.attribute_functions import RF, DBF, TF
from fql.operators.aggregates import (
    Min,
    Max,
    Count,
    Sum,
    Avg,
    Mean,
    Median,
)
from fql.operators.transforms import (
    aggregate,
    𝜞,
    partition,
    transform_items,
    partition_by_aggregate,
)
from fql.util import Item
from tests.lib import _create_testdata


def test_aggregation_functions():
    # OK
    users: RF = _create_testdata(frozen=True).users
    f = Min("yob")
    assert f(users) == 1972

    f = Max("yob")
    assert f(users) == 2003

    f = Count("yob")
    assert f(users) == 3

    f = Sum("yob")
    assert f(users) == 5958

    f = Avg("yob")
    assert f(users) == 1986

    f = Mean("yob")
    assert f(users) == 1986

    f = Median("yob")
    assert f(users) == 1983


def test_aggregate_operator():
    # OK
    rel: RF = _create_testdata(frozen=True).users

    for i in range(2):
        # 7 aggregate_keys at the same time:
        aggregated: TF | None = None
        if i == 0:
            aggregated = aggregate(
                min=Min("yob"),
                max=Max("yob"),
                count=Count("yob"),
                sum=Sum("yob"),
                avg=Avg("yob"),
                mean=Mean("yob"),
                median=Median("yob"),
            )(rel)
        else:
            # with relational algebra inspired syntax:
            aggregated = 𝜞(
                min=Min("yob"),
                max=Max("yob"),
                count=Count("yob"),
                sum=Sum("yob"),
                avg=Avg("yob"),
                mean=Mean("yob"),
                median=Median("yob"),
            )(rel)

        assert type(aggregated) is TF
        assert len(aggregated) == 7
        assert aggregated.min == 1972
        assert aggregated.max == 2003
        assert aggregated.count == 3
        assert aggregated.sum == 5958
        assert aggregated.avg == 1986
        assert aggregated.mean == 1986
        assert aggregated.median == 1983


def test_partition_by_aggregate_stepwise():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # partition the users RF into a DBF with one RF per partition: one with name Tom and one not named Tom:
    # basically projects to the grouping key:
    partitions = partition(lambda i: "Tom" if i.value.name == "Tom" else "not Tom")(
        users
    )

    # take partitions (a DBF of RFs) and return one RF with one aggregated TF per partition:
    # TODO: introduce a separate nest()-operation for this?
    aggregates = transform_items[DBF, RF](
        transformation_function=lambda item: Item(
            item.key, aggregate(min=Min("yob"), max=Max("yob"))(item.value)
        ),
        output_factory=lambda _: RF(),
    )(partitions)

    assert len(aggregates) == 2
    assert "Tom" in aggregates
    assert "not Tom" in aggregates
    assert aggregates["Tom"].min == 1983
    assert aggregates["Tom"].max == 1983
    assert aggregates["not Tom"].min == 1972
    assert aggregates["not Tom"].max == 2003


def test_partition_by_aggregate_single_operator():
    # TODO: redo with new aggregation operator
    rel: RF = _create_testdata(frozen=True).customers

    for i in range(2):
        aggregates: RF | None = None
        if i == 0:
            aggregates = partition_by_aggregate(
                partitioning_function=lambda i: (
                    "Tom" if i.value.name == "Tom" else "not Tom"
                ),
                aggregation_function=lambda i: Item(
                    key=i.key, value=TF({"count": len(i.value)})
                ),
            )(rel)
        else:
            aggregates = partition_by_aggregate(
                lambda i: "Tom" if i.value.name == "Tom" else "not Tom",
                lambda i: Item(key=i.key, value=TF({"count": len(i.value)})),
            )(rel)

        assert len(aggregates) == 2
        assert type(aggregates) == RF

        tom_aggregate: TF = aggregates["Tom"]
        assert type(tom_aggregate) == TF
        assert tom_aggregate.count == 2

        not_tom_aggregate: TF = aggregates["not Tom"]
        assert type(not_tom_aggregate) == TF
        assert not_tom_aggregate.count == 3

from fdm.attribute_functions import RF, TF
from fql.operators.aggregates import (
    Min,
    Max,
    Count,
    Sum,
    Avg,
    Mean,
    Median,
    aggregate,
    𝜞,
)
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

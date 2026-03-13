from fdm.attribute_functions import RF
from fql.operators.aggregates import Min, Max, Count, Sum, Avg, Mean, Median
from fql.operators.transforms import aggregate
from tests.lib import _create_testdata


def test_aggregation_functions():
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


def test_aggregate_single_operator():
    rel: RF = _create_testdata(frozen=True).users

    aggregates: RF | None = aggregate(lambda rf: RF({"count": len(rf)}))(rel)

    assert len(aggregates) == 1
    assert type(aggregates) == RF

    assert aggregates.count == 3

    # TODO: arbitrary aggregation functions, e.g. sum of budgets of departments referenced by users, etc.

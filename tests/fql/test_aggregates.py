from fdm.attribute_functions import RF
from fql.aggregates import Min, Max, Count, Sum, Avg, Mean, Median
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

import pytest

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

    f = Count()
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
        # 8 aggregate_keys at the same time:
        if i == 0:
            aggregated = aggregate(
                rel,
                min=Min("yob"),
                max=Max("yob"),
                count=Count("yob"),
                count_star=Count(),
                sum=Sum("yob"),
                avg=Avg("yob"),
                mean=Mean("yob"),
                median=Median("yob"),
            ).result
        else:
            # with relational algebra inspired syntax:
            aggregated = 𝜞(
                rel,
                min=Min("yob"),
                max=Max("yob"),
                count=Count("yob"),
                count_star=Count(),
                sum=Sum("yob"),
                avg=Avg("yob"),
                mean=Mean("yob"),
                median=Median("yob"),
            ).result

        assert type(aggregated) is TF
        assert not aggregated.frozen  # aggregate should not freeze the output
        assert len(aggregated) == 8
        assert aggregated.min == 1972
        assert aggregated.max == 2003
        assert aggregated.count == 3
        assert aggregated.count_star == 3
        assert aggregated.sum == 5958
        assert aggregated.avg == 1986
        assert aggregated.mean == 1986
        assert aggregated.median == 1983


def test_min_skips_tuples_missing_the_attribute():
    """A missing attribute is not a NULL value; Min ignores such tuples (JOB 1a, 29a).

    cast (RF)                      Queen (TF)
      1 --person_role------------>  name: "Queen"
        note: "(voice)"
      2  (no note, no person_role)
    """
    queen: TF = TF({"name": "Queen"})  # the character a cast tuple references
    cast: RF = (
        RF(  # tuple 2 has neither a note nor a person_role reference (both absent)
            {1: TF({"note": "(voice)", "person_role": queen}), 2: TF({})}
        )
    )
    result: TF = aggregate(  # flat attribute and a path through the missing reference
        cast, note=Min("note"), character=Min("person_role__name")
    ).result
    assert result.note == "(voice)"  # only tuple 1 contributes a note
    assert result.character == "Queen"  # only tuple 1 contributes a referenced name


def test_min_over_no_values_leaves_the_attribute_out():
    """With nothing to aggregate there is no minimum, so the output TF lacks it."""
    result: TF = aggregate(RF(), note=Min("note")).result  # empty input relation
    assert "note" not in result  # no value is expressed as a missing attribute


@pytest.mark.parametrize(
    "aggregation, expected",
    [
        (Min("yob"), 1972),  # min over the two mapped values 1972, 1990
        (Max("yob"), 1990),  # max over the two mapped values
        (Sum("yob"), 3962),  # sum over the two mapped values (1972 + 1990)
        (Avg("yob"), 1981),  # (1972 + 1990) / 2
        (Mean("yob"), 1981),  # Mean is a synonym for Avg
        (Median("yob"), 1981),  # median of two values is their mean
        (Count("yob"), 2),  # counts only the two tuples that map "yob"
    ],
    ids=lambda p: repr(p) if not isinstance(p, int) else str(p),
)
def test_aggregates_skip_tuples_missing_the_attribute(aggregation, expected):
    """Every aggregate folds only over tuples that map the attribute; the tuple
    without "yob" contributes nothing (no NULL, no error)."""
    rel: RF = RF(  # tuple 3 has no "yob" mapping at all
        {1: TF({"yob": 1972}), 2: TF({"yob": 1990}), 3: TF({"name": "no year"})}
    )
    result: TF = aggregate(rel, value=aggregation).result  # single named aggregate
    assert result.value == expected  # only the two mapped values participate


@pytest.mark.parametrize(
    "aggregation",
    [Min("yob"), Max("yob"), Sum("yob"), Avg("yob"), Mean("yob"), Median("yob")],
    ids=repr,
)
def test_value_aggregates_over_empty_bag_leave_the_key_out(aggregation):
    """Min/Max/Sum/Avg/Mean/Median over no mapped values are undefined, so the
    key is absent from the output TF (Sum is absent too, not 0)."""
    rel: RF = RF({1: TF({"name": "a"}), 2: TF({"name": "b"})})  # no "yob" anywhere
    result: TF = aggregate(rel, value=aggregation).result  # nothing to aggregate
    assert "value" not in result  # undefined aggregate -> no mapping in output


def test_count_over_empty_bag_is_zero_and_present():
    """Count is the exception: zero is a valid count, so the key stays present."""
    rel: RF = RF({1: TF({"name": "a"}), 2: TF({"name": "b"})})  # no "yob" anywhere
    result: TF = aggregate(rel, n=Count("yob"), total=Count()).result  # both counts
    assert result.n == 0  # no tuple maps "yob" -> count is 0 (COUNT(col))
    assert result.total == 2  # Count() counts all tuples (COUNT(*))


def test_stored_none_is_a_real_value_not_a_missing_attribute():
    """A stored None is a mapped value (FDM has no NULL), so it participates. A
    numeric Min over None then raises TypeError -- None is the wrong type, just
    like a string would be; it is not silently skipped."""
    with_none: RF = RF({1: TF({"yob": None}), 2: TF({"yob": 1990})})  # 1 maps None
    assert Count("yob")(with_none) == 2  # both tuples map "yob" (None counts)
    with pytest.raises(TypeError):  # None vs int is not comparable -> no skip
        aggregate(with_none, value=Min("yob")).result

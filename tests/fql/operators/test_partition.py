import pytest

from fdm.attribute_functions import DBF, RF, TF
from fql.expressions import Attr, Key
from fql.operators.filters import filter_values
from fql.operators.partition import partition
from fql.predicates.predicates import Lt, Missing, Not, Predicate
from tests.lib import _create_testdata, _memberships


def test_partitioning():
    db: DBF = _create_testdata(frozen=True)
    customers: RF = db.customers

    # partition the users relation into two RFs: those name Tom and those not named Tom:
    partitions = partition(
        customers,
        partitioning_function=lambda i: "Tom" if i.value.name == "Tom" else "not Tom",
    ).result
    assert len(partitions) == 2
    assert type(partitions) == DBF

    tom_partition: RF = partitions["Tom"]
    assert type(tom_partition) == RF
    assert len(tom_partition) == 2
    for item in tom_partition:
        assert item.value.name == "Tom"

    not_tom_partition: RF = partitions["not Tom"]
    assert type(not_tom_partition) == RF
    assert len(not_tom_partition) == 3
    for item in not_tom_partition:
        assert item.value.name != "Tom"


# ---------------------------------------------------------------------------
# Structured partition keys: Attr, Key, Predicate (008)
# ---------------------------------------------------------------------------


def _staff() -> RF:
    """Five employees; Dan has no department, Eve has no year of birth.

    ::

        key | name | dept | yob
        ----+------+------+------
         1  | Ann  | Dev  | 1970
         2  | Bob  | Ops  | 1990
         3  | Cid  | Dev  | 2000
         4  | Dan  |  --  | 1975   <- no dept mapped
         5  | Eve  | Ops  |  --    <- no yob mapped
    """
    staff: RF = RF()  # empty, unfrozen relation
    staff[1] = TF({"name": "Ann", "dept": "Dev", "yob": 1970})  # complete tuple
    staff[2] = TF({"name": "Bob", "dept": "Ops", "yob": 1990})  # complete tuple
    staff[3] = TF({"name": "Cid", "dept": "Dev", "yob": 2000})  # complete tuple
    staff[4] = TF({"name": "Dan", "yob": 1975})  # no dept
    staff[5] = TF({"name": "Eve", "dept": "Ops"})  # no yob
    staff.freeze()  # inputs to operators are frozen in the other tests too
    return staff


def test_partition_by_attr_groups_by_value_and_drops_missing() -> None:
    """``partition`` with ``Attr("dept")`` groups by department and drops Dan.
    Would fail if a tuple without the attribute raised, or were put into an
    ``UNDEFINED`` partition instead of being dropped."""
    partitions: DBF = partition(
        _staff(), partitioning_function=Attr("dept")
    ).result  # one partition per department
    assert _memberships(partitions) == {
        "Dev": {1, 3},  # Ann and Cid
        "Ops": {2, 5},  # Bob and Eve (Eve has a dept, only her yob is missing)
    }  # Dan (key 4) has no dept and is in no partition


@pytest.mark.parametrize(
    "predicate, expected",
    [
        (Lt("yob", 1980), {True: {1, 4}, False: {2, 3, 5}}),  # Ann, Dan; Eve False
        (Missing("yob"), {True: {5}, False: {1, 2, 3, 4}}),  # only Eve lacks yob
    ],
    ids=["lt-puts-eve-into-false", "missing-puts-eve-into-true"],
)
def test_partition_by_predicate_equals_filter_and_negated_filter(
    predicate: Predicate, expected: dict[bool, set[int]]
) -> None:
    """``partition(P)`` has the members of ``filter_values(P)`` under ``True`` and of
    ``filter_values(Not(P))`` under ``False``; Eve (no yob) goes to ``False`` for ``Lt``
    but to ``True`` for ``Missing``, so "missing -> False" holds only for comparisons.
    Would fail if the predicate were evaluated on the item instead of its value, if
    Eve were dropped, or if partition and filter disagreed on a tuple without yob."""
    partitions: DBF = partition(
        _staff(), partitioning_function=predicate
    ).result  # split into True / False by the predicate
    selected: set[int] = {
        item.key for item in filter_values(_staff(), predicate).result
    }  # keys the filter keeps
    rejected: set[int] = {
        item.key for item in filter_values(_staff(), Not(predicate)).result
    }  # keys the negated filter keeps
    assert _memberships(partitions) == expected  # see the parametrized expectation
    assert expected == {True: selected, False: rejected}  # same split as the filters


def test_partition_by_key_puts_every_tuple_into_its_own_partition() -> None:
    """``partition`` with ``Key()`` gives one partition per tuple, named by its key.
    Would fail if ``Key()`` were evaluated on the value instead of the item, like a
    predicate (the partition keys would be the TFs, not 1..5)."""
    partitions: DBF = partition(
        _staff(), partitioning_function=Key()
    ).result  # partition by the tuple key
    assert _memberships(partitions) == {
        k: {k} for k in range(1, 6)
    }  # every tuple alone in a partition named by its own key

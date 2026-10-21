from typing import Any

import pytest

from fdm.attribute_functions import DBF, RF, TF
from fql.expressions import Attr, Record
from fql.operators.aggregates import aggregate, AggregationFunction, Min, Max, Count
from fql.operators.partition import partition, group_by
from fql.operators.partition_and_aggregate import (
    partition_by_aggregate,
    group_by_aggregate,
)
from fql.operators.rank import rank_by
from fql.operators.transforms import transform_items, key_to_value
from fql.predicates.predicates import Eq
from fql.util import Item
from tests.lib import _create_testdata, _memberships


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


# ---------------------------------------------------------------------------
# group_by with missing grouping attributes (008)
# ---------------------------------------------------------------------------


def _staff() -> RF:
    """Four employees; Dan has no department, Eve has no city.

    ::

        key | name | dept | city
        ----+------+------+--------
         1  | Ann  | Dev  | Berlin
         2  | Bob  | Dev  | Berlin
         3  | Dan  |  --  | Paris    <- no dept mapped
         4  | Eve  | Ops  |  --      <- no city mapped
    """
    staff: RF = RF()  # empty, unfrozen relation
    staff[1] = TF({"name": "Ann", "dept": "Dev", "city": "Berlin"})  # complete
    staff[2] = TF({"name": "Bob", "dept": "Dev", "city": "Berlin"})  # complete
    staff[3] = TF({"name": "Dan", "city": "Paris"})  # no dept
    staff[4] = TF({"name": "Eve", "dept": "Ops"})  # no city
    staff.freeze()  # inputs to operators are frozen in the other tests too
    return staff


def test_group_by_drops_tuple_without_the_grouping_attribute() -> None:
    """``group_by(rel, "dept")`` drops Dan, who has no department.
    Would fail if a missing grouping attribute still raised ``AttributeError``
    (behaviour before 008) or formed its own group."""
    groups: DBF = group_by(_staff(), "dept").result  # one group per department
    memberships: dict = _memberships(groups)  # group key -> set of tuple keys
    assert memberships == {"Dev": {1, 2}, "Ops": {4}}  # Dan (key 3) is dropped


def test_group_by_two_keys_drops_tuple_missing_either_attribute() -> None:
    """``group_by(rel, "dept", "city")`` gives tuple keys and drops Dan and Eve.
    Would fail if a tuple missing only one of the two grouping attributes
    raised, or formed a group with a partly undefined key."""
    groups: DBF = group_by(_staff(), "dept", "city").result  # (dept, city) groups
    memberships: dict = _memberships(groups)  # group key -> set of tuple keys
    assert memberships == {("Dev", "Berlin"): {1, 2}}  # only Ann and Bob remain


@pytest.mark.parametrize(
    "key, message",
    [
        ("", "Empty segment in attribute path"),  # no segment at all
        ("a..b", "Empty segment in attribute path"),  # empty middle segment
        (1, "Attribute path must be a string"),  # not a path
    ],
    ids=["empty", "double-dot", "int"],
)
def test_group_by_rejects_invalid_grouping_key_at_construction(
    key: Any, message: str
) -> None:
    """``group_by`` raises ``ValueError`` for an invalid grouping key when it is created.
    Would fail if the key were checked only when the result is computed, or if an
    integer key were still accepted as before 008."""
    with pytest.raises(ValueError, match=message):  # eager check, no .result needed
        group_by(_staff(), key)  # construction alone must fail


def test_group_by_aggregate_checks_grouping_key_only_when_computed() -> None:
    """``group_by_aggregate`` with an invalid key is created fine and fails on ``.result``.
    Would fail if the documented lazy behaviour changed, i.e. if the operator built
    its ``group_by`` (and thus validated the key) already in the constructor."""
    pipeline: group_by_aggregate = group_by_aggregate(
        _staff(), "", count=Count()
    )  # no error yet: the group_by is built lazily
    with pytest.raises(ValueError, match="Empty segment in attribute path"):  # now
        pipeline.result  # computing the result validates the key


def _staff_with_org() -> RF:
    """Four employees whose organization is a nested TF; Dan has no organization.

    ::

        key | name | org -> name
        ----+------+------------
         1  | Ann  | Acme
         2  | Bob  | Acme
         3  | Cid  | Zeta
         4  | Dan  |  --          <- no org mapped
    """
    acme: TF = TF({"name": "Acme"})  # shared organization of Ann and Bob
    staff: RF = RF()  # empty, unfrozen relation
    staff[1] = TF({"name": "Ann", "org": acme})  # works at Acme
    staff[2] = TF({"name": "Bob", "org": acme})  # works at Acme
    staff[3] = TF({"name": "Cid", "org": TF({"name": "Zeta"})})  # works at Zeta
    staff[4] = TF({"name": "Dan"})  # no organization
    staff.freeze()  # inputs to operators are frozen in the other tests too
    return staff


@pytest.mark.parametrize("key", ["org.name", "org__name"], ids=["dot", "dunder"])
def test_group_by_nested_path_key(key: str) -> None:
    """``group_by`` with a dotted or dunder path groups by the nested organization name.
    Would fail if one path spelling were read as a plain attribute name (empty
    result) or if Dan without an organization raised instead of being dropped."""
    groups: DBF = group_by(_staff_with_org(), key).result  # one group per org name
    assert _memberships(groups) == {
        "Acme": {1, 2},  # Ann and Bob
        "Zeta": {3},  # Cid
    }  # Dan (key 4) has no org and is dropped


def test_group_by_aggregate_does_not_count_dropped_tuples() -> None:
    """``group_by_aggregate`` counts per department and leaves Dan out of every count.
    Would fail if a tuple without the grouping attribute were counted in some group
    or made the whole aggregation fail."""
    result: RF = group_by_aggregate(
        _staff(), "dept", count=Count()
    ).result  # number of employees per department
    counts: dict[Any, int] = {
        item.key: item.value.count for item in result
    }  # group key -> count
    assert counts == {"Dev": 2, "Ops": 1}  # Ann + Bob, Eve; Dan (no dept) dropped


# ---------------------------------------------------------------------------
# partition_by_aggregate with keyword aggregates (010)
# ---------------------------------------------------------------------------


def test_partition_by_aggregate_keyword_aggregates_per_partition() -> None:
    """Keyword aggregates give one aggregate tuple per ``Attr("dept")`` partition.
    Would fail if Dan (no dept) were counted, or if ``Min("city")`` over Eve's
    partition (no city) put a value instead of leaving ``city`` absent."""
    result: RF = partition_by_aggregate(
        _staff(), partitioning_function=Attr("dept"), n=Count(), city=Min("city")
    ).result  # per department: number of employees and smallest city
    tuples: dict[Any, dict[str, Any]] = {
        item.key: {a.key: a.value for a in item.value} for item in result
    }  # partition key -> plain dict of the aggregate tuple
    assert tuples == {
        "Dev": {"n": 2, "city": "Berlin"},  # Ann and Bob, both in Berlin
        "Ops": {"n": 1},  # Eve only; no city mapped, so no city aggregate
    }  # Dan (no dept) is in no partition


def test_partition_by_aggregate_predicate_key_counts_missing_attribute_as_false() -> (
    None
):
    """A predicate key with keyword aggregates counts per ``True`` / ``False`` partition.
    Would fail if Eve, who has no city, were dropped instead of counted under
    ``False`` (the comparison is false for a missing attribute)."""
    result: RF = partition_by_aggregate(
        _staff(), partitioning_function=Eq("city", "Berlin"), n=Count()
    ).result  # employees in Berlin vs. elsewhere
    counts: dict[Any, int] = {
        item.key: item.value.n for item in result
    }  # partition key -> count
    assert counts == {True: 2, False: 2}  # Ann, Bob | Dan (Paris), Eve (no city)


@pytest.mark.parametrize(
    "kwargs, error, message",
    [
        ({}, ValueError, "exactly one of"),  # neither form given
        (
            {"aggregation_function": lambda i: i, "n": Count()},
            ValueError,
            "exactly one of",
        ),  # both forms given
        (
            {"aggregation_function": Count()},
            TypeError,
            "as keywords",
        ),  # aggregation instead of a callable on a partition
        (
            {"aggregation_function": Record(a=Attr("dept"))},
            TypeError,
            "as keywords",
        ),  # expression instead of a callable on a partition
        (
            {"aggregation_function": Eq("dept", "Dev")},
            TypeError,
            "as keywords",
        ),  # predicate instead of a callable on a partition
        (
            {"aggregation_function": AggregationFunction(len, "dept")},
            TypeError,
            "as keywords",
        ),  # user-built aggregation: guard checks the base class, not only built-ins
    ],
    ids=["neither", "both", "count", "record", "predicate", "user-aggregation"],
)
def test_partition_by_aggregate_rejects_invalid_forms_at_construction(
    kwargs: dict[str, Any], error: type[Exception], message: str
) -> None:
    """``partition_by_aggregate`` rejects a wrong aggregation form when it is created.
    Would fail if neither/both forms were accepted, or if a structured object as
    ``aggregation_function`` failed only on ``.result`` or silently gave empty tuples.
    """
    with pytest.raises(error, match=message):  # eager check, no .result needed
        partition_by_aggregate(
            _staff(), partitioning_function=Attr("dept"), **kwargs
        )  # construction alone must fail

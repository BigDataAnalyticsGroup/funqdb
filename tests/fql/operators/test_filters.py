#
#    This is funqDB, a query processing library and system built around FDM and FQL.
#
#    Copyright (C) 2026 Prof. Dr. Jens Dittrich, Saarland University
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#


import pytest

from fdm.attribute_functions import CompositeForeignObject, DBF, RF, RSF, TF
from fql.operators.filters import (
    filter_values,
    filter_items,
    filter_keys,
    filter_items_scan_complement,
)
from fql.predicates import And, Eq, Exists, Not, Predicate
from fql.util import Item
from tests.lib import _create_testdata, _subset_DBF, _subset_highly_filtered_DBF


def test_filter_items():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    for i in range(2):
        if i == 0:
            # with output factory parameter:
            users_filtered: RF = filter_items[RF, RF](
                users,
                filter_predicate=lambda i: i.value.department.name == "Dev",
                output_factory=lambda _: RF(),
            ).result
        else:
            # without output factory parameter:
            users_filtered: RF = filter_items[RF, RF](
                users,
                filter_predicate=lambda i: i.value.department.name == "Dev",
            ).result
        assert type(users_filtered) == RF
        assert len(users_filtered) == 2
        for item in users_filtered:
            assert item.value.department.name == "Dev"
        filtered_user_names = {user.value.name for user in users_filtered}
        assert filtered_user_names == {"Horst", "Tom"}


def test_filter_items_where_clause():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    for i in range(0, 2):
        if i == 0:
            users_filtered: RF = users.where(lambda i: i.value.department.name == "Dev")
        else:
            users_filtered: RF = users.where(department__name="Dev")

        assert type(users_filtered) == RF
        assert len(users_filtered) == 2
        for item in users_filtered:
            assert item.value.department.name == "Dev"
        filtered_user_names = {user.value.name for user in users_filtered}
        assert filtered_user_names == {"Horst", "Tom"}


def test_filter_items_complement():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # filter the values in the users relation to only keep those NOT in the "Dev" department
    def filter_predicate_complement(item: Item) -> bool:
        user: TF = item.value
        return user.department.name != "Dev"

    users_filtered: RF = filter_items[RF, RF](
        users,
        filter_predicate=filter_predicate_complement,
    ).result
    assert type(users_filtered) == RF
    assert len(users_filtered) == 1
    for item in users_filtered:
        assert item.value.department.name == "Consulting"
    filtered_user_names = {user.value.name for user in users_filtered}
    assert filtered_user_names == {"John"}


def test_filter_values():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    for i in range(2):
        if i == 0:
            # with output factory parameter:
            users_filtered: RF = filter_values[RF, RF](
                users,
                filter_predicate=lambda v: v.department.name == "Dev",
                output_factory=lambda _: RF(),
            ).result
        else:
            # without output factory parameter:
            users_filtered: RF = filter_values[RF, RF](
                users,
                filter_predicate=lambda v: v.department.name == "Dev",
            ).result
        assert type(users_filtered) == RF
        assert len(users_filtered) == 2
        for item in users_filtered:
            assert item.value.department.name == "Dev"
        filtered_user_names = {user.value.name for user in users_filtered}
        assert filtered_user_names == {"Horst", "Tom"}


def test_filter_keys():
    db: DBF = _create_testdata(frozen=True)
    departments: RF = db.departments
    for i in range(2):
        if i == 0:
            # with output factory parameter:
            departments_filtered: RF = filter_keys[RF, RF](
                departments,
                filter_predicate=lambda k: k == "d1",
                output_factory=lambda _: RF(),
            ).result
        else:
            # without output factory parameter:
            departments_filtered: RF = filter_keys[RF, RF](
                departments,
                filter_predicate=lambda k: k == "d1",
            ).result
        assert type(departments_filtered) == RF
        assert len(departments_filtered) == 1
        for item in departments_filtered:
            assert item.value.name == "Dev"


def test_DB_filter_keys():
    # get subdatabase:
    db_filtered: DBF = _subset_DBF({"users", "departments"}, frozen=True)

    assert type(db_filtered) == DBF
    assert len(db_filtered) == 2  # users and departments relations only

    assert type(db_filtered.users) == RF
    assert type(db_filtered.departments) == RF


def test_filter_items_multiple_filters():
    db: DBF = _subset_highly_filtered_DBF(frozen=True)
    departments: RF = db.departments
    assert len(departments) == 1

    users: RF = db.users
    assert len(users) == 1


def test_filter_as_a_parameter():
    db: DBF = _create_testdata(frozen=True)

    class A:
        def foo(self, **kwargs):
            print("foo", kwargs)

        def __call__(self, *args, **kwargs):
            """Make the object callable through () syntax.
            @return: The result of the call.
            """
            print("args", args)
            print("kwargs", kwargs)

        def __getitem__(self, key):
            """Make the object callable through []-syntax."""
            return "bla"

    a: A = A()
    a.foo(d=4)
    a(12, d=42)
    assert a["test"] == "bla"
    assert a(a=42) == None


def test_filter_items_reused_and_chained():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    filter_kw = dict(
        filter_predicate=lambda item: item.value.department.name == "Dev",
        output_factory=lambda _: RF(),
    )
    users_filtered: RF = filter_items[RF, RF](
        filter_items[RF, RF](users, **filter_kw).result,
        **filter_kw,
    ).result  # apply filter twice by chaining

    assert type(users_filtered) == RF
    assert len(users_filtered) == 2
    for item in users_filtered:
        assert item.value.department.name == "Dev"
    filtered_user_names = {user.value.name for user in users_filtered}
    assert filtered_user_names == {"Horst", "Tom"}


def test_filter_items_explain():
    """Verify that explain() returns a descriptive string for filter_items."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    op: filter_items = filter_items[RF, RF](
        users,
        filter_predicate=lambda i: True,
        output_factory=lambda _: RF(),
    )
    explanation: str = op.explain()
    assert "filter_items" in explanation


def test_filter_values_explain():
    """Verify that explain() returns a descriptive string for filter_values."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    op: filter_values = filter_values[RF, RF](
        users,
        filter_predicate=lambda v: True,
        output_factory=lambda _: RF(),
    )
    explanation: str = op.explain()
    assert "filter_values" in explanation


def test_filter_keys_explain():
    """Verify that explain() returns a descriptive string for filter_keys."""
    db: DBF = _create_testdata(frozen=True)
    departments: RF = db.departments
    op: filter_keys = filter_keys[RF, RF](
        departments,
        filter_predicate=lambda k: True,
        output_factory=lambda _: RF(),
    )
    explanation: str = op.explain()
    assert "filter_keys" in explanation


def test_filter_items_create_lineage_not_implemented():
    """Verify that create_lineage=True raises NotImplementedError."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    with pytest.raises(NotImplementedError):
        _ = filter_items[RF, RF](
            users,
            filter_predicate=lambda i: True,
            output_factory=lambda _: RF(),
            create_lineage=True,
        ).result


def test_filter_items_scan_complement():
    """Verify that filter_items_scan_complement returns the complement of the predicate."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    op: filter_items_scan_complement = filter_items_scan_complement[RF, RF](
        users,
        filter_predicate=lambda i: i.value.department.name == "Dev",
        output_factory=lambda _: RF(),
    )
    explanation: str = op.explain()
    assert "filter_items_scan_complement" in explanation

    # The complement must keep only items that do NOT match the predicate.
    result: RF = op.result
    result_names: set[str] = {item.value.name for item in result}
    # "Dev" users are Horst and Tom; the complement must contain only non-Dev users.
    assert "Horst" not in result_names
    assert "Tom" not in result_names
    assert len(result) > 0, "complement must not be empty"


def test_where_drops_tuples_missing_the_filtered_attribute():
    """A tuple without `note` cannot satisfy a condition on `note`, so it is filtered out (JOB 1a)."""
    companies: RF = RF(  # tuple 2 has no `note` attribute (absent, not a NULL value)
        {1: TF({"note": "(presents)"}), 2: TF({})}
    )
    filtered: RF = companies.where(  # the rendered form of JOB 1a's LIKE on `note`
        lambda i: "(presents)" in i.value.note
    )
    assert {item.key for item in filtered} == {1}  # tuple 2 is dropped, not an error


def _companies_with_and_without_note() -> RF:
    """Return companies where company 2 maps no `note` (absent, not a NULL value)."""
    return RF(  # company 1 maps `note`, company 2 does not
        {1: TF({"note": "(presents)"}), 2: TF({})}
    )


def _presents_in_note(item: Item) -> bool:
    """Return whether the item's `note` contains "(presents)"; raises on an unmapped `note`."""
    return "(presents)" in item.value.note  # AttributeError if `note` is unmapped


def test_filter_items_lambda_still_raises_errors_other_than_attribute_error():
    """Only an unmapped attribute means "no match"; a wrongly typed value still raises."""
    companies: RF = RF({1: TF({"note": 42})})  # `note` maps an int, not a str
    operator: filter_items = filter_items(  # `in` on an int raises TypeError
        companies, filter_predicate=_presents_in_note
    )
    with pytest.raises(TypeError):  # the TypeError is not swallowed
        _ = operator.result  # trigger the lazy computation


def test_filter_items_drops_items_missing_the_lambda_attribute():
    """A lambda that reads an unmapped attribute off the Item drops it instead of raising."""
    companies: RF = _companies_with_and_without_note()  # company 2 has no `note`
    filtered: RF = filter_items(  # i.value.note raises AttributeError on company 2
        companies, filter_predicate=lambda i: "(presents)" in i.value.note
    ).result
    assert {item.key for item in filtered} == {1}  # the AttributeError means "no match"


def test_filter_items_masks_a_misspelled_attribute_as_no_match():
    """The accepted cost of the guard: a typo reads as an unmapped attribute."""
    companies: RF = _companies_with_and_without_note()  # company 1 maps `note`
    filtered: RF = filter_items(  # `nmae` is a typo, mapped by no company
        companies, filter_predicate=lambda i: i.value.nmae == "x"
    ).result
    assert len(filtered) == 0  # empty result instead of an AttributeError


def test_filter_items_matches_where_for_lambdas_on_unmapped_attributes():
    """`filter_items` and `.where()` take the same Item predicate and select the same items."""
    companies: RF = _companies_with_and_without_note()  # company 2 has no `note`
    by_operator: RF = filter_items(  # the predicate receives the Item, as in .where
        companies, filter_predicate=_presents_in_note
    ).result
    by_where: RF = companies.where(_presents_in_note)  # the same predicate and data
    assert {i.key for i in by_operator} == {i.key for i in by_where}  # both are {1}


def test_filter_values_lambda_still_raises_errors_other_than_attribute_error():
    """Only an unmapped attribute means "no match"; a wrongly typed value still raises."""
    companies: RF = RF({1: TF({"note": 42})})  # `note` maps an int, not a str
    operator: filter_values = filter_values(  # `in` on an int raises TypeError
        companies, filter_predicate=lambda v: "(presents)" in v.note
    )
    with pytest.raises(TypeError):  # the TypeError is not swallowed
        _ = operator.result  # trigger the lazy computation


def test_filter_values_drops_values_missing_the_structured_predicate_attribute():
    """A structured predicate over an unmapped attribute does not select the value."""
    companies: RF = _companies_with_and_without_note()  # company 2 has no `note`
    filtered: RF = filter_values(  # Eq on `note` cannot match company 2
        companies, filter_predicate=Eq("note", "(presents)")
    ).result
    assert {item.key for item in filtered} == {1}  # company 2 is dropped, no error


def test_filter_values_drops_values_missing_the_lambda_attribute():
    """A lambda that reads an unmapped attribute drops the value instead of raising."""
    companies: RF = _companies_with_and_without_note()  # company 2 has no `note`
    filtered: RF = filter_values(  # v.note raises AttributeError on company 2
        companies, filter_predicate=lambda v: "(presents)" in v.note
    ).result
    assert {item.key for item in filtered} == {1}  # the AttributeError means "no match"


def test_filter_values_drops_values_that_are_not_attribute_functions():
    """A lambda that reads an attribute off a plain int drops the value (accepted cost)."""
    counts: RF = RF({1: 7})  # the value is an int, not an attribute function
    filtered: RF = filter_values(  # an int has no `note`, so v.note raises
        counts, filter_predicate=lambda v: v.note == "x"
    ).result
    assert len(filtered) == 0  # empty result instead of an AttributeError


def test_filter_values_drops_values_missing_a_nested_attribute():
    """A path through an unmapped reference does not select the value, for both predicate kinds."""
    dev: TF = TF({"name": "Dev"})  # the department that `Horst` references
    users: RF = RF(  # `Tom` has no `department` mapping
        {1: TF({"name": "Horst", "department": dev}), 2: TF({"name": "Tom"})}
    )
    structured: RF = filter_values(  # the path `department.name` is unmapped for Tom
        users, filter_predicate=Eq("department.name", "Dev")
    ).result
    plain: RF = filter_values(  # v.department raises AttributeError for Tom
        users, filter_predicate=lambda v: v.department.name == "Dev"
    ).result
    assert {item.key for item in structured} == {1}  # Tom is dropped by Eq
    assert {item.key for item in plain} == {1}  # Tom is dropped by the lambda


def test_filter_values_negated_structured_predicate_selects_values_missing_the_attribute():
    """`Not` over an unmapped attribute is a plain `True` (two-valued, no UNKNOWN)."""
    companies: RF = _companies_with_and_without_note()  # company 2 has no `note`
    filtered: RF = filter_values(  # Eq is False for company 2, so Not(Eq) is True
        companies, filter_predicate=Not(Eq("note", "(presents)"))
    ).result
    assert {item.key for item in filtered} == {2}  # only company 2 is selected


def test_filter_values_exists_and_not_excludes_values_missing_the_attribute():
    """`And(Exists, Not(...))` recovers "present and not equal" without three-valued logic."""
    companies: RF = RF(  # company 3 maps a different `note`
        {1: TF({"note": "(presents)"}), 2: TF({}), 3: TF({"note": "(other)"})}
    )
    filtered: RF = filter_values(  # only companies that map `note` and differ
        companies,
        filter_predicate=And(Exists("note"), Not(Eq("note", "(presents)"))),
    ).result
    assert {item.key for item in filtered} == {3}  # company 2 is excluded by Exists


def test_filter_values_negated_lambda_drops_values_missing_the_attribute():
    """A negation inside a lambda still raises on the read, so the value is dropped."""
    companies: RF = _companies_with_and_without_note()  # company 2 has no `note`
    filtered: RF = filter_values(  # v.note raises before `not` is applied
        companies, filter_predicate=lambda v: not ("(presents)" in v.note)
    ).result
    assert len(filtered) == 0  # company 1 fails the negation, company 2 is dropped


@pytest.mark.parametrize(
    "predicate",
    [
        Eq("note", "(presents)"),  # plain comparison
        Not(Eq("note", "(presents)")),  # negated comparison
        And(Exists("note"), Not(Eq("note", "(presents)"))),  # guarded negation
    ],
    ids=repr,
)
def test_filter_values_matches_where_for_structured_predicates_on_unmapped_attributes(
    predicate: Predicate,
):
    """`filter_values` and `.where()` select the same values for a structured predicate."""
    companies: RF = _companies_with_and_without_note()  # company 2 has no `note`
    by_operator: RF = filter_values(  # the operator applies it to the value
        companies, filter_predicate=predicate
    ).result
    by_where: RF = companies.where(predicate)  # .where applies it to item.value
    assert {i.key for i in by_operator} == {i.key for i in by_where}  # same keys


def test_filter_values_matches_where_for_lambdas_on_unmapped_attributes():
    """`filter_values` and `.where()` select the same values for an equivalent lambda."""
    companies: RF = _companies_with_and_without_note()  # company 2 has no `note`
    by_operator: RF = filter_values(  # the lambda receives the value
        companies, filter_predicate=lambda v: not ("(presents)" in v.note)
    ).result
    by_where: RF = companies.where(  # the lambda receives the Item
        lambda i: not ("(presents)" in i.value.note)
    )
    assert {i.key for i in by_operator} == {i.key for i in by_where}  # both are empty


def test_filter_keys_drops_composite_keys_whose_part_misses_the_lambda_attribute():
    """A lambda that reads an attribute a composite key part does not map drops the item."""
    alice: TF = TF({"name": "Alice"})  # a user that maps `name`
    anonymous: TF = TF({})  # a user that maps no `name`
    acme: TF = TF({"name": "ACME Corp"})  # the customer of both meetings
    alice_meeting: CompositeForeignObject = CompositeForeignObject(alice, acme)  # key 1
    anonymous_meeting: CompositeForeignObject = CompositeForeignObject(  # key 2
        anonymous, acme
    )
    meetings: RSF = RSF(frozen=False)  # N:M relationship between users and customers
    meetings[alice_meeting] = TF({"date": "2025-01-01"})  # Alice meets ACME
    meetings[anonymous_meeting] = TF({"date": "2025-02-15"})  # the anonymous user too
    filtered: RSF = filter_keys(  # subkey(0).name raises AttributeError on key 2
        meetings, filter_predicate=lambda k: k.subkey(0).name == "Alice"
    ).result
    assert {item.key for item in filtered} == {alice_meeting}  # key 2 is dropped


def test_filter_items_scan_complement_keeps_items_missing_the_lambda_attribute():
    """The complement keeps exactly the items `filter_items` drops, unmapped ones included."""
    companies: RF = _companies_with_and_without_note()  # company 2 has no `note`
    complement: RF = filter_items_scan_complement(  # filter_items selects only 1
        companies, filter_predicate=_presents_in_note
    ).result
    assert {item.key for item in complement} == {2}  # company 2 is kept, no error


def test_filter_items_scan_complement_and_filter_items_partition_the_input():
    """`filter_items` and its complement split the input, also over unmapped attributes."""
    companies: RF = RF(  # company 2 has no `note`, company 3 a different one
        {1: TF({"note": "(presents)"}), 2: TF({}), 3: TF({"note": "(other)"})}
    )
    selected: RF = filter_items(  # keeps only company 1
        companies, filter_predicate=_presents_in_note
    ).result
    rest: RF = filter_items_scan_complement(  # the same predicate, complemented
        companies, filter_predicate=_presents_in_note
    ).result
    selected_keys: set = {item.key for item in selected}  # keys kept by filter_items
    rest_keys: set = {item.key for item in rest}  # keys kept by the complement
    assert selected_keys.isdisjoint(rest_keys)  # no item is in both
    assert selected_keys | rest_keys == {1, 2, 3}  # together they cover the input

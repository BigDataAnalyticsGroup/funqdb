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

from fdm.attribute_functions import TF, RF, DBF
from fql.operators.APIs import Operator
from fql.operators.filters import (
    filter_values,
    filter_items,
    filter_keys,
    filter_items_scan_complement,
)
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


def test_project_clause():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    users_projected: RF = users.project("name", "department")

    assert type(users_projected) == RF
    assert len(users_projected) == 3
    assert users_projected != users  # different instance
    for value in users_projected.values():
        assert "department" in value
        assert "name" in value
        assert "yob" not in value


def test_where_clause_lookups():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # exact (explicit) — same as plain equality:
    assert len(users.where(name__exact="Horst")) == 1

    # lt / lte / gt / gte on numeric field (yob):
    assert len(users.where(yob__lt=1983)) == 1  # Horst (1972)
    assert len(users.where(yob__lte=1983)) == 2  # Horst (1972), Tom (1983)
    assert len(users.where(yob__gt=1983)) == 1  # John (2003)
    assert len(users.where(yob__gte=1983)) == 2  # Tom (1983), John (2003)

    # range:
    assert len(users.where(yob__range=(1970, 1990))) == 2  # Horst, Tom

    # in:
    assert len(users.where(yob__in=[1972, 2003])) == 2  # Horst, John

    # contains / startswith / endswith on string field:
    assert len(users.where(name__contains="o")) == 3  # Horst, Tom, John
    assert len(users.where(name__startswith="H")) == 1  # Horst
    assert len(users.where(name__endswith="n")) == 1  # John

    # icontains (case-insensitive):
    assert len(users.where(name__icontains="HO")) == 1  # Horst

    # nested traversal + lookup:
    assert len(users.where(department__name__startswith="D")) == 2  # Dev department


def test_where_clause_lookups_combined():
    """Multiple lookup kwargs form a conjunct (all must match)."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # combine comparison lookups:
    result = users.where(yob__gte=1980, yob__lte=2000)
    assert len(result) == 1  # Tom (1983)
    assert {u.value.name for u in result} == {"Tom"}


def test_where_clause_lookup_vs_nested_attribute_ambiguity():
    """When a nested attribute is literally named like a lookup (e.g. 'lte'),
    the last __-segment is interpreted as a lookup operator, NOT as field traversal.
    To match the literal nested attribute, use a lambda predicate or __exact."""
    # create fake data where 'stats' has a sub-attribute literally named 'lte':
    stats_a = TF({"lte": 100, "gte": 200}, frozen=True)
    stats_b = TF({"lte": 300, "gte": 400}, frozen=True)
    items: RF = RF(
        {
            "a": TF({"name": "Alice", "score": 50, "stats": stats_a}, frozen=True),
            "b": TF({"name": "Bob", "score": 150, "stats": stats_b}, frozen=True),
        },
        frozen=True,
    )

    # AMBIGUITY: stats__lte=200 is interpreted as lookup "score of stats <= 200",
    # NOT as "stats.lte == 200":
    result_lookup = items.where(score__lte=200)
    assert len(result_lookup) == 2  # both Alice (50) and Bob (150) have score <= 200

    # To access the literal nested attribute 'stats.lte', use __exact:
    result_exact = items.where(stats__lte__exact=100)
    assert len(result_exact) == 1  # only Alice (stats.lte == 100)
    assert next(iter(result_exact)).value.name == "Alice"

    # Or use a lambda for full control:
    result_lambda = items.where(lambda i: i.value.stats.lte == 300)
    assert len(result_lambda) == 1  # only Bob (stats.lte == 300)
    assert next(iter(result_lambda)).value.name == "Bob"


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
    filter_items[RF, RF](
        filter_items[RF, RF](users, **filter_kw).result,
        **filter_kw,
    ).result.explain()

    assert type(users_filtered) == RF
    assert len(users_filtered) == 2
    for item in users_filtered:
        assert item.value.department.name == "Dev"
    filtered_user_names = {user.value.name for user in users_filtered}
    assert filtered_user_names == {"Horst", "Tom"}


# TODO: re-enable lineage and explain for filters, then re-enable this test
def _test_filter_explain():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    print(users.get_lineage())

    ret1: RF = filter_items[RF, RF](
        users,
        filter_predicate=lambda item: item.value.department.name == "Dev",
        output_factory=lambda _: RF(),
        create_lineage=True,
    ).result
    ret2: RF = filter_items[RF, RF](
        ret1,
        filter_predicate=lambda item: item.value.department.name == "bla",
        output_factory=lambda _: RF(),
        create_lineage=True,
    ).result
    # print("ret2 lineage:")
    lineage: list[str] = ret2.get_lineage()
    # for i, lin in enumerate(lineage, 1):
    #    print(f"{i}.\t->", lin)


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
        filter_items[RF, RF](
            users,
            filter_predicate=lambda i: True,
            output_factory=lambda _: RF(),
            create_lineage=True,
        ).result


def test_filter_items_scan_complement():
    """Verify that filter_items_scan_complement can be instantiated and its explain() works."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users
    op: filter_items_scan_complement = filter_items_scan_complement[RF, RF](
        users,
        filter_predicate=lambda i: i.value.department.name == "Dev",
        output_factory=lambda _: RF(),
    )
    explanation: str = op.explain()
    assert "filter_items_scan_complement" in explanation

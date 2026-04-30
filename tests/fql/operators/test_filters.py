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
        filter_items[RF, RF](
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

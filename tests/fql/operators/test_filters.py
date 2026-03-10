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


from fdm.attribute_functions import TF, RF, DBF
from fql.operators.APIs import Operator
from fql.operators.filters import filter_values, filter_items, filter_keys
from fql.util import Item
from tests.lib import _create_testdata, _subset_DBF, _subset_highly_filtered_DBF


def test_filter_items():
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    for i in range(2):
        if i == 0:
            # with output factory parameter:
            filter_RF: Operator[RF, RF] = filter_items[RF, RF](
                filter_predicate=lambda i: i.value.department.name == "Dev",
                output_factory=lambda _: RF(),
            )
        else:
            # without output factory parameter:
            filter_RF: Operator[RF, RF] = filter_items[RF, RF](
                filter_predicate=lambda i: i.value.department.name == "Dev",
            )
        users_filtered: RF = filter_RF(users)
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

    filter_RF: Operator[RF, RF] = filter_items[RF, RF](
        filter_predicate=filter_predicate_complement,
    )
    users_filtered: RF = filter_RF(users)
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
            filter_RF: Operator[RF, RF] = filter_values[RF, RF](
                filter_predicate=lambda v: v.department.name == "Dev",
                output_factory=lambda _: RF(),
            )
        else:
            # without output factory parameter:
            filter_RF: Operator[RF, RF] = filter_values[RF, RF](
                filter_predicate=lambda v: v.department.name == "Dev",
            )
        users_filtered: RF = filter_RF(users)
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
            filter_RF: Operator[RF, RF] = filter_keys[RF, RF](
                filter_predicate=lambda k: k == "d1",
                output_factory=lambda _: RF(),
            )
        else:
            # without output factory parameter:
            filter_RF: Operator[RF, RF] = filter_keys[RF, RF](
                filter_predicate=lambda k: k == "d1",
            )
        departments_filtered: RF = filter_RF(departments)
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


# TODO
def _test_filter_items_multiple_filters():
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

    filter_RF: Operator[RF, RF] = filter_items[RF, RF](
        filter_predicate=lambda item: item.value.department.name == "Dev",
        output_factory=lambda _: RF(),
    )
    users_filtered: RF = filter_RF(filter_RF(users))  # apply filter instance twice
    filter_RF(filter_RF(users)).explain()

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

    filter_RF: Operator[RF, RF] = filter_items[RF, RF](
        filter_predicate=lambda item: item.value.department.name == "Dev",
        output_factory=lambda _: RF(),
    )
    filter_RF2: Operator[RF, RF] = filter_items[RF, RF](
        filter_predicate=lambda item: item.value.department.name == "bla",
        output_factory=lambda _: RF(),
    )

    ret1: RF = filter_RF(users, create_lineage=True)
    ret2: RF = filter_RF2(ret1, create_lineage=True)
    # print("ret2 lineage:")
    lineage: list[str] = ret2.get_lineage()
    # for i, lin in enumerate(lineage, 1):
    #    print(f"{i}.\t->", lin)

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

from fdm.attribute_functions import TF, RF, DBF, RSF, CompositeForeignObject
from fql.operators.filters import filter_items
from fql.util import Item
from tests.lib import _create_testdata


def test_DictionaryTupleRelationDatabaseFunction():
    db: DBF = _create_testdata(frozen=False)
    users: RF = db.users
    departments: RF = db.departments

    assert users[1].department.name == "Dev"
    assert users[2].department.name == "Dev"
    assert users[3].department.name == "Consulting"

    # update the department name using user 1:
    users[1].department.name = "Advisory"
    assert users[1].department.name == "Advisory"
    assert users[2].department.name == "Advisory"

    # should we have the following syntax as well:
    assert users(2)("department").name == "Advisory"

    assert db.departments == departments

    # update the budget of department d1:
    db.departments.d1.budget = "15M"
    assert db.departments.d1.budget == "15M"
    assert users[1].department.budget == "15M"

    # test iterating over users in the database:
    item: Item
    for item in db.users:
        assert isinstance(item.value, TF)
        assert item.value.name in {"Horst", "Tom", "John"}

    # test python-side filtering:
    # comprehension:
    advisory_users = [
        item.value for item in db.users if item.value.department.name == "Advisory"
    ]
    assert len(advisory_users) == 2
    assert {user.name for user in advisory_users} == {"Horst", "Tom"}

    # same with filter operator:
    advisory_users_filter = list(
        filter(lambda i: i.value.department.name == "Advisory", db.users)
    )
    assert len(advisory_users_filter) == 2
    assert {i.value.name for i in advisory_users_filter} == {"Horst", "Tom"}


def test_relationship_function():
    db: DBF = _create_testdata(frozen=True, observe_items=False)
    users: RF = db.users
    customers: RF = db.customers

    # N:M-relationship between users and customers with an additional attribute "date" for each relationship:
    meetings: RSF = RSF(frozen=False)
    assert len(meetings) == 0
    # note that as we are assigning instances, we do not require an extra check like in the relational model that
    # the foreign value "exists"
    meetings[CompositeForeignObject(users[1], customers[1])] = TF(
        {"date": "2024-01-01"}
    )
    meetings[CompositeForeignObject(users[2], customers[1])] = TF(
        {"date": "2025-01-01"}
    )
    meetings[CompositeForeignObject(users[2], customers[3])] = TF(
        {"date": "2026-01-01"}
    )
    assert len(meetings) == 3

    # overwrites the previous meeting between user 2 and customer 1:
    meetings[CompositeForeignObject(users[2], customers[1])] = TF({"date": "202-01-01"})
    assert len(meetings) == 3

    # lookup meetings for user 1:
    res: RF = filter_items(
        meetings,
        filter_predicate=lambda i: i.key.subkey(0) == users[1],
        output_factory=lambda _: RF(),
    ).result
    assert len(res) == 1

    # lookup meetings for user 2:
    res: RF = filter_items(
        meetings,
        filter_predicate=lambda i: i.key.subkey(0) == users[2],
        output_factory=lambda _: RF(),
    ).result
    assert len(res) == 2

    # same through the more convenient syntax — now with separate match and return indices:
    # match_index=0 (user position), return_index=1 (customer position):
    user1_customers = list(meetings.related_values(0, users[1], 1))
    assert len(user1_customers) == 1
    assert user1_customers[0] is customers[1]

    user2_customers = list(meetings.related_values(0, users[2], 1))
    assert len(user2_customers) == 2
    assert customers[1] in user2_customers
    assert customers[3] in user2_customers

    # reverse lookup: match_index=1 (customer position), return_index=0 (user position):
    customer1_users = list(meetings.related_values(1, customers[1], 0))
    assert len(customer1_users) == 2
    assert users[1] in customer1_users
    assert users[2] in customer1_users

    customer3_users = list(meetings.related_values(1, customers[3], 0))
    assert len(customer3_users) == 1
    assert customer3_users[0] is users[2]


def test_key_constraint():
    # This is implicitly and automatically given as the dictionary attribute function will not allow this!
    # In contrast, in the relational model this has to be tested explicitly; in FDM this is automatically guaranteed
    # for all attribute functions like TFs, RFs, DBFs, etc.!
    # In addition, also for the results from FQL operators, duplicate keys cannot occur. This is again in sharp
    # contrast to SQL where this confusion may happen.

    assert True


def test_composite_foreign_object_contains_and_len():
    """Verify __contains__ and __len__ on CompositeForeignObject."""
    tf1: TF = TF({"name": "A"})
    tf2: TF = TF({"name": "B"})
    tf3: TF = TF({"name": "C"})
    cfo: CompositeForeignObject = CompositeForeignObject(tf1, tf2)

    assert tf1 in cfo
    assert tf2 in cfo
    assert tf3 not in cfo
    assert len(cfo) == 2

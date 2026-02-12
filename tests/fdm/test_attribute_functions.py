from copy import copy

import pytest

from fdm.attribute_functions import (
    DictionaryAttributeFunction,
    TF,
    RF,
    DBF,
    CompositeKey,
)
from fdm.schema import Schema
from fql.predicates.constraints import (
    max_count,
)
from fql.util import (
    Item,
    ConstraintViolationError,
    ConstraintViolationErrorFromOutside,
    ReadOnlyError,
)
from tests.lib import _create_testdata, _subset_DBF


def test_DictionaryAttributeFunction():
    daf = DictionaryAttributeFunction(data={"x": 0, "y": 0, 42: "answer"})

    # accessing a non-existing attribute raises AttributeError
    with pytest.raises(AttributeError):
        assert daf.z == 0

    # check existing attributes:
    assert "x" in daf
    assert "y" in daf
    assert "z" not in daf

    # int attributes
    assert 3 not in daf
    assert 42 in daf
    assert daf[42] == "answer"
    assert daf.x == 0
    assert daf.y == 0

    assert daf.uuid == 0

    # check assigning to an existing attribute:
    daf.x = 42
    assert daf.x == 42

    daf[42] = "a new answer"
    assert daf[42] == "a new answer"

    # create/assign to a new attribute:
    daf.z = 100

    assert daf.z == 100
    assert "z" in daf
    assert len(daf) == 4

    daf[43] = "another answer"
    assert daf[43] == "another answer"
    assert 43 in daf

    daf[42.42] = "float answer"
    assert daf[42.42] == "float answer"
    assert 42.42 in daf

    # delete an attribute:
    del daf.z
    assert "z" not in daf
    assert len(daf) == 5

    # delete int and float attributes
    del daf[42]
    assert 42 not in daf
    assert len(daf) == 4

    del daf[42.42]
    assert 42.42 not in daf
    assert len(daf) == 3


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


# TODO
def test_function_observers():
    # those tuples may be referenced anywhere else: maybe we need an event mechanism to notify dependent functions?
    db: DBF = _create_testdata(frozen=False, observe_items=True)
    users: RF = db.users
    departments: RF = db.departments
    customers: RF = db.customers

    # test that all TPs have the relation as observer:
    for i in range(1, len(users) + 1):
        assert users[i].__dict__["observers"] == [users]

    for i in range(1, len(customers) + 1):
        assert customers[i].__dict__["observers"] == [customers]

    for i in range(1, len(departments) + 1):
        assert departments[f"d{i}"].__dict__["observers"] == [departments]

    # test that all RFs have the DBF as observer:
    assert users.__dict__["observers"] == [db]
    assert customers.__dict__["observers"] == [db]
    assert departments.__dict__["observers"] == [db]

    # now change an attribute in a tuple and see that the observers are notified:
    users[1].department.name = "NewDeptName"

    assert users[1].department.name == "NewDeptName"
    assert departments.d1.name == "NewDeptName"

    # test that constraint violations are also caught with observers enabled:
    # as before, this one is caught in the RF:
    # with pytest.raises(ConstraintViolationError):
    #    users[0] = TF({"namde": "Alice", "yob": 1990, "department": db.departments.d1})

    # but this one is not, as the TP is created first, then the constraint is checked through the observer mechanism:
    # TODO: fix the following
    # with pytest.raises(ConstraintViolationErrorFromOutside):
    #    tf: TF = users[1]
    #    tf.dsf = "Alice"

    # no rollback happened, as the change was triggered through the observer mechanism
    # also see the message in ConstraintViolationErrorFromOutside
    # assert users[1].dsf == "Alice"


def test_relationship_function():
    db: DBF = _create_testdata(frozen=True, observe_items=False)
    users: RF = db.users
    customers: RF = db.customers

    # N:M-relationship between users and customers with an additional attribute "date" for each relationship:
    meetings: RF = RF(frozen=False)
    assert len(meetings) == 0
    # note that as we are assigning instances, we do not require an extra check like in the relational model that
    # the foreign key "exists"
    meetings[CompositeKey([users[1], customers[1]])] = TF({"date": "2024-01-01"})
    meetings[CompositeKey([users[2], customers[1]])] = TF({"date": "2024-01-01"})
    meetings[CompositeKey([users[2], customers[3]])] = TF({"date": "2024-01-01"})
    assert len(meetings) == 3

    # overwrites the previous meeting between user 2 and customer 1:
    meetings[CompositeKey([users[2], customers[1]])] = TF({"date": "2025-01-01"})
    assert len(meetings) == 3


def test_schema():
    user = _subset_DBF({"users"}, frozen=False).users[1]

    # create a schema that requires the keys "name", "yob" and "department" with any types:
    user_schema = Schema()
    user_schema["name"] = str
    user_schema["yob"] = int
    user_schema["department"] = TF
    user_schema.freeze()

    assert user_schema(user)

    user_wrong = _subset_DBF({"users"}, frozen=False).users[1]
    user_wrong["extra_key"] = "extra_value"
    assert user_schema(user_wrong) == False

    user_wrong = _subset_DBF({"users"}, frozen=False).users[1]
    user_wrong["name"] = "asd"

    assert user_schema(user_wrong) == True

    # TODO: add schema to RF and check that it is enforced on all items:
    users: RF = _subset_DBF({"users"}, frozen=True).users

    # still frozen, cannot work:
    with pytest.raises(ReadOnlyError):
        users.add_items_constraint(user_schema)
    users.unfreeze()

    # now it works:
    users.add_items_constraint(user_schema)

    # wrong key:
    with pytest.raises(ConstraintViolationError):
        users[4] = TF(
            {"namde": "Alice", "yob": 1990, "department": users[1].department}
        )

    # wrong type for "yob":
    with pytest.raises(ConstraintViolationError):
        users[4] = TF(
            {"name": "Alice", "yob": "1990", "department": users[1].department}
        )

    # wrong type for "department":
    with pytest.raises(ConstraintViolationError):
        users[4] = TF({"name": "Alice", "yob": "1984", "department": RF})

    # this works:
    users[4] = TF({"name": "Alice", "yob": 1990, "department": users[1].department})

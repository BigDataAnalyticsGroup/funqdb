import pytest

from fql.functions import DictionaryAttributeFunction, TF, RF, DBF
from fql.predicates.constraints import (
    attribute_name_equivalence_item,
    max_count,
)
from fql.util import Item, ConstraintViolationError
from tests.lib import _create_testdata


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


def test_attribute_functions_item_ans_self_constraints_wo_observers():

    db: DBF = _create_testdata(frozen=False, observe_values=False)
    users: RF = db.users
    users.add_items_constraint(
        attribute_name_equivalence_item({"name", "yob", "department"})
    )
    assert 0 not in users

    # newly added user only with valid attribute:
    users[0] = TF({"name": "Alice", "yob": 1990, "department": db.departments.d1})

    with pytest.raises(ConstraintViolationError):
        users[0] = TF({"namde": "Alice", "yob": 1990, "department": db.departments.d1})
    with pytest.raises(ConstraintViolationError):
        users[1] = TF({"name": "Alice", "yob": 1990, "gd": db.departments.d1})

    del users[0]
    assert len(users) == 3

    users[1].dsf = 42
    users.add_self_constraint(max_count(3))
    users[3].dsf = 42

    with pytest.raises(ConstraintViolationError):
        users[4] = TF({"name": "Timmy", "yob": 1990, "department": db.departments.d1})

    # item was inserted and then rolled back due to constraint violation, users must still have 3 items only:
    assert len(users) == 3
    assert {1, 2, 3} == set(users.keys())


def test_subscriptions():
    pass

    # those tuples may be referenced anywhere else: maybe we need an event mechanism to notify dependent functions?
    # TODO

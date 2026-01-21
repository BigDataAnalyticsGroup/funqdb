import pytest

from lib.functions import DictionaryAttributeFunction, TF, RF, DBF, Item
from lib.operators import Operator, Map


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


def _create_testdata():
    # departments tuples:
    d1: TF = TF({"name": "Dev", "budget": "11M"})
    d2: TF = TF({"name": "Consulting", "budget": "22M"})
    # departments relation:
    departments: RF = RF({"d1": d1, "d2": d2})

    # users tuples:
    t1: TF = TF({"name": "Horst", "department": d1})
    t2: TF = TF({"name": "Tom", "department": d1})
    t3: TF = TF({"name": "John", "department": d2})
    # users relation:
    users: RF = RF({1: t1, 2: t2, 3: t3})

    # database of relations:
    db: DBF = DBF({"departments": departments, "users": users})
    return db


def test_DictionaryTupleRelationDatabaseFunction():
    db: DBF = _create_testdata()
    users: RF = db.users
    departments: RF = db.departments

    assert users[1].department.name == "Dev"
    assert users[2].department.name == "Dev"
    assert users[3].department.name == "Consulting"

    # update the department name using user 1:
    users[1].department.name = "Advisory"
    assert users[1].department.name == "Advisory"
    assert users[2].department.name == "Advisory"

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
    advisory_users = [item.value for item in db.users if item.value.department.name == "Advisory"]
    assert len(advisory_users) == 2
    assert {user.name for user in advisory_users} == {"Horst", "Tom"}

    # same with filter operator:
    advisory_users_filter = list(filter(lambda i: i.value.department.name == "Advisory", db.users))
    assert len(advisory_users_filter) == 2
    assert {i.value.name for i in advisory_users_filter} == {"Horst", "Tom"}


def test_operators():
    db: DBF = _create_testdata()
    users: RF = db.users

    map: Operator[RF, RF] = Map[RF, RF]()

    def mapping_function(el: Item) -> Item:
        el.value.name = el.value.name + " User"
        return el

    db2: DBF = _create_testdata()
    users_old = db2.users
    users_old_iter = iter(users_old)

    i: Item[int, TF]
    for i in map(mapping_function, users):
        assert i.value.name == next(users_old_iter).value.name + " User"

    db: DBF = _create_testdata()
    users: RF = db.users
    users_old_iter = iter(users_old)
    # same thing with a lambda
    for i in map(lambda i: Item(i.key, i.value.update("name", i.value.name + " User")), users):
        assert i.value.name == next(users_old_iter).value.name + " User"

    users_old_iter = iter(users_old)
    for i in map(lambda i: Item(i.key, i.value.update("name", i.value.name + " User")), users):
        assert i.value.name == next(users_old_iter).value.name + " User User"

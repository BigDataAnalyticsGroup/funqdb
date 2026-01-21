import pytest

from lib.functions import DictionaryAttributeFunction, TF, RF, DBF


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

    assert users[1].department.name == "Dev"
    assert users[2].department.name == "Dev"
    assert users[3].department.name == "Consulting"

    # update the department name using user 1:
    users[1].department.name = "Advisory"
    assert users[1].department.name == "Advisory"
    assert users[2].department.name == "Advisory"

    # database of relations:
    db: DBF = DBF({"departments": departments, "users": users})
    assert db.departments == departments

    # update the budget of department d1:
    db.departments.d1.budget = "15M"
    assert db.departments.d1.budget == "15M"
    assert users[1].department.budget == "15M"

    # test iterating over users in the database:
    for user in db.users:
        assert isinstance(user, TF)
        assert user.name in {"Horst", "Tom", "John"}

    # test python-side filtering:
    # comprehension:
    advisory_users = [user for user in db.users if user.department.name == "Advisory"]
    assert len(advisory_users) == 2
    assert {user.name for user in advisory_users} == {"Horst", "Tom"}

    # same with filter operator:
    advisory_users_filter = list(filter(lambda u: u.department.name == "Advisory", db.users))
    assert len(advisory_users_filter) == 2
    assert {user.name for user in advisory_users_filter} == {"Horst", "Tom"}


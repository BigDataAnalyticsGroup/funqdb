import pytest

from fdm.attribute_functions import TF, RF
from fdm.schema import Schema
from fql.util import ReadOnlyError, ConstraintViolationError
from tests.lib import _subset_DBF


def test_schema_constraint():
    user = _subset_DBF({"users"}, frozen=False).users[1]

    # create a schema that requires the keys "name", "yob" and "department" with any types:
    user_schema = Schema({"name": str, "yob": int, "department": TF})
    assert user_schema(user)

    user_wrong = _subset_DBF({"users"}, frozen=False).users[1]
    user_wrong["extra_key"] = "extra_value"
    assert user_schema(user_wrong) == False

    user_wrong = _subset_DBF({"users"}, frozen=False).users[1]
    user_wrong["name"] = "asd"

    assert user_schema(user_wrong) == True

    # add schema to RF and check that it is enforced on all values of the RF:
    users: RF = _subset_DBF({"users"}, frozen=True).users

    # RF still frozen, cannot work:
    with pytest.raises(ReadOnlyError):
        users.add_values_constraint(user_schema)
    users.unfreeze()

    # now it works:
    users.add_values_constraint(user_schema)

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

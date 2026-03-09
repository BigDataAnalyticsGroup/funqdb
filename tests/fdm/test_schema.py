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
from fdm.schema import Schema, ForeignKeyConstraint
from fql.util import ReadOnlyError, ConstraintViolationError
from tests.lib import _subset_DBF


def test_schema_constraint():
    user = _subset_DBF({"users"}, frozen=False).users[1]

    # create a schema that requires the keys "name", "yob" and "department" with any types:
    # TODO: maybe we could be more precise here in directly specifying that it is not only some TF but rather
    # TFs from a specific RF that we expect here for the department key?
    # i.e. to express a foreign key constraint here that the department key must be a TF from the departments RF?
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


def test_foreign_key_constraint():
    # so what is a foreign key constraint anyway? It says that we reference an attribute function (tuple function)
    # that is mapped to by another relation function
    # for the departments referenced in the users relation, we want to express that the department key must reference
    # a tuple function that is mapped to by the departments relation
    # -> a foreign key constraint is a value constraint
    # -> this might be interesting for relationship functions to then also add key constraints
    db: DBF = _subset_DBF({"users", "departments"}, frozen=False)
    departments: RF = db.departments
    users: RF = db.users
    user: TF = users[1]

    # if we do it as an attribute function constraint, we have to look up the value in the parent which is not indexed

    # for all values in users: the key "department" must map to a value contained in departments:
    users.add_values_constraint(ForeignKeyConstraint("department", departments))

    users[4] = TF({"namde": "Alice", "yob": 1990, "department": departments.d1})
    with pytest.raises(ConstraintViolationError):
        users[5] = TF({"namde": "Alice", "yob": 1990, "department": users[2]})
    with pytest.raises(ConstraintViolationError):
        users[6] = TF({"namde": "Alice", "yob": 1990, "department": TF()})

    # TODO: reverse constraint, i.e. if we delete department d1 from departments,
    # how to fix:
    # (1.) some sort of ref counting in departments, if ref exists, do not allow delete
    # (2.) through observer mechanism: if we delete d1, we notify users, users check if any of their items reference d1,
    # if yes, raise an error and roll back the delete in departments

    # the following delete should trigger a warning, however currently it does not:
    del departments.d1
    assert "d1" not in departments

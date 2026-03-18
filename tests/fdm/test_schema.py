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
from fdm.schema import Schema, ForeignValueConstraint, ReverseForeignObjectConstraint
from fql.util import ReadOnlyError, ConstraintViolationError, ChangeEvent
from tests.lib import _subset_DBF


def test_schema_constraint():
    user = _subset_DBF({"users"}, frozen=False).users[1]

    # create a schema that requires the foreign_objects "name", "yob" and "department" with any types:
    user_schema = Schema({"name": str, "yob": int, "department": TF})
    assert user_schema(user, ChangeEvent.UPDATE)

    user_wrong = _subset_DBF({"users"}, frozen=False).users[1]
    user_wrong["extra_key"] = "extra_value"
    assert user_schema(user_wrong, ChangeEvent.UPDATE) == False

    user_wrong = _subset_DBF({"users"}, frozen=False).users[1]
    user_wrong["name"] = "asd"

    assert user_schema(user_wrong, ChangeEvent.UPDATE) == True

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


def test_foreign_value_constraint():
    # so what is a foreign value constraint anyway? It says that we reference an attribute function (tuple function)
    # that is mapped to by another relation function
    # for the departments referenced in the users relation, we want to express that the department key must reference
    # a tuple function that is mapped to by the departments relation
    # -> a foreign value constraint is a value constraint
    # -> this might be interesting for relationship functions to then also add key constraints
    db: DBF = _subset_DBF({"users", "departments"}, frozen=False)
    departments: RF = db.departments
    users: RF = db.users

    # for all values in users: the key "department" must map to a value contained in departments:
    users.add_values_constraint(ForeignValueConstraint("department", departments))

    users[4] = TF({"namde": "Alice", "yob": 1990, "department": departments.d1})
    with pytest.raises(ConstraintViolationError):
        users[5] = TF({"namde": "Alice", "yob": 1990, "department": users[2]})
    with pytest.raises(ConstraintViolationError):
        users[6] = TF({"namde": "Alice", "yob": 1990, "department": TF()})

    # add reverse constraint to departments, i.e. if we delete a department that is referenced by users, we should get
    # an error, see discussion below (3.):
    departments.add_values_constraint(
        ReverseForeignObjectConstraint("department", users)
    )

    # TODO, usability: should be able to add the constraint with a single method call
    #

    with pytest.raises(ConstraintViolationError):
        del departments.d1
    # there must still be 2 departments, as the delete operation should have been rolled back:
    assert len(departments) == 2
    assert "d1" in departments

    # same thing with the convenient method to add the reverse constraint at the same time as the forward constraint:
    db: DBF = _subset_DBF({"users", "departments"}, frozen=False)
    departments: RF = db.departments
    users: RF = db.users

    users.references("department", departments)

    with pytest.raises(ConstraintViolationError):
        del departments.d1
    # there must still be 2 departments, as the delete operation should have been rolled back:
    assert len(departments) == 2
    assert "d1" in departments
    # how to fix:
    # (1.) some sort of ref counting in departments, if ref exists, do not allow delete, already an optimization
    # (2.) through observer mechanism: if we delete d1, we notify users, users check if any of their items reference d1,
    # if yes, raise an error and roll back the delete in departments, again: for observers, this has to be decided
    # how to handle it with the store
    # (3.) an actual reverse constraint, i.e. if we add an fk to users why not add at the same time a reverse constraint
    # to departments that says: if any of my items is referenced by users, do not allow delete in departments
    # actually the cleanest and purest approach in my understanding of the FDM would be (3.), as it is a pure
    # constraint that is independent of the implementation and does not require any special handling in the store,
    # but it is also the most expensive one, as we have to check for all items in departments if they are referenced
    # by users, which is O(n) in the number of items in departments, while (1.) and (2.) can be implemented in O(1)
    # time. So maybe we can do (1.) or (2.) as an optimization, but also add (3.) as a pure constraint that can be
    # used if we do not want to rely on the optimization. For now, we do (3.) and then we can add (1.) or (2.) as an
    # optimization later.

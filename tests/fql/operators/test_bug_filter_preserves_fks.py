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
"""Regression tests: filter operators must preserve foreign-key constraints.

Bug: ``where`` / ``filter_items`` / ``filter_values`` built a fresh empty AF and copied
only key/value pairs, silently dropping the input's ``ForeignValueConstraint`` (FK). A
filtered relation therefore lost its reference metadata, so a downstream ``semijoin`` /
``join`` could no longer detect the reference direction.

Expected: the outgoing FK (``ForeignValueConstraint``) survives filtering, in the same
spirit as ``semijoin`` preserving its constraints (``semijoin`` copies all constraints;
filtering copies only the ForeignValueConstraints). The reverse-side
``ReverseForeignObjectConstraint`` is intentionally NOT carried onto a filtered subset.

Reference shape under test (2D, directed FK edge points source -> target):

    +-------+  department (FK)   +-------------+
    | users | -----------------> | departments |
    +-------+                    +-------------+
"""

import pytest

from fdm.attribute_functions import TF, RF, DBF, ConstraintViolationError
from fdm.schema import ForeignValueConstraint, ReverseForeignObjectConstraint
from fql.operators.filters import filter_items, filter_values
from fql.operators.semijoins import semijoin


def _dept_users() -> tuple[RF, RF]:
    """Two relations where ``users`` references ``departments`` via the ``department`` FK.

        +-------+  department (FK)   +-------------+
        | users | -----------------> | departments |
        +-------+                    +-------------+

    departments: d1 (Dev), d2 (Sales)
    users: Horst->d1, Tom->d1, John->d2
    """
    departments: RF = RF(  # target relation of the FK
        {
            "d1": TF({"name": "Dev"}),  # referenced by Horst and Tom
            "d2": TF({"name": "Sales"}),  # referenced by John
        },
        frozen=False,  # keep writable so .references() can install the constraint
    )
    users: RF = RF(  # source relation that holds the FK
        {
            1: TF(
                {"name": "Horst", "yob": 1972, "department": departments["d1"]}
            ),  # -> d1
            2: TF(
                {"name": "Tom", "yob": 1983, "department": departments["d1"]}
            ),  # -> d1
            3: TF(
                {"name": "John", "yob": 2003, "department": departments["d2"]}
            ),  # -> d2
        },
        frozen=False,  # keep writable so .references() can install the constraint
    ).references(
        "department", departments
    )  # install FVC on users + RFOC on departments
    return users, departments  # hand both relations back to the caller


def _has_department_fk(relation: RF) -> bool:
    """True iff ``relation`` still carries the outgoing ``department`` ForeignValueConstraint."""
    return any(  # scan the value-level constraint set for the outgoing FK
        isinstance(c, ForeignValueConstraint) and c.key == "department"
        for c in relation.__dict__["values_constraints"]
    )


def test_where_preserves_foreign_value_constraint():
    """``.where()`` on an FK-carrying relation keeps its outgoing ForeignValueConstraint.

    +-------+  department (FK)   +-------------+
    | users | -----------------> | departments |
    +-------+                    +-------------+
    """
    users, _departments = _dept_users()  # users holds the department FK
    assert _has_department_fk(users)  # precondition: the FK exists before filtering
    filtered: RF = users.where(yob__lt=2000)  # filter to Horst + Tom (both -> d1)
    assert len(filtered) == 2  # sanity: two users survive the filter
    assert _has_department_fk(filtered)  # the outgoing FK must survive the filter


def test_filter_items_preserves_foreign_value_constraint():
    """``filter_items`` on an FK-carrying relation keeps its outgoing ForeignValueConstraint.

    +-------+  department (FK)   +-------------+
    | users | -----------------> | departments |
    +-------+                    +-------------+
    """
    users, _departments = _dept_users()  # users holds the department FK
    filtered: RF = filter_items[RF, RF](  # keep only users whose department is Dev
        users,
        filter_predicate=lambda i: i.value.department.name == "Dev",
    ).result  # materialize the operator output
    assert len(filtered) == 2  # sanity: Horst + Tom are in Dev
    assert _has_department_fk(filtered)  # the outgoing FK must survive the filter


def test_filter_values_preserves_foreign_value_constraint():
    """``filter_values`` on an FK-carrying relation keeps its outgoing ForeignValueConstraint.

    +-------+  department (FK)   +-------------+
    | users | -----------------> | departments |
    +-------+                    +-------------+
    """
    users, _departments = _dept_users()  # users holds the department FK
    filtered: RF = filter_values[RF, RF](  # filter phrased directly on the value
        users,
        filter_predicate=lambda v: v.name == "John",
    ).result  # materialize the operator output
    assert len(filtered) == 1  # sanity: only John matches
    assert _has_department_fk(filtered)  # the outgoing FK must survive the filter


def test_where_does_not_carry_reverse_constraint_onto_filtered_target():
    """Filtering the TARGET relation must NOT carry the ``ReverseForeignObjectConstraint`` (RFOC), the reverse-delete guard.

        +-------+  department (FK)   +-------------+
        | users | -----------------> | departments |
        +-------+                    +-------------+

    RFOC lives on ``departments`` (the target) and points back at the full original
    ``users``; carrying it onto a filtered subset would validate deletes against the
    whole original source, so the fix deliberately drops it.
    """
    _users, departments = _dept_users()  # departments carries the reverse guard (RFOC)
    assert any(  # precondition: the RFOC exists on the unfiltered target
        isinstance(c, ReverseForeignObjectConstraint)
        for c in departments.__dict__["values_constraints"]
    )
    filtered: RF = departments.where(name="Dev")  # filter the target down to Dev only
    assert not any(  # the reverse guard must NOT be copied onto the filtered subset
        isinstance(c, ReverseForeignObjectConstraint)
        for c in filtered.__dict__["values_constraints"]
    )


def test_filtered_relation_still_semijoinable():
    """End-to-end: a filtered source RF still drives a ``semijoin`` via its surviving FK.

        +-------+  department (FK)   +-------------+
        | users | -----------------> | departments |
        +-------+                    +-------------+

    Filter ``users`` to Horst + Tom (both in Dev), then semijoin-reduce ``departments`` by
    the filtered ``users``. Without the surviving FK, direction detection raises ValueError.
    """
    users, departments = _dept_users()  # users holds the department FK
    filtered_users: RF = users.where(yob__lt=2000)  # Horst + Tom, both -> d1 (Dev)
    dbf: DBF = DBF(  # assemble a query DBF from the filtered source + its target
        {
            "departments": departments,
            "users": filtered_users,
        }
    )
    reduced: DBF = semijoin[
        DBF, DBF
    ](  # reduce departments to those referenced by users
        dbf,
        reduce="departments",
        by="users",
        ref_key="department",
    ).result  # materialize the operator output
    assert set(reduced.departments.keys()) == {
        "d1"
    }  # only Dev is referenced -> only d1 survives


def test_write_respecting_fk_into_filtered_where_result_succeeds():
    """A write into a ``where()`` result whose reference exists in the target is accepted.

        +-------+  department (FK)   +-------------+
        | users | -----------------> | departments |
        +-------+                    +-------------+

    The where() result is returned unfrozen and now carries the FK, so the copied
    ForeignValueConstraint validates the new row's reference against the (unchanged) target.
    """
    users, departments = _dept_users()  # users holds the department FK
    filtered: RF = users.where(yob__lt=2000)  # Horst + Tom (unfrozen result)
    filtered[4] = TF(  # add a new user whose department is a real target tuple
        {"name": "Erna", "yob": 1990, "department": departments["d1"]}
    )  # departments["d1"] exists in the target -> FK satisfied
    assert filtered[4].name == "Erna"  # the write was accepted and is readable back


def test_write_violating_fk_into_filtered_where_result_is_rejected():
    """A write into a ``where()`` result whose reference is absent from the target is rejected.

        +-------+  department (FK)   +-------------+
        | users | -----------------> | departments |
        +-------+                    +-------------+

    The surviving ForeignValueConstraint must reject a row that points at a department tuple
    which is not part of the target relation, raising ConstraintViolationError.
    """
    users, _departments = _dept_users()  # users holds the department FK
    filtered: RF = users.where(yob__lt=2000)  # Horst + Tom (unfrozen result)
    rogue_department: TF = TF(
        {"name": "Ghost"}
    )  # a department NOT in the target relation
    with pytest.raises(
        ConstraintViolationError
    ):  # the FK must reject the dangling reference
        filtered[4] = TF(  # attempt to add a user pointing at the rogue department
            {"name": "Erna", "yob": 1990, "department": rogue_department}
        )  # reference value absent from target -> ConstraintViolationError

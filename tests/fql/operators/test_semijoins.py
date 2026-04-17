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
from fql.operators.semijoins import semijoin, _find_ref_direction, RefDirection

# ---------------------------------------------------------------------------
# Helpers -- small, self-contained datasets
# ---------------------------------------------------------------------------


def _dept_users_dbf() -> tuple[DBF, RF, RF]:
    """Departments and users where users reference departments via 'dept'.

    departments: d1 (Dev), d2 (Sales), d3 (Research -- unreferenced)
    users: u1->d1, u2->d1, u3->d2
    """
    departments: RF = RF(
        {
            "d1": TF({"name": "Dev"}),
            "d2": TF({"name": "Sales"}),
            "d3": TF({"name": "Research"}),
        },
        frozen=False,
    )

    users: RF = RF(
        {
            "u1": TF({"name": "Alice", "dept": departments["d1"]}),
            "u2": TF({"name": "Bob", "dept": departments["d1"]}),
            "u3": TF({"name": "Carol", "dept": departments["d2"]}),
        },
        frozen=False,
    ).references("dept", departments)

    users.freeze()
    departments.freeze()

    dbf: DBF = DBF(
        {"departments": departments, "users": users},
        frozen=True,
    )
    return dbf, departments, users


def _orders_customers_dbf() -> tuple[DBF, RF, RF]:
    """Star-schema fragment: orders reference customers via 'customer'.

    customers: c1 (Alice), c2 (Bob), c3 (Charlie -- unreferenced)
    orders: o1->c1, o2->c2
    """
    customers: RF = RF(
        {
            "c1": TF({"name": "Alice"}),
            "c2": TF({"name": "Bob"}),
            "c3": TF({"name": "Charlie"}),
        },
        frozen=False,
    )

    orders: RF = RF(
        {
            "o1": TF({"amount": 100, "customer": customers["c1"]}),
            "o2": TF({"amount": 200, "customer": customers["c2"]}),
        },
        frozen=False,
    ).references("customer", customers)

    orders.freeze()
    customers.freeze()

    dbf: DBF = DBF(
        {"customers": customers, "orders": orders},
        frozen=True,
    )
    return dbf, customers, orders


def _dept_users_with_dangling_ref_dbf() -> tuple[DBF, RF, RF]:
    """Users reference departments, but one user references a department that
    is NOT in the departments RF passed to the DBF (simulates a dangling ref
    by building a separate TF not contained in the target RF).

    departments: d1 (Dev)
    users: u1->d1, u2->d_orphan (d_orphan is a TF not in departments)
    """
    departments: RF = RF(
        {
            "d1": TF({"name": "Dev"}),
        },
        frozen=False,
    )

    # Create an orphan TF that lives outside the departments RF
    d_orphan: TF = TF({"name": "Ghost"})

    users: RF = RF(
        {
            "u1": TF({"name": "Alice", "dept": departments["d1"]}),
            "u2": TF({"name": "Bob", "dept": d_orphan}),
        },
        frozen=False,
    ).references("dept", departments)

    users.freeze()
    departments.freeze()

    dbf: DBF = DBF(
        {"departments": departments, "users": users},
        frozen=True,
    )
    return dbf, departments, users


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_find_ref_direction_child_is_reduce() -> None:
    """When reduce RF has the ForeignValueConstraint, direction is 'reduce'."""
    _, departments, users = _dept_users_dbf()
    assert _find_ref_direction(users, departments, "dept") is RefDirection.REDUCE


def test_find_ref_direction_child_is_by() -> None:
    """When by RF has the ForeignValueConstraint, direction is 'by'."""
    _, departments, users = _dept_users_dbf()
    assert _find_ref_direction(departments, users, "dept") is RefDirection.BY


def test_find_ref_direction_invalid_ref_key() -> None:
    """A ref_key not registered as ForeignValueConstraint raises ValueError."""
    _, departments, users = _dept_users_dbf()
    with pytest.raises(ValueError, match="No ForeignValueConstraint"):
        _find_ref_direction(users, departments, "nonexistent")


def test_semijoin_reduce_parent_by_child() -> None:
    """Reduce departments by users (target reduced by source references).

    Department d3 has no users referencing it, so it must be removed.
    Departments d1 and d2 survive because they are referenced.
    """
    dbf, departments, users = _dept_users_dbf()

    result_dbf: DBF = semijoin[DBF, DBF](
        dbf, reduce="departments", by="users", ref_key="dept"
    ).result

    result_keys: set[str] = {item.key for item in result_dbf.departments}
    assert result_keys == {"d1", "d2"}, f"Expected d1, d2 but got {result_keys}"
    assert len(result_dbf.departments) == 2
    # users must be unchanged
    assert {item.key for item in result_dbf.users} == {"u1", "u2", "u3"}


def test_semijoin_reduce_child_by_parent_all_kept() -> None:
    """Reduce users by departments (source reduced by target).

    All three users reference departments that exist in the departments RF,
    so every user must survive the semi-join.
    """
    dbf, departments, users = _dept_users_dbf()

    result_dbf: DBF = semijoin[DBF, DBF](
        dbf, reduce="users", by="departments", ref_key="dept"
    ).result

    result_keys: set[str] = {item.key for item in result_dbf.users}

    assert result_keys == {
        "u1",
        "u2",
        "u3",
    }, f"Expected all users but got {result_keys}"


def test_semijoin_reduce_child_with_dangling_reference() -> None:
    """Reduce users by departments when one user has a dangling reference.

    User u2 references a TF (d_orphan) that is not in the departments RF.
    When reducing users by departments, u2 must be filtered out because its
    dept reference does not point to any TF in departments.
    """
    dbf, departments, users = _dept_users_with_dangling_ref_dbf()

    result_dbf: DBF = semijoin[DBF, DBF](
        dbf, reduce="users", by="departments", ref_key="dept"
    ).result

    result_keys: set[str] = {item.key for item in result_dbf.users}

    assert result_keys == {
        "u1"
    }, f"Expected only u1 (u2 has dangling ref) but got {result_keys}"


def test_semijoin_star_schema_reduce_dimension() -> None:
    """Star schema: reduce customers by orders via 'customer' ref_key.

    Customer c3 is not referenced by any order, so it must be removed.
    Customers c1 and c2 survive.
    """
    dbf, customers, orders = _orders_customers_dbf()

    result_dbf: DBF = semijoin[DBF, DBF](
        dbf, reduce="customers", by="orders", ref_key="customer"
    ).result

    result_keys: set[str] = {item.key for item in result_dbf.customers}

    assert result_keys == {"c1", "c2"}, f"Expected c1, c2 but got {result_keys}"


def test_semijoin_invalid_ref_key_raises_value_error() -> None:
    """Passing a ref_key that does not correspond to any ForeignValueConstraint
    between the two RFs must raise a ValueError.
    """
    dbf, _, _ = _dept_users_dbf()

    with pytest.raises(ValueError, match="No ForeignValueConstraint"):

        _ = semijoin[DBF, DBF](
            dbf, reduce="departments", by="users", ref_key="nonexistent"
        ).result


def test_semijoin_chaining_three_level() -> None:
    """Chain two semijoins on a three-level hierarchy: tasks -> projects -> departments.

    This kind of chaining enables a full-blown Yannakakis algorithm.

    Only project p1 (in department d1) has tasks. After chaining:
    1. First semijoin reduces projects by tasks → only p1 survives.
    2. Second semijoin reduces departments by (reduced) projects → only d1 survives.
    """
    departments: RF = RF(
        {
            "d1": TF({"name": "Dev"}),
            "d2": TF({"name": "Sales"}),
        },
        frozen=False,
    )

    projects: RF = RF(
        {
            "p1": TF({"title": "Alpha", "dept": departments["d1"]}),
            "p2": TF({"title": "Beta", "dept": departments["d2"]}),
        },
        frozen=False,
    ).references("dept", departments)

    tasks: RF = RF(
        {
            "t1": TF({"desc": "Design", "project": projects["p1"]}),
            "t2": TF({"desc": "Implement", "project": projects["p1"]}),
        },
        frozen=False,
    ).references("project", projects)

    departments.freeze()
    projects.freeze()
    tasks.freeze()

    dbf: DBF = DBF(
        {"departments": departments, "projects": projects, "tasks": tasks},
        frozen=True,
    )

    # chain: first reduce projects by tasks, then reduce departments by projects
    step1: DBF = semijoin[DBF, DBF](
        dbf, reduce="projects", by="tasks", ref_key="project"
    ).result
    step2: DBF = semijoin[DBF, DBF](
        step1, reduce="departments", by="projects", ref_key="dept"
    ).result

    assert {item.key for item in step2.departments} == {"d1"}
    assert {item.key for item in step2.projects} == {"p1"}
    assert {item.key for item in step2.tasks} == {"t1", "t2"}

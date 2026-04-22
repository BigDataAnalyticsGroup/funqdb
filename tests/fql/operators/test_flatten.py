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

"""Happy-path and error tests for the ``flatten`` operator.

``flatten`` converts a nested RF — typically the output of ``join`` — into
SQL-style flat rows where every attribute key is a dot-separated path
(``"relation.attribute"``).  Nested TF references stored inside a source TF
(e.g. the ``dept`` attribute of a ``users`` tuple) are themselves recursively
expanded rather than dropped, so both ``"users.name"`` and
``"users.dept.name"`` appear in the output row.  Computed attributes are
materialised at flatten time.

Tests cover:
1. Two-relation join (users → departments) flattened correctly.
2. Three-relation linear chain (tasks → projects → departments) flattened.
3. Star schema (orders → customers, orders → products) flattened.
4. Single-relation trivial case via ``join`` then ``flatten``.
5. Deep manual nesting (three TF levels) expanded to a single leaf key.
6. Computed attribute materialised as a scalar in the flat output.
7. TypeError raised when the input is not an RF.
"""

import pytest

from fdm.attribute_functions import TF, RF, DBF
from fql.operators.flatten import flatten
from fql.operators.joins import join

# ---------------------------------------------------------------------------
# Fixtures (self-contained builders; no shared mutable state)
# ---------------------------------------------------------------------------


def _users_departments_dbf() -> tuple[DBF, RF, RF]:
    """users → departments via ``RF.references()``.

    Two users (Alice → Dev, Bob → Sales), two departments.
    """
    departments: RF = RF(
        {
            "d1": TF({"name": "Dev"}),
            "d2": TF({"name": "Sales"}),
        },
        frozen=False,
    )
    users: RF = RF(
        {
            "u1": TF({"name": "Alice", "dept": departments["d1"]}),
            "u2": TF({"name": "Bob", "dept": departments["d2"]}),
        },
        frozen=False,
    ).references("dept", departments)
    users.freeze()
    departments.freeze()
    dbf: DBF = DBF({"users": users, "departments": departments}, frozen=True)
    return dbf, users, departments


def _tasks_projects_departments_dbf() -> tuple[DBF, RF, RF, RF]:
    """Linear chain: tasks → projects → departments.

    Two tasks both pointing at the same project (Alpha → Dev).
    """
    departments: RF = RF({"d1": TF({"name": "Dev"})}, frozen=False)
    projects: RF = RF(
        {"p1": TF({"title": "Alpha", "dept": departments["d1"]})},
        frozen=False,
    ).references("dept", departments)
    tasks: RF = RF(
        {
            "t1": TF({"desc": "Design", "project": projects["p1"]}),
            "t2": TF({"desc": "Implement", "project": projects["p1"]}),
        },
        frozen=False,
    ).references("project", projects)
    tasks.freeze()
    projects.freeze()
    departments.freeze()
    dbf: DBF = DBF(
        {"departments": departments, "projects": projects, "tasks": tasks},
        frozen=True,
    )
    return dbf, tasks, projects, departments


def _orders_star_dbf() -> tuple[DBF, RF, RF, RF]:
    """Star schema: orders → customers, orders → products.

    Two orders; each pointing at a distinct customer and product.
    """
    customers: RF = RF(
        {"c1": TF({"name": "Alice"}), "c2": TF({"name": "Bob"})},
        frozen=False,
    )
    products: RF = RF(
        {"p1": TF({"label": "Widget"}), "p2": TF({"label": "Gadget"})},
        frozen=False,
    )
    orders: RF = (
        RF(
            {
                "o1": TF(
                    {
                        "amount": 100,
                        "customer": customers["c1"],
                        "product": products["p1"],
                    }
                ),
                "o2": TF(
                    {
                        "amount": 200,
                        "customer": customers["c2"],
                        "product": products["p2"],
                    }
                ),
            },
            frozen=False,
        )
        .references("customer", customers)
        .references("product", products)
    )
    orders.freeze()
    customers.freeze()
    products.freeze()
    dbf: DBF = DBF(
        {"customers": customers, "products": products, "orders": orders},
        frozen=True,
    )
    return dbf, orders, customers, products


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------


def test_flatten_two_relations_produces_dot_separated_scalar_keys() -> None:
    """flatten(join(users→departments)) emits flat rows with dot-key scalars.

    The join output contains one nested TF per row with ``"users"`` and
    ``"departments"`` as top-level keys.  ``flatten`` must:

    * produce exactly as many rows as there are source tuples (2),
    * include ``"users.name"`` and ``"departments.name"`` in every row,
    * recursively expand the ``dept`` reference stored inside each users TF
      so that ``"users.dept.name"`` also appears (same scalar as
      ``"departments.name"`` since both paths reach the same leaf),
    * store the correct scalar values.
    """
    dbf, _users, _departments = _users_departments_dbf()
    out: RF = flatten(join(dbf)).result

    assert len(out) == 2

    # Collect (users.name, departments.name) pairs.
    pairs: set[tuple[str, str]] = set()
    for item in out:
        row: TF = item.value
        # Mandatory scalar keys present in every row.
        assert "users.name" in row
        assert "departments.name" in row
        # The dept TF stored inside users is also recursively expanded.
        assert "users.dept.name" in row
        # users.dept.name and departments.name reach the same leaf value.
        assert row["users.dept.name"] == row["departments.name"]
        pairs.add((row["users.name"], row["departments.name"]))

    assert pairs == {("Alice", "Dev"), ("Bob", "Sales")}


def test_flatten_three_relation_chain_keys_and_values() -> None:
    """flatten(join(tasks→projects→departments)) expands all three levels.

    Expected flat keys include the direct scalars from each relation plus
    the recursively expanded reference paths (e.g. ``"tasks.project.title"``
    and ``"tasks.project.dept.name"``).  The two task rows must differ only
    in ``"tasks.desc"``; their project and department scalar values are
    identical (same shared TF instances via the join).
    """
    dbf, _tasks, _projects, _departments = _tasks_projects_departments_dbf()
    out: RF = flatten(join(dbf)).result

    assert len(out) == 2

    descs: set[str] = set()
    for item in out:
        row: TF = item.value
        # Direct scalars from each relation.
        assert "tasks.desc" in row
        assert "projects.title" in row
        assert "departments.name" in row
        # Recursively expanded reference paths.
        assert "projects.dept.name" in row
        assert "tasks.project.title" in row
        assert "tasks.project.dept.name" in row
        # Leaf values from the single project and department.
        assert row["projects.title"] == "Alpha"
        assert row["departments.name"] == "Dev"
        assert row["projects.dept.name"] == "Dev"
        assert row["tasks.project.title"] == "Alpha"
        assert row["tasks.project.dept.name"] == "Dev"
        descs.add(row["tasks.desc"])

    # The two tasks have distinct descriptions; all other scalars are shared.
    assert descs == {"Design", "Implement"}


def test_flatten_star_schema_collects_keys_from_all_three_relations() -> None:
    """flatten(join(orders→customers, orders→products)) flattens a star.

    Every flat row must contain the direct scalars from all three relations
    and the recursively expanded references stored inside the orders TF.
    The two orders must yield the correct (customer, product, amount) combos.
    """
    dbf, _orders, _customers, _products = _orders_star_dbf()
    out: RF = flatten(join(dbf)).result

    assert len(out) == 2

    combos: set[tuple[str, str, int]] = set()
    for item in out:
        row: TF = item.value
        # Direct scalars from referenced relations.
        assert "customers.name" in row
        assert "products.label" in row
        assert "orders.amount" in row
        # Recursively expanded reference paths stored inside orders TF.
        assert "orders.customer.name" in row
        assert "orders.product.label" in row
        # The expanded paths agree with the top-level relation scalars.
        assert row["orders.customer.name"] == row["customers.name"]
        assert row["orders.product.label"] == row["products.label"]
        combos.add((row["customers.name"], row["products.label"], row["orders.amount"]))

    assert combos == {("Alice", "Widget", 100), ("Bob", "Gadget", 200)}


def test_flatten_single_relation_via_join_produces_relation_prefixed_keys() -> None:
    """flatten on a single-relation DBF (trivial join) prefixes with relation name.

    ``join`` wraps each tuple under its relation name; ``flatten`` must then
    produce keys of the form ``"users.<attr>"``.  No join crossing means the
    row contains only the attributes of the one relation, with no extra
    reference-path keys.
    """
    users: RF = RF(
        {
            "u1": TF({"name": "Alice"}),
            "u2": TF({"name": "Bob"}),
        },
        frozen=True,
    )
    dbf: DBF = DBF({"users": users}, frozen=True)
    out: RF = flatten(join(dbf)).result

    assert len(out) == 2

    names: set[str] = set()
    for item in out:
        row: TF = item.value
        # Exactly one key per row: the single scalar attribute.
        assert set(row.keys()) == {"users.name"}
        names.add(row["users.name"])

    assert names == {"Alice", "Bob"}


def test_flatten_deeply_nested_manual_rf_three_levels() -> None:
    """flatten recurses through three levels of TF nesting to reach the leaf.

    Manually construct a row TF: ``TF({"level1": TF({"level2": TF({"leaf": 42})})})``.
    After flatten the single output row must contain exactly
    ``"level1.level2.leaf"`` with value ``42``.
    """
    # Build three levels of nested TFs by hand.
    leaf_tf: TF = TF({"leaf": 42}, frozen=True)
    level2_tf: TF = TF({"level2": leaf_tf}, frozen=True)
    # The outermost row TF (what flatten receives per row in the input RF).
    row_tf: TF = TF({"level1": level2_tf}, frozen=True)

    # Wrap in a single-row RF — flatten iterates the RF and descends into values.
    nested_rf: RF = RF({0: row_tf}, frozen=True)
    out: RF = flatten(nested_rf).result

    assert len(out) == 1
    flat_row: TF = out[0]
    # Exactly one leaf key produced; intermediate TF levels are not emitted.
    assert set(flat_row.keys()) == {"level1.level2.leaf"}
    assert flat_row["level1.level2.leaf"] == 42


def test_flatten_materialises_computed_attribute_as_scalar() -> None:
    """flatten evaluates computed attributes and stores their scalar result.

    A TF with ``computed={"salary": lambda t: 1000 * t["age"]}`` must appear
    in the flat output as ``"rel.salary"`` with the evaluated value, not as an
    unevaluated callable.

    TODO: discuss: could also in certain situations still be kept as a computed attribute?

    """
    # Use the public constructor's ``computed`` parameter (no __dict__ access).
    user_tf: TF = TF(
        {"age": 20},
        computed={"salary": lambda t: 1000 * t["age"]},
        frozen=True,
    )
    # Wrap the TF under a relation name to simulate a join-style nested row.
    nested_row: TF = TF({"rel": user_tf}, frozen=True)
    nested_rf: RF = RF({0: nested_row}, frozen=True)

    out: RF = flatten(nested_rf).result

    assert len(out) == 1
    flat_row: TF = out[0]

    # Both the stored and computed attributes must appear as scalar leaves.
    assert "rel.age" in flat_row
    assert "rel.salary" in flat_row
    assert flat_row["rel.age"] == 20
    assert flat_row["rel.salary"] == 20000  # 1000 * 20
    # The materialised value must be a plain int, not a callable.
    assert isinstance(flat_row["rel.salary"], int)


def test_flatten_raises_type_error_for_non_rf_input() -> None:
    """flatten must raise TypeError immediately when the input is not an RF.

    Passing a plain string (or any non-RF) must raise ``TypeError`` on
    ``.result`` access, with an informative message naming the bad type.
    This guards against accidental mis-use in operator pipelines.
    """
    with pytest.raises(TypeError, match="flatten expects an RF input"):
        _ = flatten("not_an_rf").result


def test_flatten_empty_rf_produces_empty_rf() -> None:
    """flatten on an RF with zero rows returns an empty RF without error."""
    empty_rf: RF = RF({}, frozen=True)
    out: RF = flatten(empty_rf).result
    assert len(out) == 0


def test_flatten_all_scalar_row_tf_produces_keys_without_prefix() -> None:
    """flatten on a row TF whose top-level values are all scalars (no nested AFs).

    When the top-level keys of a row TF hold plain scalars — rather than nested
    TFs — the walk produces those keys verbatim (no dot prefix is prepended
    because the prefix starts empty at the root level).
    """
    scalar_row: TF = TF({"x": 1, "y": 2}, frozen=True)
    rf: RF = RF({0: scalar_row}, frozen=True)
    out: RF = flatten(rf).result

    assert len(out) == 1
    flat_row: TF = out[0]
    assert set(flat_row.keys()) == {"x", "y"}
    assert flat_row["x"] == 1
    assert flat_row["y"] == 2


def test_flatten_raises_value_error_on_cyclic_reference() -> None:
    """flatten raises ValueError when a reference cycle is detected.

    A TF that points back to itself (or to an ancestor in the reference
    chain) creates a cycle.  ``_flatten_af`` must detect this via the
    visited-set guard and raise ``ValueError`` with a helpful message,
    rather than overflowing the call stack with infinite recursion.
    """
    # Build a manually cyclic structure: cyclic_tf["self"] = cyclic_tf.
    # This is only possible on an unfrozen TF (frozen TFs reject writes).
    cyclic_tf: TF = TF({"x": 1}, frozen=False)
    cyclic_tf["self"] = cyclic_tf  # back-reference — creates a cycle
    # Do NOT freeze: freezing is not required for flatten to accept the TF.

    row: TF = TF({"rel": cyclic_tf}, frozen=False)
    rf: RF = RF({0: row}, frozen=True)

    with pytest.raises(ValueError, match="reference cycle detected"):
        _ = flatten(rf).result

    rf2: RF = RF({"rel": cyclic_tf}, frozen=False)

    with pytest.raises(ValueError, match="reference cycle detected"):
        _ = flatten(rf2).result

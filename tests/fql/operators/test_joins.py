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

from collections.abc import Callable
from dataclasses import dataclass

import pytest

from fdm.attribute_functions import TF, RF, DBF
from fql.operators.constraints import (
    add_join_predicate,
    add_reference,
    drop_join_predicate,
    drop_reference,
)
from fql.operators.joins import join
from fql.operators.subdatabases import subdatabase
from fql.plan.join_graph import JoinGraph
from tests.fql.operators.orientation_helpers import (
    build_directed_tree,
    orientation_params,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _single_rf_dbf() -> DBF:
    """DBF with just one RF, no references.

    Join graph (one relation, no reference edges)::

        users        (u1, u2 — no references)
    """
    users: RF = RF(
        {"u1": TF({"name": "Alice"}), "u2": TF({"name": "Bob"})},
        frozen=True,
    )
    return DBF({"users": users}, frozen=True)


def _two_rels_no_refs_dbf() -> DBF:
    """DBF with two independent RFs and no references between them —
    used to exercise the multi-RF-without-edges NotImplementedError path.

    Join graph (two independent relations, no reference edges)::

        a          b        (no references between them)
    """
    a: RF = RF({1: TF({"x": 1})}, frozen=True)
    b: RF = RF({1: TF({"y": 2})}, frozen=True)
    return DBF({"a": a, "b": b}, frozen=True)


def _users_departments_dbf(
    *, extra_unreferenced_department: bool = False
) -> tuple[DBF, RF, RF]:
    """users → departments via .references('dept', …).

    Join graph::

        users
          │ dept
          ▼
        departments        (extra unreferenced d3 reduced away if requested)

    If `extra_unreferenced_department` is set, adds a third department
    that no user references — used to prove that the internal
    subdatabase call actually reduces it away before the walk.
    """
    dept_items: dict = {
        "d1": TF({"name": "Dev"}),
        "d2": TF({"name": "Sales"}),
    }
    if extra_unreferenced_department:
        dept_items["d3"] = TF({"name": "Unreferenced"})
    departments: RF = RF(dept_items, frozen=False)
    users: RF = RF(
        {
            "u1": TF({"name": "Alice", "dept": departments["d1"]}),
            "u2": TF({"name": "Bob", "dept": departments["d2"]}),
            "u3": TF({"name": "Carol", "dept": departments["d1"]}),
        },
        frozen=False,
    ).references("dept", departments)
    users.freeze()
    departments.freeze()
    dbf: DBF = DBF({"users": users, "departments": departments}, frozen=True)
    return dbf, users, departments


def _tasks_projects_departments_dbf() -> tuple[DBF, RF, RF, RF]:
    """Linear chain: tasks → projects → departments, preref via .references().

    Join graph (linear chain)::

        tasks
          │ project
          ▼
        projects
          │ dept
          ▼
        departments
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
    """Single-source star: orders → customers, orders → products.

    Join graph (single-source star)::

              orders
         customer │ product
            ┌─────┴─────┐
            ▼           ▼
        customers    products
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


def _multi_source_star_dbf() -> DBF:
    """Two independent sources pointing at a shared target — JOB-style.

    Join graph (two pure sources fan in on hub t)::

        ci            mc
          │ t          │ t
          ╰────▶ t ◀───╯

    Shape: `ci → t` and `mc → t`, no edge between ci and mc — a multi-source
    tree (two pure sources `ci`, `mc`; one hub `t`). Both sources fan in on
    the single target `t1`: two `ci` tuples and two `mc` tuples all reference
    it, so the full join is the 2 × 2 cross product through `t1`. Exercises
    the bidirectional walk (both edges are entered backward from `t`) and the
    fan-in row multiplicity.
    """
    t: RF = RF({"t1": TF({"title": "Movie"})}, frozen=False)
    ci: RF = RF(
        {
            "ci1": TF({"note": "producer", "t": t["t1"]}),
            "ci2": TF({"note": "director", "t": t["t1"]}),
        },
        frozen=False,
    ).references("t", t)
    mc: RF = RF(
        {
            "mc1": TF({"company": "Acme", "t": t["t1"]}),
            "mc2": TF({"company": "Globex", "t": t["t1"]}),
        },
        frozen=False,
    ).references("t", t)
    ci.freeze()
    mc.freeze()
    t.freeze()
    return DBF({"ci": ci, "mc": mc, "t": t}, frozen=True)


def _diamond_dbf() -> DBF:
    """Single-pure-source diamond: A → B, A → C, B → D, C → D.

    Join graph (single-source diamond; d reached via both b and c)::

               a
         b_ref │ c_ref
            ┌──┴──┐
            ▼     ▼
            b     c
       d_ref│     │d_ref
            └──┐┌─┘
               ▼▼
               d

    Used to exercise the NotImplementedError path for non-tree acyclic
    graphs with a unique pure source — the walk reaches D via both B
    and C and must refuse rather than silently overwriting one path's
    target.
    """
    d: RF = RF({"d1": TF({"name": "leaf"})}, frozen=False)
    b: RF = RF(
        {"b1": TF({"note": "via b", "d_ref": d["d1"]})},
        frozen=False,
    ).references("d_ref", d)
    c: RF = RF(
        {"c1": TF({"note": "via c", "d_ref": d["d1"]})},
        frozen=False,
    ).references("d_ref", d)
    a: RF = (
        RF(
            {"a1": TF({"note": "root", "b_ref": b["b1"], "c_ref": c["c1"]})},
            frozen=False,
        )
        .references("b_ref", b)
        .references("c_ref", c)
    )
    a.freeze()
    b.freeze()
    c.freeze()
    d.freeze()
    return DBF({"a": a, "b": b, "c": c, "d": d}, frozen=True)


def _isolated_extra_rf_dbf() -> DBF:
    """One referenced pair (users → departments) plus an isolated RF.

    Join graph (one edge plus an isolated relation)::

        users              loose
          │ dept           (no edges — isolated)
          ▼
        departments

    The isolated RF has no edges at all — it exercises the explicit
    "isolated relations" NotImplementedError path.
    """
    departments: RF = RF({"d1": TF({"name": "Dev"})}, frozen=False)
    users: RF = RF(
        {"u1": TF({"name": "Alice", "dept": departments["d1"]})}, frozen=False
    ).references("dept", departments)
    users.freeze()
    departments.freeze()
    loose: RF = RF({"l1": TF({"tag": "orphan"})}, frozen=True)
    return DBF(
        {"users": users, "departments": departments, "loose": loose},
        frozen=True,
    )


def _make_chain_dbf(length: int) -> DBF:
    """Factory: linear chain of `length` relations.

    Join graph (linear chain; shown for length 3, generalizes)::

        r0
          │ to_r1
          ▼
        r1
          │ to_r2
          ▼
        r2

    Produces `r0 → r1 → ... → r(n-1)` where each `ri` contains a
    single tuple whose `to_r{i+1}` attribute references the only
    tuple in `r(i+1)`. The last relation has no outgoing reference.
    Matches the spirit of the external `ChainQueryFactory` but on
    FDM DBFs.

    Each hop uses a **distinct** ref_key (`to_r1`, `to_r2`, …) for
    readable edge names; reusing one name on every hop is supported too
    (see `test_subdatabase_chain_with_same_ref_key_on_every_hop`).

    @param length: number of relations in the chain (>= 2).
    @return: frozen DBF with keys `"r0"`, `"r1"`, ..., `"r{n-1}"`.
    """
    assert length >= 2, "chain needs at least two relations to form an edge"
    # Build **tail-first**: the head of the chain must reference a
    # tuple that already exists, so we construct r{n-1} first, then
    # r{n-2} pointing at r{n-1}'s single tuple, and so on. Indexing
    # by position keeps the loop straightforward and O(n) — growing
    # the list from the back with `.append()` and resolving the
    # previously-built relation via negative indexing.
    relations: list[RF] = []
    # r{n-1}: terminal, no outgoing ref
    relations.append(
        RF({f"t{length - 1}": TF({"tag": f"r{length - 1}"})}, frozen=False)
    )
    # r{n-2}, r{n-3}, ..., r0: each references the previously-built
    # tail neighbour via a unique `to_r{i+1}` ref_key
    for i in range(length - 2, -1, -1):
        next_rf: RF = relations[-1]
        ref_key: str = f"to_r{i + 1}"
        rf: RF = RF(
            {f"t{i}": TF({"tag": f"r{i}", ref_key: next_rf[f"t{i + 1}"]})},
            frozen=False,
        ).references(ref_key, next_rf)
        relations.append(rf)
    for rf in relations:
        rf.freeze()
    # `relations` is tail→head; reverse for the r0..r{n-1} mapping
    head_to_tail: list[RF] = list(reversed(relations))
    return DBF(
        {f"r{i}": head_to_tail[i] for i in range(length)},
        frozen=True,
    )


def _make_star_dbf(num_arms: int) -> DBF:
    """Factory: single-source star with `num_arms` outgoing arms.

    Join graph (single-source star; shown for 3 arms, generalizes)::

                  center
            ┌───────┼───────┐
      arm_0 │ arm_1 │ arm_2 │
            ▼       ▼       ▼
          arm_0   arm_1   arm_2

    Produces `center → arm_0`, `center → arm_1`, …,
    `center → arm_{n-1}`. Center holds one tuple referencing exactly
    one tuple per arm. Matches the spirit of the external
    `StarQueryFactory`.

    @param num_arms: number of outgoing arms (>= 2).
    @return: frozen DBF with keys `"center"`, `"arm_0"`, …,
        `"arm_{n-1}"`.
    """
    assert num_arms >= 2, "star needs at least two arms to form branching"
    arms: list[RF] = [
        RF({f"a{i}": TF({"tag": f"arm_{i}"})}, frozen=False) for i in range(num_arms)
    ]
    # Build the center tuple with a reference to the first tuple of
    # every arm; the ref_keys `arm_0`, `arm_1`, … differ because the
    # one center tuple holds one reference per arm.
    center_tf: TF = TF({"tag": "center"})
    for i, arm in enumerate(arms):
        center_tf[f"arm_{i}"] = arm[f"a{i}"]
    center: RF = RF({"c1": center_tf}, frozen=False)
    for i, arm in enumerate(arms):
        center.references(f"arm_{i}", arm)
    center.freeze()
    for arm in arms:
        arm.freeze()
    return DBF(
        {"center": center, **{f"arm_{i}": arms[i] for i in range(num_arms)}},
        frozen=True,
    )


def _two_independent_pairs_dbf() -> DBF:
    """Two disconnected reference pairs: R → S and T → U.

    Join graph (two disconnected components)::

        R            T
          │ s_ref     │ u_ref
          ▼           ▼
        S            U

    Every relation has a reference (so no isolated nodes), but the
    graph splits into two components. Used to exercise the
    "disconnected components" NotImplementedError path, which is
    distinct from the multi-source one-component case covered by
    `_multi_source_star_dbf`.
    """
    s: RF = RF({"s1": TF({"name": "S-alpha"})}, frozen=False)
    r: RF = RF(
        {"r1": TF({"desc": "R-one", "s_ref": s["s1"]})}, frozen=False
    ).references("s_ref", s)
    u: RF = RF({"u1": TF({"name": "U-alpha"})}, frozen=False)
    t: RF = RF(
        {"t1": TF({"desc": "T-one", "u_ref": u["u1"]})}, frozen=False
    ).references("u_ref", u)
    r.freeze()
    s.freeze()
    t.freeze()
    u.freeze()
    return DBF({"R": r, "S": s, "T": t, "U": u}, frozen=True)


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_join_single_rf_wraps_each_tuple_under_relation_name() -> None:
    """Zero-edge fallback: each tuple becomes its own one-entry row."""
    dbf: DBF = _single_rf_dbf()
    out: RF = join(dbf).result

    assert len(out) == 2
    # keys are sequential integers starting at 0
    assert {item.key for item in out} == {0, 1}
    # each row nests the source tuple under its relation name, by reference
    for item in out:
        assert {entry.key for entry in item.value} == {"users"}
        # names from the input reappear under users -> TF
        assert item.value["users"]["name"] in {"Alice", "Bob"}


def test_join_linear_chain_produces_nested_row_per_leaf_tuple() -> None:
    """tasks → projects → departments yields one row per leaf task."""
    dbf, tasks, projects, departments = _tasks_projects_departments_dbf()
    out: RF = join(dbf).result

    assert len(out) == 2  # two tasks
    for item in out:
        assert {entry.key for entry in item.value} == {
            "tasks",
            "projects",
            "departments",
        }
    # the tasks TFs land in the rows by object identity — each row's
    # "tasks" entry is exactly one of the two input task TFs
    seen_tasks: set[int] = {item.value["tasks"].uuid for item in out}
    assert seen_tasks == {tasks["t1"].uuid, tasks["t2"].uuid}

    # both rows reach the same project and department TFs by identity —
    # zero-redundancy contract even across the chain
    assert out[0]["projects"] is projects["p1"]
    assert out[1]["projects"] is projects["p1"]
    assert out[0]["departments"] is departments["d1"]
    assert out[1]["departments"] is departments["d1"]


def test_join_star_schema_produces_one_row_per_source_tuple() -> None:
    """orders → customers + orders → products: one row per order."""
    dbf, orders, customers, products = _orders_star_dbf()
    out: RF = join(dbf).result

    assert len(out) == 2
    # map order-name → (customer_name, product_label) per row
    seen: set[tuple[str, str]] = set()
    for item in out:
        row: TF = item.value
        assert {entry.key for entry in row} == {
            "orders",
            "customers",
            "products",
        }
        seen.add((row["customers"]["name"], row["products"]["label"]))
    assert seen == {("Alice", "Widget"), ("Bob", "Gadget")}


def test_join_shares_target_tf_across_rows() -> None:
    """Two users on the same department share that department TF by
    object identity — this is the zero-redundancy contract."""
    dbf, users, departments = _users_departments_dbf()
    out: RF = join(dbf).result

    # users u1 and u3 are both on d1 — find their rows
    rows_on_d1: list[TF] = [
        item.value for item in out if item.value["users"] is users["u1"]
    ] + [item.value for item in out if item.value["users"] is users["u3"]]
    assert len(rows_on_d1) == 2
    assert rows_on_d1[0]["departments"] is rows_on_d1[1]["departments"]
    assert rows_on_d1[0]["departments"] is departments["d1"]


def test_join_respects_yannakakis_reduction() -> None:
    """A department referenced by no user must be absent from the output."""
    dbf, users, departments = _users_departments_dbf(extra_unreferenced_department=True)
    out: RF = join(dbf).result

    # unreferenced d3 must not appear in any row
    d3 = departments["d3"]
    for item in out:
        assert item.value["departments"] is not d3


def test_join_path_access_works_on_result() -> None:
    """Path access across the nested row.

    Three forms must yield the same leaf value:
      - step-wise __getitem__: `row["departments"]["name"]`
      - TF's __-path sugar:    `row["departments__name"]`
      - structured-predicate path resolution (what `Eq`/`Min`/etc. use)

    The last one is the form that downstream aggregators rely on, and
    it traverses through `getattr` — which is why the FDM-nested output
    shape is the natural fit: `getattr(row, "departments")` returns the
    departments TF, and `getattr(departments_tf, "name")` returns the
    leaf scalar.
    """
    from fql.predicates.predicates import _resolve_attr_path

    dbf, tasks, projects, departments = _tasks_projects_departments_dbf()
    out: RF = join(dbf).result
    row: TF = out[0]

    assert row["departments"]["name"] == "Dev"
    assert row["departments__name"] == "Dev"
    assert _resolve_attr_path(row, "departments.name") == "Dev"


def test_join_explicit_root_accepts_pure_source() -> None:
    """Passing the unique pure source as `root` produces exactly the
    same output as omitting `root` (in which case `_pick_walk_start`
    picks that same relation because it is the only pure source).

    Row count, row keys, and the per-row relation TFs all match by
    object identity across the two invocations.
    """
    dbf, tasks, projects, departments = _tasks_projects_departments_dbf()

    out_auto: RF = join(dbf).result
    out_explicit: RF = join(dbf, root="tasks").result

    assert len(out_auto) == len(out_explicit)
    assert {item.key for item in out_auto} == {item.key for item in out_explicit}
    for key in (item.key for item in out_auto):
        for rel in ("tasks", "projects", "departments"):
            assert out_auto[key][rel] is out_explicit[key][rel]


def test_join_pipeline_composition_with_add_reference() -> None:
    """join composes lazily with add_reference (no intermediate .result).

    Join graph (edge added lazily via add_reference)::

        users
          │ dept
          ▼
        departments
    """
    departments: RF = RF(
        {"d1": TF({"name": "Dev"})},
        frozen=True,
    )
    users: RF = RF(
        {"u1": TF({"name": "Alice", "dept": departments["d1"]})},
        frozen=True,
    )
    dbf: DBF = DBF({"users": users, "departments": departments}, frozen=True)

    out: RF = join(
        add_reference(dbf, source="users", ref_key="dept", target="departments")
    ).result
    assert len(out) == 1
    assert out[0]["users"]["name"] == "Alice"
    assert out[0]["departments"]["name"] == "Dev"


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


def test_join_rejects_non_dbf_input() -> None:
    """join on a bare RF raises TypeError."""
    rf: RF = RF({1: TF({"x": 1})}, frozen=True)
    with pytest.raises(TypeError, match="expects a DBF"):
        _ = join(rf).result


def test_join_rejects_join_predicate_on_input() -> None:
    """Any JoinPredicate on the input DBF raises NotImplementedError."""
    dbf, _, _ = _users_departments_dbf()
    augmented: DBF = add_join_predicate(
        dbf, "users", "departments", predicate=lambda t: True, description="x"
    ).result
    with pytest.raises(NotImplementedError, match="JoinPredicate"):
        _ = join(augmented).result


def test_join_multi_source_fan_in_cross_product() -> None:
    """Multi-source star (ci→t, mc→t) flattens to the fan-in cross product.

    Join graph (arrow = "references"; two pure sources fan in on hub t)::

        ci ──┐
             ├──▶ t
        mc ──┘

    Two pure sources both reference the single hub tuple t1, so the full join
    is the 2 (ci) × 2 (mc) cross product = 4 rows, each pairing one ci and one
    mc through the shared t1. This is the behaviour Feature 003 enables (it
    used to raise NotImplementedError). Verifies (a) the exact set of pairings,
    (b) the row count, and (c) that the hub is shared across all rows by object
    identity — the zero-redundancy contract on the new backward-walk path.
    """
    dbf: DBF = _multi_source_star_dbf()  # ci→t, mc→t with 2 ci and 2 mc on hub t1
    t: RF = dbf.t  # the hub relation, to assert shared-instance below

    out: RF = join(dbf).result  # multi-source now flattens instead of raising

    assert len(out) == 4  # 2 ci × 2 mc through the single shared hub t1
    for item in out:  # every row nests exactly the three relations
        assert {entry.key for entry in item.value} == {"ci", "mc", "t"}
    pairings: set[tuple[str, str]] = {  # collect each row's (ci note, mc company)
        (item.value["ci"]["note"], item.value["mc"]["company"]) for item in out
    }
    assert pairings == {  # the full 2×2 cross product, no missing/extra pairing
        ("producer", "Acme"),
        ("producer", "Globex"),
        ("director", "Acme"),
        ("director", "Globex"),
    }
    assert all(  # the hub t1 is the SAME instance in every row (shared, not copied)
        item.value["t"] is t["t1"] for item in out
    )


def test_join_multi_rf_without_refs_raises_not_implemented() -> None:
    """Multiple independent RFs without any cross-references raise."""
    dbf: DBF = _two_rels_no_refs_dbf()
    with pytest.raises(NotImplementedError, match="no references between them"):
        _ = join(dbf).result


def test_join_diamond_raises_not_implemented() -> None:
    """A single-pure-source diamond (A→B, A→C, B→D, C→D) must refuse
    rather than silently overwriting one path's target.

    Join graph (arrow = "references"; D reached via two paths B and C)::

            ┌──▶ B ──┐
        A ──┤        ├──▶ D
            └──▶ C ──┘
    """
    dbf: DBF = _diamond_dbf()
    with pytest.raises(NotImplementedError, match="diamond"):
        _ = join(dbf).result


def test_join_isolated_relation_raises_not_implemented() -> None:
    """A DBF with an isolated (zero-edge) relation raises with a
    dedicated error message, independently of the pure-source check."""
    dbf: DBF = _isolated_extra_rf_dbf()
    with pytest.raises(NotImplementedError, match="isolated"):
        _ = join(dbf).result


def test_join_disconnected_components_raise_not_implemented() -> None:
    """Two disconnected reference pairs (R→S, T→U) raise with a
    dedicated error message — distinguishable from the multi-source
    case where all nodes share a single component.

    Join graph (arrow = "references"; two separate components, no edge between)::

        R ──▶ S     T ──▶ U
    """
    dbf: DBF = _two_independent_pairs_dbf()
    with pytest.raises(NotImplementedError, match="disconnected components"):
        _ = join(dbf).result


def test_join_explicit_root_rejects_non_source() -> None:
    """An explicit root that has incoming references is rejected."""
    dbf, _, _, _ = _tasks_projects_departments_dbf()
    with pytest.raises(ValueError, match="not a pure source"):
        _ = join(dbf, root="departments").result


def test_join_explicit_root_rejects_unknown_relation() -> None:
    """An explicit root that isn't in the DBF at all is rejected."""
    dbf, _, _, _ = _tasks_projects_departments_dbf()
    with pytest.raises(ValueError, match="not a relation in the DBF"):
        _ = join(dbf, root="nope").result


# ---------------------------------------------------------------------------
# Factory-driven shape tests (parametric)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("length", [2, 3, 5])
def test_join_chain_via_factory(length: int) -> None:
    """`join` works on a chain of arbitrary length produced by
    `_make_chain_dbf`.

    Each chain of length N has a single start tuple whose reference
    chain reaches exactly one tuple per relation, so the output RF
    has exactly one row with N top-level keys (`r0`, `r1`, …).
    """
    dbf: DBF = _make_chain_dbf(length)
    out: RF = join(dbf).result
    assert len(out) == 1
    row: TF = out[0]
    assert {entry.key for entry in row} == {f"r{i}" for i in range(length)}
    # The head tuple lands under its relation name by object identity.
    assert row["r0"] is dbf["r0"]["t0"]


@pytest.mark.parametrize("num_arms", [2, 3])
def test_join_star_via_factory(num_arms: int) -> None:
    """`join` works on a single-source star of arbitrary arm count
    produced by `_make_star_dbf`.

    Center has one tuple, each arm has one tuple — so the output has
    one row containing the center and every arm under its own key.
    """
    dbf: DBF = _make_star_dbf(num_arms)
    out: RF = join(dbf).result
    assert len(out) == 1
    row: TF = out[0]
    expected_keys: set[str] = {"center"} | {f"arm_{i}" for i in range(num_arms)}
    assert {entry.key for entry in row} == expected_keys
    assert row["center"] is dbf["center"]["c1"]


# ---------------------------------------------------------------------------
# Plan extraction
# ---------------------------------------------------------------------------


def test_join_plan_extraction_names_operator() -> None:
    """to_plan() surfaces the join operator and its public params.

    Note that `subdatabase` is called inline inside `join._compute`
    (not composed as an Operator input), so the extracted plan does
    not carry a nested `subdatabase` node — by design for this
    minimal POC. Lazy composition is a possible refinement for the
    follow-up MR, but would complicate the "extract graph from the
    original DBF" trick.
    """
    dbf, _, _, _ = _tasks_projects_departments_dbf()
    op = join(dbf)
    root = op.to_plan().root
    assert root.op == "join"
    # the plan serializes the configured root param (None by default here)
    assert root.params["root"] is None


# ---------------------------------------------------------------------------
# All 8 edge orientations of the tree A-C, B-C, C-D
# ---------------------------------------------------------------------------
#
# The flattening join walks the reference tree bidirectionally (feature 003):
# forward along an arrow via the inline pointer, backward against an arrow from
# a hub tuple to the sources that reference it. Edge direction therefore only
# decides which relation is embedded where, never whether the join runs — so
# all 2**3 = 8 orientations flatten, including the multi-source ones (two or
# more pure sources), which enter the shared hub's other edges backward. Like
# subdatabase, which treats the graph as undirected (see test_subdatabases.py).


@pytest.mark.parametrize("directed_edges", orientation_params())
def test_join_flattens_all_edge_orientations(
    directed_edges: list[tuple[str, str]],
) -> None:
    """Flattening join succeeds for every one of the 8 orientations of A-C, B-C, C-D.

    With one tuple per relation and full connectivity the full join is a single
    row nesting all four relations, whatever the edge directions — multi-source
    orientations included, since the walk is bidirectional. (Fan-in / row
    multiplicity is covered separately by the multi-source star test.)
    """
    dbf: DBF = build_directed_tree(
        directed_edges
    )  # build the 4-relation DBF for this orientation

    out: RF = join(dbf).result  # runs for every orientation, no NotImplementedError

    assert len(out) == 1  # single tuple per relation, fully connected -> one row
    row: TF = out[0]  # the sole flattened row
    assert {entry.key for entry in row} == {
        "A",
        "B",
        "C",
        "D",
    }  # the row nests all four relations, one tuple each
    for name in ("A", "B", "C", "D"):  # each nested tuple is the ORIGINAL instance...
        assert (
            row[name] is dbf[name][name.lower() + "1"]
        )  # ...shared by object identity (e.g. row["A"] is dbf["A"]["a1"]), not a copy


# ---------------------------------------------------------------------------
# Cyclic reference graph (out of scope) and mixed fan-in + forward chain
# ---------------------------------------------------------------------------


def _cyclic_dbf() -> DBF:
    """Two relations referencing each other: A → B and B → A (a 2-cycle).

    Join graph (2-cycle; a and b reference each other)::

        a  ──b──▶  b        a → b via ref_key "b"
        a  ◀──a──  b        b → a via ref_key "a"

    Built by embedding B's tuple into A's after both exist, so the references
    form a directed cycle. Used to prove the join rejects cyclic graphs (they
    are non-trees) rather than looping.
    """
    a: RF = RF({"a1": TF({"lbl": "A"})}, frozen=False)  # start without the back-ref
    b: RF = RF(
        {"b1": TF({"lbl": "B", "a": a["a1"]})},  # B → A
        frozen=False,
    ).references("a", a)
    a["a1"]["b"] = b["b1"]  # close the cycle: A → B, after b1 exists
    a = a.references("b", b)  # declare the A → B foreign-value constraint
    a.freeze()
    b.freeze()
    return DBF({"a": a, "b": b}, frozen=True)


def test_join_cyclic_graph_raises_not_implemented() -> None:
    """A cyclic reference graph (A → B → A) is a non-tree and must raise.

    Join graph (arrow = "references"; A and B reference each other, a 2-cycle)::

        A ──▶ B
        A ◀── B

    The undirected-tree gate rejects it before any traversal (a cycle has as
    many edges as nodes, not n-1), so the join refuses cleanly instead of
    walking the cycle forever. Cyclic graphs are deferred like diamonds.
    """
    dbf: DBF = _cyclic_dbf()  # A ⇄ B, a 2-cycle
    with pytest.raises(
        NotImplementedError, match="tree"
    ):  # rejected by the non-tree gate, message names the tree requirement
        _ = join(dbf).result  # must not loop or silently truncate


def _mixed_fan_in_forward_chain_dbf() -> tuple[DBF, RF]:
    """Tree A→C, B→C, C→D combining a backward fan-in with a forward chain.

    Join graph (fan-in a, b → c, then forward c → d)::

        a            b
          │ c          │ c
          ╰────▶ c ◀───╯
                 │ d
                 ▼
                 d

    C is the hub: A and B reference it (walked backward from C), and C
    references D (walked forward). A and B each have two tuples pointing at the
    single hub c1, while c1 → d1. Starting at pure source A, each A-tuple
    reaches c1, then fans in over B's tuples and folds in the single d1 — so
    the full join is |A| × |B| = 4 rows, all sharing c1 and d1. Exercises the
    product of a backward fan-in with an onward forward edge in one walk.

    @return: (dbf, D) — the DBF and the D relation, to assert the shared d1.
    """
    d: RF = RF({"d1": TF({"kind": "dept"})}, frozen=False)
    c: RF = RF(
        {"c1": TF({"hub": "c", "d": d["d1"]})},  # C → D (forward from C)
        frozen=False,
    ).references("d", d)
    a: RF = RF(
        {
            "a1": TF({"n": "a1", "c": c["c1"]}),
            "a2": TF({"n": "a2", "c": c["c1"]}),
        },
        frozen=False,
    ).references(
        "c", c
    )  # A → C (backward from C)
    b: RF = RF(
        {
            "b1": TF({"n": "b1", "c": c["c1"]}),
            "b2": TF({"n": "b2", "c": c["c1"]}),
        },
        frozen=False,
    ).references(
        "c", c
    )  # B → C (backward from C)
    a.freeze()
    b.freeze()
    c.freeze()
    d.freeze()
    return DBF({"a": a, "b": b, "c": c, "d": d}, frozen=True), d


def test_join_mixed_fan_in_and_forward_chain() -> None:
    """Backward fan-in (A,B → C) times a forward chain (C → D) enumerates correctly.

    Join graph (arrow = "references", A/B fan in on hub C, C chains on to D)::

        A ──┐
            ├──▶ C ──▶ D
        B ──┘

    A holds two tuples (a1, a2), B holds two (b1, b2); both reference the single
    hub c1, and c1 in turn references the single d1. The full join is therefore
    the 2 × 2 A×B cross product, every row also carrying c1 and d1. Verifies the
    row count, the exact A×B pairings, that every row nests all four relations,
    and that the hub c1 and the forward-chained d1 are shared by object identity.
    """
    dbf, d = _mixed_fan_in_forward_chain_dbf()  # tree A→C,B→C,C→D with fan-in at C
    c_hub: TF = dbf.c["c1"]  # the single hub tuple, shared across all rows

    out: RF = join(dbf).result  # bidirectional walk: backward to A/B, forward to D

    assert len(out) == 4  # 2 (A) × 2 (B), the single c1/d1 folding in as factors of 1
    for item in out:  # every row nests all four relations
        assert {entry.key for entry in item.value} == {"a", "b", "c", "d"}
    pairings: set[tuple[str, str]] = {  # each row's (A tuple, B tuple) labels
        (item.value["a"]["n"], item.value["b"]["n"]) for item in out
    }
    assert pairings == {  # the full 2×2 cross product, nothing missing or spurious
        ("a1", "b1"),
        ("a1", "b2"),
        ("a2", "b1"),
        ("a2", "b2"),
    }
    assert all(
        item.value["c"] is c_hub for item in out
    )  # the hub c1 is the same instance in every row
    assert all(
        item.value["d"] is d["d1"] for item in out
    )  # the forward-chained d1 is likewise shared by identity


def test_join_drops_source_tuples_missing_the_reference() -> None:
    """An undefined foreign key (JOB `cast_info.person_role_id`) is a missing reference, so the tuple joins nothing.

    Join graph::

        users ──dept──▶ departments      (u1->d1, u2 has no dept)
    """
    departments: RF = RF({"d1": TF({"name": "Dev"})}, frozen=False)  # the target
    users: RF = RF(  # u2 lacks the `dept` reference
        {"u1": TF({"dept": departments["d1"]}), "u2": TF({"name": "Carol"})},
        frozen=False,
    ).references("dept", departments)

    dbf: DBF = DBF({"users": users, "departments": departments})  # the join input
    out: RF = join(dbf).result  # join along `dept`

    assert len(out) == 1  # only u1 has a partner; u2 is dropped, not an error
    assert out[0]["users"] is users["u1"]  # the one row carries u1 by reference


# ---------------------------------------------------------------------------
# Feature 013 — self-reference edge policy. A relation that references itself
# (JOB's `title.episode_of_id -> title`) does NOT become a join edge: a
# self-reference is an object-identity pointer, not a join request. A
# self-referencing relation still joins via its real edges to other relations,
# and on its own it is a plain one-relation DBF that `join` and `subdatabase`
# pass through.
# ---------------------------------------------------------------------------


def _sole_self_referencing_title(*, freeze: bool = True) -> RF:
    """A `title` RF whose only reference is a self-reference.

    Relation shape (the episode points at its series inside the same RF)::

         title ◀─┐
           │     │ episode_of_id   (ep points at series)
           └─────┘

    @param freeze: Freeze the RF before returning it (default). Pass False if
        another relation still has to reference it: `.references()` also
        writes onto the target.
    """
    title: RF = RF(  # the only relation, carrying nothing but a self-reference
        {
            "series": TF({"name": "The Show"}),  # the series title
            "ep": TF({"name": "Pilot", "episode_of_id": None}),  # an episode
        },
        frozen=False,
    )
    title["ep"]["episode_of_id"] = title["series"]  # the episode points at its series
    title = title.references("episode_of_id", title)  # the sole link: a self-reference
    if freeze:  # callers that add references themselves freeze it afterwards
        title.freeze()  # frozen DBF requires frozen members
    return title  # ready to be entered into a DBF


def test_join_passes_a_sole_self_referencing_relation_through() -> None:
    """`join` returns one row per tuple of a relation linked only to itself.

    Join graph (a single node whose only reference is a self-reference)::

         title ◀─┐
           │     │ episode_of_id   (self-ref: no edge)
           └─────┘

    A self-reference is no join request, so this is a plain one-relation DBF and
    every title tuple comes back as its own row. Would fail if the
    self-reference became an edge (the self-loop is no tree, so `join`
    would raise) or if `join` refused such a relation instead of passing it
    through.
    """
    title: RF = _sole_self_referencing_title()  # series and ep; ep -> series
    dbf: DBF = DBF({"title": title}, frozen=True)  # one relation, no other reference

    out: RF = join(dbf).result  # the one-relation path, no self-join

    assert len(out) == 2  # one row per title tuple: series and ep
    assert all(  # each row nests nothing but the title tuple
        {entry.key for entry in item.value} == {"title"} for item in out
    )
    assert {item.value["title"].uuid for item in out} == {  # the very tuples, by uuid
        title["series"].uuid,
        title["ep"].uuid,
    }


def test_subdatabase_keeps_sole_self_referencing_relation_unchanged() -> None:
    """`subdatabase` returns a relation linked only by a self-reference unchanged.

    Join graph (a single node whose only reference is a self-reference)::

         title ◀─┐
           │     │ episode_of_id   (self-ref: no edge)
           └─────┘

    Without an edge there is nothing to reduce along, so the very same RF
    object must come back. Would fail if the self-reference became an edge
    (a semijoin of title with itself would build a new, reduced RF) or if
    `subdatabase` refused the input.
    """
    title: RF = _sole_self_referencing_title()  # fresh RF for this test
    dbf: DBF = DBF({"title": title}, frozen=True)  # one relation, no other reference

    reduced: DBF = subdatabase(dbf).result  # no edge, so nothing to reduce along
    assert reduced["title"] is title  # the very same RF object, not a reduced copy


def test_join_accepts_self_referencing_pure_source() -> None:
    """A self-referential relation with an outgoing real edge joins via that edge.

    Join graph (self-reference: no edge; the outgoing edge links title)::

         title ◀─┐
           │     │ episode_of_id   (self-ref: no edge)
           ├─────┘
           │ kind_id
           ▼
        kind_type

    Would fail if `from_dbf` turned `title.episode_of_id -> title` into an edge
    (the self-loop is no tree, so `join` would raise), or if skipping the
    self-reference also skipped title's real reference to kind_type.
    """
    kind_type: RF = RF({"kt1": TF({"kind": "tv series"})}, frozen=False)  # the kind
    title: RF = RF(  # a title that self-references AND references kind_type
        {
            "series": TF({"name": "The Show", "kind_id": kind_type["kt1"]}),  # series
            "ep": TF(  # an episode of the series, of the same kind
                {"name": "Pilot", "episode_of_id": None, "kind_id": kind_type["kt1"]}
            ),
        },
        frozen=False,
    )
    title["ep"]["episode_of_id"] = title["series"]  # the episode points at its series
    title = title.references("episode_of_id", title)  # self-reference: no edge
    title = title.references("kind_id", kind_type)  # the real edge title -> kind_type

    for af in (kind_type, title):  # freeze members before building the frozen DBF
        af.freeze()  # frozen DBF requires frozen members

    dbf: DBF = DBF(  # title is a pure source here: nothing references it back
        {"title": title, "kind_type": kind_type},
        frozen=True,
    )

    graph: JoinGraph = JoinGraph.from_dbf(dbf)  # title still has an outgoing edge

    edge_triples: set[tuple[str, str, str]] = {  # each edge as (source, target, key)
        (edge.source.name, edge.target.name, edge.ref_key) for edge in graph.edges
    }
    assert edge_triples == {("title", "kind_type", "kind_id")}  # only the real edge
    assert len(graph.edges) == 1  # no self-loop, and no duplicate of the real edge

    out: RF = join(dbf).result  # end-to-end: the tree must flatten, not raise
    assert len(out) == 2  # each of the two title tuples nests its referenced kind_type
    assert all(  # every flattened row carries both relations as nested entries
        {entry.key for entry in item.value} == {"title", "kind_type"} for item in out
    )
    assert all(  # the nested kind_type is the exact hub tuple, shared by identity
        item.value["kind_type"] is kind_type["kt1"] for item in out
    )


def test_join_self_reference_sharing_its_name_with_an_incoming_reference() -> None:
    """A skipped self-reference with the same name as a real edge joins correctly.

    Join graph (self-reference: no edge; both references are called `manager`)::

        dept ──manager──▶ emp ◀─┐
                           │    │ manager   (self-ref: no edge)
                           └────┘

    As the self-reference is no edge, the graph is the single edge dept -> emp,
    a tree, so `join` reduces it. Would fail if `from_dbf` kept the self-loop as
    an edge, or (0 rows) if the reduction guessed the edge's direction from the
    name `manager`: both relations carry it, and the guess picks the wrong side
    (fixed in MR !100, which this case depends on).
    """
    boss: TF = TF({"name": "Boss"})  # the top of the hierarchy
    boss["manager"] = boss  # the boss manages itself
    alice: TF = TF({"name": "Alice", "manager": boss})  # alice reports to the boss
    emp: RF = RF({"e0": boss, "e1": alice}, frozen=False)  # the employees
    emp = emp.references("manager", emp)  # self-reference: no edge
    dept: RF = RF(  # one department, managed by alice
        {"d1": TF({"title": "R&D", "manager": alice})}, frozen=False
    )
    dept = dept.references("manager", emp)  # the real edge dept -> emp

    for af in (emp, dept):  # freeze members before building the frozen DBF
        af.freeze()  # frozen DBF requires frozen members

    dbf: DBF = DBF({"dept": dept, "emp": emp}, frozen=True)  # the join input

    graph: JoinGraph = JoinGraph.from_dbf(dbf)  # the graph `join` works on
    edge_triples: set[tuple[str, str, str]] = {  # each edge as (source, target, key)
        (edge.source.name, edge.target.name, edge.ref_key) for edge in graph.edges
    }
    assert edge_triples == {("dept", "emp", "manager")}  # the real edge, no emp loop
    assert len(graph.edges) == 1  # no duplicate edge hides behind the set comparison

    out: RF = join(dbf).result  # end-to-end: reduce, then flatten the tree
    assert len(out) == 1  # one department, one manager: exactly one row
    assert out[0]["dept"] is dept["d1"]  # the row carries the department itself
    assert out[0]["emp"] is alice  # and its manager alice, by identity


# ---------------------------------------------------------------------------
# One RF entered under several DBF names (aliases), as JOB 12a enters
# `info_type` as `it1` and `it2`. A reference into such an RF does not say
# which name it means, so `JoinGraph.from_dbf` refuses it instead of guessing
# (a guess, e.g. by DBF position, can join `mi` with the rating copy, and
# JOB 12a then returns 0 rows instead of 1). The way to go: enter each name as
# its own relation and bind every reference into it with `add_reference`. A
# reference OUT of an aliased relation and an alias's self-reference leave no
# doubt about their target, so both stay allowed.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Job12aRelations:
    """The relations of JOB 12a in miniature, before they are entered into a DBF."""

    info_type: RF  # g = genres, r = rating; JOB 12a needs it twice, as it1 and it2
    title: RF  # the shared hub t, with the one title t1
    movie_info: RF  # mi: one genres row mi1 on t1
    movie_info_idx: RF  # mi_idx: one rating row mix1 on t1


def _job_12a_relations() -> Job12aRelations:
    """JOB 12a in miniature: two relations reference one `info_type` RF.

    References (mi and mi_idx both point at info_type and at title)::

                  info_type      g = genres, r = rating
                  ▲       ▲
     info_type_id │       │ info_type_id
                  │       │
                 mi     mi_idx   mi1 -> g, mix1 -> r
                  │       │
                t │       │ t
                  ▼       ▼
                   title         t1

    @return: The four frozen RFs, not yet entered into a DBF, so each test
        decides under which names they appear.
    """
    info_type: RF = RF(  # JOB 12a needs one info_type row per side
        {"g": TF({"info": "genres"}), "r": TF({"info": "rating"})},
        frozen=False,
    )

    title: RF = RF({"t1": TF({"title": "Movie"})}, frozen=False)  # the linking hub

    movie_info: RF = RF(  # mi: one row on title t1, with info type g
        {
            "mi1": TF(
                {"info": "Drama", "info_type_id": info_type["g"], "t": title["t1"]}
            )
        },
        frozen=False,
    )
    movie_info = movie_info.references("info_type_id", info_type)  # mi -> info_type
    movie_info = movie_info.references("t", title)  # edge to the hub t

    movie_info_idx: RF = RF(  # mi_idx: one row on title t1, with info type r
        {"mix1": TF({"info": "8.5", "info_type_id": info_type["r"], "t": title["t1"]})},
        frozen=False,
    )
    movie_info_idx = movie_info_idx.references("info_type_id", info_type)  # same RF
    movie_info_idx = movie_info_idx.references("t", title)  # edge to the hub t

    for af in (info_type, title, movie_info, movie_info_idx):  # freeze every member
        af.freeze()  # a frozen DBF requires frozen members

    return Job12aRelations(  # named result, no anonymous tuple
        info_type=info_type,
        title=title,
        movie_info=movie_info,
        movie_info_idx=movie_info_idx,
    )


@pytest.mark.parametrize(
    "run",
    [
        pytest.param(JoinGraph.from_dbf, id="from_dbf"),
        pytest.param(lambda dbf: join(dbf).result, id="join"),
        pytest.param(lambda dbf: subdatabase(dbf).result, id="subdatabase"),
    ],
)
def test_from_dbf_refuses_a_referenced_relation_under_several_names(
    run: Callable[[DBF], object],
) -> None:
    """A reference into an RF entered under two names is refused, not guessed.

    Join graph (it1, it2 = one info_type RF; which name does each reference mean?)::

                   it1 / it2        one info_type RF under two names
                  ▲         ▲
     info_type_id │         │ info_type_id
                 mi       mi_idx
                t │         │ t
                  ▼         ▼
                      t

    Both `join` and `subdatabase` build their graph with `from_dbf`, so all
    three must raise. The DBF lists mi before mi_idx, so the first reference
    checked, and named in the message, is `mi.info_type_id`. Would fail if
    `from_dbf` picked one of the names for each reference (by DBF position, mi
    can meet the rating copy: 0 rows instead of 1 when mi_idx comes before mi)
    or if `join` / `subdatabase` bypassed the check.
    """
    rels: Job12aRelations = _job_12a_relations()  # info_type, title, mi, mi_idx
    dbf: DBF = DBF(  # JOB 12a's FROM list: info_type under two names it1, it2
        {
            "it1": rels.info_type,
            "it2": rels.info_type,
            "mi": rels.movie_info,
            "mi_idx": rels.movie_info_idx,
            "t": rels.title,
        },
        frozen=True,
    )

    with pytest.raises(  # the join-graph message, naming the reference and both names
        NotImplementedError,
        match=r"'mi\.info_type_id' references a relation entered under several "
        r"names \['it1', 'it2'\].*add_reference",
    ):
        run(dbf)  # refused before any row is produced


@pytest.mark.parametrize(
    "order",
    [
        pytest.param(["it1", "it2", "mi", "mi_idx", "t"], id="from-order"),
        pytest.param(["it1", "it2", "mi_idx", "mi", "t"], id="mi_idx-before-mi"),
    ],
)
def test_aliases_as_own_relations_join_in_any_dbf_order(order: list[str]) -> None:
    """Each alias entered as its own relation and bound by name joins correctly.

    Join graph (it1, it2 = two filtered copies of info_type, bound by name)::

           it1 (genres)       it2 (rating)
                  ▲               ▲
     info_type_id │               │ info_type_id   (each bound with add_reference)
                 mi            mi_idx
                t │               │ t
                  ▼               ▼
                          t

    The route the alias refusal points to. A `where` copy is a new RF, and
    mi / mi_idx still reference the original info_type, which is not in the
    DBF; so each reference into an alias needs its own `add_reference` (without
    it, `join` refuses it1 and it2 as isolated). Would fail if a reference were
    matched to a relation by DBF position instead of by the given name: with
    mi_idx first, mi would meet the rating copy and the join would return
    0 rows instead of 1.
    """
    rels: Job12aRelations = _job_12a_relations()  # info_type, title, mi, mi_idx
    relations: dict[str, RF] = {  # every alias is its own, filtered relation
        "it1": rels.info_type.where(lambda i: i.value["info"] == "genres"),  # {g}
        "it2": rels.info_type.where(lambda i: i.value["info"] == "rating"),  # {r}
        "mi": rels.movie_info,  # references the original info_type and title
        "mi_idx": rels.movie_info_idx,  # likewise
        "t": rels.title,  # the hub, entered under one name
    }
    dbf: DBF = DBF({name: relations[name] for name in order})  # names in `order`

    bound_mi: DBF = add_reference(  # first binding: mi.info_type_id means it1
        dbf, source="mi", ref_key="info_type_id", target="it1"
    ).result
    bound: DBF = add_reference(  # second binding: mi_idx.info_type_id means it2
        bound_mi, source="mi_idx", ref_key="info_type_id", target="it2"
    ).result

    out: RF = join(bound).result  # tree it1 <- mi -> t <- mi_idx -> it2

    assert len(out) == 1  # one mi row and one mi_idx row on the one title
    row: TF = out[0]  # the flattened row
    assert row["it1"] is rels.info_type["g"]  # it1 is the genres tuple mi points at
    assert row["it2"] is rels.info_type["r"]  # it2 is the rating tuple of mi_idx
    assert row["mi"] is rels.movie_info["mi1"]  # the mi side, by identity
    assert row["mi_idx"] is rels.movie_info_idx["mix1"]  # the mi_idx side
    assert row["t"] is rels.title["t1"]  # the shared hub, not a copy


@pytest.mark.parametrize(
    ("name", "run"),
    [
        pytest.param(  # binds the reference cast_info.movie_id to the name t1
            "add_reference",
            lambda dbf: add_reference(
                dbf, source="cast_info", ref_key="movie_id", target="t1"
            ).result,
            id="add_reference",
        ),
        pytest.param(  # drops the existing reference cast_info -> t1
            "drop_reference",
            lambda dbf: drop_reference(
                dbf, source="cast_info", ref_key="movie_id", target="t1"
            ).result,
            id="drop_reference",
        ),
        pytest.param(  # registers an always-true predicate over cast_info and t1
            "add_join_predicate",
            lambda dbf: add_join_predicate(
                dbf, "cast_info", "t1", predicate=lambda row: True
            ).result,
            id="add_join_predicate",
        ),
        pytest.param(  # matches no predicate: a call that changes nothing
            "drop_join_predicate",
            lambda dbf: drop_join_predicate(dbf, matcher=lambda c: False).result,
            id="drop_join_predicate",
        ),
    ],
)
def test_constraint_operators_refuse_aliases(
    name: str, run: Callable[[DBF], DBF]
) -> None:
    """All four constraint operators refuse a DBF holding one RF under two names.

    DBF shape (t1, t2 = one title RF, which references itself)::

        cast_info
            │ movie_id
            ▼
         t1 / t2 ◀─┐       one title RF under two names
            │      │ episode_of_id   (self-ref: no edge)
            └──────┘

    Every operator copies the DBF name by name, so t1 and t2 would come back
    as two different RFs, and the self-reference would point at one of them:
    an edge t1 -> t2 nobody asked for, along which `join` would lose rows.
    `drop_join_predicate` with a matcher that matches nothing shows that even
    a call that changes nothing copies the DBF. Would fail if any of the four
    operators stopped checking for aliases and returned such a copy.
    """
    title: RF = _sole_self_referencing_title(freeze=False)  # cast_info points at it
    cast_info: RF = RF(  # Alice plays in the pilot
        {"ci1": TF({"actor": "Alice", "movie_id": title["ep"]})},
        frozen=False,
    )
    cast_info = cast_info.references("movie_id", title)  # cast_info -> title
    for af in (title, cast_info):  # freeze members before building the frozen DBF
        af.freeze()  # frozen DBF requires frozen members
    dbf: DBF = DBF(  # title under two names; cast_info's reference exists
        {"t1": title, "t2": title, "cast_info": cast_info}, frozen=True
    )

    with pytest.raises(  # this operator's own alias message, naming t1 and t2
        NotImplementedError,
        match=rf"{name}: one relation is entered under several names \(t1, t2\)",
    ):
        run(dbf)  # without aliases a valid call; .result runs the lazy operator


def test_from_dbf_allows_aliases_linked_only_by_self_references() -> None:
    """Aliases whose only reference is their self-reference are not refused.

    Join graph (t1, t2 = one title RF; its self-reference is no edge)::

         t1 ◀─┐        t2 ◀─┐
          │   │         │   │
          └───┘         └───┘
         episode_of_id (both loops: self-ref, no edge)

    A self-reference points into its own RF, so there is no name to pick and
    `from_dbf` builds two nodes without edges. Would fail if the alias refusal
    also counted self-references. `join` then reports two relations without
    references, like for any two unlinked relations, instead of returning
    unjoined rows.
    """
    title: RF = _sole_self_referencing_title()  # series and ep; ep -> series
    dbf: DBF = DBF({"t1": title, "t2": title}, frozen=True)  # one RF, two names

    graph: JoinGraph = JoinGraph.from_dbf(dbf)  # must not raise

    assert set(graph.nodes) == {"t1", "t2"}  # each name is its own node
    assert graph.edges == []  # the self-reference became no edge under either name

    with pytest.raises(  # the usual error for relations without references
        NotImplementedError, match="2 relations but no references between them"
    ):
        join(dbf).result  # no edge, so no join


def test_from_dbf_aliased_source_with_outgoing_reference_edges_per_alias() -> None:
    """An aliased SOURCE is allowed: each alias node gets its own edge.

    Join graph (an aliased SOURCE emits one edge per alias)::

        a1            a2        one source RF {s1, s2} under two names
          │ h          │ h
          ╰────▶ h ◀───╯        single-name hub {h1}; s1 -> h1, s2 -> h1

    A reference out of an RF entered under two names (`a1`, `a2`) points at a
    single-name hub, so its target is clear and `from_dbf` produces `a1 -> h`
    and `a2 -> h`. `join` treats a1 and a2 as two relations meeting at h1, so
    every pair of source tuples is a row: 2 × 2 = 4. Would fail if the alias
    refusal also fired for aliased sources, if the two edges collapsed into
    one, or (2 rows) if `join` merged the two names back into one relation.
    """
    hub: RF = RF({"h1": TF({})}, frozen=False)  # single-name target of the source
    source: RF = RF(  # aliased below; both tuples point at the one hub tuple
        {"s1": TF({"h": hub["h1"]}), "s2": TF({"h": hub["h1"]})}, frozen=False
    )
    source = source.references("h", hub)  # the single outgoing reference into the hub

    for af in (hub, source):  # freeze members before building the frozen DBF
        af.freeze()  # frozen DBF requires frozen members

    dbf: DBF = DBF(  # `source` is entered under two names a1, a2; hub under one name h
        {"a1": source, "a2": source, "h": hub},
        frozen=True,
    )

    graph: JoinGraph = JoinGraph.from_dbf(dbf)  # must not raise for this safe shape

    assert set(graph.nodes) == {"a1", "a2", "h"}  # both source aliases plus the hub
    assert graph.nodes["a1"].rf is graph.nodes["a2"].rf  # both wrap one source RF

    edge_triples: set[tuple[str, str, str]] = {  # each edge as (source, target, key)
        (edge.source.name, edge.target.name, edge.ref_key) for edge in graph.edges
    }
    assert edge_triples == {  # one edge per alias into the hub, both along `h`
        ("a1", "h", "h"),
        ("a2", "h", "h"),
    }
    assert len(graph.edges) == 2  # no duplicate edge hides behind the set comparison

    out: RF = join(dbf).result  # end-to-end: the tree a1 -> h <- a2 flattens
    assert len(out) == 4  # every (a1, a2) pair of source tuples meets at h1
    keys_by_uuid: dict[int, str] = {  # source tuple uuid -> its key, to label rows
        source[key].uuid: key for key in ("s1", "s2")
    }
    pairings: set[tuple[str, str]] = {  # each row's (a1 tuple, a2 tuple) labels
        (keys_by_uuid[item.value["a1"].uuid], keys_by_uuid[item.value["a2"].uuid])
        for item in out
    }
    assert pairings == {  # the full 2 × 2 cross product, nothing missing or spurious
        ("s1", "s1"),
        ("s1", "s2"),
        ("s2", "s1"),
        ("s2", "s2"),
    }
    assert all(  # every row nests exactly the three names
        {entry.key for entry in item.value} == {"a1", "a2", "h"} for item in out
    )
    assert all(  # the hub is the very tuple h1 in every row, not a copy
        item.value["h"] is hub["h1"] for item in out
    )


# ---------------------------------------------------------------------------
# Target relation carries a reference with the same name (ref_key reuse)
# ---------------------------------------------------------------------------


def _ambiguous_ref_key_dbf() -> DBF:
    """Two relations that BOTH carry a reference named `k`; only a's `k` stays inside the DBF.

    DBF {a, b}; c is referenced by b but lives outside the DBF::

        a   (a1)
        │ k            a1 -> b1
        ▼
        b   (b1, b2)   b2 is referenced by no a-tuple
        │ k            b1 -> c1, b2 -> c1
        ▼
        c   (c1)       NOT in the DBF

    The join graph therefore has the single edge `a -k-> b`. The RFs are
    reachable as `dbf.a` / `dbf.b`.
    """
    c: RF = RF(
        {"c1": TF({"name": "Gamma"})}, frozen=False
    )  # target of b.k, kept out of the DBF
    b: RF = RF(  # b carries its own `k`, pointing at c
        {"b1": TF({"k": c["c1"]}), "b2": TF({"k": c["c1"]})},
        frozen=False,
    ).references("k", c)
    a: RF = RF({"a1": TF({"k": b["b1"]})}, frozen=False).references(
        "k", b
    )  # a.k points at b1; nobody points at b2
    for rf in (a, b, c):  # freeze all three relations before wrapping them
        rf.freeze()  # frozen RFs, as in the other fixtures
    return DBF({"a": a, "b": b}, frozen=True)  # c deliberately left out


def test_join_when_target_also_carries_the_ref_key() -> None:
    """join yields the row (a1, b1) although b carries its own, unrelated `k`.

        a   (a1)
        │ k
        ▼
        b   (b1, b2)
        │ k
        ▼
        c   (outside the DBF)

    Symptom: join first reduces its input with subdatabase, whose semijoins
    follow b's own `k` (into c) instead of a's `k` when reducing b; that
    empties a and b, so the join has nothing left to walk.
    Repro: `join(dbf).result` on the DBF above.
    Expected: exactly one row, a1 joined with b1 (b2 has no partner).
    Observed on main: 0 rows.
    """
    dbf: DBF = _ambiguous_ref_key_dbf()  # a.k -> b, plus b.k -> c outside the DBF

    out: RF = join(dbf).result  # flattening join along the single edge a -k-> b

    assert len(out) == 1  # one a-tuple with one partner -> one row
    assert out[0]["a"] is dbf.a["a1"]  # the row carries the original a1 by reference
    assert out[0]["b"] is dbf.b["b1"]  # ...and the b1 that a1 points at

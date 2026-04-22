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
from fql.operators.constraints import add_join_predicate, add_reference
from fql.operators.joins import join

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _single_rf_dbf() -> DBF:
    """DBF with just one RF, no references."""
    users: RF = RF(
        {"u1": TF({"name": "Alice"}), "u2": TF({"name": "Bob"})},
        frozen=True,
    )
    return DBF({"users": users}, frozen=True)


def _two_rels_no_refs_dbf() -> DBF:
    """DBF with two independent RFs and no references between them —
    used to exercise the multi-RF-without-edges NotImplementedError path."""
    a: RF = RF({1: TF({"x": 1})}, frozen=True)
    b: RF = RF({1: TF({"y": 2})}, frozen=True)
    return DBF({"a": a, "b": b}, frozen=True)


def _users_departments_dbf(
    *, extra_unreferenced_department: bool = False
) -> tuple[DBF, RF, RF]:
    """users → departments via .references('dept', …).

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
    """Linear chain: tasks → projects → departments, preref via .references()."""
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
    """Single-source star: orders → customers, orders → products."""
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

    Shape: `ci → t` and `mc → t`, no edge between ci and mc. Used to
    exercise the NotImplementedError path on multi-source graphs.
    """
    t: RF = RF({"t1": TF({"title": "Movie"})}, frozen=False)
    ci: RF = RF(
        {"ci1": TF({"note": "producer", "t": t["t1"]})},
        frozen=False,
    ).references("t", t)
    mc: RF = RF(
        {"mc1": TF({"company": "Acme", "t": t["t1"]})},
        frozen=False,
    ).references("t", t)
    ci.freeze()
    mc.freeze()
    t.freeze()
    return DBF({"ci": ci, "mc": mc, "t": t}, frozen=True)


def _diamond_dbf() -> DBF:
    """Single-pure-source diamond: A → B, A → C, B → D, C → D.

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

    Produces `r0 → r1 → ... → r(n-1)` where each `ri` contains a
    single tuple whose `to_r{i+1}` attribute references the only
    tuple in `r(i+1)`. The last relation has no outgoing reference.
    Matches the spirit of the external `ChainQueryFactory` but on
    FDM DBFs.

    Each hop uses a **distinct** ref_key (`to_r1`, `to_r2`, …) —
    `semijoin._find_ref_direction` matches constraints by ref_key
    only, so a chain that reused the same name for every hop would
    confuse the direction lookup once the chain has three or more
    relations.

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
    # every arm; the ref_keys `arm_0`, `arm_1`, … are distinct per
    # the same ref_key-uniqueness invariant called out in
    # `_make_chain_dbf`.
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
    """join composes lazily with add_reference (no intermediate .result)."""
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


def test_join_multi_source_graph_raises_not_implemented() -> None:
    """Multi-source reference graphs (e.g. JOB's ci→t, mc→t) raise."""
    dbf: DBF = _multi_source_star_dbf()
    with pytest.raises(NotImplementedError, match="pure-source"):
        _ = join(dbf).result


def test_join_multi_rf_without_refs_raises_not_implemented() -> None:
    """Multiple independent RFs without any cross-references raise."""
    dbf: DBF = _two_rels_no_refs_dbf()
    with pytest.raises(NotImplementedError, match="no references between them"):
        _ = join(dbf).result


def test_join_diamond_raises_not_implemented() -> None:
    """A single-pure-source diamond (A→B, A→C, B→D, C→D) must refuse
    rather than silently overwriting one path's target."""
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
    case where all nodes share a single component."""
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

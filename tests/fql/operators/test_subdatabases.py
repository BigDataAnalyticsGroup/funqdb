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

from typing import Any, Mapping

import pytest

from fdm.attribute_functions import TF, RF, DBF
from fql.operators.subdatabases import (
    subdatabase,
)
from fql.plan.ir import PlanChild, PlanNode
from fql.plan.join_graph import Node, JoinNode, Edge, JoinGraph, SemijoinStep
from tests.fql.operators.orientation_helpers import (
    build_directed_tree,
    orientation_params,
)

# ---------------------------------------------------------------------------
# Helpers – small, self-contained datasets for each test scenario
# ---------------------------------------------------------------------------


def _two_relation_dbf() -> tuple[DBF, RF, RF]:
    """Users -> departments via references('department', departments).

    departments: d1 (Dev), d2 (Consulting), d3 (Research)
    users: three users, all referencing d1 or d2 — d3 is unreferenced.

    After subdatabase d3 should be removed from departments.
    """
    departments: RF = RF(
        {
            "d1": TF({"name": "Dev", "budget": "11M"}),
            "d2": TF({"name": "Consulting", "budget": "22M"}),
            "d3": TF({"name": "Research", "budget": "5M"}),
        },
        frozen=False,
    )

    users: RF = RF(
        {
            1: TF({"name": "Horst", "yob": 1972, "department": departments.d1}),
            2: TF({"name": "Tom", "yob": 1983, "department": departments.d1}),
            3: TF({"name": "John", "yob": 2003, "department": departments.d2}),
        },
        frozen=False,
    ).references("department", departments)

    users.freeze()
    departments.freeze()

    dbf: DBF = DBF(
        {"departments": departments, "users": users},
        frozen=True,
    )
    return dbf, departments, users


def _three_level_chain_dbf() -> DBF:
    """Tasks -> projects -> departments.

    departments: d1, d2, d3
    projects: p1->d1, p2->d2, p3->d3
    tasks: t1->p1, t2->p1

    Only d1 has projects with tasks, so after full reduction:
      - tasks: t1, t2  (unchanged, both reference p1)
      - projects: p1   (only p1 is referenced by tasks)
      - departments: d1 (only d1 is referenced by the surviving project p1)
    """
    departments: RF = RF(
        {
            "d1": TF({"name": "Dev"}),
            "d2": TF({"name": "Consulting"}),
            "d3": TF({"name": "Research"}),
        },
        frozen=False,
    )

    projects: RF = RF(
        {
            "p1": TF({"title": "Alpha", "dept": departments["d1"]}),
            "p2": TF({"title": "Beta", "dept": departments["d2"]}),
            "p3": TF({"title": "Gamma", "dept": departments["d3"]}),
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

    projects.freeze()
    departments.freeze()
    tasks.freeze()

    return DBF(
        {"departments": departments, "projects": projects, "tasks": tasks},
        frozen=True,
    )


def _star_schema_dbf() -> DBF:
    """Star schema: orders (fact) referencing customers and products (dimensions).

    customers: c1, c2, c3
    products: pr1, pr2, pr3
    orders: o1->(c1, pr1), o2->(c2, pr2)
      - c3 and pr3 are unreferenced and should be removed.
    """
    customers: RF = RF(
        {
            "c1": TF({"name": "Alice"}),
            "c2": TF({"name": "Bob"}),
            "c3": TF({"name": "Charlie"}),
        },
        frozen=False,
    )

    products: RF = RF(
        {
            "pr1": TF({"name": "Widget"}),
            "pr2": TF({"name": "Gadget"}),
            "pr3": TF({"name": "Doohickey"}),
        },
        frozen=False,
    )

    orders: RF = (
        RF(
            {
                "o1": TF(
                    {
                        "amount": 100,
                        "customer": customers["c1"],
                        "product": products["pr1"],
                    }
                ),
                "o2": TF(
                    {
                        "amount": 200,
                        "customer": customers["c2"],
                        "product": products["pr2"],
                    }
                ),
            },
            frozen=False,
        )
        .references("customer", customers)
        .references("product", products)
    )

    customers.freeze()
    products.freeze()
    orders.freeze()

    return DBF(
        {"customers": customers, "products": products, "orders": orders},
        frozen=True,
    )


def _two_relations_with_external_reference_dbf(
    include_root: bool = False,
) -> tuple[DBF, RF, RF, RF]:
    """Reference chain C -> B -> A; whether A is part of the DBF is configurable.

    Underlying data (independent of include_root):
        A: a1, a2, a3                ; a3 is unreferenced by any B-tuple
        B: b1->a1, b2->a2, b3->a1    ; B.references("a", A) ; b3 is unreferenced by C
        C: c1->b1, c2->b2            ; C.references("b", B)

    @param include_root:
        - False (default): the DBF contains only {B, C}. A is referenced by B
          (via .references()) but lives OUTSIDE the DBF, so the B -> A edge
          targets a relation that is not in the DBF. Exercises the
          "edge target not in DBF" branch of JoinGraph.from_dbf
          (fql/plan/join_graph.py:127).
        - True: the DBF contains {A, B, C}. All three reference edges are
          intra-DBF, so the full Yannakakis cascade runs over the chain.

    @return: (dbf, A, B, C) — the configured DBF and the three RFs (A is
        returned in both modes regardless of DBF membership).
    """
    A: RF = RF(
        {
            "a1": TF({"name": "Alpha"}),
            "a2": TF({"name": "Beta"}),
            "a3": TF({"name": "Gamma"}),
        },
        frozen=False,
    )

    B: RF = RF(
        {
            "b1": TF({"label": "first", "a": A["a1"]}),
            "b2": TF({"label": "second", "a": A["a2"]}),
            "b3": TF({"label": "third", "a": A["a1"]}),
        },
        frozen=False,
    ).references("a", A)

    C: RF = RF(
        {
            "c1": TF({"value": 10, "b": B["b1"]}),
            "c2": TF({"value": 20, "b": B["b2"]}),
        },
        frozen=False,
    ).references("b", B)

    A.freeze()
    B.freeze()
    C.freeze()

    # A is included only when the caller asked for the full chain; the default
    # (include_root=False) intentionally leaves A out so its incoming edge from
    # B targets a relation that is not in the DBF.
    dbf: DBF = (
        DBF({"A": A, "B": B, "C": C}, frozen=True)  # full chain: A is the root
        if include_root
        else DBF({"B": B, "C": C}, frozen=True)  # A omitted: dangling B -> A
    )
    return dbf, A, B, C


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_subdatabase_two_relations_simple_reduction() -> None:
    """Two-relation subdatabase: users -> departments.

    Department d3 ('Research') has no users referencing it. After Yannakakis
    semi-join reduction it must be eliminated from departments while the two
    referenced departments (d1, d2) and all three users survive.
    """
    dbf, _departments, _users = _two_relation_dbf()

    result: DBF = subdatabase[DBF, DBF](dbf).result

    # departments reduced: d3 gone
    result_dept_keys: set[str] = {item.key for item in result.departments}
    assert result_dept_keys == {"d1", "d2"}

    # users unchanged (all reference surviving departments)
    result_user_keys: set[int] = {item.key for item in result.users}
    assert result_user_keys == {1, 2, 3}


def test_subdatabase_three_level_chain() -> None:
    """Three-level chain: tasks -> projects -> departments.

    Only project p1 (in department d1) has tasks. After full reduction the
    result should contain only d1, p1, and both tasks t1/t2.
    """
    dbf: DBF = _three_level_chain_dbf()

    result: DBF = subdatabase[DBF, DBF](dbf).result

    assert {item.key for item in result.departments} == {"d1"}
    assert {item.key for item in result.projects} == {"p1"}
    assert {item.key for item in result.tasks} == {"t1", "t2"}


def test_subdatabase_no_references_returns_unchanged() -> None:
    """A DBF whose RFs have no references() should be returned unchanged.

    No ForeignValueConstraints means no join graph, so the operator returns the
    input DBF as-is.
    """
    r1: RF = RF({"a": TF({"x": 1}), "b": TF({"x": 2})}, frozen=True)
    r2: RF = RF({"c": TF({"y": 10}), "d": TF({"y": 20})}, frozen=True)
    dbf: DBF = DBF({"r1": r1, "r2": r2}, frozen=True)

    result: DBF = subdatabase[DBF, DBF](dbf).result

    # result is the original dbf since there are no edges
    assert result is dbf


def test_subdatabase_explicit_root() -> None:
    """Specifying root='users' should still produce a correct reduction.

    The join graph is the same (users -> departments) but the BFS tree is
    rooted at 'users' instead of the auto-selected 'departments'. The final
    result must be identical: d3 removed, all users kept.
    """
    dbf, _departments, _users = _two_relation_dbf()

    result: DBF = subdatabase[DBF, DBF](dbf, root="users").result

    result_dept_keys: set[str] = {item.key for item in result.departments}
    assert result_dept_keys == {"d1", "d2"}

    result_user_keys: set[int] = {item.key for item in result.users}
    assert result_user_keys == {1, 2, 3}


def test_subdatabase_star_schema() -> None:
    """Star schema: orders reference both customers and products.

    Customer c3 and product pr3 are not referenced by any order. After
    reduction they must be removed while c1, c2, pr1, pr2, and both orders
    survive.
    """
    dbf: DBF = _star_schema_dbf()

    result: DBF = subdatabase[DBF, DBF](dbf).result

    assert {item.key for item in result.customers} == {"c1", "c2"}
    assert {item.key for item in result.products} == {"pr1", "pr2"}
    assert {item.key for item in result.orders} == {"o1", "o2"}


def test_subdatabase_drops_reference_to_relation_outside_dbf() -> None:
    """References pointing to a relation that is not in the DBF must be ignored.

    Schema: C -> B -> A, but only {B, C} are in the DBF (A is referenced by B
    via .references() but lives outside the DBF). The B -> A edge must be
    excluded from the join graph (fql/plan/join_graph.py:127), so the
    subdatabase reduction effectively runs on the single surviving edge C -> B.

    Expected:
      - JoinGraph from the DBF has 2 nodes {B, C} and exactly one edge C -> B
      - After Yannakakis reduction:
          * B is reduced to {b1, b2} (b3 is not referenced by any C-tuple)
          * C is unchanged: {c1, c2} (both reference surviving B-tuples)
    """
    dbf, _A, _B, _C = _two_relations_with_external_reference_dbf(
        include_root=False
    )  # build the DBF where B's target relation A lives outside

    graph: JoinGraph = JoinGraph.from_dbf(
        dbf
    )  # extract the join graph from the DBF under test
    assert set(graph.nodes.keys()) == {
        "B",
        "C",
    }  # only B and C are nodes; A is not in the DBF and must not appear
    edge_tuples: set[tuple[str, str, str]] = (
        {  # build a structural snapshot of edges as (source, target, ref_key) tuples
            (e.source.name, e.target.name, e.ref_key)
            for e in graph.edges  # collect one tuple per edge in the graph
        }
    )
    assert edge_tuples == {
        ("C", "B", "b")
    }  # only the C -> B edge survives; B -> A is dropped because A is not in the DBF

    result: DBF = subdatabase[DBF, DBF](
        dbf
    ).result  # run subdatabase end-to-end; reduction must succeed despite the dangling constraint

    assert {item.key for item in _A} == {
        "a1",
        "a2",
        "a3",
    }  # A must be unchanged as it was not part of the DBF

    assert {item.key for item in result.B} == {
        "b1",
        "b2",
    }  # b3 has no referencing C-tuple, so Yannakakis must drop it
    assert {item.key for item in result.C} == {
        "c1",
        "c2",
    }  # C is unchanged: both c1 and c2 reference surviving B-tuples


def test_subdatabase_drops_reference_to_relation_outside_dbf_w_root() -> None:
    """Variant with A in the DBF: confirms the full chain reduces correctly.

    Same underlying data as test_subdatabase_drops_reference_to_relation_outside_dbf,
    but now {A, B, C} are all in the DBF, so the C -> B -> A chain is fully
    visible to the join graph. This is the control case: it shows that the
    fixture's dataset reduces sensibly when no edge is dangling, which makes
    the asymmetry with the include_root=False variant a meaningful difference.

    Expected:
      - JoinGraph: 3 nodes {A, B, C}, 2 edges {B -> A, C -> B}
      - After Yannakakis cascade:
          * A is reduced to {a1, a2} (a3 has no referencing B-tuple)
          * B is reduced to {b1, b2} (b3 has no referencing C-tuple)
          * C is unchanged: {c1, c2}
    """
    dbf, _A, _B, _C = _two_relations_with_external_reference_dbf(
        include_root=True
    )  # build the DBF that includes A as the chain root

    graph: JoinGraph = JoinGraph.from_dbf(
        dbf
    )  # extract the join graph from the DBF under test
    assert set(graph.nodes.keys()) == {
        "A",
        "B",
        "C",
    }  # all three relations are nodes; A is now in the DBF, unlike the no-root variant
    edge_tuples: set[tuple[str, str, str]] = (
        {  # build a structural snapshot of edges as (source, target, ref_key) tuples
            (e.source.name, e.target.name, e.ref_key)
            for e in graph.edges  # collect one tuple per edge in the graph
        }
    )
    assert edge_tuples == {
        ("B", "A", "a"),
        ("C", "B", "b"),
    }  # both edges survive: nothing is dangling because A is in the DBF

    result: DBF = subdatabase[DBF, DBF](
        dbf
    ).result  # run subdatabase end-to-end over the full chain C -> B -> A

    assert {item.key for item in result.A} == {
        "a1",
        "a2",
    }  # a3 has no referencing B-tuple, so Yannakakis must drop it

    assert {item.key for item in result.B} == {
        "b1",
        "b2",
    }  # b3 has no referencing C-tuple, so Yannakakis must drop it
    assert {item.key for item in result.C} == {
        "c1",
        "c2",
    }  # C is unchanged: both c1 and c2 reference surviving B-tuples


def _four_relation_dbf() -> DBF:
    """tasks -> projects -> departments, tasks -> assignees.

    4 relations, 3 reference edges. Used by plan and join-graph tests.
    """
    departments: RF = RF(
        {"d1": TF({"name": "Dev"}), "d2": TF({"name": "Sales"})},
        frozen=False,
    )
    projects: RF = RF(
        {
            "p1": TF({"title": "Alpha", "dept": departments["d1"]}),
            "p2": TF({"title": "Beta", "dept": departments["d2"]}),
        },
        frozen=False,
    ).references("dept", departments)
    assignees: RF = RF(
        {"a1": TF({"name": "Alice"}), "a2": TF({"name": "Bob"})},
        frozen=False,
    )
    tasks: RF = (
        RF(
            {
                "t1": TF(
                    {
                        "desc": "Design",
                        "project": projects["p1"],
                        "assignee": assignees["a1"],
                    }
                ),
                "t2": TF(
                    {
                        "desc": "Implement",
                        "project": projects["p1"],
                        "assignee": assignees["a2"],
                    }
                ),
            },
            frozen=False,
        )
        .references("project", projects)
        .references("assignee", assignees)
    )

    departments.freeze()
    projects.freeze()
    assignees.freeze()
    tasks.freeze()

    return DBF(
        {
            "departments": departments,
            "projects": projects,
            "assignees": assignees,
            "tasks": tasks,
        },
        frozen=True,
    )


def test_subdatabase_plan_shows_semijoin_cascade() -> None:
    """The extracted plan of a subdatabase on 4 relations must expose the
    individual semijoin steps so that the full Yannakakis cascade is visible.

    Schema: tasks -> projects -> departments, tasks -> assignees
    (star around tasks with 3 dimensions, 4 relations total)

    The join tree has 3 edges, so Yannakakis produces 3 bottom-up + 3 top-down
    = 6 semijoin steps. Each step's params also name its `source`, the relation
    whose tuples hold the reference (e.g. tasks for the tasks/projects steps).
    """
    dbf: DBF = _four_relation_dbf()

    # extract plan WITHOUT executing
    op = subdatabase[DBF, DBF](dbf)
    plan = op.to_plan()

    # walk the plan tree and collect all operator names
    op_names: list[str] = []

    def _collect_ops(node) -> None:
        if hasattr(node, "op"):
            op_names.append(node.op)
            for child in node.inputs:
                _collect_ops(child)

    _collect_ops(plan.root)

    # the plan must contain semijoin nodes — one per Yannakakis step
    semijoin_count: int = op_names.count("semijoin")
    # 3 edges × 2 passes (bottom-up + top-down) = 6 semijoins
    assert semijoin_count == 6, (
        f"Expected 6 semijoin steps in the plan, got {semijoin_count}. "
        f"Operators found: {op_names}"
    )

    # the outermost operator should be subdatabase
    assert op_names[0] == "subdatabase"

    # every semijoin node must carry origin="subdatabase" in its params
    def _collect_semijoin_origins(node) -> list[str | None]:
        origins: list[str | None] = []
        if hasattr(node, "op"):
            if node.op == "semijoin":
                origins.append(node.params.get("origin"))
            for child in node.inputs:
                origins.extend(_collect_semijoin_origins(child))
        return origins

    semijoin_origins: list[str | None] = _collect_semijoin_origins(plan.root)
    assert len(semijoin_origins) == 6
    assert all(
        o == "subdatabase" for o in semijoin_origins
    ), f"Expected all semijoin origins to be 'subdatabase', got {semijoin_origins}"

    # every semijoin node must also say which of its two relations holds the
    # reference (`source`), so another backend need not guess the direction
    def _collect_semijoin_params(node: PlanChild) -> list[Mapping[str, Any]]:
        """Params of all semijoin nodes below `node`, in plan-walk order."""
        found: list[Mapping[str, Any]] = []  # params collected in this subtree
        if isinstance(node, PlanNode):  # leaves (LeafRef) carry no params
            if node.op == "semijoin":  # only semijoin nodes are of interest
                found.append(node.params)  # keep this node's params
            for child in node.inputs:  # descend into the pipeline
                found.extend(_collect_semijoin_params(child))  # gather the subtree
        return found  # all semijoin params of this subtree

    referencing: dict[frozenset[str], str] = {  # {source, target} -> source
        frozenset({edge.source.name, edge.target.name}): edge.source.name
        for edge in JoinGraph.from_dbf(dbf).edges  # the 3 reference edges
    }
    semijoin_params: list[Mapping[str, Any]] = _collect_semijoin_params(
        plan.root
    )  # one params mapping per semijoin step
    assert len(semijoin_params) == 6  # same 6 steps as counted above
    for params in semijoin_params:  # check every step of the cascade
        assert "source" in params  # the direction is part of the plan
        assert params["source"] in (
            params["reduce"],
            params["by"],
        )  # source is one of the two relations the step touches
        assert (
            params["source"] == referencing[frozenset({params["reduce"], params["by"]})]
        )  # ...and it is the relation whose tuples hold the reference


def test_extract_join_graph_four_relations() -> None:
    """_extract_join_graph must correctly identify all reference edges in a DBF.

    Schema: tasks -> projects -> departments, tasks -> assignees
    (4 relations, 3 edges)

    Expected edges:
    - tasks: [(project, projects), (assignee, assignees)]
    - projects: [(dept, departments)]
    """
    dbf: DBF = _four_relation_dbf()

    graph: JoinGraph = JoinGraph.from_dbf(dbf)

    # 4 nodes, 3 edges
    assert len(graph.nodes) == 4
    assert set(graph.nodes.keys()) == {"departments", "projects", "assignees", "tasks"}
    assert len(graph.edges) == 3

    # verify edge structure by (source.name, target.name, ref_key)
    edge_tuples: set[tuple[str, str, str]] = {
        (e.source.name, e.target.name, e.ref_key) for e in graph.edges
    }
    assert edge_tuples == {
        ("tasks", "projects", "project"),
        ("tasks", "assignees", "assignee"),
        ("projects", "departments", "dept"),
    }


# ---------------------------------------------------------------------------
# Helpers for check_acyclicity tests — manually constructed JoinGraphs
# ---------------------------------------------------------------------------


def _make_graph(node_names: list[str], edges: list[Edge]) -> JoinGraph:
    """Build a JoinGraph from node names and Edge instances.

    Uses JoinNode with dummy RFs for nodes — only graph structure matters
    for algorithm tests like check_acyclicity.
    """
    nodes: dict[str, JoinNode] = {
        n: JoinNode(name=n, rf=RF({1: TF({"x": 1})}, frozen=True)) for n in node_names
    }
    return JoinGraph(nodes=nodes, edges=edges)


# ---------------------------------------------------------------------------
# check_acyclicity tests
# ---------------------------------------------------------------------------


def test_check_acyclicity_dag_passes() -> None:
    """A valid DAG (diamond shape) must not raise.

    A -> B, A -> C, B -> D, C -> D (4 nodes, 4 edges, no cycle).
    """
    A, B, C, D = Node("A"), Node("B"), Node("C"), Node("D")
    graph: JoinGraph = _make_graph(
        ["A", "B", "C", "D"],
        [
            Edge(A, B, "r1"),
            Edge(A, C, "r2"),
            Edge(B, D, "r3"),
            Edge(C, D, "r4"),
        ],
    )
    graph.check_acyclicity()  # should not raise


def test_check_acyclicity_simple_cycle_raises() -> None:
    """A simple cycle (A -> B -> C -> A) must raise ValueError."""
    A, B, C = Node("A"), Node("B"), Node("C")
    graph: JoinGraph = _make_graph(
        ["A", "B", "C"],
        [Edge(A, B, "r1"), Edge(B, C, "r2"), Edge(C, A, "r3")],
    )
    with pytest.raises(ValueError, match="cyclic"):
        graph.check_acyclicity()


def test_check_acyclicity_two_node_cycle_raises() -> None:
    """A two-node cycle (A -> B -> A) must raise ValueError."""
    A, B = Node("A"), Node("B")
    graph: JoinGraph = _make_graph(
        ["A", "B"],
        [Edge(A, B, "r1"), Edge(B, A, "r2")],
    )
    with pytest.raises(ValueError, match="cyclic"):
        graph.check_acyclicity()


def test_check_acyclicity_empty_graph_passes() -> None:
    """A graph with no edges is trivially acyclic."""
    graph: JoinGraph = _make_graph(["A", "B"], [])
    graph.check_acyclicity()  # should not raise


def test_check_acyclicity_linear_chain_passes() -> None:
    """A linear chain (A -> B -> C -> D) is acyclic."""
    A, B, C, D = Node("A"), Node("B"), Node("C"), Node("D")
    graph: JoinGraph = _make_graph(
        ["A", "B", "C", "D"],
        [Edge(A, B, "r1"), Edge(B, C, "r2"), Edge(C, D, "r3")],
    )
    graph.check_acyclicity()  # should not raise


def test_join_graph_len_counts_nodes() -> None:
    """`len(graph)` returns the number of relations, matching the
    external reference `JoinGraph.__len__`."""
    graph: JoinGraph = _make_graph(["A", "B", "C"], [])
    assert len(graph) == 3

    empty: JoinGraph = _make_graph([], [])
    assert len(empty) == 0


def test_join_graph_iter_yields_relation_names() -> None:
    """`for name in graph:` iterates relation names in insertion order
    — not JoinNode objects, and not edges."""
    graph: JoinGraph = _make_graph(
        ["Alpha", "Bravo", "Charlie"],
        [Edge(Node("Alpha"), Node("Bravo"), "r")],
    )
    assert list(graph) == ["Alpha", "Bravo", "Charlie"]


# ---------------------------------------------------------------------------
# All 8 edge orientations of the tree A-C, B-C, C-D
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("directed_edges", orientation_params())
def test_subdatabase_works_for_all_edge_orientations(
    directed_edges: list[tuple[str, str]],
) -> None:
    """subdatabase must succeed for every one of the 8 orientations of A-C, B-C, C-D.

    Undirected join graph (fixed shape; C is the hub, D hangs off it)::

        A ── C ── D
             │
             B

    Each of the three edges A-C, B-C, C-D can be oriented either way (which
    relation embeds which), giving 2**3 = 8 directed variants — e.g. ``A→C,
    B→C, C→D`` (everything points at the hub / at D) or ``C→A, C→B, C→D`` (the
    hub points outward). `directed_edges` is one such orientation, supplied by
    `orientation_params`; `build_directed_tree` turns it into the DBF.

    Direction only decides embedding, never connectivity: the undirected shape
    stays the tree above in all 8 cases, so the join graph is always connected
    and acyclic. Hence, Yannakakis reduction must run without error in every
    orientation, and — since each relation holds exactly one tuple and every
    reference hits that single tuple, so nothing dangles — no tuple is reduced
    away: all four survive.

    Asserts, for every orientation: the graph has 4 nodes and 3 edges and passes
    the acyclicity check, subdatabase runs end-to-end without raising, and the
    surviving keys are exactly {a1, b1, c1, d1}.
    """
    dbf: DBF = build_directed_tree(
        directed_edges
    )  # build the 4-relation DBF for this orientation

    graph: JoinGraph = JoinGraph.from_dbf(
        dbf
    )  # extract the join graph the operator will traverse
    assert len(graph.nodes) == 4  # all four relations become nodes
    assert len(graph.edges) == 3  # the three reference edges are all present
    graph.check_acyclicity()  # every orientation of a tree is acyclic — must not raise

    result: DBF = subdatabase[DBF, DBF](
        dbf
    ).result  # run subdatabase end-to-end; must not raise in any orientation

    surviving: dict[str, set[str]] = {
        name: {item.key for item in getattr(result, name)}  # keys left after reduction
        for name in ("A", "B", "C", "D")
    }
    assert surviving == {
        "A": {"a1"},
        "B": {"b1"},
        "C": {"c1"},
        "D": {"d1"},
    }  # fully-connected single tuples survive regardless of edge direction


# ---------------------------------------------------------------------------
# Several relations carry a reference with the same name (ref_key reuse)
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


def _same_ref_key_chain_dbf() -> DBF:
    """A three-relation chain whose two hops are both called `next`.

    DBF {a, b, c}::

        a   (a1)       a1 -> b1
        │ next
        ▼
        b   (b1, b2)   b1 -> c1, b2 -> c2; no a-tuple points at b2
        │ next
        ▼
        c   (c1, c2)   c2 is reached only via the unconnected b2

    The full join is the single path a1 -> b1 -> c1. The RFs are reachable as
    `dbf.a` / `dbf.b` / `dbf.c`.
    """
    c: RF = RF(
        {"c1": TF({"name": "Gamma"}), "c2": TF({"name": "Delta"})}, frozen=False
    )  # end of the chain
    b: RF = RF(  # middle of the chain: b.next points into c
        {"b1": TF({"next": c["c1"]}), "b2": TF({"next": c["c2"]})},
        frozen=False,
    ).references("next", c)
    a: RF = RF({"a1": TF({"next": b["b1"]})}, frozen=False).references(
        "next", b
    )  # start of the chain: a.next points into b, with the same attribute name
    for rf in (a, b, c):  # freeze all three relations before wrapping them
        rf.freeze()  # frozen RFs, as in the other fixtures
    return DBF({"a": a, "b": b, "c": c}, frozen=True)  # all three are in the DBF


@pytest.mark.parametrize(
    "root", [pytest.param(None, id="auto-root"), pytest.param("a", id="root-a")]
)
def test_subdatabase_when_target_also_carries_the_ref_key(root: str | None) -> None:
    """subdatabase keeps a1 and b1 although b carries its own, unrelated `k`.

        a   (a1)
        │ k
        ▼
        b   (b1, b2)
        │ k
        ▼
        c   (outside the DBF)

    Symptom: every semijoin between a and b looks for "the" relation holding
    `k`, checks `reduce` first and, when b is reduced, wrongly follows b's own
    `k` (into c), so b is emptied; the next step then empties a.
    Repro: `subdatabase(dbf).result` (auto root b) or `subdatabase(dbf,
    root="a").result` on the DBF above.
    Expected: a == {a1}, b == {b1} (b2 is referenced by no a-tuple).
    Observed on main: auto root gives a == {}, b == {}; root="a" gives
    a == {a1}, b == {}.
    """
    dbf: DBF = _ambiguous_ref_key_dbf()  # a.k -> b, plus b.k -> c outside the DBF

    result: DBF = subdatabase[DBF, DBF](
        dbf, root=root
    ).result  # Yannakakis reduction over the single edge a -k-> b

    assert {item.key for item in result.a} == {"a1"}  # a1 reaches b1, so it stays
    assert {item.key for item in result.b} == {
        "b1"
    }  # b1 is referenced by a1; b2 by nobody, so it is dropped


@pytest.mark.parametrize(
    "root", [pytest.param(None, id="auto-root"), pytest.param("a", id="root-a")]
)
def test_subdatabase_chain_with_same_ref_key_on_every_hop(root: str | None) -> None:
    """A chain whose hops share one attribute name reduces to the connected path.

        a   (a1)
        │ next
        ▼
        b   (b1, b2)
        │ next
        ▼
        c   (c1, c2)

    Symptom: a and b both carry `next`, so the semijoin between them cannot
    tell which side references the other and picks `reduce`; the wrong
    reduction empties every relation.
    Repro: `subdatabase(dbf).result` (auto root c) or `subdatabase(dbf,
    root="a").result` on the DBF above.
    Expected: a == {a1}, b == {b1}, c == {c1}; b2 is dropped because no
    a-tuple references it, and c2 because only b2 referenced it.
    Observed on main: auto root gives all three empty; root="a" gives
    a == {a1}, b == {}, c == {}.
    """
    dbf: DBF = _same_ref_key_chain_dbf()  # a -next-> b -next-> c, all in the DBF

    result: DBF = subdatabase[DBF, DBF](
        dbf, root=root
    ).result  # full Yannakakis reduction over both hops

    assert {item.key for item in result.a} == {"a1"}  # the start of the only path
    assert {item.key for item in result.b} == {
        "b1"
    }  # b2 is referenced by no a-tuple, so it is dropped
    assert {item.key for item in result.c} == {
        "c1"
    }  # c2 hangs only off the dropped b2, so it is dropped as well


@pytest.mark.parametrize(
    "root_choice", [pytest.param("auto"), pytest.param("pure_source")]
)
@pytest.mark.parametrize("directed_edges", orientation_params())
def test_semijoin_cascade_steps_name_the_referencing_relation(
    directed_edges: list[tuple[str, str]], root_choice: str
) -> None:
    """Every cascade step records which of its two relations holds the reference.

    Undirected join graph (fixed shape; each edge oriented per
    `orientation_params`)::

        A ── C ── D
             │
             B

    For all 8 orientations, rooted both at the auto-selected root and at a
    pure source, each edge yields one bottom-up and one top-down step, and
    both must carry `source == edge.source.name`, whichever relation the step
    reduces. Would fail if `build_semijoin_cascade` set `source` to the
    reduced relation, to the BFS parent, or lost it in one of the two passes.
    """
    dbf: DBF = build_directed_tree(
        directed_edges
    )  # the 4-relation DBF for this orientation
    graph: JoinGraph = JoinGraph.from_dbf(dbf)  # its 3 directed reference edges
    root: str = (
        graph.select_root(None)  # the root subdatabase picks by itself
        if root_choice == "auto"
        else sorted(graph.pure_sources())[0]  # a relation nothing references
    )

    steps: list[SemijoinStep] = graph.build_semijoin_cascade(
        root
    )  # bottom-up steps followed by top-down steps

    for edge in graph.edges:  # check the steps of every reference edge
        pair: set[str] = {edge.source.name, edge.target.name}  # the edge's relations
        edge_steps: list[SemijoinStep] = [
            step for step in steps if {step.reduce, step.by} == pair
        ]  # the steps between exactly these two relations
        assert len(edge_steps) == 2  # one bottom-up and one top-down step per edge
        assert all(
            step.source == edge.source.name for step in edge_steps
        )  # both name the relation whose tuples hold the reference

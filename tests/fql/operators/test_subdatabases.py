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
from fql.operators.subdatabases import (
    subdatabase,
)
from fql.plan.join_graph import Node, JoinNode, Edge, JoinGraph

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
    = 6 semijoin steps.
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

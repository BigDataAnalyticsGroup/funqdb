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

"""Tests for views in the ``JoinGraph`` (designs 011 and 015).

A ``View`` draws a box around a subgraph, groups the joined subgraph by named
grouping keys and aggregates each group. The view is itself a node of the
enclosing graph: ``JoinGraph.add_view`` adds it, ``JoinGraph.add_edge``
connects it to a base relation through a grouping key that is a reference.
Since 015 a view may instead carry a projection (named output paths; a
projection name whose path is a reference can carry an edge, like a grouping
key) or pass the joined rows on, plus join predicates over several relations
of its subgraph.

All examples use the tiny sales schema of ``tests/fql/plan/sales_schema.py``::

    L (lineitems) --order--> O (orders) --customer--> C (customers) --nation--> N (nations)

Edges are compared as ``"A -customer-> C"`` labels (see ``edge_labels``) or by
``is`` identity, never via ``Edge ==`` or view ``==`` (that would compare
relations by content). The one deliberate ``==`` is in
``_decoy_reference_target_scenario``: a precondition that the decoy RF has the
same content as the real target, so that only the ``uuid`` tells them apart.

The happy paths come first, then one section per rule family (view
construction, join predicates, projections, pass-through views, ``add_view``,
``add_edge``). A section holds the rule violations, mostly as one
parametrized test, and may end with the cases its checks deliberately accept,
documented gaps included.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from fdm.attribute_functions import TF, RF
from fdm.schema import JoinPredicate
from fql.operators.aggregates import AggregationFunction, Count, Max, Sum
from fql.plan.join_graph import (
    AttributeName,
    JoinGraph,
    Node,
    RelationName,
    View,
)
from fql.predicates.predicates import And, Eq, Gt, Not, Predicate, Ref
from tests.fql.plan.sales_schema import (
    GtSubclass,
    OpaquePredicate,
    SalesRelations,
    edge_labels,
    graph_of,
    pass_through_view_graph,
    revenue_per_customer_view,
    revenue_view_joined_to_customers_graph,
    sales_relations,
    view_over_lineitems_and_orders,
)

if TYPE_CHECKING:  # only needed for the annotations of the case lists
    from _pytest.mark.structures import ParameterSet

# ---------------------------------------------------------------------------
# Helper types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EdgeArgs:
    """The arguments of one ``JoinGraph.add_edge`` call."""

    source: RelationName
    target: RelationName
    ref_key: AttributeName


@dataclass(frozen=True)
class EdgeScenario:
    """A graph plus one ``add_edge`` call that is to be tried on it."""

    graph: JoinGraph
    call: EdgeArgs


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_view_joins_outer_graph_through_reference_grouping_key() -> None:
    """A revenue-per-customer view is a node of the outer graph and joins C.

    ::

        +-- A ------------------------------+
        |  customer = O.customer            |
        |  revenue  = Sum(L.price)          |
        |                                   |
        |     L --order--> O                |
        +-----------------------------------+
                  |
               customer
                  v
                  C --nation--> N

    Would fail if ``add_view`` did not register the view as a node, if
    ``add_edge`` rejected the ``O.customer`` reference to ``C`` or stored
    other endpoint objects than the graph's own nodes, or if a topology query
    looked into the view (e.g. counted its inner ``L``/``O`` as nodes).
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    view: View = revenue_per_customer_view(rels)  # view A over L -> O
    graph: JoinGraph = graph_of(C=rels.customers, N=rels.nations)  # C -nation-> N

    graph.add_view(view)  # A becomes a node next to C and N
    graph.add_edge("A", "C", "customer")  # A.customer references C (via O.customer)

    assert list(graph) == ["C", "N", "A"]  # view appended after the base relations
    assert edge_labels(graph) == {
        "C -nation-> N",  # from the outer DBF
        "A -customer-> C",  # the hand-added view edge
    }  # exactly these two edges
    assert graph.edges[-1].source is view  # the edge starts at the view object itself
    assert graph.edges[-1].target is graph.nodes["C"]  # and ends at the outer C node
    assert edge_labels(view.subgraph) == {
        "L -order-> O"
    }  # O -customer-> C is not in the subgraph: C lives outside the view
    assert graph.is_tree() is True  # A - C - N is one undirected tree
    assert graph.pure_sources() == {"A"}  # only the view has no incoming edge
    assert graph.isolated_nodes() == set()  # every node takes part in an edge
    graph.check_acyclicity()  # does not raise: A -> C -> N is acyclic


def test_view_without_aggregates_is_a_distinct_projection() -> None:
    """A view with grouping keys only is a DISTINCT projection.

    ::

        +-- P ------------------------------+
        |  customer = O.customer            |
        |  (no aggregates)                  |
        |                                   |
        |     O                             |
        +-----------------------------------+

    Would fail if ``is_distinct()`` did not report a view with grouping keys
    and no aggregates.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    view: View = View(
        name="P",  # name of the view
        subgraph=graph_of(O=rels.orders),  # just the orders, no edges
        group_keys={"customer": "O.customer"},  # distinct customers that ordered
    )  # no aggregates: DISTINCT projection

    assert view.is_distinct() is True  # keys, no aggregates: DISTINCT projection
    assert view.output_names() == ["customer"]  # only the grouping key


def test_global_aggregate_view_without_grouping_keys() -> None:
    """A view with ``Count()`` and no grouping keys is a global aggregate.

    ::

        +-- G -----------------+
        |  orders = Count()    |
        |                      |
        |     O                |
        +----------------------+

    Would fail if a ``Count()`` without attribute were rejected by the path
    check, or if a view without grouping keys could not be added to a graph.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    view: View = View(
        name="G",  # name of the view in the outer graph
        subgraph=graph_of(O=rels.orders),  # count all orders
        aggregates={"orders": Count()},  # Count() has no attribute, so no path
    )  # no grouping keys: one result tuple
    graph: JoinGraph = JoinGraph()  # empty outer graph

    graph.add_view(view)  # G is the only node

    assert view.output_names() == ["orders"]  # only the aggregate
    assert view.is_distinct() is False  # Count() is an aggregate
    assert list(graph) == ["G"]  # the view is the single node
    assert graph.sole_relation_name() == "G"  # trivial graph of one view


def test_view_accepts_user_built_aggregation_without_attribute() -> None:
    """A user-built aggregation (``op_name`` None) may have no attribute.

    ::

        +-- G ------------------------------------+
        |  rows = AggregationFunction(len)        |
        |         (user-built, no attribute)      |
        |                                         |
        |     O                                   |
        +-----------------------------------------+

    Without an attribute a user-built aggregation works on the joined rows
    themselves, which only its own function knows how to read. Would fail if
    the attribute-less rule allowed only ``Count`` (e.g. tested the type
    instead of ``op_name``), rejecting user-built aggregations over whole rows.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    rows: AggregationFunction = AggregationFunction(len)  # op_name None, no attribute
    view: View = View(
        name="G",  # name of the view
        subgraph=graph_of(O=rels.orders),  # just O
        aggregates={"rows": rows},  # a global aggregate over whole rows
    )  # validated on construction: does not raise

    assert view.aggregates["rows"] is rows  # the very aggregation object
    assert view.output_names() == ["rows"]  # the one aggregate is the output


def test_view_grouping_by_tuples_joins_same_relation_outside() -> None:
    """A view grouping by the order tuples themselves joins the outer O.

    ::

        +-- A ------------------------------+
        |  order   = O                      |
        |  revenue = Sum(L.price)           |
        |                                   |
        |     L --order--> O                |
        +-----------------------------------+
                  |
                order
                  v
                  O --customer--> C

    The same RF ``orders`` appears inside and outside the view, as two
    separate ``JoinNode`` objects (one per scope). Would fail if the one-part
    key ``"O"`` were not recognised as a reference to the outer O, or if the
    edge pointed at the view's inner O node instead of the outer one.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    view: View = view_over_lineitems_and_orders(
        rels,
        group_keys={"order": "O"},  # one group per order tuple
        aggregates={"revenue": Sum("L.price")},  # revenue per order
    )  # view A over L -> O
    graph: JoinGraph = graph_of(O=rels.orders, C=rels.customers)  # O -customer-> C

    graph.add_view(view)  # A becomes a node next to O and C
    graph.add_edge("A", "O", "order")  # the grouped tuples are the outer O tuples

    assert view.output_names() == ["order", "revenue"]  # keys, then aggregates
    assert edge_labels(graph) == {
        "O -customer-> C",  # from the outer DBF
        "A -order-> O",  # the hand-added view edge
    }  # exactly these two edges
    assert graph.edges[-1].source is view  # the edge starts at the view object itself
    assert graph.edges[-1].target is graph.nodes["O"]  # the outer O, not the inner one


def test_nested_view_aggregates_over_inner_view() -> None:
    """A view can contain another view and aggregate over its output.

    ::

        +-- B ---------------------------------------------+
        |  nation      = C__nation                         |
        |  top_revenue = Max(A.revenue)                    |
        |                                                  |
        |   +-- A ---------------------------+             |
        |   |  customer = O.customer         |             |
        |   |  revenue  = Sum(L.price)       |             |
        |   |                                |             |
        |   |     L --order--> O             |             |
        |   +--------------------------------+             |
        |              |                                   |
        |           customer                               |
        |              v                                   |
        |              C --nation--> N                     |
        +--------------------------------------------------+
                       |
                    nation
                       v
                       N

    Would fail if a view could not be added to a subgraph, if a path into an
    inner view (``A.revenue``) were rejected, or if the outer view could not
    join ``N`` through ``C__nation`` (the ``__`` spelling of ``C.nation`` that
    ``where()`` also accepts, i.e. if ``add_edge`` split paths on ``.`` only).
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    subgraph: JoinGraph = revenue_view_joined_to_customers_graph(rels)  # A -> C -> N
    inner_view: Node = subgraph.nodes["A"]  # view A inside B's subgraph

    outer_view: View = View(
        name="B",  # name of the view in the outermost graph
        subgraph=subgraph,  # C, N and view A
        group_keys={"nation": "C__nation"},  # dunder spelling of C.nation
        aggregates={"top_revenue": Max("A.revenue")},  # best customer per nation
    )  # outer view B around A -> C -> N
    graph: JoinGraph = graph_of(N=rels.nations)  # outermost graph: just N
    graph.add_view(outer_view)  # B becomes a node next to N
    graph.add_edge("B", "N", "nation")  # B.nation references N (via C__nation)

    assert outer_view.output_names() == ["nation", "top_revenue"]  # keys, aggregates
    assert outer_view.subgraph.nodes["A"] is inner_view  # A is nested inside B
    assert edge_labels(outer_view.subgraph) == {
        "C -nation-> N",  # from the DBF of B's subgraph
        "A -customer-> C",  # the inner view edge
    }  # edges inside B
    assert edge_labels(graph) == {
        "B -nation-> N"
    }  # only the outer view edge; inner edges stay inside B
    assert graph.edges[-1].source is outer_view  # the edge starts at view B itself
    assert graph.edges[-1].target is graph.nodes["N"]  # and ends at the outermost N
    assert list(graph) == ["N", "B"]  # A is not a node of the outermost graph


def _view_grouping_by_inner_view(rels: SalesRelations) -> View:
    """View ``B`` around inner view ``A``, grouping by A's result tuples (path ``A``)."""
    subgraph: JoinGraph = JoinGraph()  # B's subgraph holds only view A
    subgraph.add_view(revenue_per_customer_view(rels))  # A: revenue per customer
    return View(
        name="B",  # view around view A
        subgraph=subgraph,  # just A
        group_keys={"rev_view": "A"},  # one-segment path: A's result tuples
    )  # no aggregates: distinct result tuples of A


def test_view_groups_by_result_tuples_of_inner_view() -> None:
    """A one-segment grouping key naming an inner view groups by its result tuples.

    ::

        +-- B ---------------------------------------+
        |  rev_view = A                              |
        |                                            |
        |   +-- A ------------------------------+    |
        |   |  customer = O.customer            |    |
        |   |  revenue  = Sum(L.price)          |    |
        |   |                                   |    |
        |   |     L --order--> O                |    |
        |   +-----------------------------------+    |
        +--------------------------------------------+

    Would fail if the path check required a path into an inner view to go on
    with one of the view's output names (``A.customer``), i.e. rejected the
    bare ``"A"``.
    """
    view: View = _view_grouping_by_inner_view(sales_relations())  # does not raise

    assert isinstance(view.subgraph.nodes["A"], View)  # precondition: "A" is a view
    assert view.output_names() == ["rev_view"]  # the key over A's result tuples


# ---------------------------------------------------------------------------
# Happy paths: join predicates, projections and pass-through views (015)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "predicate",
    [
        pytest.param(
            Gt("O.date", Ref("C.name")), id="structured"
        ),  # reads O directly and C through the Ref
        pytest.param(
            lambda row: row["O"]["date"] > row["C"]["name"], id="lambda"
        ),  # opaque: only its declared relations are checked
    ],
)
def test_view_accepts_join_predicate_over_two_relations(
    predicate: Callable[..., bool],
) -> None:
    """A view keeps a join predicate that spans two relations of its subgraph.

    ::

        +-- W ------------------------------------+
        |  join: Gt(O.date, Ref(C.name))          |
        |        or a lambda declared over (O, C) |
        |                                         |
        |     O --customer--> C                   |
        +-----------------------------------------+

    Would fail if the read-relations check ignored the ``Ref`` (it would see
    only ``O`` and reject the declared ``("O", "C")``), if a lambda were
    inspected like a structured predicate, or if the view dropped or copied
    the ``JoinPredicate``.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    join_predicate: JoinPredicate = JoinPredicate(
        ("O", "C"), predicate
    )  # declared over the two relations it reads
    view: View = View(
        name="W",  # name of the view
        subgraph=graph_of(O=rels.orders, C=rels.customers),  # O -customer-> C
        join_predicates=[join_predicate],  # one predicate over O and C
    )  # validated on construction: does not raise

    assert len(view.join_predicates) == 1  # exactly the one predicate
    assert view.join_predicates[0] is join_predicate  # the very object, not a copy


def test_projection_view_outputs_its_projection_names_in_order() -> None:
    """A projection view outputs its projection names, in the order given.

    ::

        +-- P ------------------------------+
        |  proj: name = C.name              |
        |        date = O.date              |
        |                                   |
        |     O --customer--> C             |
        +-----------------------------------+

    Would fail if ``output_names()`` ignored the projection (e.g. fell back to
    the relation names), sorted the names, if a projection were taken for a
    DISTINCT, or if ``output_path`` did not look into the projection.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    view: View = View(
        name="P",  # name of the view
        subgraph=graph_of(O=rels.orders, C=rels.customers),  # O -customer-> C
        projection={
            "name": "C.name",  # first output: the customer's name
            "date": "O.date",  # second output: the order date
        },  # not alphabetical, to see that the order is kept
    )  # one output tuple per joined row

    assert view.output_names() == ["name", "date"]  # projection names, as given
    assert view.is_distinct() is False  # a projection keeps duplicates
    assert view.output_path("name") == "C.name"  # the path behind "name"
    assert view.output_path("date") == "O.date"  # the path behind "date"


def test_pass_through_view_outputs_its_relation_names() -> None:
    """A view with neither grouping nor projection passes the joined rows on.

    ::

        +-- J ------------------------------+
        |  (no grouping, no projection)     |
        |                                   |
        |     L --order--> O                |
        +-----------------------------------+

    Would fail if a view without grouping keys and aggregates were still
    rejected (011's old rule), if its output names were not the subgraph's
    relation names, or if it counted as a DISTINCT projection.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    view: View = View(
        name="J",  # name of the view
        subgraph=graph_of(L=rels.lineitems, O=rels.orders),  # L -order-> O
    )  # nothing but the subgraph: does not raise

    assert view.output_names() == ["L", "O"]  # one output per subgraph relation
    assert view.passes_rows_on() is True  # neither grouping nor projection
    assert view.is_distinct() is False  # no grouping keys, so no DISTINCT
    assert view.output_path("L") is None  # a whole relation is no single path


def test_view_joins_outer_graph_through_projection_name() -> None:
    """A projection name whose path is a reference carries an edge to C.

    ::

        +-- W ------------------------------+
        |  proj: customer = O.customer      |
        |        date     = O.date          |
        |                                   |
        |     O                             |
        +-----------------------------------+
                  |
               customer
                  v
                  C

    Would fail if ``add_edge`` still accepted grouping keys only, or did not
    resolve the projection name ``customer`` to its path ``O.customer`` before
    checking the reference to ``C``.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    view: View = View(
        name="W",  # name of the view in the outer graph
        subgraph=graph_of(O=rels.orders),  # just the orders
        projection={
            "customer": "O.customer",  # a reference to C
            "date": "O.date",  # a scalar, carried along
        },  # one output tuple per order
    )  # a projection view
    graph: JoinGraph = graph_of(C=rels.customers)  # outer graph: just C

    graph.add_view(view)  # W becomes a node next to C
    graph.add_edge("W", "C", "customer")  # W.customer references C (via O.customer)

    assert edge_labels(graph) == {"W -customer-> C"}  # exactly the view edge
    assert graph.edges[-1].source is view  # the edge starts at the view object itself
    assert graph.edges[-1].target is graph.nodes["C"]  # and ends at the outer C


def test_view_around_whole_graph_with_join_predicate_projection_and_having() -> None:
    """A view around a whole graph combines a join predicate, a projection and HAVING.

    ::

        outer graph = { W }

        +-- W ------------------------------+
        |  join: Gt(O.date, Ref(C.name))    |
        |  proj: date = O.date              |
        |        name = C.name              |
        |                                   |
        |     O --customer--> C             |
        +-----------------------------------+
          HAVING Eq(name, 'Alice')

    The motivating example of design 015: a condition over two relations hangs
    at the view, the output shape is the projection, and a filter on the view
    node reads a projection output. Would fail if any of the three parts were
    rejected together, if the HAVING did not accept a projection name as an
    output, or if it landed in the view's subgraph instead of the outer graph.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    join_predicate: JoinPredicate = JoinPredicate(
        ("O", "C"), Gt("O.date", Ref("C.name"))
    )  # a condition over two relations of the subgraph
    view: View = View(
        name="W",  # name of the view in the outer graph
        subgraph=graph_of(O=rels.orders, C=rels.customers),  # O -customer-> C
        join_predicates=[join_predicate],  # kept rows satisfy the condition
        projection={"date": "O.date", "name": "C.name"},  # the output shape
    )  # the view around the whole graph O -> C
    graph: JoinGraph = JoinGraph()  # empty outer graph
    graph.add_view(view)  # W is the only node
    having: Eq = Eq("name", "Alice")  # reads the projection output "name"

    graph.add_filter("W", having)  # HAVING on W's result tuples

    assert list(graph) == ["W"]  # the view is the single node of the outer graph
    assert graph.sole_relation_name() == "W"  # trivial graph of one view
    assert edge_labels(view.subgraph) == {
        "O -customer-> C"
    }  # the subgraph keeps its own edge
    assert view.join_predicates[0] is join_predicate  # the condition hangs at W
    assert view.output_names() == ["date", "name"]  # the projection names
    assert graph.filters["W"][0] is having  # HAVING sits in the outer graph
    assert view.subgraph.filters == {}  # and not inside the view


# ---------------------------------------------------------------------------
# Rule violations: view construction
# ---------------------------------------------------------------------------

#: View constructions that each break exactly one validation rule, with the
#: regex the ``ValueError`` message must match.
_INVALID_VIEW_CASES: list[ParameterSet] = [
    pytest.param(
        lambda rels: View(
            name="A", subgraph=JoinGraph(), aggregates={"orders": Count()}
        ),  # nothing to aggregate over
        r"View 'A' needs a non-empty subgraph",
        id="empty-subgraph",
    ),
    pytest.param(
        lambda rels: view_over_lineitems_and_orders(
            rels,
            group_keys={"customer": "O.customer"},  # output name "customer" ...
            aggregates={"customer": Count()},  # ... used a second time
        ),  # view A with a clashing output name
        r"View 'A': output names used twice: \['customer'\]",
        id="output-name-clash",
    ),
    pytest.param(
        lambda rels: view_over_lineitems_and_orders(
            rels, group_keys={"a.b": "O.customer"}, aggregates={}
        ),  # output name with a dot
        r"View 'A': output name 'a\.b' must be non-empty and contain neither",
        id="output-name-dot",
    ),
    pytest.param(
        lambda rels: view_over_lineitems_and_orders(
            rels, group_keys={"": "O.customer"}, aggregates={}
        ),  # empty output name
        r"View 'A': output name '' must be non-empty and contain neither",
        id="output-name-empty",
    ),
    pytest.param(
        lambda rels: view_over_lineitems_and_orders(
            rels, group_keys={}, aggregates={"a.b": Count()}
        ),  # aggregate name with a dot
        r"View 'A': output name 'a\.b' must be non-empty and contain neither",
        id="aggregate-name-dot",
    ),
    pytest.param(
        lambda rels: view_over_lineitems_and_orders(
            rels, group_keys={"customer": "C.name"}, aggregates={}
        ),  # C lives outside the view's subgraph L -> O
        r"View 'A': path 'C\.name' of 'customer' does not start with a relation "
        r"of the subgraph \['L', 'O'\]",
        id="group-key-path-outside-subgraph",
    ),
    pytest.param(
        lambda rels: view_over_lineitems_and_orders(
            rels, group_keys={}, aggregates={"revenue": Sum("X.price")}
        ),  # there is no relation X in the subgraph
        r"View 'A': path 'X\.price' of 'revenue' does not start with a relation",
        id="aggregate-path-outside-subgraph",
    ),
    pytest.param(
        lambda rels: view_over_lineitems_and_orders(
            rels, group_keys={"d": "O..date"}, aggregates={}
        ),  # an empty segment between the two dots
        r"Empty segment in attribute path 'O\.\.date'",
        id="path-with-empty-segment",
    ),
    pytest.param(
        lambda rels: view_over_lineitems_and_orders(
            rels, group_keys={}, aggregates={"big": Gt("L.price", 20)}
        ),  # a predicate, not an aggregation
        r"View 'A': aggregate 'big' is not an AggregationFunction: Gt\('L\.price', 20\)",
        id="aggregate-is-predicate",
    ),
    pytest.param(
        lambda rels: view_over_lineitems_and_orders(
            rels, group_keys={}, aggregates={"total": Sum(None)}
        ),  # a built-in aggregation other than Count() without attribute
        r"View 'A': aggregate 'total' needs an attribute: Sum\(\)",
        id="builtin-aggregate-without-attribute",
    ),
    pytest.param(
        lambda rels: View(
            name="B",  # outer view around A -> C -> N
            subgraph=revenue_view_joined_to_customers_graph(rels),  # inner view A
            aggregates={"top": Max("A.nope")},  # A has no output 'nope'
        ),  # view B with a path into an unknown output of A
        r"View 'B': path 'A\.nope' of 'top' names no output of inner view 'A': "
        r"\['customer', 'revenue'\]",
        id="path-into-inner-view-unknown-output",
    ),
    pytest.param(
        lambda rels: View(
            name="A__B",  # view name with a dunder
            subgraph=graph_of(O=rels.orders),  # just O
            group_keys={"customer": "O.customer"},  # a valid grouping key
        ),  # otherwise valid view
        r"View 'A__B': view name 'A__B' must be non-empty and contain neither",
        id="view-name-dunder",
    ),
]  # one parameter set per broken rule


@pytest.mark.parametrize("build, message", _INVALID_VIEW_CASES)
def test_view_construction_rejects_invalid_view(
    build: Callable[[SalesRelations], View], message: str
) -> None:
    """Each view-construction rule raises its own ``ValueError`` message.

    ::

        +-- A ------------------------------+
        |  <key> = <path>                   |   <- one broken rule per case,
        |  <agg> = <AggregationFunction>    |      named by the case id
        |                                   |
        |     L --order--> O                |
        +-----------------------------------+

        other shapes: empty-subgraph (A over an empty subgraph),
        path-into-inner-view-unknown-output (B over A -> C -> N, A the
        revenue view), view-name-dunder (A__B over O)

    Would fail if a rule check were missing (no error), or if a case tripped a
    different rule than intended (the message would not match).
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this case
    with pytest.raises(ValueError, match=message):  # the rule-specific message
        build(rels)  # constructing the view runs its validation


# ---------------------------------------------------------------------------
# Rule violations: join predicates (015)
# ---------------------------------------------------------------------------


def _view_over_o_c_n(
    rels: SalesRelations, *join_predicates: JoinPredicate | Predicate
) -> View:
    """View ``W`` over ``O -customer-> C -nation-> N`` with the given join predicates.

    A join predicate may also be a bare ``Predicate``, for the case that
    checks the rejection of a join predicate that is no ``JoinPredicate``.
    """
    return View(
        name="W",  # name of the view
        subgraph=graph_of(
            O=rels.orders, C=rels.customers, N=rels.nations
        ),  # O -customer-> C -nation-> N
        join_predicates=list(join_predicates),  # in the given order, from index 0
    )  # validated on construction


def _view_over_revenue_graph(
    rels: SalesRelations, join_predicate: JoinPredicate
) -> View:
    """View ``W`` over ``A -customer-> C -nation-> N`` with one join predicate."""
    return View(
        name="W",  # name of the view
        subgraph=revenue_view_joined_to_customers_graph(rels),  # inner view A, C, N
        join_predicates=[join_predicate],  # the predicate under test, at index 0
    )  # validated on construction


#: Views whose one join predicate breaks exactly one rule, with the regex the
#: ``ValueError`` message must match.
_INVALID_JOIN_PREDICATE_CASES: list[ParameterSet] = [
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels, Gt("O.date", Ref("C.name"))
        ),  # a bare predicate: no declared relations
        r"View 'W': join predicate 0 is not a JoinPredicate: "
        r"Gt\('O\.date', Ref\('C\.name'\)\)",
        id="not-a-join-predicate",
    ),
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels, JoinPredicate(("O",), Gt("O.date", "2025-12-31"))
        ),  # one declared relation: a node filter in disguise
        r"View 'W': join predicate 0 must declare at least two relations of the "
        r"subgraph, declares \['O'\]; a predicate on one relation belongs on that "
        r"node \(JoinGraph\.add_filter on the subgraph, with paths relative to its "
        r"tuples: 'O\.date' becomes 'date'\)",
        id="declares-one-relation",
    ),
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels, JoinPredicate(("O", "O"), Gt("O.date", "2025-12-31"))
        ),  # two declared names, but the same relation twice
        r"View 'W': join predicate 0 must declare at least two relations of the "
        r"subgraph, declares \['O'\]",
        id="declares-same-relation-twice",
    ),
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels, JoinPredicate(("O",), lambda row: row["O"]["date"] > "2025-12-31")
        ),  # a lambda is not inspected, but its declaration still is
        r"View 'W': join predicate 0 must declare at least two relations of the "
        r"subgraph, declares \['O'\]",
        id="lambda-declares-one-relation",
    ),
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels, JoinPredicate((), lambda row: True)
        ),  # declares nothing at all
        r"View 'W': join predicate 0 must declare at least two relations of the "
        r"subgraph, declares \[\]",
        id="lambda-declares-no-relation",
    ),
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels, JoinPredicate(("O",), Gt("O.date", Ref("C.name")))
        ),  # reads two relations, but the declaration check comes first
        r"View 'W': join predicate 0 must declare at least two relations of the "
        r"subgraph, declares \['O'\]",
        id="declares-one-relation-but-reads-two",
    ),
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels, JoinPredicate(("O", "C"), Gt("O.date", Ref("O.customer")))
        ),  # two paths, but both into O: C is declared but never read
        r"View 'W': join predicate 0 reads \['O'\] but declares \['C', 'O'\]",
        id="two-paths-one-relation",
    ),
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels, JoinPredicate(("O", "L"), Gt("O.date", Ref("L.price")))
        ),  # L is not part of W's subgraph O -> C -> N
        r"View 'W': join predicate 0 declares relations outside the subgraph "
        r"\['C', 'N', 'O'\]: \['L'\]",
        id="declares-relation-outside-subgraph",
    ),
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels,
            JoinPredicate(("O", "L"), lambda row: row["O"]["date"] > row["L"]["price"]),
        ),  # not inspectable, but its declared relations still are checked
        r"View 'W': join predicate 0 declares relations outside the subgraph "
        r"\['C', 'N', 'O'\]: \['L'\]",
        id="lambda-declares-relation-outside-subgraph",
    ),
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels, JoinPredicate(("O", "C"), Gt("O.date", Ref("N.name")))
        ),  # reads N, which it does not declare (and not C, which it does)
        r"View 'W': join predicate 0 reads \['N', 'O'\] but declares \['C', 'O'\]",
        id="reads-undeclared-relation",
    ),
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels, JoinPredicate(("O", "C", "N"), Gt("O.date", Ref("C.name")))
        ),  # declares N but never reads it: reads must EQUAL declared
        r"View 'W': join predicate 0 reads \['C', 'O'\] but declares "
        r"\['C', 'N', 'O'\]",
        id="declares-unread-relation",
    ),
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels,
            JoinPredicate(
                ("O", "C"),
                And(Gt("O.date", "2025-12-31"), Not(Eq("O.date", "2026-06-01"))),
            ),
        ),  # every nested part reads O only; C is declared but never read
        r"View 'W': join predicate 0 reads \['O'\] but declares \['C', 'O'\]",
        id="nested-parts-read-one-relation",
    ),
    pytest.param(
        lambda rels: _view_over_revenue_graph(
            rels, JoinPredicate(("A", "C"), Gt("A.nope", Ref("C.name")))
        ),  # A exists, but has no output 'nope'
        r"View 'W': path 'A\.nope' of 'join predicate 0' names no output of inner "
        r"view 'A': \['customer', 'revenue'\]",
        id="path-into-unknown-output-of-inner-view",
    ),
    pytest.param(
        lambda rels: _view_over_o_c_n(
            rels,
            JoinPredicate(("O", "C"), Gt("O.date", Ref("C.name"))),  # valid, index 0
            JoinPredicate(("O", "C"), Gt("O.date", Ref("N.name"))),  # broken, index 1
        ),  # the message must name the broken one
        r"View 'W': join predicate 1 reads \['N', 'O'\] but declares \['C', 'O'\]",
        id="second-predicate-broken",
    ),
]  # one parameter set per broken rule


@pytest.mark.parametrize("build, message", _INVALID_JOIN_PREDICATE_CASES)
def test_view_construction_rejects_invalid_join_predicate(
    build: Callable[[SalesRelations], View], message: str
) -> None:
    """Each join-predicate rule raises its own ``ValueError`` message.

    ::

        +-- W ----------------------------------------+
        |  join: <JoinPredicate>                      |   <- one broken rule per
        |                                             |      case, named by the
        |     O --customer--> C --nation--> N         |      case id
        +---------------------------------------------+

        other shapes: path-into-unknown-output-of-inner-view (W over
        A -> C -> N, A the revenue view); second-predicate-broken (a valid
        join predicate at index 0, the broken one at index 1)

    Would fail if a rule check were missing (no error); if duplicate declared
    relations were counted twice (``declares-same-relation-twice``); if a
    lambda skipped the declaration checks, or the outside-the-subgraph check
    came after the early return for uninspectable predicates
    (``lambda-declares-*``); if the read/declared comparison ran before the
    at-least-two check (``declares-one-relation-but-reads-two`` must get the
    declaration message); if the read relations only had to be a subset of
    the declared ones (``declares-unread-relation``); if ``And`` / ``Not``
    parts or ``Ref`` values were not read; if join-predicate paths skipped
    ``_check_path``; or if the message named the wrong predicate
    (``second-predicate-broken``).
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this case
    with pytest.raises(ValueError, match=message):  # the rule-specific message
        build(rels)  # constructing the view runs its validation


@pytest.mark.parametrize(
    "predicate",
    [
        pytest.param(
            OpaquePredicate("X.nope"), id="user-defined-predicate-not-inspected"
        ),  # X is no relation at all; a check would reject the path
        pytest.param(
            GtSubclass("O.date", "2025-12-31"), id="subclass-of-built-in-not-inspected"
        ),  # if inspected, it would read only O and fail the declared (O, C)
        pytest.param(
            And(Gt("O.date", "2025-12-31"), OpaquePredicate("C.name")),
            id="and-with-user-defined-part-not-inspected",
        ),  # the built-in part alone reads only O; the opaque part hides C
        pytest.param(
            And(Eq("O.date", "2026-01-01"), Eq("C.name", "Alice")),
            id="and-of-single-relation-parts-documented-gap",
        ),  # reads O and C, but each part belongs on its node
    ],
)
def test_view_accepts_join_predicate_the_checks_cannot_see_into(
    predicate: Predicate,
) -> None:
    """Join predicates the checks cannot see into (or split) are accepted.

    ::

        +-- W ---------------------------------------------+
        |  join over (O, C): <predicate>                   |
        |    - OpaquePredicate('X.nope'): user-defined,    |
        |      so not inspected                            |
        |    - GtSubclass(O.date, ..): a Gt subclass, so   |
        |      not inspected (exact type only)             |
        |    - And(Gt(O.date, ..),                         |
        |          OpaquePredicate(C.name)): one opaque    |
        |      part, so the WHOLE And is not inspected     |
        |    - And(Eq(O.date, ..), Eq(C.name, ..)): each   |
        |      part reads ONE relation (known gap)         |
        |                                                  |
        |     O --customer--> C                            |
        +--------------------------------------------------+

    The first three cases are intended: only built-in predicate classes (by
    exact type) are inspected, and one part the check cannot see into makes
    the whole predicate uninspectable, so it is checked like a lambda
    (declared relations only). The last case pins a known gap of design 015
    (``DECISION.md``, "Known gaps"): an ``And`` of single-relation parts
    declared over two relations is accepted although its parts belong on the
    nodes O and C. That is documented, not intended; closing the gap should
    turn the case into a rejection. Would fail if the exact-type check became
    an ``isinstance`` check (only the ``GtSubclass`` case pins this:
    ``OpaquePredicate`` is no subclass of a built-in), if the walk skipped the
    opaque part of an ``And`` and checked the rest (it would read only O and
    reject the declared ``("O", "C")``), or if the gap were closed without
    updating this test.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this case
    join_predicate: JoinPredicate = JoinPredicate(
        ("O", "C"), predicate
    )  # declared over two relations of the subgraph
    view: View = View(
        name="W",  # name of the view
        subgraph=graph_of(O=rels.orders, C=rels.customers),  # O -customer-> C
        join_predicates=[join_predicate],  # the predicate under test
    )  # validated on construction: does not raise

    assert len(view.join_predicates) == 1  # exactly the one predicate
    assert view.join_predicates[0] is join_predicate  # the very object, not a copy


# ---------------------------------------------------------------------------
# Rule violations: projections (015)
# ---------------------------------------------------------------------------

#: Projection views that each break exactly one rule, with the regex the
#: ``ValueError`` message must match.
_INVALID_PROJECTION_CASES: list[ParameterSet] = [
    pytest.param(
        lambda rels: View(
            name="W",  # name of the view
            subgraph=graph_of(L=rels.lineitems, O=rels.orders),  # L -order-> O
            projection={"date": "O.date"},  # a projection ...
            group_keys={"customer": "O.customer"},  # ... plus a grouping key
        ),  # two output shapes at once
        r"View 'W' has a projection and a grouping; use one of them "
        r"\(or an outer view\)",
        id="projection-and-group-keys",
    ),
    pytest.param(
        lambda rels: View(
            name="W",  # name of the view
            subgraph=graph_of(L=rels.lineitems, O=rels.orders),  # L -order-> O
            projection={"date": "O.date"},  # a projection ...
            aggregates={"revenue": Sum("L.price")},  # ... plus an aggregate
        ),  # two output shapes at once
        r"View 'W' has a projection and a grouping; use one of them "
        r"\(or an outer view\)",
        id="projection-and-aggregates",
    ),
    pytest.param(
        lambda rels: View(
            name="W",  # name of the view
            subgraph=graph_of(L=rels.lineitems, O=rels.orders),  # L -order-> O
            projection={"name": "C.name"},  # C lives outside the subgraph
        ),  # projection path outside the subgraph
        r"View 'W': path 'C\.name' of 'name' does not start with a relation of "
        r"the subgraph \['L', 'O'\]",
        id="projection-path-outside-subgraph",
    ),
    pytest.param(
        lambda rels: View(
            name="W",  # name of the view
            subgraph=graph_of(L=rels.lineitems, O=rels.orders),  # L -order-> O
            projection={"o.date": "O.date"},  # the name would split into two segments
        ),  # projection name with a dot
        r"View 'W': output name 'o\.date' must be non-empty and contain neither",
        id="projection-name-dot",
    ),
]  # one parameter set per broken rule


@pytest.mark.parametrize("build, message", _INVALID_PROJECTION_CASES)
def test_view_construction_rejects_invalid_projection(
    build: Callable[[SalesRelations], View], message: str
) -> None:
    """Each projection rule raises its own ``ValueError`` message.

    ::

        +-- W ------------------------------+
        |  proj: <name> = <path>            |   <- the one broken rule:
        |  <key> = <path>                   |      projection together with
        |  <agg> = <AggregationFunction>    |      grouping keys or with
        |                                   |      aggregates; projection path
        |     L --order--> O                |      outside the subgraph;
        +-----------------------------------+      projection name with a dot

    Would fail if projection and grouping could be combined (the check tested
    only one of grouping keys and aggregates), or if projection names and
    paths skipped the name and path checks that grouping keys get.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this case
    with pytest.raises(ValueError, match=message):  # the rule-specific message
        build(rels)  # constructing the view runs its validation


def test_view_accepts_projection_path_unknown_to_base_relation_documented_gap() -> None:
    """A projection path into a base relation is not checked against its attributes.

    ::

        +-- W ------------------------------+
        |  proj: x = O.nope                 |   <- O has no attribute 'nope',
        |                                   |      accepted anyway
        |     O                             |
        +-----------------------------------+

    This pins a known gap of design 015 (``DECISION.md``, "Known gaps"): a
    view's projection paths are not checked against base relations, which
    have no schema here; only the first segment must name a relation of the
    subgraph. The gap is documented, not intended; a schema-based check should
    turn this test into a rejection. Would fail if such a check were added
    without updating this test.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    view: View = View(
        name="W",  # name of the view
        subgraph=graph_of(O=rels.orders),  # O has only "date" and "customer"
        projection={"x": "O.nope"},  # an attribute O does not have
    )  # validated on construction: does not raise

    assert view.output_names() == ["x"]  # the one projection name
    assert view.output_path("x") == "O.nope"  # the unchecked path, kept as given


# ---------------------------------------------------------------------------
# Pass-through views (015)
# ---------------------------------------------------------------------------


def test_pass_through_view_does_not_name_check_its_relation_names() -> None:
    """A pass-through view's outputs are relation names and are not name-checked.

    ::

        +-- P (pass-through) ---------------+
        |  outputs: my__rel                 |
        |                                   |
        |     my__rel  (the orders RF)      |
        +-----------------------------------+

    Would fail if ``__post_init__`` ran the output-name check over
    ``output_names()`` instead of over the named outputs only: a DBF may name a
    relation ``my__rel``, and passing it on must not fail.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    view: View = View(
        name="P",  # name of the view
        subgraph=graph_of(my__rel=rels.orders),  # a relation name with "__"
    )  # no grouping, no projection: does not raise

    assert view.output_names() == ["my__rel"]  # the relation name, unchanged
    assert view.passes_rows_on() is True  # a pass-through view


def test_path_cannot_reach_relation_named_with_dunder_documented_gap() -> None:
    """No path reaches a subgraph relation whose name contains ``__``.

    ::

        +-- W ------------------------------+
        |  proj: d = my__rel.date           |   <- rejected: the path splits
        |                                   |      into my / rel / date
        |     my__rel  (the orders RF)      |
        +-----------------------------------+

    ``__`` separates path segments like ``.``, so ``my__rel.date`` starts with
    ``my``, which is no relation of the subgraph. This pins the second half of
    a known gap of design 015 (``DECISION.md``): such a relation can be put
    into a view (see the test above) but no path reaches it. The gap is
    documented, not intended. Would fail if paths stopped splitting on ``__``
    (the relation would become reachable) without updating this test.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test

    with pytest.raises(
        ValueError,
        match=r"View 'W': path 'my__rel\.date' of 'd' does not start with a "
        r"relation of the subgraph \['my__rel'\]",
    ):  # the first segment is "my", not "my__rel"
        View(
            name="W",  # name of the view
            subgraph=graph_of(my__rel=rels.orders),  # a relation name with "__"
            projection={"d": "my__rel.date"},  # meant: date of my__rel
        )  # construction runs the path check


#: Views of each output kind over O, with the expected ``passes_rows_on()``.
_PASSES_ROWS_ON_CASES: list[ParameterSet] = [
    pytest.param(
        lambda rels: View(name="P", subgraph=graph_of(O=rels.orders)),  # nothing else
        True,
        id="pass-through",
    ),
    pytest.param(
        lambda rels: View(
            name="P",  # name of the view
            subgraph=graph_of(O=rels.orders),  # just O
            group_keys={"customer": "O.customer"},  # grouping keys only
        ),  # a DISTINCT projection
        False,
        id="grouping-keys-only",
    ),
    pytest.param(
        lambda rels: View(
            name="P",  # name of the view
            subgraph=graph_of(O=rels.orders),  # just O
            aggregates={"orders": Count()},  # aggregates only
        ),  # a global aggregate
        False,
        id="aggregates-only",
    ),
    pytest.param(
        lambda rels: View(
            name="P",  # name of the view
            subgraph=graph_of(O=rels.orders),  # just O
            projection={"date": "O.date"},  # projection only
        ),  # one output tuple per order
        False,
        id="projection",
    ),
]  # one parameter set per output kind


@pytest.mark.parametrize("build, expected", _PASSES_ROWS_ON_CASES)
def test_passes_rows_on_only_without_grouping_and_projection(
    build: Callable[[SalesRelations], View], expected: bool
) -> None:
    """``passes_rows_on()`` is True only without grouping and without projection.

    ::

        +-- P -------------------------------------+
        |  one of:  (nothing)             -> True  |
        |           customer = O.customer -> False |
        |           orders   = Count()    -> False |
        |           proj: date = O.date   -> False |
        |                                          |
        |     O                                    |
        +------------------------------------------+

    Would fail if ``passes_rows_on`` ignored one of grouping keys, aggregates
    or projection; ``output_names()`` and ``add_filter`` would then treat that
    view as a pass-through.
    """
    view: View = build(sales_relations())  # fresh RFs for this case

    assert view.passes_rows_on() is expected  # True only without any output shape


def test_view_rejects_two_level_path_into_unknown_output() -> None:
    """A view path through an inner pass-through view is checked down to inner view A.

    ::

        +-- B ------------------------------------------------+
        |  proj: revenue = P.A.nope     <- rejected           |
        |                                                     |
        |  +-- P (pass-through: outputs C, N, A) ----------+  |
        |  |  +-- A ---------------------------+           |  |
        |  |  |  customer = O.customer         |           |  |
        |  |  |  revenue  = Sum(L.price)       |           |  |
        |  |  |     L --order--> O             |           |  |
        |  |  +--------------------------------+           |  |
        |  |              |                                |  |
        |  |           customer                            |  |
        |  |              v                                |  |
        |  |              C --nation--> N                  |  |
        |  +-----------------------------------------------+  |
        +-----------------------------------------------------+

    ``A`` is an output of P, so only the third segment is wrong. The error
    comes from P's path check on the rest of the path (``A.nope``), with the
    projection name as the owner. Would fail if ``_check_path`` stopped after
    one level instead of going on through an inner view that passes rows on
    (a misspelt output of A would then drop every row silently).
    """
    subgraph: JoinGraph = pass_through_view_graph(sales_relations())  # only P

    with pytest.raises(
        ValueError,
        match=r"View 'P': path 'A\.nope' of 'revenue' names no output of inner "
        r"view 'A': \['customer', 'revenue'\]",
    ):  # raised one level down, by P, with B's projection name as the owner
        View(
            name="B",  # the outer view
            subgraph=subgraph,  # holds the pass-through view P
            projection={"revenue": "P.A.nope"},  # P and A exist, 'nope' does not
        )  # construction runs the path check


def test_view_accepts_two_level_path_into_inner_view_output() -> None:
    """A view path through an inner pass-through view may name an output of A.

    ::

        +-- B ------------------------------------------------+
        |  proj: revenue = P.A.revenue                        |
        |                                                     |
        |  +-- P (pass-through: outputs C, N, A) ----------+  |
        |  |  +-- A ---------------------------+           |  |
        |  |  |  customer = O.customer         |           |  |
        |  |  |  revenue  = Sum(L.price)       |           |  |
        |  |  |     L --order--> O             |           |  |
        |  |  +--------------------------------+           |  |
        |  |              |                                |  |
        |  |           customer                            |  |
        |  |              v                                |  |
        |  |              C --nation--> N                  |  |
        |  +-----------------------------------------------+  |
        +-----------------------------------------------------+

    Would fail if the recursive path check passed the wrong rest of the path
    down (e.g. the whole path, or one segment too few), or checked the second
    segment against A's outputs instead of P's.
    """
    view: View = View(
        name="B",  # the outer view
        subgraph=pass_through_view_graph(sales_relations()),  # holds P
        projection={"revenue": "P.A.revenue"},  # A's real output, through P
    )  # validated on construction: does not raise

    assert view.output_names() == ["revenue"]  # the one projection name
    assert view.output_path("revenue") == "P.A.revenue"  # the full path, as given


# ---------------------------------------------------------------------------
# add_view: name clashes and view loops
# ---------------------------------------------------------------------------


def test_add_view_rejects_name_already_used() -> None:
    """A view cannot take the name of a node that is already in the graph.

    ::

        +-- C (view) ----------------------+
        |  customer = O.customer           |
        |  revenue  = Sum(L.price)         |      C --nation--> N
        |     L --order--> O               |      (base relation C)
        +----------------------------------+

    Would fail if ``add_view`` silently replaced the base relation ``C`` by the
    view, which would leave the edge ``C -nation-> N`` starting at a node that
    is no longer in the graph.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    view: View = View(
        name="C",  # same name as the base relation C
        subgraph=graph_of(L=rels.lineitems, O=rels.orders),  # L -order-> O
        group_keys={"customer": "O.customer"},  # revenue per customer
        aggregates={"revenue": Sum("L.price")},  # sum of line prices per group
    )  # a valid view, only its name clashes
    graph: JoinGraph = graph_of(C=rels.customers, N=rels.nations)  # C -nation-> N
    base_c: Node = graph.nodes["C"]  # the base relation C before the call

    with pytest.raises(ValueError, match=r"Node name 'C' is already used"):  # clash
        graph.add_view(view)  # C is taken by the base relation

    assert graph.nodes["C"] is base_c  # the base relation was not replaced


def test_add_view_rejects_view_around_its_own_graph() -> None:
    """A view cannot be added to the very graph that is its subgraph (direct loop).

    ::

        graph G = { O, A }
                      ^
        +-- A --------|-----------------+
        |  customer = O.customer        |
        |                               |
        |     subgraph = G  (itself)    |
        +-------------------------------+

    Would fail if ``add_view`` accepted a view whose subgraph is the target
    graph itself, i.e. if it added the view or raised another error than the
    own-graph ``ValueError``.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    graph: JoinGraph = graph_of(O=rels.orders)  # outer graph: just O
    view: View = View(
        name="A",  # a fresh, unused name
        subgraph=graph,  # the very graph the view is added to below
        group_keys={"customer": "O.customer"},  # a valid grouping key
    )  # a valid view on its own
    nodes_before: list[RelationName] = list(graph)  # node names before the call

    with pytest.raises(
        ValueError, match=r"View 'A' cannot contain the graph it is added to"
    ):  # rejected with the own-graph message
        graph.add_view(view)  # the view would sit inside its own subgraph

    assert list(graph) == nodes_before  # the view was not added


def test_add_view_rejects_two_level_view_loop() -> None:
    """A view cannot be added to a graph that its subgraph encloses via another view.

    ::

        graph G = { O, A? }              graph H = { L, B }
                       |                               |
        +-- A ---------v----------+     +-- B ---------v----------+
        |  revenue = Sum(L.price) |     |  customer = O.customer  |
        |  subgraph = H           |     |  subgraph = G           |
        +-------------------------+     +-------------------------+

        H.add_view(B) is fine; G.add_view(A) would close the loop
        G -> A -> H -> B -> G.

    Would fail if ``add_view`` only compared ``view.subgraph is self`` (the
    direct case) instead of asking ``view.subgraph._encloses(self)``: A's
    subgraph H is not G, but H holds view B whose subgraph is G.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    graph_g: JoinGraph = graph_of(O=rels.orders)  # G: just O
    graph_h: JoinGraph = graph_of(L=rels.lineitems)  # H: just L
    loop_view_a: View = View(
        name="A",  # the view that would close the loop
        subgraph=graph_h,  # A lives over H
        aggregates={"revenue": Sum("L.price")},  # a valid aggregate over H
    )  # a valid view on its own
    loop_view_b: View = View(
        name="B",  # the view that links H back to G
        subgraph=graph_g,  # B lives over G
        group_keys={"customer": "O.customer"},  # a valid grouping key over G
    )  # a valid view on its own
    graph_h.add_view(loop_view_b)  # allowed: G does not enclose H (yet)
    nodes_before: list[RelationName] = list(graph_g)  # G's node names before

    with pytest.raises(
        ValueError, match=r"View 'A' cannot contain the graph it is added to"
    ):  # rejected with the own-graph message
        graph_g.add_view(loop_view_a)  # H encloses G through B: a two-level loop

    assert list(graph_g) == nodes_before  # A was not added to G
    assert list(graph_h) == ["L", "B"]  # H still holds L and view B, unchanged


# ---------------------------------------------------------------------------
# Rule violations: add_edge endpoints and kinds
# ---------------------------------------------------------------------------


def _endpoint_graph(rels: SalesRelations) -> JoinGraph:
    """Graph with base relations C, N and views A and B."""
    graph: JoinGraph = graph_of(C=rels.customers, N=rels.nations)  # C -nation-> N
    graph.add_view(revenue_per_customer_view(rels))  # view A: revenue per customer
    graph.add_view(
        View(
            name="B",  # second view, a possible (but forbidden) edge target
            subgraph=graph_of(O=rels.orders),  # count all orders
            aggregates={"orders": Count()},  # global aggregate
        )  # the view to add
    )  # the view becomes a node of the graph
    return graph  # no view edges yet


#: ``add_edge`` call sequences on ``_endpoint_graph``: all calls but the last
#: succeed, the last breaks exactly one rule and must match the regex.
_INVALID_ENDPOINT_CASES: list[ParameterSet] = [
    pytest.param(
        [EdgeArgs("Z", "C", "customer")],  # no node Z
        r"'Z' is not a node of the graph",
        id="source-not-in-graph",
    ),
    pytest.param(
        [EdgeArgs("A", "Z", "customer")],  # no node Z
        r"'Z' is not a node of the graph",
        id="target-not-in-graph",
    ),
    pytest.param(
        [EdgeArgs("C", "N", "nation")],  # a real reference, but C is no view
        r"Edge source 'C' must be a view \(edges between base relations come "
        r"from from_dbf\)",
        id="source-is-base-relation",
    ),
    pytest.param(
        [EdgeArgs("A", "B", "customer")],  # B is a computed result
        r"Edge target 'B' must be a base relation",
        id="target-is-view",
    ),
    pytest.param(
        [EdgeArgs("A", "C", "revenue")],  # revenue is an aggregate, not a key
        r"'revenue' is neither a grouping key nor a projection name of view 'A': "
        r"\['customer'\]",
        id="ref-key-is-aggregate",
    ),
    pytest.param(
        [
            EdgeArgs("A", "C", "customer"),  # first time: accepted
            EdgeArgs("A", "C", "customer"),  # same triple again
        ],
        r"Edge A -customer-> C already exists",
        id="duplicate-edge",
    ),
]  # one parameter set per broken rule


@pytest.mark.parametrize("calls, message", _INVALID_ENDPOINT_CASES)
def test_add_edge_rejects_invalid_endpoint_or_key(
    calls: list[EdgeArgs], message: str
) -> None:
    """Each endpoint / kind rule of ``add_edge`` raises its own message.

    ::

        +-- A ---------------------------+     +-- B ------------------+
        |  customer = O.customer         |     |  orders = Count()     |
        |  revenue  = Sum(L.price)       |     |     O                 |
        |     L --order--> O             |     +-----------------------+
        +--------------------------------+

        C --nation--> N

    Would fail if ``add_edge`` accepted an unknown endpoint, a base relation
    as source, a view as target, an aggregate as ``ref_key`` or a duplicate
    edge, or if it appended the edge before validating it.
    """
    graph: JoinGraph = _endpoint_graph(sales_relations())  # fresh graph per case
    for call in calls[:-1]:  # the preparing calls, if any
        graph.add_edge(call.source, call.target, call.ref_key)  # must succeed
    labels_before: set[str] = edge_labels(graph)  # edges before the bad call
    count_before: int = len(graph.edges)  # a duplicate is invisible in labels
    bad: EdgeArgs = calls[-1]  # the call that breaks one rule

    with pytest.raises(ValueError, match=message):  # the rule-specific message
        graph.add_edge(bad.source, bad.target, bad.ref_key)  # rejected

    assert edge_labels(graph) == labels_before  # no new edge label
    assert len(graph.edges) == count_before  # nothing was appended at all


# ---------------------------------------------------------------------------
# Rule violations: add_edge through a key that is no reference to the target
# ---------------------------------------------------------------------------


def _scalar_key_scenario() -> EdgeScenario:
    """View A groups by the scalar ``O.date``; edge ``A -date-> C``."""
    rels: SalesRelations = sales_relations()  # fresh RFs
    graph: JoinGraph = graph_of(C=rels.customers)  # outer graph: just C
    graph.add_view(
        View(
            name="A",  # view over the orders
            subgraph=graph_of(O=rels.orders),  # just O
            group_keys={"date": "O.date"},  # O.date exists but is no reference
        )  # the view to add
    )  # the view becomes a node of the graph
    return EdgeScenario(graph=graph, call=EdgeArgs("A", "C", "date"))  # named result


def _decoy_reference_target_scenario() -> EdgeScenario:
    """``O.customer`` references customers; the outer C is an equal-content decoy."""
    customers: RF = RF({"c1": TF({"name": "Alice"})}, frozen=False)  # real target
    decoy: RF = RF({"c1": TF({"name": "Alice"})}, frozen=False)  # same content
    orders: RF = RF({"o1": TF({"customer": customers["c1"]})}, frozen=False).references(
        "customer", customers
    )  # O.customer references the real customers, not the decoy
    for rf in (customers, decoy, orders):  # freeze like a real DBF
        rf.freeze()  # no further changes to the relation
    assert decoy == customers  # precondition: only the uuid tells them apart
    graph: JoinGraph = graph_of(C=decoy)  # outer C is the decoy
    graph.add_view(
        View(
            name="A",  # view over the orders
            subgraph=graph_of(O=orders),  # just O
            group_keys={"customer": "O.customer"},  # a reference, to another RF
        )  # the view to add
    )  # the view becomes a node of the graph
    return EdgeScenario(graph=graph, call=EdgeArgs("A", "C", "customer"))  # named


def _tuples_of_other_relation_scenario() -> EdgeScenario:
    """View A groups by the tuples of O (path ``O``); edge ``A -order-> C``."""
    rels: SalesRelations = sales_relations()  # fresh RFs
    graph: JoinGraph = graph_of(C=rels.customers)  # outer graph: just C
    graph.add_view(
        view_over_lineitems_and_orders(rels, group_keys={"order": "O"}, aggregates={})
    )  # A groups by O's tuples, which are no C tuples
    return EdgeScenario(graph=graph, call=EdgeArgs("A", "C", "order"))  # named


def _three_part_path_scenario() -> EdgeScenario:
    """View A groups by ``O.customer.name``; edge ``A -name-> C``.

    The first two segments ``O.customer`` DO reference C, so only the
    three-segment guard in ``_path_references`` rejects the edge: the key's
    values are customer names, not customer tuples.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs
    graph: JoinGraph = graph_of(C=rels.customers)  # outer graph: just C
    graph.add_view(
        View(
            name="A",  # view over the orders
            subgraph=graph_of(O=rels.orders),  # just O
            group_keys={"name": "O.customer.name"},  # O.customer references C
        )  # the view to add
    )  # the view becomes a node of the graph
    return EdgeScenario(graph=graph, call=EdgeArgs("A", "C", "name"))  # named


def _inner_view_tuples_key_scenario() -> EdgeScenario:
    """View B groups by inner view A's result tuples (path ``A``); edge via that key."""
    rels: SalesRelations = sales_relations()  # fresh RFs
    graph: JoinGraph = graph_of(C=rels.customers)  # outer graph: just C
    graph.add_view(_view_grouping_by_inner_view(rels))  # B groups by A's tuples
    return EdgeScenario(graph=graph, call=EdgeArgs("B", "C", "rev_view"))  # named


def _projection_scalar_scenario() -> EdgeScenario:
    """Projection view W carries the scalar ``date = O.date``; edge ``W -date-> C``."""
    rels: SalesRelations = sales_relations()  # fresh RFs
    graph: JoinGraph = graph_of(C=rels.customers)  # outer graph: just C
    graph.add_view(
        View(
            name="W",  # projection view over the orders
            subgraph=graph_of(O=rels.orders),  # just O
            projection={
                "customer": "O.customer",  # a reference to C (the valid edge key)
                "date": "O.date",  # a scalar: exists, but is no reference
            },  # one output tuple per order
        )  # the view to add
    )  # the view becomes a node of the graph
    return EdgeScenario(graph=graph, call=EdgeArgs("W", "C", "date"))  # named result


def _projection_unknown_key_scenario() -> EdgeScenario:
    """Projection view W of ``_projection_scalar_scenario``; edge ``W -nope-> C``."""
    graph: JoinGraph = _projection_scalar_scenario().graph  # W (customer, date), C
    return EdgeScenario(graph=graph, call=EdgeArgs("W", "C", "nope"))  # no such key


def _pass_through_relation_name_scenario() -> EdgeScenario:
    """Pass-through view J over O; edge ``J -O-> O`` through the relation name ``O``."""
    rels: SalesRelations = sales_relations()  # fresh RFs
    graph: JoinGraph = graph_of(O=rels.orders)  # outer graph: the same orders RF
    graph.add_view(
        View(name="J", subgraph=graph_of(O=rels.orders))
    )  # pass-through: its only output is the relation name "O"
    return EdgeScenario(graph=graph, call=EdgeArgs("J", "O", "O"))  # named result


#: ``add_edge`` calls through a view key that carries no reference to the
#: target, with the regex of the full message per case.
_NO_REFERENCE_KEY_CASES: list[ParameterSet] = [
    pytest.param(
        _scalar_key_scenario,  # O.date is a scalar
        r"Grouping key 'date' of view 'A' \(path 'O\.date'\) is not a reference to 'C'",
        id="scalar-attribute",
    ),
    pytest.param(
        _decoy_reference_target_scenario,  # O.customer points at another RF
        r"Grouping key 'customer' of view 'A' \(path 'O\.customer'\) is not a "
        r"reference to 'C'",
        id="reference-to-equal-content-decoy",
    ),
    pytest.param(
        _tuples_of_other_relation_scenario,  # path O, target C
        r"Grouping key 'order' of view 'A' \(path 'O'\) is not a reference to 'C'",
        id="tuples-of-other-relation",
    ),
    pytest.param(
        _three_part_path_scenario,  # O.customer.name, O.customer references C
        r"Grouping key 'name' of view 'A' \(path 'O\.customer\.name'\) is not a "
        r"reference to 'C'",
        id="three-part-path",
    ),
    pytest.param(
        _inner_view_tuples_key_scenario,  # path "A", A an inner view
        r"Grouping key 'rev_view' of view 'B' \(path 'A'\) is not a reference to 'C'",
        id="inner-view-tuples",
    ),
    pytest.param(
        _projection_scalar_scenario,  # W.date = O.date, a scalar
        r"Projection name 'date' of view 'W' \(path 'O\.date'\) is not a "
        r"reference to 'C'",
        id="projection-name-not-a-reference",
    ),
    pytest.param(
        _projection_unknown_key_scenario,  # W has no output "nope" at all
        r"'nope' is neither a grouping key nor a projection name of view 'W': "
        r"\['customer', 'date'\]",
        id="projection-view-unknown-key",
    ),
    pytest.param(
        _pass_through_relation_name_scenario,  # "O" is an output, not an edge key
        r"'O' is neither a grouping key nor a projection name of view 'J': \[\]",
        id="pass-through-relation-name",
    ),
]  # one parameter set per kind of key


@pytest.mark.parametrize("scenario, message", _NO_REFERENCE_KEY_CASES)
def test_add_edge_rejects_key_that_is_no_reference_to_the_target(
    scenario: Callable[[], EdgeScenario], message: str
) -> None:
    """A view key whose path is no reference to the target carries no edge.

    ::

        +-- view --------------------------+
        |  key = <path>                    |
        |                                  |
        |     <subgraph>                   |
        +----------------------------------+
                  |
                 key     <- rejected
                  v
                target  (base relation)

        case id                           view: key = path               target
        scalar-attribute                  A: date = O.date               C
        reference-to-equal-content-decoy  A: customer = O.customer       C'
        tuples-of-other-relation          A: order = O                   C
        three-part-path                   A: name = O.customer.name      C
        inner-view-tuples                 B: rev_view = A (inner view)   C
        projection-name-not-a-reference   W: proj date = O.date          C
        projection-view-unknown-key       W: nope (no output of W)       C
        pass-through-relation-name        J: O (relation name, no key)   O

        C' has the content of the customers that O.customer references, but
        another uuid.

    Would fail if ``_path_references`` accepted a scalar attribute, compared RFs
    by content (``==``) instead of ``uuid``, accepted a one-part key without
    comparing its relation to the target, accepted a path longer than two
    segments because its first two form a reference (dropped
    ``len(segments) > 2`` guard), or lacked the ``JoinNode`` check on the
    first segment (an inner view's bare name would crash on its absent
    ``rf``); if ``add_edge`` reported a projection name as a "Grouping key",
    did not list the projection names for an unknown key, or accepted a
    pass-through view's relation name as ``ref_key`` (edges from pass-through
    views are deferred by design 015, even when the relation is the target's
    RF); or if a rejected edge were appended anyway.
    """
    built: EdgeScenario = scenario()  # fresh graph and the call to try
    labels_before: set[str] = edge_labels(built.graph)  # edges before the call
    count_before: int = len(built.graph.edges)  # edge count before the call
    call: EdgeArgs = built.call  # the call that must be rejected

    with pytest.raises(ValueError, match=message):  # the case-specific message
        built.graph.add_edge(call.source, call.target, call.ref_key)  # rejected

    assert edge_labels(built.graph) == labels_before  # no new edge label
    assert len(built.graph.edges) == count_before  # nothing was appended at all


def test_add_edge_rejects_projection_through_inner_view_documented_limitation() -> None:
    """An outer projection view cannot be joined through a path into an inner view.

    ::

        +-- H ------------------------------------------+
        |  proj: customer = A.customer                  |
        |                                               |
        |  +-- A ------------------------------+        |
        |  |  customer = O.customer            |        |
        |  |  revenue  = Sum(L.price)          |        |
        |  |     L --order--> O                |        |
        |  +-----------------------------------+        |
        |    HAVING Gt(revenue, 20)                     |
        +-----------------------------------------------+
                  |
               customer     <- rejected: A is a view, not a base relation
                  v
                  C

    H hides the aggregate ``revenue`` that only A's HAVING needs (the way
    design 015 suggests). Its ``customer`` is a customer tuple, but the edge
    rule of 011 wants a path ``X.attr`` with X a base relation, so the edge is
    refused. This pins a known limitation (``DECISION.md`` of 015, "Known
    gaps"); it is documented, not intended, and lifting it should turn this
    test into an acceptance test. Would fail if ``_path_references`` accepted
    a path through an inner view without the limitation being lifted on
    purpose, or if a rejected edge were appended anyway.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    subgraph: JoinGraph = JoinGraph()  # H's subgraph holds only view A
    subgraph.add_view(revenue_per_customer_view(rels))  # A: revenue per customer
    subgraph.add_filter("A", Gt("revenue", 20))  # HAVING on A, inside H
    graph: JoinGraph = graph_of(C=rels.customers)  # outer graph: just C
    graph.add_view(
        View(
            name="H",  # outer projection view over A
            subgraph=subgraph,  # just A (with its HAVING)
            projection={"customer": "A.customer"},  # hides A's revenue
        )  # the view to add
    )  # H becomes a node next to C
    labels_before: set[str] = edge_labels(graph)  # edges before the call
    count_before: int = len(graph.edges)  # edge count before the call

    with pytest.raises(
        ValueError,
        match=r"Projection name 'customer' of view 'H' \(path 'A\.customer'\) is "
        r"not a reference to 'C'",
    ):  # the not-a-reference message for a projection name
        graph.add_edge("H", "C", "customer")  # X = A is a view, not a base relation

    assert edge_labels(graph) == labels_before  # no new edge label
    assert len(graph.edges) == count_before  # nothing was appended at all

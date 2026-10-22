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

"""Tests for aggregate boxes in the ``JoinGraph`` (design 011).

An ``AggregateBox`` draws a box around a subgraph, groups the joined subgraph
by named grouping keys and aggregates each group. The box is itself a node of
the enclosing graph: ``JoinGraph.add_box`` adds it, ``JoinGraph.add_edge``
connects it to a base relation through a grouping key that is a reference.

All examples use a tiny sales schema::

    L (lineitems) --order--> O (orders) --customer--> C (customers) --nation--> N (nations)

Edges are compared as ``"A -customer-> C"`` labels (see ``_edge_labels``) or by
``is`` identity, never via ``Edge ==`` or box ``==`` (that would compare
relations by content).

The happy paths come first, then the rule violations, one test (mostly
parametrized) per rule family.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from fdm.attribute_functions import TF, RF, DBF
from fql.operators.aggregates import AggregationFunction, Count, Max, Sum
from fql.plan.join_graph import (
    AggregateBox,
    AttributeName,
    AttributePath,
    JoinGraph,
    Node,
    RelationName,
)
from fql.predicates.predicates import Gt

if TYPE_CHECKING:  # only needed for the annotations of the case lists
    from _pytest.mark.structures import ParameterSet

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SalesRelations:
    """The four RFs of the sales schema ``L -> O -> C -> N``."""

    nations: RF
    customers: RF
    orders: RF
    lineitems: RF


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


def _sales_relations() -> SalesRelations:
    """Build fresh, frozen RFs ``L -order-> O -customer-> C -nation-> N``.

    Every call creates new RFs (new uuids), so tests never share state.
    """
    nations: RF = RF({"n1": TF({"name": "Germany"})}, frozen=False)  # target of C
    customers: RF = RF(
        {"c1": TF({"name": "Alice", "nation": nations["n1"]})}, frozen=False
    ).references(
        "nation", nations
    )  # C.nation is a reference to N
    orders: RF = RF(
        {"o1": TF({"date": "2026-01-01", "customer": customers["c1"]})}, frozen=False
    ).references(
        "customer", customers
    )  # O.customer is a reference to C
    lineitems: RF = RF(
        {
            "l1": TF({"price": 10, "order": orders["o1"]}),  # first line of o1
            "l2": TF({"price": 32, "order": orders["o1"]}),  # second line of o1
        },
        frozen=False,
    ).references(
        "order", orders
    )  # L.order is a reference to O
    for rf in (nations, customers, orders, lineitems):  # freeze like a real DBF
        rf.freeze()  # no further changes to the relation
    return SalesRelations(
        nations=nations, customers=customers, orders=orders, lineitems=lineitems
    )  # named result instead of an anonymous tuple


def _graph(**relations: RF) -> JoinGraph:
    """Build the join graph of a DBF holding exactly the given relations."""
    return JoinGraph.from_dbf(
        DBF(relations, frozen=True)
    )  # edges between these relations come from their references


def _edge_labels(graph: JoinGraph) -> set[str]:
    """The graph's edges as ``"source -ref_key-> target"`` labels (by name)."""
    return {
        f"{edge.source.name} -{edge.ref_key}-> {edge.target.name}"
        for edge in graph.edges
    }  # names only: never compares relations by content


def _box_a(
    rels: SalesRelations,
    group_keys: dict[AttributeName, AttributePath],
    aggregates: dict[AttributeName, AggregationFunction],
) -> AggregateBox:
    """Box ``A`` over ``L -order-> O`` with the given grouping keys and aggregates."""
    return AggregateBox(
        name="A",  # name of the box in the enclosing graph
        subgraph=_graph(L=rels.lineitems, O=rels.orders),  # L -order-> O
        group_keys=group_keys,  # as given by the caller
        aggregates=aggregates,  # as given by the caller
    )  # validated on construction


def _revenue_per_customer_box(rels: SalesRelations) -> AggregateBox:
    """Box ``A``: revenue per customer over ``L -order-> O``."""
    return _box_a(
        rels,
        group_keys={"customer": "O.customer"},  # one group per referenced customer
        aggregates={"revenue": Sum("L.price")},  # sum of line prices per group
    )  # box A with one key and one aggregate


def _revenue_box_joined_to_customers_graph(rels: SalesRelations) -> JoinGraph:
    """Graph ``A -customer-> C -nation-> N`` with A the revenue-per-customer box."""
    graph: JoinGraph = _graph(C=rels.customers, N=rels.nations)  # C -nation-> N
    graph.add_box(_revenue_per_customer_box(rels))  # A next to C and N
    graph.add_edge("A", "C", "customer")  # A.customer references C
    return graph  # three nodes, two edges


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_box_joins_outer_graph_through_reference_grouping_key() -> None:
    """A revenue-per-customer box is a node of the outer graph and joins C.

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

    Would fail if ``add_box`` did not register the box as a node, if
    ``add_edge`` rejected the ``O.customer`` reference to ``C`` or stored
    other endpoint objects than the graph's own nodes, or if a topology query
    looked into the box (e.g. counted its inner ``L``/``O`` as nodes).
    """
    rels: SalesRelations = _sales_relations()  # fresh RFs for this test
    box: AggregateBox = _revenue_per_customer_box(rels)  # box A over L -> O
    graph: JoinGraph = _graph(C=rels.customers, N=rels.nations)  # C -nation-> N

    graph.add_box(box)  # A becomes a node next to C and N
    graph.add_edge("A", "C", "customer")  # A.customer references C (via O.customer)

    assert list(graph) == ["C", "N", "A"]  # box appended after the base relations
    assert _edge_labels(graph) == {
        "C -nation-> N",  # from the outer DBF
        "A -customer-> C",  # the hand-added box edge
    }  # exactly these two edges
    assert graph.edges[-1].source is box  # the edge starts at the box object itself
    assert graph.edges[-1].target is graph.nodes["C"]  # and ends at the outer C node
    assert _edge_labels(box.subgraph) == {
        "L -order-> O"
    }  # O -customer-> C is not in the subgraph: C lives outside the box
    assert graph.is_tree() is True  # A - C - N is one undirected tree
    assert graph.pure_sources() == {"A"}  # only the box has no incoming edge
    assert graph.isolated_nodes() == set()  # every node takes part in an edge
    graph.check_acyclicity()  # does not raise: A -> C -> N is acyclic


def test_box_without_aggregates_is_a_distinct_projection() -> None:
    """A box with grouping keys only is a DISTINCT projection.

    ::

        +-- P ------------------------------+
        |  customer = O.customer            |
        |  (no aggregates)                  |
        |                                   |
        |     O                             |
        +-----------------------------------+

    Would fail if ``is_projection()`` did not report a box without aggregates.
    """
    rels: SalesRelations = _sales_relations()  # fresh RFs for this test
    box: AggregateBox = AggregateBox(
        name="P",  # name of the box
        subgraph=_graph(O=rels.orders),  # just the orders, no edges
        group_keys={"customer": "O.customer"},  # distinct customers that ordered
    )  # no aggregates: DISTINCT projection

    assert box.is_projection() is True  # no aggregates means projection
    assert box.output_names() == ["customer"]  # only the grouping key


def test_global_aggregate_box_without_grouping_keys() -> None:
    """A box with ``Count()`` and no grouping keys is a global aggregate.

    ::

        +-- G -----------------+
        |  orders = Count()    |
        |                      |
        |     O                |
        +----------------------+

    Would fail if a ``Count()`` without attribute were rejected by the path
    check, or if a box without grouping keys could not be added to a graph.
    """
    rels: SalesRelations = _sales_relations()  # fresh RFs for this test
    box: AggregateBox = AggregateBox(
        name="G",  # name of the box in the outer graph
        subgraph=_graph(O=rels.orders),  # count all orders
        aggregates={"orders": Count()},  # Count() has no attribute, so no path
    )  # no grouping keys: one result tuple
    graph: JoinGraph = JoinGraph()  # empty outer graph

    graph.add_box(box)  # G is the only node

    assert box.output_names() == ["orders"]  # only the aggregate
    assert box.is_projection() is False  # Count() is an aggregate
    assert list(graph) == ["G"]  # the box is the single node
    assert graph.sole_relation_name() == "G"  # trivial graph of one box


def test_box_grouping_by_tuples_joins_same_relation_outside() -> None:
    """A box grouping by the order tuples themselves joins the outer O.

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

    The same RF ``orders`` appears inside and outside the box, as two
    separate ``JoinNode`` objects (one per scope). Would fail if the one-part
    key ``"O"`` were not recognised as a reference to the outer O, or if the
    edge pointed at the box's inner O node instead of the outer one.
    """
    rels: SalesRelations = _sales_relations()  # fresh RFs for this test
    box: AggregateBox = _box_a(
        rels,
        group_keys={"order": "O"},  # one group per order tuple
        aggregates={"revenue": Sum("L.price")},  # revenue per order
    )  # box A over L -> O
    graph: JoinGraph = _graph(O=rels.orders, C=rels.customers)  # O -customer-> C

    graph.add_box(box)  # A becomes a node next to O and C
    graph.add_edge("A", "O", "order")  # the grouped tuples are the outer O tuples

    assert box.output_names() == ["order", "revenue"]  # keys, then aggregates
    assert _edge_labels(graph) == {
        "O -customer-> C",  # from the outer DBF
        "A -order-> O",  # the hand-added box edge
    }  # exactly these two edges
    assert graph.edges[-1].source is box  # the edge starts at the box object itself
    assert graph.edges[-1].target is graph.nodes["O"]  # the outer O, not the inner one


def test_nested_box_aggregates_over_inner_box() -> None:
    """A box can contain another box and aggregate over its output.

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

    Would fail if a box could not be added to a subgraph, if a path into an
    inner box (``A.revenue``) were rejected, or if the outer box could not
    join ``N`` through ``C__nation`` (the ``__`` spelling of ``C.nation`` that
    ``where()`` also accepts, i.e. if ``add_edge`` split paths on ``.`` only).
    """
    rels: SalesRelations = _sales_relations()  # fresh RFs for this test
    subgraph: JoinGraph = _revenue_box_joined_to_customers_graph(rels)  # A -> C -> N
    inner_box: Node = subgraph.nodes["A"]  # box A inside B's subgraph

    outer_box: AggregateBox = AggregateBox(
        name="B",  # name of the box in the outermost graph
        subgraph=subgraph,  # C, N and box A
        group_keys={"nation": "C__nation"},  # dunder spelling of C.nation
        aggregates={"top_revenue": Max("A.revenue")},  # best customer per nation
    )  # outer box B around A -> C -> N
    graph: JoinGraph = _graph(N=rels.nations)  # outermost graph: just N
    graph.add_box(outer_box)  # B becomes a node next to N
    graph.add_edge("B", "N", "nation")  # B.nation references N (via C__nation)

    assert outer_box.output_names() == ["nation", "top_revenue"]  # keys, aggregates
    assert outer_box.subgraph.nodes["A"] is inner_box  # A is nested inside B
    assert _edge_labels(outer_box.subgraph) == {
        "C -nation-> N",  # from the DBF of B's subgraph
        "A -customer-> C",  # the inner box edge
    }  # edges inside B
    assert _edge_labels(graph) == {
        "B -nation-> N"
    }  # only the outer box edge; inner edges stay inside B
    assert graph.edges[-1].source is outer_box  # the edge starts at box B itself
    assert graph.edges[-1].target is graph.nodes["N"]  # and ends at the outermost N
    assert list(graph) == ["N", "B"]  # A is not a node of the outermost graph


def _box_grouping_by_inner_box(rels: SalesRelations) -> AggregateBox:
    """Box ``B`` around inner box ``A``, grouping by A's result tuples (path ``A``)."""
    subgraph: JoinGraph = JoinGraph()  # B's subgraph holds only box A
    subgraph.add_box(_revenue_per_customer_box(rels))  # A: revenue per customer
    return AggregateBox(
        name="B",  # box around box A
        subgraph=subgraph,  # just A
        group_keys={"rev_box": "A"},  # one-segment path: A's result tuples
    )  # no aggregates: distinct result tuples of A


def test_box_groups_by_result_tuples_of_inner_box() -> None:
    """A one-segment grouping key naming an inner box groups by its result tuples.

    ::

        +-- B ---------------------------------------+
        |  rev_box = A                               |
        |                                            |
        |   +-- A ------------------------------+    |
        |   |  customer = O.customer            |    |
        |   |  revenue  = Sum(L.price)          |    |
        |   |                                   |    |
        |   |     L --order--> O                |    |
        |   +-----------------------------------+    |
        +--------------------------------------------+

    Would fail if the path check required a path into an inner box to go on
    with one of the box's output names (``A.customer``), i.e. rejected the
    bare ``"A"``.
    """
    box: AggregateBox = _box_grouping_by_inner_box(_sales_relations())  # does not raise

    assert isinstance(
        box.subgraph.nodes["A"], AggregateBox
    )  # precondition: "A" is a box
    assert box.output_names() == ["rev_box"]  # the key over A's result tuples


# ---------------------------------------------------------------------------
# Rule violations: box construction
# ---------------------------------------------------------------------------

#: Box constructions that each break exactly one validation rule, with the
#: regex the ``ValueError`` message must match.
_INVALID_BOX_CASES: list[ParameterSet] = [
    pytest.param(
        lambda rels: AggregateBox(
            name="A", subgraph=JoinGraph(), aggregates={"orders": Count()}
        ),  # nothing to aggregate over
        r"Box 'A' needs a non-empty subgraph",
        id="empty-subgraph",
    ),
    pytest.param(
        lambda rels: _box_a(rels, group_keys={}, aggregates={}),  # box without output
        r"Box 'A' needs grouping keys or aggregates \(or both\)",
        id="no-keys-no-aggregates",
    ),
    pytest.param(
        lambda rels: _box_a(
            rels,
            group_keys={"customer": "O.customer"},  # output name "customer" ...
            aggregates={"customer": Count()},  # ... used a second time
        ),  # box A with a clashing output name
        r"Box 'A': output names used twice: \['customer'\]",
        id="output-name-clash",
    ),
    pytest.param(
        lambda rels: _box_a(
            rels, group_keys={"a.b": "O.customer"}, aggregates={}
        ),  # output name with a dot
        r"Box 'A': output name 'a\.b' must be non-empty and contain neither",
        id="output-name-dot",
    ),
    pytest.param(
        lambda rels: _box_a(
            rels, group_keys={"customer": "C.name"}, aggregates={}
        ),  # C lives outside the box's subgraph L -> O
        r"Box 'A': path 'C\.name' of 'customer' does not start with a relation "
        r"of the subgraph \['L', 'O'\]",
        id="group-key-path-outside-subgraph",
    ),
    pytest.param(
        lambda rels: _box_a(
            rels, group_keys={}, aggregates={"revenue": Sum("X.price")}
        ),  # there is no relation X in the subgraph
        r"Box 'A': path 'X\.price' of 'revenue' does not start with a relation",
        id="aggregate-path-outside-subgraph",
    ),
    pytest.param(
        lambda rels: _box_a(
            rels, group_keys={}, aggregates={"big": Gt("L.price", 20)}
        ),  # a predicate, not an aggregation
        r"Box 'A': aggregate 'big' is not an AggregationFunction: Gt\('L\.price', 20\)",
        id="aggregate-is-predicate",
    ),
    pytest.param(
        lambda rels: _box_a(
            rels, group_keys={}, aggregates={"total": Sum(None)}
        ),  # a built-in aggregation other than Count() without attribute
        r"Box 'A': aggregate 'total' needs an attribute: Sum\(\)",
        id="builtin-aggregate-without-attribute",
    ),
    pytest.param(
        lambda rels: AggregateBox(
            name="B",  # outer box around A -> C -> N
            subgraph=_revenue_box_joined_to_customers_graph(rels),  # inner box A
            aggregates={"top": Max("A.nope")},  # A has no output 'nope'
        ),  # box B with a path into an unknown output of A
        r"Box 'B': path 'A\.nope' of 'top' names no output of inner box 'A': "
        r"\['customer', 'revenue'\]",
        id="path-into-inner-box-unknown-output",
    ),
    pytest.param(
        lambda rels: AggregateBox(
            name="A__B",  # box name with a dunder
            subgraph=_graph(O=rels.orders),  # just O
            group_keys={"customer": "O.customer"},  # a valid grouping key
        ),  # otherwise valid box
        r"Box 'A__B': box name 'A__B' must be non-empty and contain neither",
        id="box-name-dunder",
    ),
]  # one parameter set per broken rule


@pytest.mark.parametrize("build, message", _INVALID_BOX_CASES)
def test_box_construction_rejects_invalid_box(
    build: Callable[[SalesRelations], AggregateBox], message: str
) -> None:
    """Each box-construction rule raises its own ``ValueError`` message.

    ::

        +-- A ------------------------------+
        |  <key> = <path>                   |   <- the one broken rule:
        |  <agg> = <AggregationFunction>    |      empty subgraph, no output,
        |                                   |      bad / clashing output name,
        |     L --order--> O                |      path outside the subgraph,
        +-----------------------------------+      non-aggregation, built-in
                                                   aggregation without attribute,
                                                   unknown output of an inner
                                                   box, bad box name

    Would fail if a rule check were missing (no error), or if a case tripped a
    different rule than intended (the message would not match).
    """
    rels: SalesRelations = _sales_relations()  # fresh RFs for this case
    with pytest.raises(ValueError, match=message):  # the rule-specific message
        build(rels)  # constructing the box runs its validation


# ---------------------------------------------------------------------------
# add_box: name clashes and box loops
# ---------------------------------------------------------------------------


def test_add_box_rejects_name_already_used() -> None:
    """A box cannot take the name of a node that is already in the graph.

    ::

        +-- C (box) -----------------------+
        |  customer = O.customer           |
        |  revenue  = Sum(L.price)         |      C --nation--> N
        |     L --order--> O               |      (base relation C)
        +----------------------------------+

    Would fail if ``add_box`` silently replaced the base relation ``C`` by the
    box, which would leave the edge ``C -nation-> N`` starting at a node that
    is no longer in the graph.
    """
    rels: SalesRelations = _sales_relations()  # fresh RFs for this test
    box: AggregateBox = AggregateBox(
        name="C",  # same name as the base relation C
        subgraph=_graph(L=rels.lineitems, O=rels.orders),  # L -order-> O
        group_keys={"customer": "O.customer"},  # revenue per customer
        aggregates={"revenue": Sum("L.price")},  # sum of line prices per group
    )  # a valid box, only its name clashes
    graph: JoinGraph = _graph(C=rels.customers, N=rels.nations)  # C -nation-> N
    base_c: Node = graph.nodes["C"]  # the base relation C before the call

    with pytest.raises(ValueError, match=r"Node name 'C' is already used"):  # clash
        graph.add_box(box)  # C is taken by the base relation

    assert graph.nodes["C"] is base_c  # the base relation was not replaced


def test_add_box_rejects_box_around_its_own_graph() -> None:
    """A box cannot be added to the very graph that is its subgraph (direct loop).

    ::

        graph G = { O, A }
                      ^
        +-- A --------|-----------------+
        |  customer = O.customer        |
        |                               |
        |     subgraph = G  (itself)    |
        +-------------------------------+

    Would fail if ``add_box`` accepted a box whose subgraph is the target
    graph itself, i.e. if it added the box or raised another error than the
    own-graph ``ValueError``.
    """
    rels: SalesRelations = _sales_relations()  # fresh RFs for this test
    graph: JoinGraph = _graph(O=rels.orders)  # outer graph: just O
    box: AggregateBox = AggregateBox(
        name="A",  # a fresh, unused name
        subgraph=graph,  # the very graph the box is added to below
        group_keys={"customer": "O.customer"},  # a valid grouping key
    )  # a valid box on its own
    nodes_before: list[RelationName] = list(graph)  # node names before the call

    with pytest.raises(
        ValueError, match=r"Box 'A' cannot contain the graph it is added to"
    ):  # rejected with the own-graph message
        graph.add_box(box)  # the box would sit inside its own subgraph

    assert list(graph) == nodes_before  # the box was not added


def test_add_box_rejects_two_level_box_loop() -> None:
    """A box cannot be added to a graph that its subgraph encloses via another box.

    ::

        graph G = { O, A? }              graph H = { L, B }
                       |                               |
        +-- A ---------v----------+     +-- B ---------v----------+
        |  revenue = Sum(L.price) |     |  customer = O.customer  |
        |  subgraph = H           |     |  subgraph = G           |
        +-------------------------+     +-------------------------+

        H.add_box(B) is fine; G.add_box(A) would close the loop
        G -> A -> H -> B -> G.

    Would fail if ``add_box`` only compared ``box.subgraph is self`` (the
    direct case) instead of asking ``box.subgraph._encloses(self)``: A's
    subgraph H is not G, but H holds box B whose subgraph is G.
    """
    rels: SalesRelations = _sales_relations()  # fresh RFs for this test
    graph_g: JoinGraph = _graph(O=rels.orders)  # G: just O
    graph_h: JoinGraph = _graph(L=rels.lineitems)  # H: just L
    box_a: AggregateBox = AggregateBox(
        name="A",  # the box that would close the loop
        subgraph=graph_h,  # A lives over H
        aggregates={"revenue": Sum("L.price")},  # a valid aggregate over H
    )  # a valid box on its own
    box_b: AggregateBox = AggregateBox(
        name="B",  # the box that links H back to G
        subgraph=graph_g,  # B lives over G
        group_keys={"customer": "O.customer"},  # a valid grouping key over G
    )  # a valid box on its own
    graph_h.add_box(box_b)  # allowed: G does not enclose H (yet)
    nodes_before: list[RelationName] = list(graph_g)  # G's node names before

    with pytest.raises(
        ValueError, match=r"Box 'A' cannot contain the graph it is added to"
    ):  # rejected with the own-graph message
        graph_g.add_box(box_a)  # H encloses G through B: a two-level loop

    assert list(graph_g) == nodes_before  # A was not added to G
    assert list(graph_h) == ["L", "B"]  # H still holds L and box B, unchanged


# ---------------------------------------------------------------------------
# Rule violations: add_edge endpoints and kinds
# ---------------------------------------------------------------------------


def _endpoint_graph(rels: SalesRelations) -> JoinGraph:
    """Graph with base relations C, N and boxes A and B."""
    graph: JoinGraph = _graph(C=rels.customers, N=rels.nations)  # C -nation-> N
    graph.add_box(_revenue_per_customer_box(rels))  # box A: revenue per customer
    graph.add_box(
        AggregateBox(
            name="B",  # second box, a possible (but forbidden) edge target
            subgraph=_graph(O=rels.orders),  # count all orders
            aggregates={"orders": Count()},  # global aggregate
        )  # the box to add
    )  # the box becomes a node of the graph
    return graph  # no box edges yet


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
        [EdgeArgs("C", "N", "nation")],  # a real reference, but C is no box
        r"Edge source 'C' must be a box",
        id="source-is-base-relation",
    ),
    pytest.param(
        [EdgeArgs("A", "B", "customer")],  # B is a computed result
        r"Edge target 'B' must be a base relation",
        id="target-is-box",
    ),
    pytest.param(
        [EdgeArgs("A", "C", "revenue")],  # revenue is an aggregate, not a key
        r"'revenue' is not a grouping key of box 'A': \['customer'\]",
        id="ref-key-not-grouping-key",
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
    as source, a box as target, an aggregate as ``ref_key`` or a duplicate
    edge, or if it appended the edge before validating it.
    """
    graph: JoinGraph = _endpoint_graph(_sales_relations())  # fresh graph per case
    for call in calls[:-1]:  # the preparing calls, if any
        graph.add_edge(call.source, call.target, call.ref_key)  # must succeed
    labels_before: set[str] = _edge_labels(graph)  # edges before the bad call
    count_before: int = len(graph.edges)  # a duplicate is invisible in labels
    bad: EdgeArgs = calls[-1]  # the call that breaks one rule

    with pytest.raises(ValueError, match=message):  # the rule-specific message
        graph.add_edge(bad.source, bad.target, bad.ref_key)  # rejected

    assert _edge_labels(graph) == labels_before  # no new edge label
    assert len(graph.edges) == count_before  # nothing was appended at all


# ---------------------------------------------------------------------------
# Rule violations: add_edge on a grouping key that is not a reference
# ---------------------------------------------------------------------------


def _scalar_key_scenario() -> EdgeScenario:
    """Box A groups by the scalar ``O.date``; edge ``A -date-> C``."""
    rels: SalesRelations = _sales_relations()  # fresh RFs
    graph: JoinGraph = _graph(C=rels.customers)  # outer graph: just C
    graph.add_box(
        AggregateBox(
            name="A",  # box over the orders
            subgraph=_graph(O=rels.orders),  # just O
            group_keys={"date": "O.date"},  # O.date exists but is no reference
        )  # the box to add
    )  # the box becomes a node of the graph
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
    graph: JoinGraph = _graph(C=decoy)  # outer C is the decoy
    graph.add_box(
        AggregateBox(
            name="A",  # box over the orders
            subgraph=_graph(O=orders),  # just O
            group_keys={"customer": "O.customer"},  # a reference, to another RF
        )  # the box to add
    )  # the box becomes a node of the graph
    return EdgeScenario(graph=graph, call=EdgeArgs("A", "C", "customer"))  # named


def _tuples_of_other_relation_scenario() -> EdgeScenario:
    """Box A groups by the tuples of O (path ``O``); edge ``A -order-> C``."""
    rels: SalesRelations = _sales_relations()  # fresh RFs
    graph: JoinGraph = _graph(C=rels.customers)  # outer graph: just C
    graph.add_box(
        _box_a(rels, group_keys={"order": "O"}, aggregates={})
    )  # A groups by O's tuples, which are no C tuples
    return EdgeScenario(graph=graph, call=EdgeArgs("A", "C", "order"))  # named


def _three_part_path_scenario() -> EdgeScenario:
    """Box A groups by ``O.customer.name``; edge ``A -name-> C``.

    The first two segments ``O.customer`` DO reference C, so only the
    three-segment guard in ``_group_key_references`` rejects the edge: the key's
    values are customer names, not customer tuples.
    """
    rels: SalesRelations = _sales_relations()  # fresh RFs
    graph: JoinGraph = _graph(C=rels.customers)  # outer graph: just C
    graph.add_box(
        AggregateBox(
            name="A",  # box over the orders
            subgraph=_graph(O=rels.orders),  # just O
            group_keys={"name": "O.customer.name"},  # O.customer references C
        )  # the box to add
    )  # the box becomes a node of the graph
    return EdgeScenario(graph=graph, call=EdgeArgs("A", "C", "name"))  # named


def _inner_box_tuples_key_scenario() -> EdgeScenario:
    """Box B groups by inner box A's result tuples (path ``A``); edge via that key."""
    rels: SalesRelations = _sales_relations()  # fresh RFs
    graph: JoinGraph = _graph(C=rels.customers)  # outer graph: just C
    graph.add_box(_box_grouping_by_inner_box(rels))  # B groups by A's tuples
    return EdgeScenario(graph=graph, call=EdgeArgs("B", "C", "rev_box"))  # named


#: Grouping keys that are no reference to the edge target, all with the same
#: "not a reference" message (regex of the full message per case).
_NOT_A_REFERENCE_CASES: list[ParameterSet] = [
    pytest.param(
        _scalar_key_scenario,  # O.date is a scalar
        r"Grouping key 'date' of box 'A' \(path 'O\.date'\) is not a reference to 'C'",
        id="scalar-attribute",
    ),
    pytest.param(
        _decoy_reference_target_scenario,  # O.customer points at another RF
        r"Grouping key 'customer' of box 'A' \(path 'O\.customer'\) is not a "
        r"reference to 'C'",
        id="reference-to-equal-content-decoy",
    ),
    pytest.param(
        _tuples_of_other_relation_scenario,  # path O, target C
        r"Grouping key 'order' of box 'A' \(path 'O'\) is not a reference to 'C'",
        id="tuples-of-other-relation",
    ),
    pytest.param(
        _three_part_path_scenario,  # O.customer.name, O.customer references C
        r"Grouping key 'name' of box 'A' \(path 'O\.customer\.name'\) is not a "
        r"reference to 'C'",
        id="three-part-path",
    ),
    pytest.param(
        _inner_box_tuples_key_scenario,  # path "A", A an inner box
        r"Grouping key 'rev_box' of box 'B' \(path 'A'\) is not a reference to 'C'",
        id="inner-box-tuples",
    ),
]  # one parameter set per kind of non-reference


@pytest.mark.parametrize("scenario, message", _NOT_A_REFERENCE_CASES)
def test_add_edge_rejects_grouping_key_that_is_no_reference(
    scenario: Callable[[], EdgeScenario], message: str
) -> None:
    """A grouping key whose path is no reference to the target is rejected.

    ::

        +-- box ---------------------------+
        |  key = <path>                    |   <path> is one of: a scalar
        |                                  |   attribute, a reference to another
        |     <subgraph>                   |   RF with equal content, the tuples
        +----------------------------------+   of another relation, a three-part
                  |                            path that starts with a
                 key                           reference, or the bare name of
                  v                            an inner box
                target  (base relation)

    Would fail if ``_group_key_references`` accepted a scalar attribute, compared RFs
    by content (``==``) instead of ``uuid``, accepted a one-part key without
    comparing its relation to the target, accepted a path longer than two
    segments because its first two form a reference (dropped
    ``len(segments) > 2`` guard), or lacked the ``JoinNode`` check on the
    first segment (an inner box's bare name would crash on its absent ``rf``).
    """
    built: EdgeScenario = scenario()  # fresh graph and the call to try
    labels_before: set[str] = _edge_labels(built.graph)  # edges before the call
    count_before: int = len(built.graph.edges)  # edge count before the call
    call: EdgeArgs = built.call  # the call that is no reference

    with pytest.raises(ValueError, match=message):  # one shared message shape
        built.graph.add_edge(call.source, call.target, call.ref_key)  # rejected

    assert _edge_labels(built.graph) == labels_before  # no new edge label
    assert len(built.graph.edges) == count_before  # nothing was appended at all

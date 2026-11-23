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

"""Tests for node filters in the ``JoinGraph`` (design 015).

``JoinGraph.add_filter(name, predicate)`` attaches a filter to one node of a
graph; it applies to that node's tuples, as ``filter_values(X, predicate)``
does. Several filters on one node are a conjunction. A filter on a base
relation is a WHERE, a filter on a view node is a HAVING (it applies to the
view's result tuples). Filters of a view's inner nodes live in the view's own
subgraph (``view.subgraph.add_filter(...)``).

All examples use the tiny sales schema of ``tests/fql/plan/sales_schema.py``::

    L (lineitems) --order--> O (orders) --customer--> C (customers) --nation--> N (nations)

Stored predicates are compared by ``is`` identity: the graph keeps the very
objects it was given.

The happy paths come first, then one section per rule family (unknown node
and ``JoinPredicate``, HAVING names, pass-through views). A section holds the
rule violations and may also hold the cases its checks deliberately accept,
documented gaps included. The last section collects the HAVING predicates the
check cannot see into or whose names are literals, not paths.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest

from fdm.schema import JoinPredicate
from fql.operators.aggregates import Max, Sum
from fql.plan.join_graph import FilterPredicate, JoinGraph, Node, RelationName, View
from fql.predicates.predicates import (
    And,
    Eq,
    Exists,
    Gt,
    In,
    Like,
    Lt,
    Missing,
    Not,
    Or,
    Predicate,
    Ref,
)
from tests.fql.plan.sales_schema import (
    GtSubclass,
    OpaquePredicate,
    SalesRelations,
    graph_of,
    pass_through_view_graph,
    revenue_view_joined_to_customers_graph,
    sales_relations,
    view_over_lineitems_and_orders,
)

if TYPE_CHECKING:  # only needed for the annotation of the case list
    from _pytest.mark.structures import ParameterSet

# ---------------------------------------------------------------------------
# Happy paths: filters on base relations (WHERE)
# ---------------------------------------------------------------------------


def test_structured_filter_on_base_relation_is_stored_under_its_name() -> None:
    """A structured filter on a base relation is stored under that relation's name.

    ::

        C --nation--> N
        [Eq(name, 'Alice')]

    Would fail if ``add_filter`` stored the predicate under another key, copied
    or wrapped it, or touched the filters of the other nodes, or if a topology
    query took the filter into account (they ignore annotations).
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    graph: JoinGraph = graph_of(C=rels.customers, N=rels.nations)  # C -nation-> N
    alice: Eq = Eq("name", "Alice")  # path relative to C's tuples, no "C." prefix

    graph.add_filter("C", alice)  # WHERE on C

    assert list(graph.filters) == ["C"]  # only C carries a filter, N has none
    assert len(graph.filters["C"]) == 1  # exactly the one filter
    assert graph.filters["C"][0] is alice  # the very predicate object, not a copy
    assert graph.is_tree() is True  # topology unchanged: C - N is still one tree
    assert graph.pure_sources() == {"C"}  # the filter adds no node or edge


def test_second_filter_on_same_node_is_appended_after_the_first() -> None:
    """Two filters on one node are kept as a list, in the order they were added.

    ::

        L --order--> O
                     [Gt(date, '2025-12-31'), Lt(date, '2027-01-01')]

    The list is a conjunction: a tuple of O must satisfy both. Would fail if
    the second call replaced the first filter, or if the filters were reordered.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    graph: JoinGraph = graph_of(L=rels.lineitems, O=rels.orders)  # L -order-> O
    after: Gt = Gt("date", "2025-12-31")  # orders after 2025 ...
    before: Lt = Lt("date", "2027-01-01")  # ... and before 2027

    graph.add_filter("O", after)  # first filter on O
    graph.add_filter("O", before)  # second filter on O: appended, not replaced

    assert len(graph.filters["O"]) == 2  # both filters are kept
    assert graph.filters["O"][0] is after  # the first one stays first
    assert graph.filters["O"][1] is before  # the second one follows
    assert "L" not in graph.filters  # L was never filtered


def test_lambda_filter_on_base_relation_is_stored_as_given() -> None:
    """A plain lambda is accepted as a filter and stored unchanged.

    ::

        L --order--> O
        [lambda t: t["price"] > 20]

    Would fail if ``add_filter`` only accepted structured predicates, or
    wrapped the lambda into another object.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    graph: JoinGraph = graph_of(L=rels.lineitems, O=rels.orders)  # L -order-> O
    expensive: FilterPredicate = lambda t: t["price"] > 20  # opaque to the graph

    graph.add_filter("L", expensive)  # WHERE on L, as a lambda

    assert len(graph.filters["L"]) == 1  # exactly the one filter
    assert graph.filters["L"][0] is expensive  # the very lambda object


# ---------------------------------------------------------------------------
# Happy paths: filters on view nodes (HAVING) and inside views (WHERE)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "having",
    [
        pytest.param(Gt("revenue", 10), id="output-vs-literal"),  # revenue > 10
        pytest.param(
            Gt("revenue", Ref("top")), id="output-vs-output-ref"
        ),  # revenue > largest line, i.e. more than one line
        pytest.param(lambda t: t["revenue"] > 10, id="lambda"),  # opaque, not inspected
        pytest.param(
            Eq("customer.name", "Alice"), id="path-through-tuple-valued-key"
        ),  # customer is a customer tuple, so its name can be read
        pytest.param(
            Eq("customer__name", "Alice"), id="dunder-path-through-tuple-valued-key"
        ),  # the same path in the "__" spelling
    ],
)
def test_filter_on_view_node_is_having_in_outer_graph(
    having: FilterPredicate,
) -> None:
    """A filter on a view node reading its outputs is stored in the OUTER graph.

    ::

        +-- A ------------------------------+
        |  customer = O.customer            |
        |  revenue  = Sum(L.price)          |
        |  top      = Max(L.price)          |
        |                                   |
        |     L --order--> O                |
        +-----------------------------------+
          HAVING <having>
                  |
               customer
                  v
                  C --nation--> N

    Would fail if ``add_filter`` rejected a predicate that only reads the view's
    output names (e.g. if a ``Ref`` to another output were taken for a foreign
    path, or a lambda were inspected), if it compared a whole path instead of
    its first segment with the outputs (``customer.name`` goes on through the
    tuple-valued grouping key ``customer``, also in the ``__`` spelling), or if
    the HAVING landed in the view's subgraph instead of the graph that holds
    the view node.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    view: View = view_over_lineitems_and_orders(
        rels,
        group_keys={"customer": "O.customer"},  # one group per customer
        aggregates={
            "revenue": Sum("L.price"),  # sum of line prices per group
            "top": Max("L.price"),  # largest line price per group
        },  # two aggregates, so a Ref can compare them
    )  # view A over L -> O
    graph: JoinGraph = graph_of(C=rels.customers, N=rels.nations)  # C -nation-> N
    graph.add_view(view)  # A becomes a node next to C and N
    graph.add_edge("A", "C", "customer")  # A.customer references C

    graph.add_filter("A", having)  # HAVING on the view's result tuples

    assert list(graph.filters) == ["A"]  # only the view node carries a filter
    assert graph.filters["A"][0] is having  # the very predicate object
    assert view.subgraph.filters == {}  # HAVING is not a WHERE inside the view


def test_filter_inside_view_is_stored_in_the_view_subgraph() -> None:
    """A filter on an inner node of a view lives in the view's subgraph (WHERE).

    ::

        +-- A ---------------------------------------+
        |  customer = O.customer                     |
        |  revenue  = Sum(L.price)                   |
        |                                            |
        |     L --order--> O                         |
        |                  [Gt(date, '2025-12-31')]  |
        +--------------------------------------------+
                  |
               customer
                  v
                  C --nation--> N

    Would fail if the subgraph did not store the filter under the inner node
    O. The last assertion only documents that the WHERE stays out of the outer
    graph: the two graphs have separate ``filters`` dicts by construction.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    graph: JoinGraph = revenue_view_joined_to_customers_graph(rels)  # A -> C -> N
    view: Node = graph.nodes["A"]  # the revenue view inside the outer graph
    assert isinstance(view, View)  # precondition: A is a view, so it has a subgraph
    recent: Gt = Gt("date", "2025-12-31")  # path relative to O's tuples

    view.subgraph.add_filter("O", recent)  # WHERE on the inner O, before grouping

    assert list(view.subgraph.filters) == ["O"]  # only the inner O is filtered
    assert view.subgraph.filters["O"][0] is recent  # the very predicate object
    assert graph.filters == {}  # the outer graph carries no filter at all


# ---------------------------------------------------------------------------
# Rule violations: unknown node, JoinPredicate as a node filter
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        pytest.param("Z", id="invented-name"),  # no node of that name anywhere
        pytest.param("L", id="node-only-inside-view"),  # L lives in A's subgraph only
    ],
)
def test_add_filter_rejects_name_that_is_no_node_of_the_graph(
    name: RelationName,
) -> None:
    """A filter on a name that is no node of this very graph is rejected.

    ::

        +-- A ------------------------------+
        |  customer = O.customer            |
        |  revenue  = Sum(L.price)          |
        |                                   |
        |     L --order--> O                |   <- "L": a node of A's subgraph,
        +-----------------------------------+      not of the outer graph
                  |
               customer                          "Z": no node anywhere
                  v
                  C --nation--> N

        outer.add_filter(<name>, Gt(price, 20))   <- rejected

    Would fail if ``add_filter`` stored a filter under an unknown name (a typo
    would then filter nothing, silently), looked the name up in nested
    subgraphs (an inner node's WHERE belongs in ``view.subgraph``), or
    created the ``filters`` entry before checking the name.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this case
    graph: JoinGraph = revenue_view_joined_to_customers_graph(rels)  # A -> C -> N
    view: Node = graph.nodes["A"]  # the revenue view, whose subgraph holds L and O
    assert isinstance(view, View)  # precondition: A is a view, so it has a subgraph

    with pytest.raises(
        ValueError, match=rf"'{re.escape(name)}' is not a node of the graph"
    ):  # the unknown-node message, naming the given name
        graph.add_filter(name, Gt("price", 20))  # neither Z nor L is an outer node

    assert graph.filters == {}  # no (empty) entry was created for the name
    assert view.subgraph.filters == {}  # nor did the filter land on the inner L


def test_add_filter_rejects_join_predicate() -> None:
    """A ``JoinPredicate`` is no node filter: it spans several relations.

    ::

        O --customer--> C
        add_filter(O, JoinPredicate((O, C), Gt(O.date, Ref(C.name))))   <- rejected

    A ``JoinPredicate`` is callable and no built-in ``Predicate``, so the path
    check cannot see into it. Would fail if ``add_filter`` lacked the explicit
    ``JoinPredicate`` check: the predicate would be stored on O silently,
    where its relation-prefixed paths (``O.date``, ``C.name``) cannot resolve
    against O's tuples.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    graph: JoinGraph = graph_of(O=rels.orders, C=rels.customers)  # O -customer-> C
    join_predicate: JoinPredicate = JoinPredicate(
        ("O", "C"), Gt("O.date", Ref("C.name"))
    )  # a valid condition over two relations, but for a view, not a node

    with pytest.raises(
        ValueError,
        match=r"Filter on 'O' is a JoinPredicate; a predicate over several "
        r"relations belongs on a view \(View\.join_predicates\)",
    ):  # the message points at the view's join predicates
        graph.add_filter("O", join_predicate)  # rejected before it is stored

    assert graph.filters == {}  # nothing was stored on O (or anywhere)


def test_add_filter_accepts_join_predicate_nested_in_and_documented_gap() -> None:
    """A ``JoinPredicate`` inside an ``And`` is not refused by ``add_filter``.

    ::

        O --customer--> C
        add_filter(O, And(Gt(date, '2025-12-31'),
                          JoinPredicate((O, C), Gt(O.date, Ref(C.name)))))
                                                          <- accepted

    Only a top-level ``JoinPredicate`` is refused; nested in an ``And`` it
    makes the whole predicate uninspectable, so it is stored. This pins a
    known gap of design 015 (``DECISION.md``): documented, not intended;
    refusing it should turn this test into a rejection. Would fail if
    ``add_filter`` started to look for ``JoinPredicate``s inside ``And`` /
    ``Or`` / ``Not`` without updating this test.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    graph: JoinGraph = graph_of(O=rels.orders, C=rels.customers)  # O -customer-> C
    nested: And = And(
        Gt("date", "2025-12-31"),  # a proper filter part on O's tuples
        JoinPredicate(("O", "C"), Gt("O.date", Ref("C.name"))),  # belongs on a view
    )  # the JoinPredicate hides inside the And

    graph.add_filter("O", nested)  # accepted: only a top-level one is refused

    assert len(graph.filters["O"]) == 1  # exactly the one filter on O
    assert graph.filters["O"][0] is nested  # the very And object, stored as given


# ---------------------------------------------------------------------------
# Rule violations: HAVING that names no output of the view
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_having, path",
    [
        pytest.param(Gt("revenu", 1), "revenu", id="top-level"),  # typo in the leaf
        pytest.param(
            And(Gt("revenue", 1), Not(Eq("custmer", 1))), "custmer", id="in-and-not"
        ),  # first part is fine, the typo hides in the second, under Not
        pytest.param(
            Gt("revenue", Ref("revenu")), "revenu", id="inside-ref"
        ),  # the compared-with output is misspelt
        pytest.param(
            Or(Gt("revenue", 1), Eq("custmer", 1)), "custmer", id="in-or"
        ),  # the typo hides in the second part of an Or
        pytest.param(In("custmer", [1, 2]), "custmer", id="in-leaf"),  # In leaf
        pytest.param(Like("custmer", "c%"), "custmer", id="like-leaf"),  # Like leaf
        pytest.param(Exists("custmer"), "custmer", id="exists-leaf"),  # Exists leaf
        pytest.param(Missing("custmer"), "custmer", id="missing-leaf"),  # Missing leaf
    ],
)
def test_having_rejects_path_that_names_no_output_of_the_view(
    bad_having: Predicate, path: str
) -> None:
    """A HAVING path that names no output of the view is rejected, wherever it sits.

    ::

        +-- A ------------------------------+
        |  customer = O.customer            |
        |  revenue  = Sum(L.price)          |
        |                                   |
        |     L --order--> O                |
        +-----------------------------------+
          HAVING Gt(revenue, 10)             <- added first, kept
          HAVING <bad_having>                <- 'revenu' / 'custmer' in a leaf
                  |                             (Gt, In, Like, Exists, Missing),
               customer                         in And / Not / Or, or in a Ref:
                  v                             rejected
                  C --nation--> N

    A misspelt output would otherwise drop every row silently (missing
    attribute, no match). Would fail if the check looked only at the
    top-level leaf (missing the parts of ``And`` / ``Not`` / ``Or`` or the
    ``Ref``), only at the first path, if ``_predicate_paths`` dropped one of
    the leaf kinds (it would then give up and accept the filter unchecked),
    or if the rejected filter were appended anyway.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this case
    graph: JoinGraph = revenue_view_joined_to_customers_graph(rels)  # A -> C -> N
    first: Gt = Gt("revenue", 10)  # a valid HAVING on A
    graph.add_filter("A", first)  # stored: reads the output "revenue"

    with pytest.raises(
        ValueError,
        match=rf"Filter on view 'A': path '{re.escape(path)}' names no output of "
        r"the view: "
        r"\['customer', 'revenue'\]",
    ):  # names the bad path and lists A's outputs
        graph.add_filter("A", bad_having)  # the misspelt path is found and rejected

    assert list(graph.filters) == ["A"]  # still only A carries filters
    assert len(graph.filters["A"]) == 1  # the bad filter was not appended
    assert graph.filters["A"][0] is first  # the earlier filter is untouched


# ---------------------------------------------------------------------------
# Filters on a pass-through view (its outputs are its relation names)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_having, message",
    [
        pytest.param(
            Eq("A.nope", 1),
            r"View 'P': path 'A\.nope' of 'filter' names no output of inner "
            r"view 'A': \['customer', 'revenue'\]",
            id="unknown-output-of-inner-view",
        ),  # A exists, its output 'nope' does not
        pytest.param(
            Eq("X.name", "Alice"),
            r"View 'P': path 'X\.name' of 'filter' does not start with a relation "
            r"of the subgraph \['A', 'C', 'N'\]",
            id="no-relation-of-the-subgraph",
        ),  # X is no relation of P's subgraph, so no output of P
    ],
)
def test_filter_on_pass_through_view_rejects_unknown_relation_or_inner_output(
    bad_having: Predicate, message: str
) -> None:
    """On a pass-through view, a filter path must pass the view's own path check.

    ::

        +-- P (pass-through: outputs C, N, A) -------------+
        |   +-- A ---------------------------+             |
        |   |  customer = O.customer         |             |
        |   |  revenue  = Sum(L.price)       |             |
        |   |     L --order--> O             |             |
        |   +--------------------------------+             |
        |              |                                   |
        |           customer                               |
        |              v                                   |
        |              C --nation--> N                     |
        +--------------------------------------------------+
          HAVING Eq(A.nope, 1)       <- rejected: A has no output 'nope'
          HAVING Eq(X.name, 'Alice') <- rejected: X is no relation of P

    P's outputs are its subgraph relations, so ``add_filter`` runs P's own
    path check, with "filter" as the owner. Would fail if ``add_filter``
    skipped ``_check_path`` for pass-through views (``A.nope`` would pass, as
    ``A`` is an output of P), or if a path that names no relation of the
    subgraph got another message than the path check's own.
    """
    graph: JoinGraph = pass_through_view_graph(
        sales_relations()
    )  # P around A -> C -> N

    with pytest.raises(ValueError, match=message):  # _check_path's message
        graph.add_filter("P", bad_having)  # the path check rejects the path

    assert graph.filters == {}  # nothing was stored on P


@pytest.mark.parametrize(
    "having",
    [
        pytest.param(Eq("A.revenue", 1), id="inner-view-output"),  # A has "revenue"
        pytest.param(
            Eq("C.name", "Alice"), id="base-relation-path-documented-gap"
        ),  # a WHERE on C in disguise: the documented gap
        pytest.param(
            Eq("C.name", Ref("N.name")), id="two-relations-documented-gap"
        ),  # a join predicate over C and N in disguise: the widened gap
    ],
)
def test_filter_on_pass_through_view_accepts_path_into_its_relations(
    having: Predicate,
) -> None:
    """On a pass-through view, a filter may read an inner view output or a relation.

    ::

        +-- P (pass-through: outputs C, N, A) -------------+
        |   +-- A ---------------------------+             |
        |   |  customer = O.customer         |             |
        |   |  revenue  = Sum(L.price)       |             |
        |   |     L --order--> O             |             |
        |   +--------------------------------+             |
        |              |                                   |
        |           customer                               |
        |              v                                   |
        |              C --nation--> N                     |
        +--------------------------------------------------+
          HAVING Eq(A.revenue, 1)  or  Eq(C.name, 'Alice')
                                 or  Eq(C.name, Ref(N.name))

    The second and third cases pin a known gap of design 015 (``DECISION.md``,
    "Known gaps", widened in the 015 notes): a filter on a pass-through view
    node can read its subgraph relations, so it is a second place for a WHERE
    on ``C`` (one relation) and for a join predicate over ``C`` and ``N``
    without declared relations (two relations). The gap is documented, not
    intended; closing it should turn these cases into rejections. Would fail
    if the pass-through check rejected a valid output of an inner view, or if
    the gap were closed without updating this test.
    """
    graph: JoinGraph = pass_through_view_graph(
        sales_relations()
    )  # P around A -> C -> N

    graph.add_filter("P", having)  # accepted: every path starts with an output of P

    assert list(graph.filters) == ["P"]  # stored on the pass-through view node
    assert graph.filters["P"][0] is having  # the very predicate object


def test_filter_rejects_two_level_path_through_pass_through_views() -> None:
    """A filter path through two pass-through views is checked down to inner view A.

    ::

        +-- Q (pass-through: output P) -----------------------+
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
          HAVING Eq(P.A.nope, 1)     <- rejected: A has no output 'nope'

    ``P`` is an output of Q and ``A`` an output of P, so only the third segment
    is wrong. The error comes from P's path check on the rest of the path
    (``A.nope``), with "filter" as the owner. Would fail if ``_check_path``
    stopped after one level instead of going on through an inner view that
    passes rows on, or if the rejected filter were stored anyway.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this test
    view_q: View = View(
        name="Q",  # the outer pass-through view
        subgraph=pass_through_view_graph(rels),  # holds only P (around A, C, N)
    )  # no grouping, no projection: its only output is "P"
    graph: JoinGraph = JoinGraph()  # outermost graph
    graph.add_view(view_q)  # Q is the only node

    with pytest.raises(
        ValueError,
        match=r"View 'P': path 'A\.nope' of 'filter' names no output of inner "
        r"view 'A': \['customer', 'revenue'\]",
    ):  # raised one level down, by P, for the rest of the path
        graph.add_filter("Q", Eq("P.A.nope", 1))  # P and A exist, 'nope' does not

    assert graph.filters == {}  # nothing was stored on Q
    assert view_q.subgraph.filters == {}  # nor on P inside Q


# ---------------------------------------------------------------------------
# HAVING names the check does not read as paths (accepted unchecked)
# ---------------------------------------------------------------------------


#: HAVING predicates on view A that mention the non-output ``nope`` but are
#: accepted: the check cannot see into them, or ``nope`` is only a literal.
_HAVING_CASES_WITH_UNCHECKED_NAME: list[ParameterSet] = [
    pytest.param(lambda t: t["nope"] > 1, id="lambda"),  # opaque callable
    pytest.param(
        OpaquePredicate("nope"), id="user-defined-predicate"
    ),  # a Predicate subclass outside the built-ins
    pytest.param(
        GtSubclass("nope", 1), id="subclass-of-built-in"
    ),  # built-ins are matched by exact type, not isinstance
    pytest.param(
        And(Gt("nope", 1), OpaquePredicate("x")), id="and-with-user-defined-part"
    ),  # the wrong name sits in the BUILT-IN part; the opaque part hides it
    pytest.param(
        Eq("revenue", {"type": "ref", "attr": "nope"}), id="literal-shaped-like-ref"
    ),  # a dict literal that looks like a serialized Ref is still a literal
    pytest.param(
        In("customer", ["nope"]), id="in-values-are-literals"
    ),  # In is inspected: its path is "customer", its values are literals
    pytest.param(
        Like("customer", "nope"), id="like-pattern-is-literal"
    ),  # Like is inspected: its path is "customer", its pattern is a literal
]  # one parameter set per way the name escapes the check


@pytest.mark.parametrize("having", _HAVING_CASES_WITH_UNCHECKED_NAME)
def test_having_accepts_unknown_name_that_is_no_checked_path(
    having: FilterPredicate,
) -> None:
    """A HAVING whose unknown name is no path the check reads is stored unchecked.

    ::

        +-- A ------------------------------+
        |  customer = O.customer            |
        |  revenue  = Sum(L.price)          |
        |                                   |
        |     L --order--> O                |
        +-----------------------------------+
          HAVING <having>    <- mentions 'nope' (no output of A): accepted
                  |
               customer
                  v
                  C --nation--> N

        not inspected:                lambda, user-defined-predicate,
                                      subclass-of-built-in,
                                      and-with-user-defined-part
        inspected, 'nope' a literal:  literal-shaped-like-ref,
                                      in-values-are-literals,
                                      like-pattern-is-literal

    Only the built-in predicate classes (by exact type) are inspected, and
    literal values are never paths. One part the check cannot see into makes
    the WHOLE predicate uninspectable (design decision of 015), so in the
    ``And`` case even the wrong name in the built-in ``Gt`` part goes
    unchecked. Would fail if the exact-type check became an ``isinstance``
    check (only ``subclass-of-built-in`` pins this: ``OpaquePredicate`` is no
    subclass of a built-in), if the walk skipped an opaque part of an ``And``
    and checked the remaining built-in parts, or if a literal (a
    ``{"type": "ref"}`` dict, ``In`` values, a ``Like`` pattern) were read as
    a path.
    """
    rels: SalesRelations = sales_relations()  # fresh RFs for this case
    graph: JoinGraph = revenue_view_joined_to_customers_graph(rels)  # A -> C -> N

    graph.add_filter("A", having)  # accepted: nothing the check can read is wrong

    assert len(graph.filters["A"]) == 1  # exactly the one HAVING on A
    assert graph.filters["A"][0] is having  # the very predicate object

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

"""The tiny sales schema shared by the ``JoinGraph`` view and filter tests.

::

    L (lineitems) --order--> O (orders) --customer--> C (customers) --nation--> N (nations)

Every builder creates fresh RFs (new uuids), so tests never share state.
``OpaquePredicate`` and ``GtSubclass`` are user-defined predicates that the
graph's path checks do not inspect.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fdm.attribute_functions import TF, RF, DBF
from fql.operators.aggregates import AggregationFunction, Sum
from fql.plan.join_graph import AttributeName, AttributePath, JoinGraph, View
from fql.predicates.predicates import Gt, Predicate


@dataclass(frozen=True)
class SalesRelations:
    """The four RFs of the sales schema ``L -> O -> C -> N``."""

    nations: RF
    customers: RF
    orders: RF
    lineitems: RF


def sales_relations() -> SalesRelations:
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


def graph_of(**relations: RF) -> JoinGraph:
    """Build the join graph of a DBF holding exactly the given relations."""
    return JoinGraph.from_dbf(
        DBF(relations, frozen=True)
    )  # edges between these relations come from their references


def edge_labels(graph: JoinGraph) -> set[str]:
    """The graph's edges as ``"source -ref_key-> target"`` labels (by name)."""
    return {
        f"{edge.source.name} -{edge.ref_key}-> {edge.target.name}"
        for edge in graph.edges
    }  # names only: never compares relations by content


def view_over_lineitems_and_orders(
    rels: SalesRelations,
    group_keys: dict[AttributeName, AttributePath],
    aggregates: dict[AttributeName, AggregationFunction],
) -> View:
    """View ``A`` over ``L -order-> O`` with the given grouping keys and aggregates."""
    return View(
        name="A",  # name of the view in the enclosing graph
        subgraph=graph_of(L=rels.lineitems, O=rels.orders),  # L -order-> O
        group_keys=group_keys,  # as given by the caller
        aggregates=aggregates,  # as given by the caller
    )  # validated on construction


def revenue_per_customer_view(rels: SalesRelations) -> View:
    """View ``A``: revenue per customer over ``L -order-> O``."""
    return view_over_lineitems_and_orders(
        rels,
        group_keys={"customer": "O.customer"},  # one group per referenced customer
        aggregates={"revenue": Sum("L.price")},  # sum of line prices per group
    )  # view A with one key and one aggregate


def revenue_view_joined_to_customers_graph(rels: SalesRelations) -> JoinGraph:
    """Graph ``A -customer-> C -nation-> N`` with A the revenue-per-customer view."""
    graph: JoinGraph = graph_of(C=rels.customers, N=rels.nations)  # C -nation-> N
    graph.add_view(revenue_per_customer_view(rels))  # A next to C and N
    graph.add_edge("A", "C", "customer")  # A.customer references C
    return graph  # three nodes, two edges


def pass_through_view_graph(rels: SalesRelations) -> JoinGraph:
    """Graph holding only view ``P``, which passes ``A -> C -> N`` on unchanged.

    P has neither grouping nor projection, so its outputs are the relation
    names ``C``, ``N`` and ``A`` (A the revenue-per-customer view).
    """
    graph: JoinGraph = JoinGraph()  # empty graph around P
    graph.add_view(
        View(
            name="P",  # the pass-through view
            subgraph=revenue_view_joined_to_customers_graph(rels),  # A, C and N
        )  # no grouping, no projection: outputs are "C", "N" and "A"
    )  # P becomes the only node
    return graph  # no filters yet


class OpaquePredicate(Predicate):
    """A user-defined structured predicate: a ``Predicate`` subclass, no built-in.

    It carries an ``attr`` like the built-in leaves (``Eq``, ``Gt``, ...), so a
    path check that inspected every ``Predicate`` instead of the built-in
    classes only would read it. It matches every value; the tests only store
    it and never evaluate it.
    """

    def __init__(self, attr: str) -> None:
        """Remember an attribute path that no check may read.

        @param attr: Any path; the tests pass one that no check would accept.
        """
        self.attr: str = attr  # looks like a leaf's path, but is opaque to the graph

    def __call__(self, value: Any) -> bool:
        """Match every value."""
        return True  # never evaluated in the plan tests

    def to_dict(self) -> dict:
        """Serialize under an op name no built-in predicate uses."""
        return {"type": "predicate", "op": "opaque", "attr": self.attr}  # user op

    @classmethod
    def _from_dict(cls, data: dict) -> OpaquePredicate:
        """Rebuild the predicate from the output of ``to_dict``."""
        return cls(data["attr"])  # never deserialized in the plan tests


class GtSubclass(Gt):
    """A user-defined subclass of the built-in ``Gt``: same fields, other type.

    It is an instance of a built-in leaf class (``Gt``), so only the exact-type
    check of ``_predicate_paths`` keeps it uninspected.
    """

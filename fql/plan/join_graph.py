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

"""The join graph: relations as nodes, references as edges, plus node filters and
views (boxes around subgraphs with join predicates, a grouping or a projection).

A logical description of a query; `join` and `subdatabase` read only its
topology. Designs: `docs/changes/011-join-graph-aggregate-boxes/DECISION.md`
and `docs/changes/015-join-graph-views/DECISION.md`.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from fdm.attribute_functions import RF, DBF
from fdm.schema import ForeignValueConstraint, JoinPredicate
from fql.predicates.predicates import (
    And,
    ComparisonPredicate,
    Eq,
    Exists,
    Gt,
    Gte,
    In,
    Like,
    Lt,
    Lte,
    Missing,
    Not,
    Or,
    Ref,
    _split_attr_path,
)

if TYPE_CHECKING:
    from fql.operators.aggregates import AggregationFunction

# Type aliases — the first three are `str` for the type checker, but the alias names make
# every signature in this file (and in operators that import these
# types) self-documenting about which kind of name it is.
#
# `RelationName` is a key in the DBF — the outer name of an RF.
# `AttributeName` is a key inside a TF — the `ref_key` of a
# `ForeignValueConstraint`, i.e. the attribute that carries a
# reference to another TF. Two `str`s collapsed into the same
# variable name used to hide that difference; these aliases surface
# it.
# `AttributePath` is a dot- or `__`-separated path into the rows of a
# joined subgraph, e.g. `"O.customer"` (relation `O`, attribute `customer`)
# — used by `View`, whose output *names* are `AttributeName`s.
# `FilterPredicate` is a node filter or the condition of a `JoinPredicate`:
# a structured `Predicate` or a plain callable (e.g. a lambda), as
# `filter_values` accepts; a `Predicate` is callable too, so one callable
# type covers both.
type RelationName = str
type AttributeName = str
type AttributePath = str
type FilterPredicate = Callable[[Any], Any]


@dataclass(frozen=True)
class Node:
    """A named node in the graph. `name` is a relation name or, for a
    `View`, the name of the view's result relation.

    Shared base of `JoinNode` (a base relation) and `View`; a bare
    `Node` is neither and only carries a name (e.g. in hand-built test graphs).
    """

    name: RelationName


@dataclass(frozen=True)
class JoinNode(Node):
    """A node in the join graph, wrapping a named RF from the input DBF."""

    rf: RF


@dataclass(frozen=True)
class View(Node):
    """A view around a subgraph: the joined subgraph, filtered, then grouped,
    projected or passed on.

    The view's result is again a relation, so the view is a node of the
    enclosing graph and can be joined over (see `JoinGraph.add_edge`), and a
    subgraph may itself contain views (nesting). This is a *logical*
    representation; nothing here executes a view.

    Paths address the rows of the joined subgraph, whose top-level keys are
    relation names (e.g. `"O.customer"`, or `"O"` for the tuples of `O`):

    - `join_predicates`: `JoinPredicate`s over several relations of the
      subgraph (a predicate on one relation belongs on that node, see
      `JoinGraph.add_filter`).
    - `group_keys` / `aggregates`: grouping (011). With grouping keys and no
      aggregates a DISTINCT projection (`is_distinct()`); with aggregates and
      no grouping keys a global aggregate (one result tuple).
    - `projection`: named output paths, one output tuple per joined row (no
      DISTINCT). Projection and grouping exclude each other.

    Output names: the projection names, else the grouping keys and
    aggregates, else (pass-through view) the subgraph's relation names.

    Meaning (for a later executor): (1) rows = join of the subgraph along its
    own edges; (2) a row is kept iff each node's part satisfies that node's
    filters and the row satisfies all join predicates (two-valued, as
    `satisfies` evaluates filters: a comparison on a missing attribute is
    false; `Missing` / `Not` hold there); (3) then grouping, or projection, or
    neither. The keys of a projection view's result tuples are left to the
    executor (a joined row has no single key). Not executable today:
    `group_by` cannot group by tuples, and `group_by_aggregate` does not write
    the grouping keys into the result tuples. Edges from a grouping view
    presuppose grouping by tuple identity.

    Validation runs once, at construction. Details, limitations and the
    rejected alternatives: `docs/changes/011-join-graph-aggregate-boxes/DECISION.md`
    and `docs/changes/015-join-graph-views/DECISION.md`.
    """

    subgraph: JoinGraph
    group_keys: dict[AttributeName, AttributePath] = field(default_factory=dict)
    aggregates: dict[AttributeName, AggregationFunction] = field(default_factory=dict)
    join_predicates: list[JoinPredicate] = field(default_factory=list)
    projection: dict[AttributeName, AttributePath] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the view against its subgraph.

        Rules:

        - the subgraph is non-empty;
        - projection and grouping (grouping keys / aggregates) exclude each
          other;
        - the view name and all named outputs are single non-empty segments
          (no `.` / `__`), and output names are unique;
        - every path starts with a relation of the subgraph and, if it
          continues into an inner view, its next segment is one of the inner
          view's output names (through inner views that pass rows on, at any
          depth);
        - every aggregate is an `AggregationFunction`, and only `Count` among
          the built-ins may have no attribute;
        - every join predicate is a `JoinPredicate` declaring at least two
          relations of the subgraph, and a built-in structured one reads
          exactly the relations it declares.

        @raises ValueError: If a rule is violated; each rule has its own message.
        """
        # imported here, not at module level, so that `fql.plan` keeps no
        # import-time dependency on operator modules (as in the plan IR)
        from fql.operators.aggregates import AggregationFunction

        if not self.subgraph.nodes:
            raise ValueError(f"View '{self.name}' needs a non-empty subgraph")
        if self.projection and (self.group_keys or self.aggregates):
            raise ValueError(
                f"View '{self.name}' has a projection and a grouping; "
                f"use one of them (or an outer view)"
            )
        clashes: set[AttributeName] = set(self.group_keys) & set(self.aggregates)
        if clashes:
            raise ValueError(
                f"View '{self.name}': output names used twice: {sorted(clashes)}"
            )
        # the view name and the named outputs become path segments
        # (`"A.revenue"` in an enclosing view) and output names become
        # `ref_key`s, so each must be a single non-empty segment; the outputs of
        # a pass-through view are relation names of the subgraph, not checked
        self._check_name("view name", self.name)
        for output_name in [*self.group_keys, *self.aggregates, *self.projection]:
            self._check_name("output name", output_name)
        for output_name, path in [*self.group_keys.items(), *self.projection.items()]:
            self._check_path(output_name, path)
        for index, join_predicate in enumerate(self.join_predicates):
            self._check_join_predicate(index, join_predicate)
        for output_name, aggregation in self.aggregates.items():
            if not isinstance(aggregation, AggregationFunction):
                raise ValueError(
                    f"View '{self.name}': aggregate '{output_name}' is not an "
                    f"AggregationFunction: {aggregation!r}"
                )
            if aggregation.attribute is not None:
                self._check_path(output_name, aggregation.attribute)
            # without attribute only `Count()` and user-built aggregations
            # (`op_name` None, e.g. over whole rows) make sense; `Sum(None)`
            # would aggregate the joined rows themselves
            elif aggregation.op_name not in (None, "count"):
                raise ValueError(
                    f"View '{self.name}': aggregate '{output_name}' needs an "
                    f"attribute: {aggregation!r}"
                )

    def _check_name(self, kind: str, name: RelationName | AttributeName) -> None:
        """Check that `name` is a single non-empty path segment.

        @param kind: What the name is (`"view name"` / `"output name"`), for the
            error message.
        @param name: The name to check.
        @raises ValueError: If `name` is empty or contains `.` or `__`.
        """
        if not name or "." in name or "__" in name:
            raise ValueError(
                f"View '{self.name}': {kind} {name!r} must be "
                f"non-empty and contain neither '.' nor '__'"
            )

    def _check_path(self, owner: str, path: AttributePath) -> None:
        """Check that `path` starts with a relation of the subgraph and, if it
        continues into an inner view, that its next segment is one of the inner
        view's output names.

        @param owner: What the path belongs to: an output name, a join-predicate
            label (`"join predicate 0"`) or `"filter"` (only used in error
            messages).
        @param path: The attribute path to check.
        @raises ValueError: If the first segment is not a subgraph node, or the
            path continues into an inner view and its next segment is not an
            output name of that inner view. Through an inner view that passes
            rows on, whose outputs are relations again, the rest of the path is
            checked the same way. An empty segment raises `_split_attr_path`'s
            own `ValueError`.
        """
        segments: list[str] = _split_attr_path(path)
        inner: Node | None = self.subgraph.nodes.get(segments[0])
        if inner is None:
            raise ValueError(
                f"View '{self.name}': path {path!r} of '{owner}' does not "
                f"start with a relation of the subgraph {sorted(self.subgraph.nodes)}"
            )
        if not isinstance(inner, View) or len(segments) == 1:
            return
        # an inner view's output names are known exactly, so a misspelt
        # attribute of it can be caught here (base relations have no schema)
        if segments[1] not in inner.output_names():
            raise ValueError(
                f"View '{self.name}': path {path!r} of '{owner}' names no "
                f"output of inner view '{inner.name}': {inner.output_names()}"
            )
        # the outputs of an inner view that passes rows on are its subgraph's
        # relations, so the rest of the path is checked one level further down
        if inner.passes_rows_on():
            inner._check_path(owner, ".".join(segments[1:]))

    def _check_join_predicate(self, index: int, join_predicate: JoinPredicate) -> None:
        """Check one join predicate against the subgraph.

        @param index: Position of the predicate in `join_predicates` (for
            error messages).
        @param join_predicate: The predicate to check.
        @raises ValueError: If it is no `JoinPredicate`, declares fewer than two
            distinct relations or one outside the subgraph, or (built-in
            structured predicates only) reads a path that `_check_path` rejects
            or other relations than it declares. Duplicate declared relations
            count once.
        """
        label: str = f"join predicate {index}"
        if not isinstance(join_predicate, JoinPredicate):
            raise ValueError(
                f"View '{self.name}': {label} is not a JoinPredicate: "
                f"{join_predicate!r}"
            )
        declared: set[RelationName] = set(join_predicate.relations)
        if len(declared) < 2:
            raise ValueError(
                f"View '{self.name}': {label} must declare at least two relations "
                f"of the subgraph, declares {sorted(declared)}; a predicate on one "
                f"relation belongs on that node (JoinGraph.add_filter on the "
                f"subgraph, with paths relative to its tuples: 'O.date' becomes "
                f"'date')"
            )
        unknown: set[RelationName] = declared - set(self.subgraph.nodes)
        if unknown:
            raise ValueError(
                f"View '{self.name}': {label} declares relations outside the "
                f"subgraph {sorted(self.subgraph.nodes)}: {sorted(unknown)}"
            )
        # a lambda or a user-defined predicate cannot be inspected (None)
        paths: list[AttributePath] | None = _predicate_paths(join_predicate.predicate)
        if paths is None:
            return
        for path in paths:
            self._check_path(label, path)
        # a built-in structured predicate must read exactly what it declares
        read_relations: set[RelationName] = {
            _split_attr_path(path)[0] for path in paths
        }
        if read_relations != declared:
            raise ValueError(
                f"View '{self.name}': {label} reads {sorted(read_relations)} but "
                f"declares {sorted(declared)}"
            )

    def passes_rows_on(self) -> bool:
        """True iff the view has neither a grouping nor a projection, i.e. its
        result rows are the (filtered) joined rows of the subgraph. Such a view
        is called a *pass-through view*.

        @return: Whether the view passes the joined rows on.
        """
        return not (self.group_keys or self.aggregates or self.projection)

    def output_names(self) -> list[AttributeName]:
        """The view's output attribute names.

        @return: The projection names, else the grouping keys then the
            aggregates, else (pass-through view) the subgraph's relation names.
        """
        if self.projection:
            return list(self.projection)
        if self.passes_rows_on():
            return list(self.subgraph.nodes)
        return [*self.group_keys, *self.aggregates]

    def output_path(self, output_name: AttributeName) -> AttributePath | None:
        """The path behind a grouping key or projection name.

        @param output_name: An output name of the view.
        @return: The path, or None for an aggregate, a pass-through output or
            an unknown name (they have no single path).
        """
        return self.group_keys.get(output_name, self.projection.get(output_name))

    def is_distinct(self) -> bool:
        """True iff the view has grouping keys and no aggregates.

        @return: Whether the view is a DISTINCT projection.
        """
        return bool(self.group_keys) and not self.aggregates


@dataclass(frozen=True)
class Edge:
    """A directed reference edge: a source tuple's `ref_key` attribute
    references a tuple of `target` (`source_tf[ref_key] is target_tf`).

    Compare edges and nodes by name, not with `==`: `==` compares the
    relations they hold by content, not by identity.

    Derived from a ForeignValueConstraint set via .references(), or added by
    `JoinGraph.add_edge` from a `View`; in that case the source tuple is a
    result tuple of the view and `ref_key` one of its grouping keys or
    projection names.

    The `ref_key` is the *attribute* name on a source tuple (as in
    `source_tf[ref_key] is target_tf`) — it is emphatically *not* a
    relation name; the endpoints' relation names are
    `source.name` / `target.name`.
    """

    source: Node
    target: Node
    ref_key: AttributeName


@dataclass(frozen=True)
class Neighbor:
    """An entry in an adjacency list: a neighbor relation connected via ref_key.

    Shared value type for `outgoing_adjacency()` (neighbor is the edge target)
    and `incoming_adjacency()` (neighbor is the edge source). `ref_key` is
    always the attribute on the *source* tuple, regardless of which map the
    entry is in.
    """

    name: RelationName
    ref_key: AttributeName


@dataclass
class JoinGraph:
    """The join graph of a DBF, extracted from ForeignValueConstraint metadata.

    Nodes are named relations: the RFs of the DBF (`JoinNode`s) and
    `View`s added with `add_view`. Edges are directed references:
    between RFs (found by `from_dbf`, `source_tf[ref_key] is target_tf`) and
    from a view to a base relation (added by `add_edge`). The topology
    queries treat a view like any other node. `filters` holds the per-node
    filters added with `add_filter` (WHERE; HAVING on a view node); `from_dbf`
    and the topology queries ignore them.

    Project convention: **every read-only query about the graph's
    topology lives here, not in the operator that happens to need it.**
    That includes acyclicity checks, component enumeration, pure-source
    / isolated-node detection, adjacency maps, trivial-graph predicates
    and any similar structural query added for future join-rework MRs.
    Operators (`join`, `subdatabase`, follow-ups) only encode policy
    (error messages, which raise, which branch) on top of the graph's
    answers. Keep new graph algorithms on this class.
    """

    nodes: dict[RelationName, Node] = field(default_factory=dict)
    edges: list[Edge] = field(default_factory=list)
    filters: dict[RelationName, list[FilterPredicate]] = field(default_factory=dict)

    def __len__(self) -> int:
        """Number of relations in the graph — lets callers write
        `len(graph)` instead of `len(graph.nodes)`. Same surface as
        the reference JoinGraph in `query_optimization/join_graph.py`.
        """
        return len(self.nodes)

    def __iter__(self) -> Iterator[RelationName]:
        """Iterate the relation names in dict-insertion order.

        Matches the external `JoinGraph.__iter__` semantics (yields
        the graph's nodes). Use e.g. `for name in graph: ...` instead
        of `for name in graph.nodes: ...`.
        """
        return iter(self.nodes)

    @classmethod
    def from_dbf(cls, dbf: DBF) -> JoinGraph:
        """Extract the join graph from a DBF by inspecting ForeignValueConstraint
        on each RF.

        A relation's self-reference (source RF is the target RF, e.g.
        `title.episode_of_id -> title`) is not turned into an edge: a reference
        is a pointer, not a join request (feature 013, see
        `docs/changes/013-self-reference-edge-policy/DECISION.md`).

        @param dbf: The input DBF to extract the join graph from.
        @return: A JoinGraph with nodes and edges. Edges only include references
            where both endpoints are relations in the DBF.
        @raises NotImplementedError: If an RF entered under several DBF names
            (aliases) is referenced by another relation: nothing tells which
            alias the reference means, and guessing would give wrong answers.
        """
        graph = cls()

        # create one JoinNode per named RF in the DBF; kept in a local dict
        # typed `JoinNode` so that `.rf` below is well-typed (`graph.nodes`
        # holds any `Node`)
        join_nodes: dict[RelationName, JoinNode] = {
            item.key: JoinNode(name=item.key, rf=item.value) for item in dbf
        }
        graph.nodes.update(join_nodes)

        # build a lookup from RF uuid to every relation name it is entered
        # under, so we can resolve the target_attribute_function stored in each
        # ForeignValueConstraint back to a named node in this graph
        uuid_to_names: dict[int, list[RelationName]] = {}
        for name, node in join_nodes.items():
            uuid_to_names.setdefault(node.rf.uuid, []).append(name)

        # scan each RF's values_constraints for ForeignValueConstraint instances;
        # these are created by .references() and encode source_rf[ref_key] -> target_rf
        for name, node in join_nodes.items():
            for constraint in node.rf.__dict__["values_constraints"]:
                if isinstance(constraint, ForeignValueConstraint):
                    target_uuid: int = constraint.target_attribute_function.uuid
                    # a self-reference is no edge (feature 013)
                    if target_uuid == node.rf.uuid:
                        continue
                    target_names: list[RelationName] = uuid_to_names.get(
                        target_uuid, []
                    )
                    # an aliased target: refuse rather than guess the alias
                    if len(target_names) > 1:
                        raise NotImplementedError(
                            f"join graph: '{name}.{constraint.key}' references a "
                            f"relation entered under several names "
                            f"{sorted(target_names)}; the reference does not say "
                            f"which one it means. Enter every name as its own "
                            f"relation, e.g. `rf.where(lambda i: True)` for each, "
                            f"so the original RF itself is not in the DBF, and "
                            f"bind the reference with add_reference."
                        )
                    # only include edges where the target is also in this DBF
                    if target_names:
                        target_name: RelationName = target_names[0]
                        graph.edges.append(
                            Edge(
                                source=node,
                                target=graph.nodes[target_name],
                                ref_key=constraint.key,
                            )
                        )

        return graph

    def add_view(self, view: View) -> None:
        """Add a view as a node of this graph.

        @param view: The view; its name must not be used by another node.
        @raises ValueError: If the name is already taken, or the view's subgraph
            is this graph or encloses it through nested views (a view cannot
            contain the graph it sits in, directly or indirectly).
        """
        if view.name in self.nodes:
            raise ValueError(f"Node name '{view.name}' is already used in the graph")
        if view.subgraph._encloses(self):
            raise ValueError(
                f"View '{view.name}' cannot contain the graph it is added to "
                f"(directly or through nested views)"
            )
        self.nodes[view.name] = view

    def _encloses(self, graph: JoinGraph) -> bool:
        """True iff `graph` is this graph or the subgraph of a view nested in it
        (at any depth).

        Terminates because `add_view` rejects every view that would close a
        loop; a loop built by writing to `nodes` directly bypasses that.

        @param graph: The graph to look for (compared by identity).
        @return: Whether `graph` occurs in this graph's view nesting.
        """
        return graph is self or any(
            isinstance(node, View) and node.subgraph._encloses(graph)
            for node in self.nodes.values()
        )

    def add_filter(self, name: RelationName, predicate: FilterPredicate) -> None:
        """Attach a filter to one node of this graph.

        The predicate applies to the node's tuples, as in
        `filter_values(X, predicate)` where X is the node's relation; several
        filters on one node are a
        conjunction. On a view node it is HAVING: it applies to the view's
        result tuples, and a built-in structured predicate may only read the
        view's output names. Filters of a view's inner nodes live in its subgraph
        (`view.subgraph.add_filter(...)`).

        @param name: Name of a node of this graph.
        @param predicate: A structured `Predicate` or a plain callable.
        @raises ValueError: If `name` is not a node of this graph, the
            predicate is a `JoinPredicate` (it spans several relations and
            belongs on a view), or a built-in structured predicate on a view
            node reads a path that does not start with one of the view's output
            names (on a view that passes rows on: a path the view's own path
            check rejects). Lambdas and user-defined predicates are not checked.
        """
        node: Node | None = self.nodes.get(name)
        if node is None:
            raise ValueError(f"'{name}' is not a node of the graph")
        if isinstance(predicate, JoinPredicate):
            raise ValueError(
                f"Filter on '{name}' is a JoinPredicate; a predicate over several "
                f"relations belongs on a view (View.join_predicates)"
            )
        # a view's outputs are known exactly, so a misspelt one is caught
        # here; it would otherwise drop every row silently
        if isinstance(node, View):
            outputs: list[AttributeName] = node.output_names()
            # None (a lambda or user-defined predicate): accepted unchecked
            for path in _predicate_paths(predicate) or []:
                # a pass-through view's outputs are its subgraph relations, so
                # its own path check applies (also into inner views)
                if node.passes_rows_on():
                    node._check_path("filter", path)
                elif _split_attr_path(path)[0] not in outputs:
                    raise ValueError(
                        f"Filter on view '{name}': path {path!r} names no "
                        f"output of the view: {outputs}"
                    )
        self.filters.setdefault(name, []).append(predicate)

    def add_edge(
        self, source: RelationName, target: RelationName, ref_key: AttributeName
    ) -> None:
        """Add a reference edge from a view to a base relation of this graph.

        The edge must be backed by a reference: `ref_key` is a grouping key or
        a projection name of the view whose path is either `X` (the tuples of
        the subgraph relation X; X's RF is the target's RF) or `X.attr` (X
        carries a `ForeignValueConstraint` on `attr` pointing at the target).
        X must be a base relation of the subgraph, not an inner view.
        Relations are compared by `uuid`, as in `from_dbf`.

        @param source: Name of the view the edge starts at.
        @param target: Name of the base relation the edge points to.
        @param ref_key: Grouping-key or projection name of the view that
            carries the reference.
        @raises ValueError: If an endpoint is not a node of the graph, the
            source is not a view, the target is not a base relation, `ref_key`
            is neither a grouping key nor a projection name, its path is not a
            reference to the target, or the edge already exists; each rule
            has its own message.
        """
        for name in (source, target):
            if name not in self.nodes:
                raise ValueError(f"'{name}' is not a node of the graph")
        source_node: Node = self.nodes[source]
        target_node: Node = self.nodes[target]
        if not isinstance(source_node, View):
            raise ValueError(
                f"Edge source '{source}' must be a view (edges between base "
                f"relations come from from_dbf)"
            )
        if not isinstance(target_node, JoinNode):
            raise ValueError(f"Edge target '{target}' must be a base relation")
        path: AttributePath | None = source_node.output_path(ref_key)
        if path is None:
            edge_keys: list[AttributeName] = sorted(
                [*source_node.group_keys, *source_node.projection]
            )
            raise ValueError(
                f"'{ref_key}' is neither a grouping key nor a projection name of "
                f"view '{source}': {edge_keys}"
            )
        if not _path_references(source_node, path, target_node.rf):
            kind: str = (
                "Grouping key"
                if ref_key in source_node.group_keys
                else "Projection name"
            )
            raise ValueError(
                f"{kind} '{ref_key}' of view '{source}' (path "
                f"{path!r}) is not a reference to '{target}'"
            )
        # compare by names: `Edge ==` would compare relations by content
        if any(
            (edge.source.name, edge.target.name, edge.ref_key)
            == (source, target, ref_key)
            for edge in self.edges
        ):
            raise ValueError(f"Edge {source} -{ref_key}-> {target} already exists")
        self.edges.append(Edge(source=source_node, target=target_node, ref_key=ref_key))

    def check_acyclicity(self) -> None:
        """Verify that the directed reference graph is acyclic using Kahn's algorithm
        (topological sort).

        The algorithm works by repeatedly removing nodes with in-degree 0 (no incoming
        edges). If all nodes can be removed, the graph is acyclic. If some nodes remain,
        they form a cycle.

        In the reference graph, edges point from source to target (source.rf[ref_key] ->
        target.rf), so in-degree counts how many sources reference a given target node.

        @raises ValueError: If the graph contains a cycle.
        """
        # count in-degree for each node: how many edges point TO this node
        edge_nodes: set[str] = set()
        in_degree: dict[str, int] = {}
        for edge in self.edges:
            edge_nodes.add(edge.source.name)
            edge_nodes.add(edge.target.name)
            in_degree.setdefault(edge.source.name, 0)
            in_degree[edge.target.name] = in_degree.get(edge.target.name, 0) + 1

        # seed the queue with nodes that have no incoming edges (leaves)
        queue: deque[str] = deque(
            name for name in edge_nodes if in_degree.get(name, 0) == 0
        )
        visited: int = 0
        while queue:
            name = queue.popleft()
            visited += 1
            # decrement in-degree for all parents of this node
            for edge in self.edges:
                if edge.source.name == name:
                    in_degree[edge.target.name] -= 1
                    if in_degree[edge.target.name] == 0:
                        queue.append(edge.target.name)

        if visited < len(edge_nodes):
            raise ValueError(
                "Join graph is cyclic. Only acyclic join graphs are supported."
            )

    def pure_sources(self) -> set[str]:
        """Names of all nodes with at least one outgoing edge and no
        incoming ones.

        A "pure source" is the natural Kahn's-algorithm seed (in-degree
        zero in a node that actually participates in at least one
        edge). `join._pick_walk_start` starts its (bidirectional) walk at
        `sorted(pure_sources)[0]`, and the set is useful any time the
        calling code wants the start set for a topological sort on the
        directed reference graph.
        """
        incoming: set[str] = {edge.target.name for edge in self.edges}
        outgoing: set[str] = {edge.source.name for edge in self.edges}
        return {n for n in self.nodes if n not in incoming and n in outgoing}

    def isolated_nodes(self) -> set[str]:
        """Names of all nodes with neither incoming nor outgoing edges.

        Isolated nodes are explicitly kept distinct from pure sources
        so that callers (e.g. `join`) can surface them in a separate
        error path — "you have an orphan relation" reads differently
        from "your graph has multiple pure sources".
        """
        involved: set[str] = {edge.source.name for edge in self.edges} | {
            edge.target.name for edge in self.edges
        }
        return {n for n in self.nodes if n not in involved}

    def is_trivial(self) -> bool:
        """True iff the graph carries no reference edges at all.

        Either the DBF has a single RF (in which case there is nothing
        to reference), or it contains multiple RFs that happen to be
        unrelated — both cases produce zero edges. Operators use this
        as the short-circuit for the "no join structure" branch.
        """
        return not self.edges

    def sole_relation_name(self) -> str | None:
        """Return the one relation name if the graph has exactly one
        node, else None.

        Thin wrapper around `next(iter(self.nodes))` that names the
        semantic concept ("this DBF has only one relation") and hides
        the dict-iteration idiom from callers.
        """
        if len(self.nodes) != 1:
            return None
        return next(iter(self.nodes))

    def outgoing_adjacency(self) -> dict[RelationName, list[Neighbor]]:
        """Outgoing-edge adjacency map keyed by source relation name.

        Each value is a list of `Neighbor(name=target_relation,
        ref_key=attribute_on_source)` — reusing the existing
        `Neighbor` dataclass, which already names the two fields
        distinctly so callers never confuse a relation name with an
        attribute name. For a current tuple `tf` at the source node,
        iterate `outgoing_adjacency()[source_name]` and follow each
        neighbour by `tf[neighbor.ref_key]` to reach the next hop's
        target tuple (which lives in the RF named `neighbor.name`).

        Built once per call from the edges list — O(|edges|).
        `build_semijoin_cascade` builds its own *undirected* variant
        internally because Yannakakis's spanning-tree BFS needs both
        directions; it keeps the `Edge`s themselves, because each
        semijoin step needs the edge's direction.
        """
        adjacency: dict[RelationName, list[Neighbor]] = {}
        for edge in self.edges:
            adjacency.setdefault(edge.source.name, []).append(
                Neighbor(name=edge.target.name, ref_key=edge.ref_key)
            )
        return adjacency

    def incoming_adjacency(self) -> dict[RelationName, list[Neighbor]]:
        """Incoming-edge adjacency map keyed by *target* relation name.

        Mirror of `outgoing_adjacency()`: each value lists the
        `Neighbor(name=source_relation, ref_key=attribute_on_source)` for
        every edge pointing *at* this relation. Where `outgoing_adjacency`
        lets a walk follow a reference forward via the inline pointer
        (`source_tf[ref_key] is target_tf`), this map lets it walk an edge
        **backward** — from a referenced (hub) tuple to the source tuples
        that reference it — which the forward pointer cannot express. Reuses
        `Neighbor`; the direction is implicit in which map a neighbor came
        from, so no extra field is needed.

        Built once per call from the edges list — O(|edges|).
        """
        adjacency: dict[RelationName, list[Neighbor]] = {}
        for edge in self.edges:
            adjacency.setdefault(edge.target.name, []).append(
                Neighbor(name=edge.source.name, ref_key=edge.ref_key)
            )
        return adjacency

    def is_tree(self) -> bool:
        """True iff the undirected reference graph is a single tree.

        A tree is weakly connected (exactly one connected component) and has
        exactly ``len(nodes) - 1`` edges. This is the shape the flattening
        `join` accepts in any edge orientation; anything else (a diamond, a
        cycle, a parallel reference, or a disconnected graph) has a different
        edge count or component count and returns False.

        Note this is deliberately **not** `check_acyclicity()`: that tests
        *directed* acyclicity, but a diamond (``A->B, A->C, B->D, C->D``) is
        directed-acyclic while being undirected-cyclic. The edge-count test
        here is the correct undirected-tree test. On a multigraph (parallel
        edges) the edge count exceeds ``n - 1``, so those are rejected too.
        """
        return (
            len(self.connected_components()) == 1
            and len(self.edges) == len(self.nodes) - 1
        )

    def connected_components(self) -> list[set[str]]:
        """Weakly-connected components of the reference graph.

        Treats edges as undirected and groups nodes by reachability.
        Isolated relations form singleton components. A fully-connected
        DBF returns exactly one component covering every node.

        Used by `join` to distinguish "disconnected DBF" (two or more
        unrelated component sub-joins, semantically a Cartesian
        product across components) from "single component with
        multiple pure sources" (JOB-style `ci→t, mc→t`). Both are
        deferred in the minimal POC but they deserve different error
        messages — this helper is the structural distinction.

        Returned sets are mutable — callers that merely inspect them
        (which is the current use) don't care; if future callers need
        to mutate they can copy per component.

        Example: on the four-relation shape `R→S, T→U` (no link
        between {R,S} and {T,U}), the return value is
        `[{"R", "S"}, {"T", "U"}]`.

        Order of components follows iteration order of `self.nodes`
        (dict insertion order), so the first component is the one
        containing the first-inserted relation name.
        """
        # Step 1 — build an undirected adjacency map. Every node in
        # `self.nodes` gets an entry so isolated nodes (no edges)
        # naturally appear as their own singleton components during
        # the BFS below. Each edge adds both endpoints to each
        # other's neighbour set — direction is intentionally thrown
        # away here, because weak connectivity is what we want.
        adjacency: dict[str, set[str]] = {name: set() for name in self.nodes}
        for edge in self.edges:
            adjacency[edge.source.name].add(edge.target.name)
            adjacency[edge.target.name].add(edge.source.name)

        # Step 2 — flood-fill BFS. Iterate every node as a potential
        # seed; skip ones already assigned to a previous component.
        # For each fresh seed, grow a component via BFS on the
        # undirected adjacency. The `queue.extend(adjacency[node] -
        # visited)` line is what turns this into component discovery:
        # we only enqueue neighbours we haven't seen yet, so a single
        # BFS run terminates when the connected set is exhausted.
        visited: set[str] = set()
        components: list[set[str]] = []
        for start in self.nodes:
            if start in visited:
                # already covered by a previous component — skip
                continue
            component: set[str] = set()
            queue: deque[str] = deque([start])
            while queue:
                node: str = queue.popleft()
                if node in visited:
                    # may have been enqueued multiple times via
                    # different neighbours before being popped; drop
                    # the duplicates here rather than guarding every
                    # enqueue site (simpler and cheaper for the POC).
                    continue
                visited.add(node)
                component.add(node)
                # only enqueue neighbours not yet visited — keeps the
                # queue bounded and avoids revisiting nodes endlessly
                # on a cyclic undirected graph.
                queue.extend(adjacency[node] - visited)
            components.append(component)
        return components

    def select_root(self, root: str | None) -> str:
        """Select or auto-detect the root node for the join tree.

        @param root: User-specified root, or None for auto-selection.
        @return: The selected root node name.
        """
        if root is not None:
            assert root in self.nodes, f"Root '{root}' is not a node of the graph."
            return root

        # auto-select: pick a node with no incoming references (pure target)
        target_nodes: set[str] = {edge.source.name for edge in self.edges}
        candidates: list[str] = [
            name for name in self.nodes if name not in target_nodes
        ]
        return candidates[0] if candidates else next(iter(self.nodes))

    def build_semijoin_cascade(self, root: str) -> list[SemijoinStep]:
        """Build an ordered list of semijoin steps for Yannakakis reduction.

        Constructs a rooted BFS tree from the undirected version of the join graph,
        then generates:
        - Bottom-up (post-order): each tree edge once, reduce the parent by its child.
        - Top-down (pre-order): each tree edge once, reduce the child by its parent.

        Parent and child are positions in the BFS tree, not the FDM direction. Each
        step carries the edge's FDM source (`SemijoinStep.source`, the relation whose
        tuples hold `ref_key`), so the semijoin operator never guesses it.

        This method checks the cyclicity of the graph and throws a ValueError if it detects a cycle.

        @param root: The root node name for the join tree.
        @return: Ordered list of SemijoinStep to execute sequentially.
        """
        self.check_acyclicity()

        # build undirected adjacency list from edges; each edge is listed at
        # both endpoints and keeps its direction for the semijoin steps
        adjacency: dict[str, list[Edge]] = {name: [] for name in self.nodes}
        for edge in self.edges:
            adjacency[edge.source.name].append(edge)
            adjacency[edge.target.name].append(edge)

        # BFS from the root to build a rooted spanning tree (child_edges).
        # Each node discovered for the first time becomes a child of the node
        # that discovered it, turning the undirected adjacency list into a
        # parent→child-edges mapping suitable for the post-order (bottom-up)
        # and pre-order (top-down) traversals of the Yannakakis algorithm.
        child_edges: dict[str, list[Edge]] = {}
        visited: set[str] = {root}
        queue: deque[str] = deque([root])

        while queue:
            node = queue.popleft()
            for edge in adjacency[node]:
                child: str = _other_end(edge, node)
                if child in visited:
                    continue  # already discovered — skip to avoid cycles in the tree
                visited.add(child)
                queue.append(child)
                # record the edge to child under its parent in the rooted tree
                child_edges.setdefault(node, []).append(edge)

        # check that all edge-participating nodes were reached by BFS
        edge_nodes: set[str] = {edge.source.name for edge in self.edges} | {
            edge.target.name for edge in self.edges
        }
        if not edge_nodes.issubset(visited):
            unvisited: set[str] = edge_nodes - visited
            raise ValueError(
                f"Join graph is not connected from root '{root}'. "
                f"Unreachable relations: {unvisited}"
            )

        # generate semijoin cascade
        steps: list[SemijoinStep] = []

        def _post_order(current: str) -> None:
            """Bottom-up: reduce each parent by its child."""
            for edge in child_edges.get(current, []):
                child: str = _other_end(edge, current)
                _post_order(child)
                steps.append(
                    SemijoinStep(
                        reduce=current,
                        by=child,
                        ref_key=edge.ref_key,
                        source=edge.source.name,
                    )
                )

        def _pre_order(current: str) -> None:
            """Top-down: reduce each child by its parent."""
            for edge in child_edges.get(current, []):
                child: str = _other_end(edge, current)
                steps.append(
                    SemijoinStep(
                        reduce=child,
                        by=current,
                        ref_key=edge.ref_key,
                        source=edge.source.name,
                    )
                )
                _pre_order(child)

        _post_order(root)
        _pre_order(root)
        return steps


#: The built-in leaf predicates `_predicate_paths` can read: each has an `attr`.
_LEAF_PREDICATES: tuple[type, ...] = (Eq, Gt, Lt, Gte, Lte, Like, In, Exists, Missing)


def _predicate_paths(predicate: FilterPredicate) -> list[AttributePath] | None:
    """All attribute paths a built-in structured predicate reads, or None if
    the predicate cannot be inspected.

    Only the built-in predicate classes are inspected, by exact type, so their
    fields are known: `attr` of every leaf (`_LEAF_PREDICATES`), the `attr` of
    a `Ref` used as a comparison value, and the parts of `And` / `Or` / `Not`.
    Literal values (e.g. of `In`) are never read as paths. A lambda or a
    user-defined `Predicate` subclass anywhere in the tree (also inside `And` /
    `Or` / `Not`) makes the whole predicate uninspectable.

    @param predicate: A structured predicate or any other callable.
    @return: The paths in the order they occur, or None if not inspectable.
    """
    if type(predicate) in (And, Or):
        paths: list[AttributePath] = []
        for part in predicate.predicates:
            part_paths: list[AttributePath] | None = _predicate_paths(part)
            if part_paths is None:
                return None
            paths.extend(part_paths)
        return paths
    if type(predicate) is Not:
        return _predicate_paths(predicate.predicate)
    if type(predicate) not in _LEAF_PREDICATES:
        return None
    # a comparison may compare with another attribute of the same subject
    # through a `Ref`
    leaf_paths: list[AttributePath] = [predicate.attr]
    if isinstance(predicate, ComparisonPredicate) and isinstance(predicate.value, Ref):
        leaf_paths.append(predicate.value.attr)
    return leaf_paths


def _path_references(view: View, path: AttributePath, target_rf: RF) -> bool:
    """Whether `path` (of a grouping key or projection name of `view`) refers
    to the tuples of `target_rf`.

    The path must be `X` (X is `target_rf` itself) or `X.attr` (X
    carries a `ForeignValueConstraint` on `attr` pointing at `target_rf`),
    with X a base relation of the view's subgraph. Any other shape (more
    segments, X an inner view) does not count as a reference.

    @param view: The view the edge starts at.
    @param path: The path behind the edge's `ref_key`.
    @param target_rf: The RF of the edge's target relation.
    @return: True iff the path is a reference to `target_rf`.
    """
    segments: list[str] = _split_attr_path(path)
    # `.get`: the view was validated at construction, but its dicts may have
    # been changed since; a missing relation is then simply no reference
    inner: Node | None = view.subgraph.nodes.get(segments[0])
    if not isinstance(inner, JoinNode) or len(segments) > 2:
        return False
    if len(segments) == 1:
        return inner.rf.uuid == target_rf.uuid
    return any(
        isinstance(constraint, ForeignValueConstraint)
        and constraint.key == segments[1]
        and constraint.target_attribute_function.uuid == target_rf.uuid
        for constraint in inner.rf.__dict__["values_constraints"]
    )


def _other_end(edge: Edge, name: str) -> str:
    """The endpoint of `edge` that is not `name` (the edge walked undirected).

    @param edge: An edge with `name` as one of its endpoints.
    @param name: The relation the walk comes from.
    @return: The relation at the other end of `edge`.
    """
    return edge.target.name if edge.source.name == name else edge.source.name


@dataclass(frozen=True)
class SemijoinStep:
    """A single step in a Yannakakis semijoin cascade.

    `source` names the relation whose tuples hold `ref_key` (either `reduce`
    or `by`), taken from the graph's edge, so `semijoin` need not guess it.
    """

    reduce: str
    by: str
    ref_key: str
    source: str

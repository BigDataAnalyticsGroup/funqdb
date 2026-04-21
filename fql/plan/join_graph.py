from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field

from fdm.attribute_functions import RF, DBF
from fdm.schema import ForeignValueConstraint

# Type aliases — both are `str` at runtime, but the alias names make
# every signature in this file (and in operators that import these
# types) self-documenting about which kind of name it is.
#
# `RelationName` is a key in the DBF — the outer name of an RF.
# `AttributeName` is a key inside a TF — the `ref_key` of a
# `ForeignValueConstraint`, i.e. the attribute that carries a
# reference to another TF. Two `str`s collapsed into the same
# variable name used to hide that difference; these aliases surface
# it.
type RelationName = str
type AttributeName = str


@dataclass(frozen=True)
class Node:
    """A named node in the graph. `name` is a relation name."""

    name: RelationName


@dataclass(frozen=True)
class JoinNode(Node):
    """A node in the join graph, wrapping a named RF from the input DBF."""

    rf: RF


@dataclass(frozen=True)
class Edge:
    """A directed reference edge: source.rf[ref_key] references target.rf.

    Derived from a ForeignValueConstraint set via .references().
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
    """An entry in an undirected adjacency list: a neighbor connected via ref_key."""

    name: str
    ref_key: str


@dataclass
class JoinGraph:
    """The join graph of a DBF, extracted from ForeignValueConstraint metadata.

    Nodes are the named RFs in the DBF, edges are the directed references
    between them (source.rf[ref_key] -> target.rf).

    Project convention: **every read-only query about the graph's
    topology lives here, not in the operator that happens to need it.**
    That includes acyclicity checks, component enumeration, pure-source
    / isolated-node detection, adjacency maps, trivial-graph predicates
    and any similar structural query added for future join-rework MRs.
    Operators (`join`, `subdatabase`, follow-ups) only encode policy
    (error messages, which raise, which branch) on top of the graph's
    answers. Keep new graph algorithms on this class.
    """

    nodes: dict[str, JoinNode] = field(default_factory=dict)
    edges: list[Edge] = field(default_factory=list)

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

        @param dbf: The input DBF to extract the join graph from.
        @return: A JoinGraph with nodes and edges. Edges only include references
            where both endpoints are relations in the DBF.
        """
        graph = cls()

        # create one JoinNode per named RF in the DBF
        for item in dbf:
            graph.nodes[item.key] = JoinNode(name=item.key, rf=item.value)

        # build a lookup from RF uuid to relation name so we can resolve
        # the target_attribute_function stored in each ForeignValueConstraint
        # back to a named node in this graph
        uuid_to_name: dict[int, str] = {
            node.rf.uuid: name for name, node in graph.nodes.items()
        }

        # scan each RF's values_constraints for ForeignValueConstraint instances;
        # these are created by .references() and encode source_rf[ref_key] -> target_rf
        for name, node in graph.nodes.items():
            for constraint in node.rf.__dict__["values_constraints"]:
                if isinstance(constraint, ForeignValueConstraint):
                    target_uuid: int = constraint.target_attribute_function.uuid
                    # only include edges where the target is also in this DBF
                    if target_uuid in uuid_to_name:
                        target_name: str = uuid_to_name[target_uuid]
                        graph.edges.append(
                            Edge(
                                source=node,
                                target=graph.nodes[target_name],
                                ref_key=constraint.key,
                            )
                        )

        return graph

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
        edge). Used by `join._pick_walk_start` as the only legal walk
        origin in the minimal POC, and useful any time the calling
        code wants the start set for a topological sort on the
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
        directions; the two helpers are intentionally separate but
        share `Neighbor` as the value type.
        """
        adjacency: dict[RelationName, list[Neighbor]] = {}
        for edge in self.edges:
            adjacency.setdefault(edge.source.name, []).append(
                Neighbor(name=edge.target.name, ref_key=edge.ref_key)
            )
        return adjacency

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
            assert root in self.nodes, f"Root '{root}' is not a relation in the DBF."
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
        - Bottom-up (post-order): each tree edge once, reduce the target by its source.
        - Top-down (pre-order): each tree edge once, reduce the source by its target.

        The semijoin operator auto-detects the FDM reference direction via constraints,
        so only reduce/by/ref_key are needed.

        This method checks the cyclicity of the graph and throws a ValueError if it detects a cycle.

        @param root: The root node name for the join tree.
        @return: Ordered list of SemijoinStep to execute sequentially.
        """
        self.check_acyclicity()

        # build undirected adjacency list from edges
        adjacency: dict[str, list[Neighbor]] = {name: [] for name in self.nodes}
        for edge in self.edges:
            adjacency[edge.source.name].append(
                Neighbor(name=edge.target.name, ref_key=edge.ref_key)
            )
            adjacency[edge.target.name].append(
                Neighbor(name=edge.source.name, ref_key=edge.ref_key)
            )

        # BFS from the root to build a rooted spanning tree (children_map).
        # Each node discovered for the first time becomes a source of the node
        # that discovered it, turning the undirected adjacency list into a
        # directed target→children mapping suitable for the post-order (bottom-up)
        # and pre-order (top-down) traversals of the Yannakakis algorithm.
        children_map: dict[str, list[Neighbor]] = {}
        visited: set[str] = {root}
        queue: deque[str] = deque([root])

        while queue:
            node = queue.popleft()
            for nb in adjacency[node]:
                if nb.name in visited:
                    continue  # already discovered — skip to avoid cycles in the tree
                visited.add(nb.name)
                queue.append(nb.name)
                # record nb as a source of node in the rooted tree
                children_map.setdefault(node, []).append(nb)

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
            """Bottom-up: reduce target by source."""
            for nb in children_map.get(current, []):
                _post_order(nb.name)
                steps.append(
                    SemijoinStep(reduce=current, by=nb.name, ref_key=nb.ref_key)
                )

        def _pre_order(current: str) -> None:
            """Top-down: reduce source by target."""
            for nb in children_map.get(current, []):
                steps.append(
                    SemijoinStep(reduce=nb.name, by=current, ref_key=nb.ref_key)
                )
                _pre_order(nb.name)

        _post_order(root)
        _pre_order(root)
        return steps


@dataclass(frozen=True)
class SemijoinStep:
    """A single step in a Yannakakis semijoin cascade."""

    reduce: str
    by: str
    ref_key: str

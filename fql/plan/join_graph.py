from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from docutils.nodes import target

from fdm.attribute_functions import RF, DBF
from fdm.schema import ForeignValueConstraint


@dataclass(frozen=True)
class Node:
    """A named node in the graph."""

    name: str


@dataclass(frozen=True)
class JoinNode(Node):
    """A node in the join graph, wrapping a named RF from the input DBF."""

    rf: RF


@dataclass(frozen=True)
class Edge:
    """A directed reference edge: source.rf[ref_key] references target.rf.

    Derived from a ForeignValueConstraint set via .references().
    """

    source: Node
    target: Node
    ref_key: str


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
    """

    nodes: dict[str, JoinNode] = field(default_factory=dict)
    edges: list[Edge] = field(default_factory=list)

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

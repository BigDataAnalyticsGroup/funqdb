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

"""The aliases of a query and the equalities between them, as a graph and as the
tree with at most one join that the graph turns into."""

from __future__ import annotations

from dataclasses import dataclass

from benchmarks.datasets.schema import Schema
from benchmarks.tools.ir.expressions import (
    Column,
    ExistsExpression,
    Expression,
    InSubqueryExpression,
    SubqueryExpression,
    TupleIdentityExpression,
    conjunction,
)
from benchmarks.tools.ir.join_equalities import Edge, ForeignKeyEquality, ReferenceRead
from benchmarks.tools.ir.names import as_python_name, fresh_name
from benchmarks.tools.ir.paths import Path


@dataclass(frozen=True)
class AliasGraph:
    """The aliases of a query `depth` levels deep with their tables in declaration order,
    and the equalities between them in conjunct order; a provisional alias avoids `reserved`.
    """

    depth: int
    schema: Schema
    tables: dict[str, str]
    edges: tuple[Edge, ...]
    foreign_key_equalities: tuple[ForeignKeyEquality, ...]
    reserved: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        """Rejects an undeclared alias, an edge stated twice and a foreign key equated with itself."""
        aliases: set[str] = set()
        for edge in self.edges:
            aliases.add(edge.source)
            aliases.add(edge.target)
        for equality in self.foreign_key_equalities:
            aliases.add(equality.left.alias)
            aliases.add(equality.right.alias)

        undeclared = aliases - self.tables.keys()
        if undeclared:
            raise ValueError(f"Undeclared aliases: {undeclared}")
        if len(set(self.edges)) != len(self.edges):
            raise ValueError("An edge is stated twice.")
        if any(
            equality.left == equality.right for equality in self.foreign_key_equalities
        ):
            raise ValueError("A foreign key is equated with itself.")

    def tree(self) -> JoinTree:
        """The join tree of this graph."""
        builder = _TreeBuilder(self)
        return builder.build()


@dataclass(frozen=True)
class JoinTree:
    """The aliases of a query as a tree with at most one join; only `AliasGraph.tree` builds it."""

    schema: Schema
    identities: tuple[TupleIdentityExpression, ...]
    _graph: _Graph
    _paths: dict[str, dict[str, Path]]
    depth: int

    @property
    def aliases(self) -> tuple[str, ...]:
        """Every alias in declaration order, provisional ones last."""
        return tuple(self._graph.tables)

    @property
    def roots(self) -> tuple[str, ...]:
        """The aliases no edge leads to, in declaration order."""
        return self._graph.roots

    @property
    def is_joined(self) -> bool:
        """True when the tree joins two or more roots."""
        return len(self.roots) > 1

    @property
    def join_members(self) -> tuple[str, ...]:
        """The join members in declaration order: roots, the hubs joining them and aliases between; empty for one root."""
        if not self.is_joined:
            return ()

        graph = self._graph
        upstream = graph.upstream(graph.hubs)
        return tuple(
            alias for alias in graph.tables if alias in graph.roots or alias in upstream
        )

    def path_of(self, alias: str) -> Path | None:
        """The path to `alias` from the first root reaching it; `None` for an alias of another query."""
        paths = self._paths.get(alias)
        if paths is None:
            return None

        return _first_path(paths)

    def table_of(self, alias: str) -> str | None:
        """The table of `alias`; `None` for an alias of another query."""
        return self._graph.tables.get(alias)

    def join(self, condition: Expression | None) -> Join | None:
        """The join of the roots with `condition` split over it; `None` for one root."""
        if not self.is_joined:
            return None

        return self._split(condition)

    def _split(self, condition: Expression | None) -> Join:
        """The join with each conjunct of `condition` pushed into a root that answers it, else left on the join tuples.
        A conjunct several roots answer goes to the one answering most, then to the earlier.
        """
        condition_conjuncts = [] if condition is None else condition.conjuncts()
        conjunct_roots = [
            (conjunct, self._roots_answering(conjunct))
            for conjunct in condition_conjuncts
        ]

        # how many conjuncts each root answers
        answered_by_root = {
            root: sum(root in roots for _, roots in conjunct_roots)
            for root in self.roots
        }
        # roots ordered by how many conjuncts they answer, ties in declaration order
        roots_ordered = sorted(self.roots, key=lambda root: -answered_by_root[root])

        pushed: dict[str, list[Expression]] = {}
        remaining: list[Expression] = []
        for conjunct, roots in conjunct_roots:
            if not roots:
                remaining.append(conjunct)
                continue

            base = next(root for root in roots_ordered if root in roots)
            rebased_conjunct = self._rebase(conjunct, base)
            pushed.setdefault(base, []).append(rebased_conjunct)

        pushed_conditions = {
            root: conjunction(*conditions) for root, conditions in pushed.items()
        }
        remaining_condition = conjunction(*remaining) if remaining else None
        dropped_edges = self._dropped_edges(pushed_conditions)
        return Join(pushed_conditions, remaining_condition, dropped_edges)

    def _roots_answering(self, conjunct: Expression) -> frozenset[str]:
        """The roots reaching every alias `conjunct` reads; none if it holds a subquery, which may read any alias."""
        expressions = list(conjunct.nested_expressions())
        subqueries = (InSubqueryExpression, ExistsExpression, SubqueryExpression)
        if any(isinstance(node, subqueries) for node in expressions):
            return frozenset()

        roots = frozenset(self.roots)
        for path in conjunct.paths():
            if path.depth == self.depth:
                # keep the roots with a path to the alias the path reads
                roots &= self._paths[path.alias].keys()
        return roots

    def _rebase(self, conjunct: Expression, root: str) -> Expression:
        """`conjunct` with every path of this query starting at `root`; paths of an enclosing query stay."""

        def rebase(path: Path) -> Path:
            """`path` from `root` if it is a path of this query."""
            if path.depth != self.depth:
                return path

            return self._paths[path.alias][root]

        return conjunct.map_paths(rebase)

    def _dropped_edges(
        self, pushed_conditions: dict[str, Expression]
    ) -> tuple[Edge, ...]:
        """The schema references between join members that are no join edge, except self-references
        and references into a filtered root; none if a table has several members."""
        graph = self._graph
        join_members = self.join_members
        aliases = {graph.tables[member]: member for member in join_members}
        # FQL refuses a relation entered under several names, so there is no
        # join whose references could be dropped
        if len(aliases) != len(join_members):
            return ()

        join_edges = {
            (edge.source, edge.ref_key)
            for edge in graph.edges
            if edge.target in join_members
        }

        dropped: list[Edge] = []
        for member in join_members:
            table = graph.tables[member]
            for reference in self.schema.references_of(table):
                # the join follows it
                if (member, reference.ref_key) in join_edges:
                    continue
                # FQL never follows a self-reference
                if reference.target_table == table:
                    continue
                target = aliases.get(reference.target_table)
                # the target table is no member, so the reference is no edge
                if target is None:
                    continue
                # FQL `where` returns a new RF, so no reference leads to a pushed
                # root and drop_reference would find nothing to drop
                if target in pushed_conditions:
                    continue

                dropped.append(Edge(member, reference.ref_key, target))
        return tuple(dropped)


@dataclass(frozen=True)
class Join:
    """The join of a query's roots: the conjuncts pushed into a root, those left on the join tuples,
    and the schema references dropped so that the join does not follow them."""

    pushed_conditions: dict[str, Expression]
    remaining_condition: Expression | None
    dropped_edges: tuple[Edge, ...]


@dataclass(frozen=True)
class _Graph:
    """Aliases in declaration order and the edges between them."""

    tables: dict[str, str]
    edges: tuple[Edge, ...]

    @property
    def roots(self) -> tuple[str, ...]:
        """The aliases no edge leads to, in declaration order."""
        targets = {edge.target for edge in self.edges}
        return tuple(alias for alias in self.tables if alias not in targets)

    @property
    def hubs(self) -> list[str]:
        """The aliases with more than one incoming edge."""
        return [alias for alias in self.tables if len(self.incoming(alias)) > 1]

    def incoming(self, alias: str) -> list[Edge]:
        """The edges leading to `alias`."""
        return [edge for edge in self.edges if edge.target == alias]

    def outgoing(self, alias: str) -> list[Edge]:
        """The edges leaving `alias`."""
        return [edge for edge in self.edges if edge.source == alias]

    def upstream(self, aliases: list[str]) -> set[str]:
        """`aliases` and every alias with a chain of edges leading to one of them."""
        reached = set(aliases)
        pending = list(aliases)
        while pending:
            alias = pending.pop()
            for edge in self.incoming(alias):
                if edge.source not in reached:
                    reached.add(edge.source)
                    pending.append(edge.source)
        return reached

    def root_of(self, alias: str) -> str:
        """The first root in declaration order that reaches `alias`."""
        upstream = self.upstream([alias])
        return next(root for root in self.roots if root in upstream)

    def reaches(self, root: str, alias: str) -> bool:
        """True when a chain of edges leads from `root` to `alias`."""
        upstream = self.upstream([alias])
        return root in upstream

    def reach(self, alias: str) -> int:
        """The number of roots with a chain of edges to `alias`."""
        upstream = self.upstream([alias])
        return sum(root in upstream for root in self.roots)

    def hub_order(self) -> list[str]:
        """The hubs of an acyclic graph, those with the largest reach first, ties in topological order,
        each after its upstream hubs."""
        hub_aliases = self.hubs
        hubs = [alias for alias in self.topological_order() if alias in hub_aliases]
        # a stable sort, so a tie keeps topological order
        hubs_by_reach = sorted(hubs, key=self.reach, reverse=True)

        # a hub groups its edges by `root_of`, which names one root per source;
        # its upstream hubs must have joined the other roots of that source before
        order: list[str] = []
        for hub in hubs_by_reach:
            upstream = self.upstream([hub])
            order += [
                pending
                for pending in hubs
                if pending in upstream and pending not in order
            ]
        return order

    def topological_order(self) -> list[str]:
        """The aliases of an acyclic graph, each after every alias leading to it; ties in declaration order."""
        order: list[str] = []
        pending = list(self.tables)
        while pending:
            # the first pending alias, in declaration order, whose incoming edges all
            # start at placed aliases; as the graph is acyclic, one always exists
            alias = next(alias for alias in pending if self._follows(alias, order))
            order.append(alias)
            pending.remove(alias)
        return order

    def kept_edge_for(self, edge: Edge, kept_edges: list[Edge]) -> Edge:
        """The kept edge a removed `edge` is equated with: the first its root reaches, else the first."""
        root = self.root_of(edge.source)
        reached = [kept for kept in kept_edges if self.reaches(root, kept.source)]
        return reached[0] if reached else kept_edges[0]

    def _follows(self, alias: str, order: list[str]) -> bool:
        """True when every edge into `alias` starts at an alias in `order`."""
        incoming = self.incoming(alias)
        return all(edge.source in order for edge in incoming)


@dataclass(frozen=True)
class _EdgeIdentity:
    """A removed `edge` and its `partner`, the kept edge that leads to the same tuple;
    `None` when `edge` closes a cycle and so leads back to the tuple of its target."""

    edge: Edge
    partner: Edge | None


class _TreeBuilder:
    """The graph of one query turning into a tree: its edges in stated order minus `_removed`,
    and the identities the removals produced."""

    def __init__(self, graph: AliasGraph) -> None:
        """Starts from the stated edges of `graph`, with nothing removed yet."""
        self._graph = graph
        self._tables = dict(graph.tables)
        self._provisional: set[str] = set()
        self._edges = list(graph.edges)
        self._removed: set[Edge] = set()
        self._edge_identities: list[_EdgeIdentity] = []
        # per root, a root of the same component; set by `_resolve_hubs`
        self._components: dict[str, str] = {}
        # set by `_assign_paths`
        self._paths: dict[str, dict[str, Path]] = {}

    def build(self) -> JoinTree:
        """The join tree of the graph."""
        self._attach_foreign_key_equalities()
        self._break_cycles()
        self._resolve_hubs()
        self._assign_paths()
        identities = self._identities()
        schema = self._graph.schema
        depth = self._graph.depth
        return JoinTree(schema, identities, self._kept, self._paths, depth)

    @property
    def _kept(self) -> _Graph:
        """The graph of the edges not removed so far."""
        edges = tuple(edge for edge in self._edges if edge not in self._removed)
        return _Graph(self._tables, edges)

    def _attach_foreign_key_equalities(self) -> None:
        """Adds each foreign-key equality as an edge into the first alias one of its references leads to,
        else as two edges into a provisional alias."""
        for equality in self._graph.foreign_key_equalities:
            left_targets = self._targets(equality.left)
            right_targets = self._targets(equality.right)
            if left_targets & right_targets:
                # restated: both references already lead to one alias
                continue

            held = [
                alias for alias in self._tables if alias in left_targets | right_targets
            ]
            if held:
                target = held[0]
                read = equality.right if target in left_targets else equality.left
                self._edges.append(Edge(read.alias, read.ref_key, target))
                continue

            taken = self._tables.keys() | self._graph.reserved
            # the renderer may write an alias as a Python name
            base = as_python_name(equality.target_table)
            target = fresh_name(base, taken)
            self._tables[target] = equality.target_table
            self._provisional.add(target)
            left = equality.left
            right = equality.right
            self._edges.append(Edge(left.alias, left.ref_key, target))
            self._edges.append(Edge(right.alias, right.ref_key, target))

    def _break_cycles(self) -> None:
        """Removes each edge closing a cycle, walking from the roots,
        then from each unreached alias in declaration order."""
        graph = self._kept
        starts = [*graph.roots, *graph.tables]
        visited: set[str] = set()

        def visit(alias: str, walked: list[str]) -> None:
            """Walks the edges out of `alias`; `walked` holds the aliases on the way to it."""
            visited.add(alias)
            walked.append(alias)
            for edge in graph.outgoing(alias):
                if edge.target in walked:
                    self._remove(edge, None)
                elif edge.target not in visited:
                    visit(edge.target, walked)
            walked.pop()

        for start in starts:
            if start not in visited:
                visit(start, [])

    def _resolve_hubs(self) -> None:
        """Leaves each alias one incoming edge per component, hubs in `_Graph.hub_order`;
        a hub reached from several components joins them."""
        acyclic = self._kept
        self._components = {root: root for root in acyclic.roots}
        for hub in acyclic.hub_order():
            self._resolve_hub(hub)

    def _resolve_hub(self, hub: str) -> None:
        """Removes all but one incoming edge of `hub` per component and merges those components;
        dissolves a provisional `hub` joining nothing."""
        graph = self._kept
        incoming = graph.incoming(hub)
        components: dict[str, list[Edge]] = {}
        for edge in incoming:
            root = graph.root_of(edge.source)
            component = self._component_of(root)
            components.setdefault(component, []).append(edge)

        if hub in self._provisional and len(components) < 2:
            self._dissolve(hub, incoming)
            return

        # each component keeps its first edge in stated order
        component_kept_edges = {edges[0] for edges in components.values()}
        kept_edges = [edge for edge in incoming if edge in component_kept_edges]
        for edge in incoming:
            if edge not in component_kept_edges:
                partner = graph.kept_edge_for(edge, kept_edges)
                self._remove(edge, partner)
        self._merge_components(list(components))

    def _dissolve(self, hub: str, incoming: list[Edge]) -> None:
        """Removes a provisional alias that joins nothing; each later edge becomes an identity with the first."""
        first, *rest = incoming
        # a new dict: graphs read before keep their aliases
        self._tables = {
            alias: table for alias, table in self._tables.items() if alias != hub
        }
        self._removed.add(first)
        for edge in rest:
            self._remove(edge, first)

    def _assign_paths(self) -> None:
        """Records every alias's path from each root reaching it, roots in declaration order."""
        graph = self._kept
        self._paths = {alias: {} for alias in graph.tables}

        def walk(alias: str, root: str, ref_keys: tuple[str, ...]) -> None:
            """Records the path from `root` along `ref_keys` to `alias`, then walks on."""
            path = Path(self._graph.depth, alias, root, ref_keys)
            self._paths[alias][root] = path
            for edge in graph.outgoing(alias):
                walk(edge.target, root, (*ref_keys, edge.ref_key))

        for root in graph.roots:
            walk(root, root, ())

    def _identities(self) -> tuple[TupleIdentityExpression, ...]:
        """One identity per removed edge in stated order, except trivial or repeated ones."""
        edge_identities = sorted(
            self._edge_identities,
            key=lambda edge_identity: self._edges.index(edge_identity.edge),
        )
        identities: list[TupleIdentityExpression] = []
        stated: set[frozenset[_ReachedTuple]] = set()
        for edge_identity in edge_identities:
            tuple_identity = self._identity(edge_identity)
            left_tuple = _reached_tuple(tuple_identity.left)
            right_tuple = _reached_tuple(tuple_identity.right)
            tuples = frozenset({left_tuple, right_tuple})
            if len(tuples) < 2 or tuples in stated:
                continue

            stated.add(tuples)
            identities.append(tuple_identity)
        return tuple(identities)

    def _identity(self, edge_identity: _EdgeIdentity) -> TupleIdentityExpression:
        """`edge_identity` as reads along paths from the root of its removed edge."""
        edge = edge_identity.edge
        root = self._kept.root_of(edge.source)
        left_path = self._path(edge.source, root)
        left = Column(left_path, edge.ref_key, True)
        partner = edge_identity.partner
        # a cycle's closing edge leads back to a tuple no reference read names
        if partner is None:
            tuple_path = self._path(edge.target, root)
            tuple_read = Column.tuple_at(tuple_path)
            return TupleIdentityExpression(left, tuple_read)

        right_path = self._path(partner.source, root)
        right = Column(right_path, partner.ref_key, True)
        return TupleIdentityExpression(left, right)

    def _remove(self, edge: Edge, partner: Edge | None) -> None:
        """Removes `edge` and records its identity with `partner`."""
        self._removed.add(edge)
        edge_identity = _EdgeIdentity(edge, partner)
        self._edge_identities.append(edge_identity)

    def _path(self, alias: str, root: str) -> Path:
        """The path to `alias` from `root`, else from the first root reaching it."""
        paths = self._paths[alias]
        if root in paths:
            return paths[root]

        return _first_path(paths)

    def _component_of(self, root: str) -> str:
        """The root that stands for the component of `root`."""
        while self._components[root] != root:
            root = self._components[root]
        return root

    def _merge_components(self, components: list[str]) -> None:
        """Merges `components` into the first of them."""
        first, *rest = components
        for component in rest:
            self._components[component] = first

    def _targets(self, read: ReferenceRead) -> set[str]:
        """The aliases that the reference `read` leads to."""
        return {
            edge.target
            for edge in self._edges
            if edge.source == read.alias and edge.ref_key == read.ref_key
        }


@dataclass(frozen=True)
class _ReachedTuple:
    """The tuple that `ref_keys` lead to from `root`, in the query `depth` levels deep."""

    depth: int
    root: str
    ref_keys: tuple[str, ...]


def _reached_tuple(operand: Column) -> _ReachedTuple:
    """The tuple `operand` reads."""
    path = operand.path
    # a column without a name is the tuple itself, as a reference leading to it
    ref_keys = (*path.ref_keys, operand.name) if operand.name else path.ref_keys
    return _ReachedTuple(path.depth, path.root, ref_keys)


def _first_path(paths: dict[str, Path]) -> Path:
    """The path from the first root in declaration order."""
    return next(iter(paths.values()))

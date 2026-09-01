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

"""FQL join operator — reference-based flattening join (feature 003).

Consumes a constraint-decorated DBF (assembled via `add_reference` or
eager `RF.references()`) and materializes the surviving tuple
combinations as an RF indexed by row. Each row is a nested TF of the
form::

    TF({relation_name_1: relation_tf_1,
        relation_name_2: relation_tf_2,
        ...})

Relations enter each row **by reference**, not by copy. Two rows whose
reference chains lead to the same target tuple share that tuple by
object identity — no SQL-style denormalization.

Scope: reference-based joins on any connected reference graph that is an
**undirected tree** in any edge orientation (0..n pure sources). The walk
is bidirectional — a reference is followed forward via the inline pointer
(`source_tf[ref_key] is target_tf`) or backward, from a referenced hub
tuple to the source tuples that reference it. `JoinPredicate`s on the DBF,
diamonds / non-tree graphs, disconnected graphs, and cyclic graphs raise
`NotImplementedError` with a pointer at the follow-up MR.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterator
from typing import Any

from fdm.attribute_functions import TF, RF, DBF
from fdm.schema import ForeignValueConstraint, JoinPredicate
from fql.operators.APIs import Operator, OperatorInput
from fql.operators.subdatabases import subdatabase
from fql.plan.join_graph import JoinGraph, Neighbor

# Follow-up MR pointer — shared between the two NotImplementedError sites so
# that users who hit either know where to look.
_FOLLOWUP_HINT: str = (
    "This POC supports reference-based joins on connected undirected trees "
    "in any orientation (multi-source included). JoinPredicate pushdown, "
    "diamonds / non-tree acyclic graphs, disconnected graphs (Cartesian "
    "across components), and cyclic graphs are the scope of a follow-up MR."
)


class join[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Materialize a constraint-decorated DBF as an RF of tuple combinations.

    Runs `subdatabase` (Yannakakis reduction) on the input first, then walks
    the reduced reference tree **bidirectionally** to enumerate the full
    (natural) join: a reference is followed forward via the inline pointer
    (`source_tf[ref_key] is target_tf`) or backward, from a referenced hub
    tuple to the source tuples that reference it. Emits one row per surviving
    tuple combination; each row is a TF whose top-level keys are the relation
    names and whose values are the original relation TFs shared by object
    identity across rows.

    Scope — the in-scope graph is any connected reference graph that is an
    **undirected tree** in any edge orientation (0..n pure sources). The
    following are out of scope and raise `NotImplementedError` (honest errors,
    not silent mis-behaviour):

    * diamonds / non-tree acyclic graphs (deferred to a follow-up MR),
    * disconnected reference graphs (Cartesian across components),
    * cyclic reference graphs,
    * a `JoinPredicate` on the input DBF (predicate pushdown deferred).
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        *,
        root: str | None = None,
    ):
        """Initialize the join operator.

        @param input_function: A DBF (or an Operator producing one).
        @param root: Optional explicit relation to start the walk from. It
            must be a pure source (a known limitation — under the bidirectional
            walk a hub would work too). If None, the start is picked
            deterministically as the first pure source, `sorted(pure_sources)[0]`.
        """
        self.input_function = input_function
        self.root = root

    def _compute(self) -> RF:
        dbf: Any = self._resolve_input(self.input_function)
        if not isinstance(dbf, DBF):
            raise TypeError(f"join expects a DBF input, got {type(dbf).__name__}")

        # Refuse early if the DBF carries any JoinPredicate — predicate
        # pushdown is scoped to a follow-up MR; silently ignoring would
        # mask correctness bugs.
        if any(
            isinstance(c, JoinPredicate) for c in dbf.__dict__["values_constraints"]
        ):
            raise NotImplementedError(
                f"join: input DBF carries at least one JoinPredicate. "
                f"{_FOLLOWUP_HINT}"
            )

        # Extract the reference graph from the **original** DBF. Rationale:
        # `subdatabase`/`semijoin` clone RFs, so the reduced RFs get fresh
        # UUIDs while the FVCs on untouched RFs still point at the
        # originals. `JoinGraph.from_dbf(reduced)` would therefore drop
        # edges because its UUID-based lookup mismatches. The original
        # DBF has the structurally-correct graph.
        #
        # Load-bearing invariant for this to be sound: `semijoin` must
        # preserve contained TFs by **object identity** (shallow copy of
        # the data dict) so that `tf[ref_key]` on a tuple from the
        # reduced DBF resolves to the same TF instance it did before
        # reduction — that is exactly what the current semijoin
        # implementation does, and it lets us walk reduced tuples
        # against a graph extracted from the original DBF. If a future
        # semijoin ever deep-copies TFs, this operator breaks silently
        # and must be revisited (along with `constraints.py`'s clone
        # helper, which relies on the same invariant).
        graph: JoinGraph = JoinGraph.from_dbf(dbf)

        # All graph-level validation happens **before** the Yannakakis
        # reduction runs — subdatabase itself raises ValueError (via
        # `JoinGraph.build_semijoin_cascade`) on disconnected graphs,
        # so we want our own NotImplementedError path to fire first and
        # carry the clearer follow-up hint.

        # Single-RF / zero-edge fallback: pass each tuple through as a
        # one-entry row. Multi-RF without references would be a Cartesian
        # product; scoped out for this MR. Both queries are routed
        # through `JoinGraph` so the topology checks stay on the graph
        # class (see JoinGraph class docstring).
        if graph.is_trivial():
            sole: str | None = graph.sole_relation_name()
            if sole is not None:
                reduced_single: DBF = subdatabase[DBF, DBF](dbf).result
                return self._wrap_single_relation(reduced_single, sole)
            raise NotImplementedError(
                f"join: input DBF has {len(graph.nodes)} relations but no "
                f"references between them. {_FOLLOWUP_HINT}"
            )

        # Pick the relation to start the walk from. Enforces the in-scope
        # shape (no isolated relations, single connected component, undirected
        # tree) and returns sorted(pure_sources)[0]. Respects self.root if set;
        # see _pick_walk_start.
        start: str = self._pick_walk_start(graph)

        # Yannakakis reduction (reference-based) — reuses existing
        # subdatabase operator. The reduced DBF has the same relation
        # names but only tuples that participate in the full join.
        # Runs AFTER the graph-level validation above so our explicit
        # errors take precedence over subdatabase's internal ones.
        reduced: DBF = subdatabase[DBF, DBF](dbf).result

        # Forward adjacency (source -> target, followed via the inline FK
        # pointer) and backward adjacency (target/hub -> its referencing
        # sources, followed via an object-identity scan of the reduced
        # source relation), both delegated to `JoinGraph` so the graph class
        # owns the construction. The bidirectional walk needs both because a
        # multi-source tree has edges that must be entered against their
        # arrow from any start relation.
        forward_adj: dict[str, list[Neighbor]] = graph.outgoing_adjacency()
        backward_adj: dict[str, list[Neighbor]] = graph.incoming_adjacency()

        # Row materialization. Iterate every surviving tuple of the start
        # relation (already Yannakakis-reduced, so every such tuple extends
        # to at least one full-join row) and enumerate all combinations
        # reachable from it. A single start tuple now yields *several* rows
        # whenever a hub is referenced by more than one source tuple (fan-in),
        # so one monotonic counter across the whole enumeration assigns the
        # sequential integer output keys. Relations enter each row by
        # reference (identity preserved across reduction), so rows sharing a
        # hub share the same instance — the zero-redundancy contract.
        result: RF = RF(frozen=False)
        counter: int = 0
        for item in reduced[start]:
            for combination in _combinations(
                node_name=start,
                node_tf=item.value,
                came_from=None,
                forward_adj=forward_adj,
                backward_adj=backward_adj,
                reduced=reduced,
            ):
                result[counter] = _wrap_combination(combination)
                counter += 1
        result.freeze()
        return result

    def _pick_walk_start(self, graph: JoinGraph) -> str:
        """Validate the graph shape and return the relation to start the walk from.

        In-scope graphs are connected **undirected trees** in any edge
        orientation (0..n pure sources). The bidirectional walk reaches every
        node from any start, so the start need only be deterministic; we use
        the first pure source, `sorted(pure_sources)[0]`.

        The three out-of-scope shapes are rejected in this order, each with its
        own message, and all **before** the start is picked (so the tree gate
        also guarantees `pure_sources` is non-empty — a cyclic graph, which has
        none, is rejected first):

        1. isolated relations (no edges at all),
        2. disconnected reference graph (more than one component),
        3. non-tree graph (diamond, cycle, parallel or self reference).

        Topology queries live on `JoinGraph` (`isolated_nodes`,
        `connected_components`, `is_tree`, `pure_sources`); this method encodes
        only the policy and the explicit-`root` handling. An explicit `root`
        still has to be a pure source (a known limitation — a hub would be a
        valid root too under bidirectional walking).
        """
        isolated: set[str] = graph.isolated_nodes()
        if isolated:
            raise NotImplementedError(
                f"join: DBF has isolated relations with no references "
                f"at all: {sorted(isolated)}. A Cartesian fallback is "
                f"out of scope for this POC. {_FOLLOWUP_HINT}"
            )

        # Disconnected reference graph — e.g. R→S plus T→U with no link
        # between them. Semantically the join is the Cartesian product of the
        # component-wise joins; its own message so the cause is not
        # misattributed to a non-tree shape (a single-component property).
        components: list[set[str]] = graph.connected_components()
        if len(components) > 1:
            raise NotImplementedError(
                f"join: DBF reference graph has {len(components)} "
                f"disconnected components: "
                f"{[sorted(c) for c in components]}. A Cartesian "
                f"product across components is out of scope for this "
                f"POC. {_FOLLOWUP_HINT}"
            )

        # Non-tree: connected but with more than n-1 edges — a diamond, a
        # cycle, or a parallel/self reference. Enumerating these needs a
        # residual-edge consistency check that is deferred; caught here rather
        # than mid-walk so the bidirectional generator can assume a tree.
        if not graph.is_tree():
            raise NotImplementedError(
                f"join: reference graph is not a tree (diamond, cycle, "
                f"parallel reference, or self-reference). {_FOLLOWUP_HINT}"
            )

        pure_sources: set[str] = graph.pure_sources()
        if self.root is not None:
            if self.root not in graph.nodes:
                raise ValueError(
                    f"join: root '{self.root}' is not a relation in the "
                    f"DBF. Available: {sorted(graph.nodes)}"
                )
            if self.root not in pure_sources:
                raise ValueError(
                    f"join: root '{self.root}' has incoming references and "
                    f"is therefore not a pure source. Pure sources in this "
                    f"DBF: {sorted(pure_sources)}"
                )
            return self.root

        return sorted(pure_sources)[0]

    @staticmethod
    def _wrap_single_relation(reduced: DBF, relation_name: str) -> RF:
        """Zero-edge fallback: wrap each tuple under the sole relation name."""
        result: RF = RF(frozen=False)
        counter: int = 0
        for item in reduced[relation_name]:
            result[counter] = _wrap_combination({relation_name: item.value})
            counter += 1
        result.freeze()
        return result


def _combinations(
    *,
    node_name: str,
    node_tf: TF,
    came_from: str | None,
    forward_adj: dict[str, list[Neighbor]],
    backward_adj: dict[str, list[Neighbor]],
    reduced: DBF,
) -> Iterator[dict[str, TF]]:
    """Yield every full-join combination reachable from ``(node_name, node_tf)``.

    Functional generator over a reference **tree** (the caller has already
    rejected non-trees via `is_tree`). Each yielded value is a fresh
    ``{relation_name: tf}`` dict for one full-join row — nothing is mutated in
    place, so sibling branches cannot corrupt each other.

    The graph is walked in **both** directions:

    * **Forward** (this node is the edge source, from `forward_adj`): follow
      the inline foreign-value pointer `node_tf[ref_key]` to the single
      referenced tuple. Exactly one target — a factor of size 1.
    * **Backward** (this node is the referenced hub, from `backward_adj`):
      every reduced source tuple whose reference *is* this hub tuple. Object
      identity (`item.value[ref_key] is node_tf`) is the match — semijoin
      preserves the contained TF instance across reduction (see the invariant
      documented in `join._compute`), so it is exactly the right test. This is
      a one-to-many fan-in — the factor that can exceed size 1.

    ``came_from`` is the relation name of the edge already consumed and is
    skipped; on a tree that single guard prevents walking back to the parent,
    and no stronger visited-set is needed.

    Each incident edge (except `came_from`) is first **recursed** into a list
    of completed sub-dicts, then the Cartesian **product across the neighbours'
    result lists** is taken and merged with ``{node_name: node_tf}``. Taking
    the product over fully-expanded results (not over raw neighbour tuples) is
    what makes a backward fan-in and any onward forward chain multiply
    correctly. A leaf (no incident edge other than the parent) yields the
    single ``{node_name: node_tf}``.

    @param node_name: Relation name of the current node.
    @param node_tf: Concrete tuple at this node in the current walk.
    @param came_from: Relation name of the parent (already-consumed) edge, or
        None at the walk root.
    @param forward_adj: `JoinGraph.outgoing_adjacency()` — source -> targets.
    @param backward_adj: `JoinGraph.incoming_adjacency()` — target -> sources.
    @param reduced: The Yannakakis-reduced DBF, scanned for backward matches.
    @return: An iterator of ``{relation_name: tf}`` row dicts.
    """
    # One entry per incident edge (except the parent), each a list of the
    # completed sub-dicts obtainable by descending through that edge.
    per_neighbor: list[list[dict[str, TF]]] = []

    for neighbor in forward_adj.get(node_name, []):
        if neighbor.name == came_from:
            continue
        # forward: exactly one referenced tuple via the inline pointer
        per_neighbor.append(
            list(
                _combinations(
                    node_name=neighbor.name,
                    node_tf=node_tf[neighbor.ref_key],
                    came_from=node_name,
                    forward_adj=forward_adj,
                    backward_adj=backward_adj,
                    reduced=reduced,
                )
            )
        )

    for neighbor in backward_adj.get(node_name, []):
        if neighbor.name == came_from:
            continue
        # backward: every reduced source tuple pointing at this hub tuple
        sub: list[dict[str, TF]] = []
        for item in reduced[neighbor.name]:
            if item.value[neighbor.ref_key] is node_tf:
                sub.extend(
                    _combinations(
                        node_name=neighbor.name,
                        node_tf=item.value,
                        came_from=node_name,
                        forward_adj=forward_adj,
                        backward_adj=backward_adj,
                        reduced=reduced,
                    )
                )
        per_neighbor.append(sub)

    if not per_neighbor:
        # leaf: only this node contributes to the row
        yield {node_name: node_tf}
        return

    for combo in itertools.product(*per_neighbor):
        row: dict[str, TF] = {node_name: node_tf}
        for sub_dict in combo:
            row.update(sub_dict)
        yield row


def _wrap_combination(accumulator: dict[str, TF]) -> TF:
    """Build the frozen per-row TF from an accumulator dict.

    Values are assigned as references, not copies — two rows sharing a
    referenced target tuple share it by object identity.
    """
    row: TF = TF(frozen=False)
    for relation_name, relation_tf in accumulator.items():
        row[relation_name] = relation_tf
    row.freeze()
    return row

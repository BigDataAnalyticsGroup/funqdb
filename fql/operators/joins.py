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

"""FQL join operator — minimal POC (MR 2 of the join-rework).

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

Minimal scope: only reference-based joins on acyclic reference graphs
with exactly one **pure source** (a relation with outgoing references
but no incoming ones). `JoinPredicate`s on the DBF and multi-source /
non-tree shapes raise `NotImplementedError` with a pointer at the
follow-up MR.
"""

from __future__ import annotations

from typing import Any

from fdm.attribute_functions import TF, RF, DBF
from fdm.schema import ForeignValueConstraint, JoinPredicate
from fql.operators.APIs import Operator, OperatorInput
from fql.operators.subdatabases import subdatabase
from fql.plan.join_graph import JoinGraph, Neighbor

# Follow-up MR pointer — shared between the two NotImplementedError sites so
# that users who hit either know where to look.
_FOLLOWUP_HINT: str = (
    "Minimal POC only supports reference-based joins on acyclic graphs "
    "with exactly one pure source. JoinPredicate pushdown, multi-source "
    "graphs (e.g. JOB's `ci->t`, `mc->t`), and non-tree acyclic graphs "
    "are the scope of the next MR."
)


class join[INPUT_AttributeFunction, OUTPUT_AttributeFunction](
    Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]
):
    """Materialize a constraint-decorated DBF as an RF of tuple combinations.

    Runs `subdatabase` (Yannakakis reduction) on the input first, then
    walks the reduced reference tree from the DBF's unique pure-source
    relation following outgoing `ForeignValueConstraint` edges. Emits
    one row per surviving tuple combination; each row is a TF whose
    top-level keys are the relation names and whose values are the
    original relation TFs shared by object identity across rows.

    Scope — explicit limitations of this minimal POC:

    * Requires exactly **one** pure-source relation (no incoming
      references). Multi-source graphs raise `NotImplementedError`.
    * `JoinPredicate` on the input DBF raises `NotImplementedError` —
      predicate pushdown is deferred to a follow-up MR.

    Both limitations are honest errors, not silent mis-behaviour.
    """

    def __init__(
        self,
        input_function: OperatorInput[INPUT_AttributeFunction],
        *,
        root: str | None = None,
    ):
        """Initialize the join operator.

        @param input_function: A DBF (or an Operator producing one).
        @param root: Optional explicit pure-source relation to start the
            walk from. If None, auto-picked as the unique relation with
            no incoming references.
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

        # Pick the pure source to start the walk from. Enforces the
        # minimal-POC invariants: no isolated relations, graph is
        # connected (single component), and exactly one pure source.
        # Respects self.root if set; see _pick_walk_start.
        start: str = self._pick_walk_start(graph)

        # Yannakakis reduction (reference-based) — reuses existing
        # subdatabase operator. The reduced DBF has the same relation
        # names but only tuples that participate in the full join.
        # Runs AFTER the graph-level validation above so our explicit
        # errors take precedence over subdatabase's internal ones.
        reduced: DBF = subdatabase[DBF, DBF](dbf).result

        # Outgoing-edge adjacency, delegated to `JoinGraph` so the
        # graph class owns the construction (see JoinGraph class
        # docstring). Used by `_build_combination` to follow references
        # forward via FDM object identity
        # (`source_tf[neighbor.ref_key] is neighbor_tf`, where
        # `neighbor.name` is the target relation).
        #
        # Shape of the returned map for users -> departments:
        #     {"users": [Neighbor(name="departments", ref_key="dept")]}
        # For the orders star (orders -> customers, orders -> products):
        #     {"orders": [Neighbor(name="customers", ref_key="customer"),
        #                 Neighbor(name="products",  ref_key="product")]}
        adjacency: dict[str, list[Neighbor]] = graph.outgoing_adjacency()

        # Row materialization. Iterate every surviving tuple of the
        # start relation (already Yannakakis-reduced, so every such
        # tuple is guaranteed to participate in the full join), then
        # DFS-walk from there to build one `{relation_name: tf}`
        # accumulator per start tuple. Each accumulator becomes exactly
        # one row in the output RF:
        #
        #   for u1 (Alice, dept=d1):   accumulator = {users: u1, departments: d1}
        #   for u2 (Bob,   dept=d2):   accumulator = {users: u2, departments: d2}
        #   for u3 (Carol, dept=d1):   accumulator = {users: u3, departments: d1}
        #
        # `d1` / `d2` are the **original** department TFs (identity
        # preserved across the reduction) — so row u1 and row u3 share
        # the same `d1` instance, the property the zero-redundancy
        # contract rests on.
        #
        # Output keys are sequential integers starting at 0 — no
        # tuple-shaped composite keys, which would complicate the
        # type of the RF for no benefit at this stage.
        result: RF = RF(frozen=False)
        counter: int = 0
        for item in reduced[start]:
            combination: dict[str, TF] = {}
            _build_combination(
                node_name=start,
                node_tf=item.value,
                adjacency=adjacency,
                accumulator=combination,
            )
            result[counter] = _wrap_combination(combination)
            counter += 1
        result.freeze()
        return result

    def _pick_walk_start(self, graph: JoinGraph) -> str:
        """Return the unique pure-source relation, or raise a helpful error.

        A **pure source** is a relation with at least one outgoing
        `ForeignValueConstraint` and zero incoming references. Starting
        the walk there lets us follow every tree edge forward via FDM
        object identity, O(1) per step, without a reverse index. An
        isolated relation (no edges at all) is *not* a pure source —
        it is reported separately so the error message stays readable.

        The graph-level definitions live on `JoinGraph`
        (`pure_sources` / `isolated_nodes`); this method encodes only
        the policy (exactly one pure source, no isolated relations)
        and the explicit-root short-circuit.
        """
        pure_sources: set[str] = graph.pure_sources()
        isolated: set[str] = graph.isolated_nodes()

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

        if isolated:
            raise NotImplementedError(
                f"join: DBF has isolated relations with no references "
                f"at all: {sorted(isolated)}. A Cartesian fallback is "
                f"out of scope for the minimal POC. {_FOLLOWUP_HINT}"
            )
        # Disconnected reference graph — e.g. R→S plus T→U with no
        # link between them. Semantically the join is the Cartesian
        # product of the component-wise joins; flagged with its own
        # error so the message doesn't misattribute the cause to a
        # multi-source shape (which is a single-component property).
        components: list[set[str]] = graph.connected_components()
        if len(components) > 1:
            raise NotImplementedError(
                f"join: DBF reference graph has {len(components)} "
                f"disconnected components: "
                f"{[sorted(c) for c in components]}. A Cartesian "
                f"product across components is out of scope for the "
                f"minimal POC. {_FOLLOWUP_HINT}"
            )
        if len(pure_sources) != 1:
            raise NotImplementedError(
                f"join: expected exactly one pure-source relation, got "
                f"{sorted(pure_sources)}. {_FOLLOWUP_HINT}"
            )
        return next(iter(pure_sources))

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


def _build_combination(
    *,
    node_name: str,
    node_tf: TF,
    adjacency: dict[str, list[Neighbor]],
    accumulator: dict[str, TF],
) -> None:
    """DFS walk following outgoing FVC edges via FDM object identity.

    Assumes the reference graph is a **tree** rooted at the pure
    source. Because every edge is a `ForeignValueConstraint`, each
    source tuple reaches exactly one target tuple per outgoing edge
    (`source_tf[ref_key] is target_tf`), so the walk never needs a
    Cartesian product: for one start tuple there is exactly one
    combined row. This minimises the algorithm to a single accumulator
    passed through the whole traversal and mutated in place.

    Diamond detection: if a relation name is reached via two distinct
    walk paths, the accumulator would be overwritten — we raise
    `NotImplementedError` with the follow-up-MR hint instead of
    silently dropping one path's target. Diamonds / reverse edges
    would need this to become a generator yielding multiple
    combinations; deferred to the follow-up MR.

    @param node_name: Relation name of the current node.
    @param node_tf: Concrete tuple at this node in the current walk.
    @param adjacency: Outgoing edges per source relation name; each
        value is a list of `Neighbor(name=target_relation,
        ref_key=attribute_on_source)` — `name` is emphatically the
        relation name, `ref_key` the attribute on the source TF.
    @param accumulator: Mutated in place; afterwards contains every
        `relation_name -> tf` entry reachable from the starting tuple.
    """
    if node_name in accumulator:
        raise NotImplementedError(
            f"join: relation '{node_name}' is reached via more than one "
            f"walk path (diamond / non-tree reference graph). "
            f"{_FOLLOWUP_HINT}"
        )
    accumulator[node_name] = node_tf
    for neighbor in adjacency.get(node_name, []):
        _build_combination(
            node_name=neighbor.name,
            node_tf=node_tf[neighbor.ref_key],
            adjacency=adjacency,
            accumulator=accumulator,
        )


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

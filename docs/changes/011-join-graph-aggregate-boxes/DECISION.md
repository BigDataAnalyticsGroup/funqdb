# 011 — Aggregate and projection boxes in the JoinGraph
**Date:** 2026-10
**MR:** https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/merge_requests/92
---
## Context

The `JoinGraph` can only express relations (nodes) connected by references (edges). It cannot express aggregation,
DISTINCT projection or nesting, so it is no complete logical representation of a query. The idea: draw a box around
a subgraph, annotate it with grouping keys and aggregate functions; the box's result is again a node of the graph,
which can be joined over, boxed again, and so on (the idea behind the Query Graph Model of Starburst/DB2). User
decisions: the graph is a logical query representation (not only a drawing, not executable yet); it is built by hand
for now, deriving it from FQL pipelines comes later; edges are reference edges only; edges crossing a box boundary
(correlated subqueries) are rejected for now; a projection is DISTINCT, i.e. grouping without aggregate functions.
The plan was checked by the plan challenger in two rounds.

## Decision

- **A box is a node.** `AggregateBox(Node)` holds a nested `JoinGraph` (the subgraph it aggregates over), named
  grouping keys (`output name -> attribute path`) and named aggregates (`output name -> AggregationFunction`). Its
  output attributes are the grouping-key names plus the aggregate names. Because a box is a `Node`, the outer graph
  joins over it like over a relation, and a subgraph may itself contain boxes (nesting).
- **Meaning** (for a later executor): `group_by_aggregate(join(subgraph), *paths, **aggregates)`. Paths address the
  joined rows, whose top-level keys are relation names (`"O.customer"`, `"L.price"`). A box without aggregates is a
  DISTINCT projection (`is_projection`); a box without grouping keys is a global aggregate with one result tuple.
- **Box validation** (`ValueError` with a rule-specific message): grouping keys and aggregates not both empty; the
  subgraph has at least one node; output names unique, non-empty, without `.` or `__`; every path is a string whose
  first segment is a node of the subgraph. An aggregate without attribute (e.g. `Count()`) is allowed; a non-string
  attribute is rejected. No check of the subgraph's shape: as a logical description any shape is valid; whether it
  can be executed is decided later (user decision).
- **Every hand-added edge is backed by a stored reference.** `JoinGraph.add_edge(source, target, ref_key)` takes
  relation names. The target is always a base relation (a box is never a target: no stored reference can point into
  a computed result). From a base relation, its RF must carry a `ForeignValueConstraint(key=ref_key)` pointing at the
  target's RF. From a box, `ref_key` must be one of its grouping keys, whose path is either `X.attr` (reference
  constraint on X pointing at the target) or `X` (grouping by the tuple itself, the target is X); X must be a base
  relation of the subgraph. Longer paths, an inner box as X (user decision), a plain `Node` as an endpoint and a
  duplicate of an existing edge are rejected. RFs are compared by `uuid`, as in `from_dbf`.
- **No edge crosses a box boundary**, by construction: an edge can only connect nodes of the same graph, and inner
  nodes live only in the subgraph. The same relation may appear inside and outside a box (separate scopes).
- `from_dbf`, the existing topology queries and their users (`join`, `subdatabase`) stay unchanged; the queries
  treat a box like any other node. `AggregateBox` keeps the default dataclass equality and is unhashable, like
  `JoinNode`.

## Consequences
**Positive:** aggregation, DISTINCT projection and their nesting become part of the logical query representation;
the graph stays one structure with one place for topology queries; every edge is guaranteed to correspond to a real
reference.
**Negative:** validation runs once (on construction, `add_box`, `add_edge`); changing the dicts or `nodes` / `edges`
directly bypasses it. A graph built from a filtered or cloned DBF has fresh `uuid`s, so a semantically correct edge
is rejected (the known retarget problem). Executing a box later needs `group_by_aggregate` to carry the grouping
keys as attributes of the result tuples; today they are only the item keys.

## Out of scope & deferred
- Deriving boxes from FQL pipelines (`LogicalPlan` / operator tree to `JoinGraph`).
- Deriving box edges automatically from reference-valued grouping keys.
- Executing a box (`group_by_aggregate(join(...))`, see Negative above).
- Rendering nested boxes (ASCII, `funqdb-viz` HTML).
- Serializing the graph (`to_dict` / `from_dict`) next to the plan IR.
- Edges with join predicates (value joins, e.g. on `year`).
- Subqueries and correlated edges across box boundaries.
- Box edges through a grouping key that comes from an inner box.
- The key-preserving FQL `project` as an annotation on a node (not a box).

## Alternatives rejected
- A separate `QueryGraph` class next to `JoinGraph`: duplicates all topology queries.
- A box as a set of node names over one flat graph: no clean nesting; edges across the boundary are ambiguous.
- A separate `ProjectionBox` class: a projection is a box without aggregates.
- Grouping keys as plain paths without output names: an edge needs an attribute name as `ref_key`.

## Scope-shift 2026-10-22

Revision after a five-reviewer pass (FDM semantics, minimality, API, executability, tests). Overrides the sections
above where they differ.

- **Grouping semantics are those of `group_by`** (user decision): a box defines no equality of its own. Verified:
  `group_by` over a reference-valued key raises `TypeError: unhashable type: 'TF'` today, so a box with an outgoing
  edge (key `X.attr` or `X`) cannot be executed yet. How `group_by` should group tuples is decided there, not here;
  TODO.md records the reproduction.
- **Meaning, per case:** with grouping keys `group_by_aggregate(join(subgraph), ...)`, result keys are the group
  values; without grouping keys `aggregate(join(subgraph), ...)` wrapped into a one-tuple relation with a fixed key.
  Aggregates run over the flattened join rows, so a value can be counted once per row it appears in (fan-out);
  aggregation without fan-out is deferred. Aggregates may be absent in single result tuples (004).
- **The graph's edge list is authoritative:** references between subgraph relations that are not edges must be
  dropped by a later executor before `join`.
- **Edges only from a box** (user decision): `add_edge` accepts a box as source only; base-to-base edges come from
  `from_dbf`. Nodes are added with `add_box` only (user decision). Duplicate edges are detected by
  `(source name, target name, ref_key)`, never by `Edge ==` (content comparison of relations).
- **One message per rule:** every failing key path shape (wrong relation, no reference, three or more parts, inner
  box as X) raises the same "not a reference to the target" message.
- **Aggregates must be `AggregationFunction` instances** (also closes, for boxes, the gap noted in 010). Non-string
  type checks are dropped. A path `B.x` into an inner box also checks `x` against `B`'s output names.
- **Known limitations:** relations with composite keys never get an edge (no `ForeignValueConstraint`). Deriving
  boxes from `LogicalPlan`s will likely need a node kind that points at a plan subtree instead of a live RF.
- **Correction:** `JoinNode` is hashable (`RF` hashes by `uuid`); `AggregateBox` is not, because of its dict fields.
  Box equality is not relied upon; tests compare names and edge triples.

<!-- notes below: skill never overwrites past this marker -->

## Note 2026-10-22 (mini-reviews during /develop)

- The box name follows the same rule as output names (non-empty, no `.` / `__`): it becomes a path segment in an
  enclosing box.
- `add_box` rejects a box whose subgraph is the graph itself or encloses it through nested boxes; the check is the
  new public topology query `JoinGraph.encloses(graph)` (identity, recursive). Loops built by writing to `nodes`
  directly bypass it.
- Aggregate attributes are read with `getattr(..., "attribute", None)`, so a user subclass of
  `AggregationFunction` that skips `super().__init__` counts as attribute-less.

## Note 2026-10-22 (final review, /finish-mr)

- Not executable today is every box that groups by tuples (a reference `X.attr`, a relation `X` or an inner box
  `A`), not only boxes with an outgoing edge: `group_by` cannot hash TFs.
- A one-segment grouping key may name an inner box (`"A"`, group by its result tuples); such a key is never an edge
  key, because X of an edge key must be a base relation.

## Note 2026-10-22 (five-reviewer pass on MR !92)

- **Correction of the Scope-shift's correction:** `JoinNode` is *not* hashable (`RF` defines `__eq__` but no
  `__hash__`); no node or edge is hashable. The original wording ("unhashable, like `JoinNode`") was right.
- **Output attributes vs. execution:** the grouping keys are output attributes of a box, but `group_by_aggregate`
  only uses them as item keys today; an executor must write them into the result tuples.
- **Box edges presuppose identity grouping** (one group per referenced tuple). If `group_by` chose content
  grouping (`TF.__eq__`), two equal-content customers would merge and every box edge would be wrong.
- **Executing needs more than a DBF:** `join` rebuilds its graph from the live constraints of a DBF; dropping a
  reference clones the RF (fresh `uuid`), which breaks box edges. Executing a box needs `join` to accept a
  `JoinGraph` or an edge list.
- The fixed key of a global aggregate's one-tuple result is left to the executor.
- Composite keys: only a reference held *inside* a composite key (RSF) never becomes an edge; references on
  attributes of such a relation do.
- Built-in aggregations other than `Count` must have an attribute (`Sum(None)` is rejected).
- Trims (user decisions): the loop check is a private helper (`_encloses`); aggregate attributes are read directly
  (no `getattr` for subclasses skipping `super().__init__`); no test for changing a box after construction;
  duplicate test cases removed; the `AggregateBox` docstring points here instead of repeating this record.
- Deferred additionally: selection predicates on nodes and boxes (WHERE / HAVING).

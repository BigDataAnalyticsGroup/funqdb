# 015 — Filters, projections and views in the JoinGraph
**Date:** 2026-11
**MR:** https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/merge_requests/114
---
## Context

Feature 011 made the `JoinGraph` able to express aggregation and DISTINCT projection through boxes, but deferred
selection predicates on nodes and boxes (WHERE / HAVING) and the key-preserving FQL `project`. Without them the graph
is still no complete logical representation of a query. User request: integrate the information of filters and
projections into the `JoinGraph`.

User decisions: single-relation filters hang at the node and should always be put there; predicates spanning several
relations of a box's subgraph hang at the box. A box turns a graph into a **view**, to which projections, groupings and
filters are attached. The graph stays a logical representation (built by hand, not executed); structured predicates
and lambdas are both allowed. The plan was checked by the plan challenger in four rounds.

## Decision

- **Node filters.** `JoinGraph.add_filter(name, predicate)` attaches a predicate to a node of that graph; it applies to
  that node's tuples, as `filter_values(X, p)` does. Several filters on one node are a conjunction. Inner nodes of a
  view keep their filters in the view's subgraph (WHERE); a filter on a view node in the enclosing graph is HAVING. On
  a view node, a structured predicate may only read the view's output names (checked; a misspelt name would otherwise
  drop every row silently). Lambdas and filters on base relations are not checked (no schema).
- **The box becomes a view.** `AggregateBox` is renamed `View`; `is_projection()` becomes `is_distinct()` (grouping
  keys without aggregates), because "projection" now means the key-preserving output list. Besides 011's grouping keys
  and aggregates a view may carry:
  - `join_predicates`: FDM `JoinPredicate`s (the type `add_join_predicate` already uses) over the joined rows of the
    subgraph. Each element must be a `JoinPredicate`; it must declare at least two distinct relations, all nodes of the
    subgraph (fewer is rejected with a pointer to `add_filter`, where the relation prefix is dropped: `"O.date"` becomes
    `"date"`). A structured predicate must read exactly the relations it declares; for a lambda only the declared
    relations are checked.
  - `projection`: named output paths into the joined rows (`date="O.date"`); one output tuple per joined row, no
    DISTINCT. DISTINCT stays grouping keys without aggregates.
- **Rules.** Projection and grouping (grouping keys and/or aggregates) exclude each other. 011's rule "grouping keys or
  aggregates required" is dropped: a view without both passes the joined rows on, and its output names are the
  subgraph's relation names (not name-checked). The non-empty-subgraph rule stays. A global aggregate is a view with
  aggregates but no grouping keys.
- **Edges from a view** go through a grouping key or a projection name whose path is `X` or `X.attr`, with X a base
  relation of the subgraph (011's reference rule, extended to projection names).
- **Meaning** (for a later executor): (1) rows = join of the subgraph along the subgraph's own edges; (2) a row is kept
  iff each node's part satisfies that node's filters and the row satisfies all join predicates; a missing attribute
  counts as no match, as for filters; (3) then grouping, or projection, or neither. References inside outputs point at
  full tuples. Hiding an aggregate that only HAVING needs takes an outer view with a projection.
- `from_dbf`, the topology queries, `join` and `subdatabase` stay unchanged and ignore the annotations.

## Consequences
**Positive:** WHERE, HAVING, multi-relation conditions, key-preserving projection and views nest in one structure;
single-relation filters have one place; multi-relation predicates reuse an existing FDM type, whose declared relations
make even lambdas checkable.
**Negative:** revises three 011 decisions (projection = DISTINCT, grouping or aggregates required, the class name);
validation runs once, so changing fields or a pass-through view's subgraph later bypasses it; the rename touches 011's
tests, tutorial and SPEC entries. Known gaps, documented only: `And(...)` of single-relation parts declared over two
relations is accepted although its parts belong on nodes; a node filter on a pass-through view node can read one
subgraph relation (`"O.date"`), a second place for a WHERE; a view's projection paths are not checked against base
relations; an outer projection view over a HAVING view is not joinable through `"A.customer"` (X must be a base
relation, 011).

## Out of scope & deferred
- Deriving filters, projections and views from FQL pipelines or from the transpiler IR (`benchmarks/tools/ir/`).
- Executing views (`join` over a `JoinGraph`, `group_by` over tuples by identity).
- Rendering and serializing annotations.
- Attribute checks for base relations (needs a schema).
- Wrapping a DBF's `JoinPredicate`s into a view in `from_dbf`; the view rules are stricter than `add_join_predicate`
  (one relation allowed there, "demand more or less" in `evaluate`), so wrapping must reject or relax cases.
- Edges from a pass-through view (`ref_key` would be a relation name).
- Splitting a top-level `And` into node filters.

## Alternatives rejected
- Projections at individual nodes (user decision): the view's named output list is the one place for output shape.
- Projection and grouping on the same view (user decision): grouping keys and aggregates already are the output list.
- Single-relation join predicates on a view (user decision): they belong on the node.
- Annotations as fields on `Node`: nodes are frozen and shared by identity with edges.
- Separate view classes per kind (filter box, projection box): one class with optional parts is the view.
- A new predicate type for view filters: `JoinPredicate` already has the row shape and declared relations.
- Multi-relation filters on the graph outside a view: wrapping the graph in a view expresses them.
- A second HAVING list on the view: a filter on the view node already is HAVING.

<!-- notes below: skill never overwrites past this marker -->

## Note 2026-11-23 (reviews during /develop)

- `AggregateBox` → `View` also renamed `add_box` → `add_view`; messages say "View" instead of "Box".
- Only the built-in predicate classes (exact type) are inspected for paths. A lambda or a user-defined `Predicate`
  subclass anywhere in the tree (also inside `And` / `Or` / `Not`) makes the whole predicate uninspectable: no HAVING
  path check, and for a join predicate only the declared relations are checked. Literal values are never read as
  paths.
- `add_filter` refuses a `JoinPredicate` (it spans several relations and belongs on a view).
- `View.passes_rows_on()` names the pass-through case. On such a view a HAVING path is also checked into inner views,
  and every path check walks on through inner views that pass rows on (`"P.A.revenue"`).
- Known gap widened: a filter on a view that passes rows on may read several of its relations, a second place for a
  join predicate without declared relations; documented only, like the `And(...)` gap. A relation whose name
  contains `__` can be put into a view but no path reaches it.

## Note 2026-11-23 (final review, /finish-mr)

- A built-in structured join predicate that declares fewer than two relations but reads two or more gets the
  "reads … but declares …" message, not the hint to move it to a node (it could not work there).
- Not changed (optional findings, mentioned to the user): a `JoinPredicate` nested inside `And` / `Or` / `Not` is not
  refused by `add_filter`; an error from a nested path check names the inner view and the rest of the path.
- Correction (round 2): the node hint is given only when the declared and the read relations together are one
  relation. Any other join predicate that declares fewer than two relations does not get the hint and is rejected by
  a later check (usually "reads … but declares …"; a path outside the subgraph or a declared relation outside it
  can win first).

## Note 2026-11-23 (review by three Fable 5.2 reviewers)

- User decision "make it smaller": a join predicate declaring fewer than two relations always gets one neutral
  message ("must declare at least two relations …; a predicate on one relation belongs on that node"), whatever it
  reads. This replaces the hint/correction of the two notes above; "reads exactly what it declares" stays for
  built-in structured predicates with at least two declared relations.
- User decision: the path check through inner views that pass rows on stays (several levels).
- `_predicate_paths` inspects by exact type (a fixed tuple of leaf classes plus `And` / `Or` / `Not`) and no longer
  reads `Predicate._REGISTRY`.
- Wording: rows are filtered two-valued, as `satisfies` evaluates filters (a comparison on a missing attribute is
  false; `Missing` / `Not` hold there), not "a missing attribute counts as no match". The keys of a projection view's
  result tuples are left to the executor (a joined row has no single key; "key-preserving" in the Context refers to
  `project` keeping duplicates). No path reaches a relation whose name contains `__` or `.`.

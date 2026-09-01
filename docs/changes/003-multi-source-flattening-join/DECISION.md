# 003 — Multi-source flattening join
**Date:** 2026-08
**MR:** link TBD
---
## Context

The flattening `join` operator (`fql/operators/joins.py`) materialises a reference-decorated DBF as an RF
of nested tuple-combination rows. Today it walks the reference graph strictly **forward**, following the
inline foreign-value pointer `src_tf[ref_key] is target_tf` from a single **pure source** (a relation with
no incoming reference). Two restrictions follow from that single-forward-walk design and are raised as
`NotImplementedError`:

- **multi-source** — more than one pure source (`_pick_walk_start`, the `!= 1` rejection);
- **diamond / non-tree** — a relation reachable by two paths (`_build_combination` revisit guard).

A multi-source graph is common and legitimate (e.g. two relations `A` and `B` both referencing a hub `C`,
the JOB-style `ci→t, mc→t`). To reach the second source from any single start you must traverse an edge
**against** its arrow (from the hub down to its referencing tuples) — which the forward-only pointer walk
cannot do. Lifting this is the goal of 003.

An analysis session established the mechanism and verified two load-bearing facts against the code:
1. `subdatabase` (Yannakakis) reduces first. Its cascade builds a spanning **tree** from the undirected
   graph. For a graph that is *already* an undirected tree (the 003 scope) there is no residual edge, so
   reduction is **complete** — every surviving tuple extends to a full-join tuple.
2. `semijoin` stores surviving tuples **by object reference** and never clones the contained TF
   (`semijoins.py`), so after any number of reduction passes a reduced source tuple's `[ref_key]` is the
   *same* instance as the reduced hub tuple. Backward matching by object identity therefore works.

## Decision

Lift **only** the multi-source restriction. In-scope graphs are connected, acyclic, and an **undirected
tree** in any edge orientation (0..n pure sources). Diamonds / non-tree graphs stay an honest
`NotImplementedError`, deferred to 004.

The walk becomes **bidirectional and functional**:

- **Gating** (`_pick_walk_start`), in this order, all **before** the `subdatabase` reduction runs:
  isolated-relation check → disconnected-components check (both keep their tailored messages) → an
  undirected-tree gate → then pick the start deterministically as `sorted(pure_sources)[0]` (relaxing the
  old "exactly one" to "≥ 1"). Placing the tree gate before the pick also removes the crash where a cyclic
  graph has no pure source.
- **Topology on `JoinGraph`** (the class owns topology queries): `is_tree()` =
  `one connected component AND len(edges) == len(nodes) - 1`; and `incoming_adjacency()`, a mirror of the
  existing `outgoing_adjacency()`, keyed by the edge target and reusing the existing `Neighbor` type. Note
  the existing directed `check_acyclicity()` does **not** substitute for the tree test — a diamond is
  directed-acyclic but undirected-cyclic; the edge-count is the correct undirected-tree test.
- **Enumeration** (replacing `_build_combination` with a functional generator, `came_from` parent-name
  only, no visited set — a simple tree needs nothing more): at a node, each incident edge except the one
  arrived through is expanded by **recursing first** into a list of completed sub-dicts — a forward edge
  follows the pointer to its single target; a backward edge concatenates the recursion of every reduced
  source tuple whose `[ref_key] is` this hub tuple (an inline identity filter over the reduced source
  relation, no prebuilt index). The row is then `itertools.product` **across the neighbours'** expanded
  results, merged; a leaf yields itself. Output rows keep sequential integer keys from one monotonic
  counter across the whole enumeration.

Grouping the referencing tuples per hub is conceptually a co-group, but operationally each backward edge
groups a single relation, which the generator's product recombines — so it is a per-edge inline filter,
not the `cogroup` operator.

## Consequences

**Positive:**
- Multi-source trees in any orientation flatten correctly; the operator matches the FDM/FQL full-join
  intent rather than an artefact of forward-only traversal.
- Reuses `subdatabase`/`semijoin` (reduction, unchanged) and the object-identity pointer contract; adds
  only two small symmetric topology methods and a functional generator.
- The tree gate turns diamonds/cycles/parallel/self-references into one honest up-front error instead of a
  mid-walk failure, and removes a latent crash on cyclic input.

**Negative:**
- Row count is a Cartesian product across a hub's referencing relations and can exceed any single input —
  acceptable, since performance is an explicit non-goal for this prototype.
- The backward match is an O(rows) scan over the reduced source per hub tuple (no reverse index) — again a
  deliberate clarity-over-performance choice.
- The explicit `root` parameter keeps its "must be a pure source" constraint, which is now an artificial
  limitation under bidirectional walking (a hub would be a valid root too). Retained as the smaller,
  documented diff rather than reworked.

## Out of scope & deferred

- **004 — diamonds / non-tree graphs.** The spanning-tree reduction leaves one residual edge per
  undirected cycle unenforced; enumerating them correctly needs spanning-tree enumeration plus a
  residual-edge identity filter (`row[source][ref_key] is row[target]`). Design already sketched; kept out
  of 003 per one-concern-per-PR.
- JoinPredicate pushdown during join (still `NotImplementedError`).
- Disconnected reference graph → Cartesian product across components (still `NotImplementedError`).
- Cyclic (directed) reference graphs.
- Relaxing the `root` parameter to accept any node.
- Representing the backward grouping / reduction as its own node in the plan IR (`subdatabase` stays an
  inline call inside `_compute`, as today).

## Alternatives rejected

- **Bespoke reverse-index attached to `subdatabase` output via a flag** — duplicates a plain per-edge
  grouping, couples the DBF with out-of-band metadata not represented in the plan IR, and yields no real
  byproduct saving because a correct index must be built after the full reduction anyway.
- **Reuse the `cogroup` operator for the hub buckets (teach it to key by referenced `.uuid`)** — the joint
  co-bucket is redundant with the generator's own product step, so it would be dead code, and it needs a
  `.uuid` dereference patch (scalar and composite branches) on a shared operator. A TF is unhashable
  (verified), so grouping by the raw reference value is impossible regardless.
- **Grouping via a computed `.uuid` attribute (`add_computed_per_value`)** — per-edge boilerplate that must
  run before freeze; superseded by the inline identity filter.
- **Diamonds in this MR** — split to 004 (distinct, riskier concern; one concern per PR).
- **Arbitrary-root reframe: drop the pure-source policy entirely, start at `sorted(nodes)[0]`, retire the
  isolated/disconnected/pure-source queries from the join path** — a smaller end state but a larger, riskier
  diff (churns the explicit-root test and root-param semantics), and, decisively, an *arbitrary* root reaches
  only its own connected component, turning the isolated/disconnected honest errors into silent wrong
  results. Kept the pure-source start (relaxed to ≥ 1).
- **A direction-tagged `WalkStep` record / a single direction-tagged undirected adjacency** — replaced by two
  symmetric adjacency methods reusing the existing 2-field `Neighbor`, so the direction is implicit in which
  map a neighbour came from.
- **A prebuilt `{hub_uuid: [source_tf]}` reverse index** — replaced by the inline `is` identity filter over
  the reduced source (same idiom the forward path already uses; drops the uuid-keying subtlety). Performance
  is a non-goal.
- **A second, new multi-source fixture** — instead repurpose the existing `_multi_source_star_dbf` and its
  now-obsolete "raises" test into the positive full-join test.

<!-- notes below: skill never overwrites past this marker -->

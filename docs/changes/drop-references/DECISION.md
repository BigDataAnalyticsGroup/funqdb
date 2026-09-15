# drop-references
**Date:** 2026-09
**MR:** https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/merge_requests/74
---
## Context
`JoinGraph.from_dbf` turns *every* `ForeignValueConstraint` in a DBF into a mandatory
join edge. FDM is opt-out: a relation carries all its schema references. In the Join
Order Benchmark, ~19 queries fail with `NotImplementedError` "not a tree (… parallel
reference)" because a relation carries two schema FKs to the *same* target (e.g.
`movie_link.movie_id → title` **and** `movie_link.linked_movie_id → title`) while the
query joins along only one; `join` makes both edges, producing a parallel edge → non-tree
→ rejected. SQL is the dual: it opts in, naming each join it wants; the unused FK never
appears in the query. FQL needs the opt-out: a query blacklists the reference edge it
does not join along.

## Decision
**No new code.** The existing clone-based operator `drop_reference(dbf, source, ref_key,
target)` (`fql/operators/constraints.py`) already implements the blacklist correctly and
safely: it clones the DBF (rebinding all cross-references), removes the FVC on the source
and the matching RFOC on the target, and raises on a missing edge. A POC confirmed it
turns a parallel-reference DBF that `join` rejects into a joinable tree with the
SQL-matching result. Being clone-based, it is per-query safe — it never mutates the shared
schema RFs.

This feature therefore only **documents** the reference-blacklist pattern: a tutorial
section (`02 FQL/constraints.md` "Blacklisting a parallel reference before a join", and a
cross-linked note in `02 FQL/join.md`) and an extension of the `drop_reference` entry in
`SPEC.md`. No fdm/fql source changes.

## Consequences
**Positive:** documents how to unblock the parallel-reference JOB queries with the tool
that already exists; frames the blacklist as the opt-out dual of SQL's opt-in; no new API
surface to maintain; no risk of shared-schema corruption.
**Negative:** the headline goal — actually unblocking the 19 JOB queries and verifying
FQL == SQL — is not delivered here; that is query-authoring / benchmark-harness work on a
branch that carries the harness. Correctness of a blacklist rests on the author dropping
only edges the query does not join along.

## Out of scope & deferred
- An integration test tying `drop_reference` to the parallel-reference `join` fix
  (build the two-FVC-same-target shape; assert `join` rejects it, then succeeds after the
  drop) — a real coverage gap, deferred by choice for this docs-only MR.
- Applying the blacklists to the 19 parallel-reference JOB queries and verifying FQL == SQL
  on the benchmark harness (harness not on this branch).
- A two-alias self-join for queries that genuinely join along *both* parallel references —
  needs explicit aliasing, a separate feature, not a blacklist.

## Alternatives rejected
- **An eager, in-place `drop_reference(key, target)` method symmetric to `.references()`**
  (on the RF / `DictionaryAttributeFunction`). Rejected: it mutates the *shared* schema RF
  globally and only on an unfrozen RF, so it cannot serve per-query differing topologies
  and would corrupt other queries sharing the schema. Ten independent design reviews
  converged on this; the clone-based operator is the correct per-query tool.
- **An RF-level copy-returning drop** (`ml.drop_reference(...)` returns a copy of the RF).
  Rejected: `.copy()` mints a fresh uuid, so other relations' references *into* the copied
  RF (matched by uuid in `from_dbf`) would silently lose their edges. Rebinding must span
  the whole DBF — which is exactly what the clone-based operator does.
- **New fluent `DBF.drop_reference` / `DBF.drop_references` methods delegating to the
  operator, plus a `ReferenceEdge` batch type.** Rejected as unnecessary surface: the
  operator already covers the need; a call-site loop already gives batch semantics; the
  motivating JOB queries each drop a single edge.

<!-- notes below: skill never overwrites past this marker -->

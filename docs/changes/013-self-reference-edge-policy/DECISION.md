# 013 — Self-reference edge policy
**Date:** 2026-09
**MR:** https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/merge_requests/73 (merged into !71)
---
## Context
`JoinGraph.from_dbf` (`fql/plan/join_graph.py`) turns *every* `ForeignValueConstraint`
whose target is in the DBF into a mandatory join edge — including a relation that
references itself (JOB's `title.episode_of_id → title`). That self-edge is a self-loop,
so `check_acyclicity` / `is_tree` reject the graph, and any fully-referenced DBF that
merely *contains* `title` as a hub fails to join. This blocks ~110 JOB queries. (*28 with this
rule alone, ~110 together with !79, see note 2026-10-28 (five reviewers).*)

Surfaced in the review of the held MR !71 (JOB 33a/b/c). This feature is stacked on the
`self_joins` branch because it builds directly on !71's aliased-node split (`e57b399`, then
unmerged); branching off `main` was not viable, as the positional alias-pairing logic this
feature cleans up did not exist there. (*The alias split was removed, see note 2026-10-28
(five reviewers).*)

Deliberately scoped to the self-reference rule only. The larger *explicit alias binding* (a
per-reference binding to an alias name) — required for the 33a/b/c *family* (a target aliased
under several names with more references than aliases) — is a separate feature and out of
scope here. (*Aliases are now refused, see note 2026-10-28 (five reviewers).*)

## Decision
A `ForeignValueConstraint` is an object-identity *pointer*, not a join request. Treating
every reference as a join edge is `join`-operator *policy*, not an FDM law; forcing a
self-join on any relation that happens to reference itself is exactly the SQL/ORM
"every foreign key is a join" assumption the project avoids. So:

- A self-referential constraint (the source RF instance *is* the target RF instance) no
  longer generates an automatic join edge in `from_dbf`. Self-joins become opt-in, expressed
  explicitly via the explicit alias binding, never inferred.
- Self-referential constraints are excluded from the positional alias-pairing count, so they
  no longer inflate the reference count of an aliased target. (*Superseded: the alias pairing
  was removed, see note 2026-10-28 (five reviewers).*)

**No-silent-wrong-answer contract.** If a dropped self-reference is the *only* structural
connection at a node — an un-aliased sole self-join (e.g. `title` alone with just
`episode_of_id → title`, where the user wants episode↔series pairs) — dropping the self-loop
would leave the graph trivial and `join` would fall through to its single-relation path and
silently return every row unflattened. `from_dbf` must instead raise a clear
`NotImplementedError` directing the user to alias the relation and bind the reference
explicitly. (*Moved from `from_dbf` to `join`, see note 2026-10-28; then removed, see note
2026-10-28 (five reviewers).*)

## Consequences
**Positive:** unblocks ~110 JOB queries that carry `title` as a hub (*28 alone, ~110 with
!79*); removes spurious
self-cycles from the reference graph; faithful to the FDM (a self-reference is schema
metadata, not an implicit join) and less SQL-ish; self-references no longer pollute the
positional alias count, which unblocks the aliased cases the alias binding will complete.
(*Superseded: no alias count any more.*)
**Negative:** un-aliased self-joins are unsupported and raise (they must be expressed
explicitly once the alias binding lands) (*superseded: no guard any more*); reverses the
self-reference-as-rejected note in `JoinGraph.is_tree`'s docstring (updated in this MR);
`from_dbf` gains a new sole-connection guard path (*now in `join`, see note 2026-10-28;
removed, see note 2026-10-28 (five reviewers)*). Does **not** fix 33a/b/c — those still
raise clearly on the alias count mismatch until the alias binding. (*Aliases are now refused
outright, see that note.*)

## Out of scope & deferred
- Explicit per-reference alias binding (`AliasReference` constraint + `add_alias_reference`
  operator; all-or-nothing per aliased target; cross-alias self-references like
  `t1.episode_of_id → t2`) — own feature, required for the 33a/b/c family.
- `drop_alias_reference` (symmetry with `drop_join_predicate`) — with the alias binding.
- Diamonds / non-tree acyclic graphs — deferred (later MR).
- Auto-inference of alias edges from tuple-level inline pointers — deferred.

## Alternatives rejected
- Keep the self-reference as an automatic join edge and let `check_acyclicity` reject the
  cycle: leaves the ~110 hub queries blocked with no recourse. Rejected.
- Drop self-loops unconditionally, without the sole-connection guard: silently returns all
  rows unflattened for an un-aliased sole self-join — a wrong answer with no error. Rejected.
  (*Adopted after all, see note 2026-10-28 (five reviewers).*)
- Carry an alias hint on the source `ForeignValueConstraint` (`.references(..., as_=)`):
  cannot express an aliased source (aliases of one RF share a single constraint object), and
  belongs to the explicit-binding work anyway. Rejected here; superseded by the alias
  binding's DBF-level binding.

<!-- notes below: skill never overwrites past this marker -->

- 2026-10-28, merge of main into !71 and /finish-mr review:
  - Renumbered 004 -> 013 (main's 004 is `004-undefined-attribute-semantics`); the placeholder
    number 005 for the explicit alias binding is dropped (main's 005 is the benchmark report).
  - **The guard moved from `from_dbf` to `join`.** Since !92, `from_dbf` also builds the graphs
    of aggregate boxes, where a single self-referencing relation (`{t: title}` under a `Count()`
    box) is legitimate; raising there broke it. `from_dbf` now records the relations whose
    self-reference it skipped (`JoinGraph.self_referencing`), `JoinGraph.isolated_by_self_reference()`
    answers the topology question, and `join` raises. `subdatabase` does not raise: with no edge
    there is nothing to reduce, so it returns the relation unchanged (no wrong answer). Boxes
    are meant to run as `aggregate(join(subgraph), …)`; the future box executor must therefore
    aggregate a one-relation subgraph directly, without `join`, or the guard needs an opt-out.
  - Only the exact RF counts as a self-reference: a filtered copy (`title.where(...)`) still
    references the original `title` and is not detected. Next to the original,
    `{t: title, t_f: title.where(...)}` even gets an automatic edge `t_f -episode_of_id-> t`, an
    inferred self-join. Resolving filtered and reduced copies is identity provenance (MR !79).
  - Known interaction, fixed by !100 (merged into this branch): `semijoin` guessed an edge's
    direction from the attribute name, so a self-reference sharing its name with a reference
    into the relation (`emp.manager -> emp`, `dept.manager -> emp`) reduced the wrong side and
    `join` returned no rows. On main this shape raised (cycle); now it joins to one row
    (`test_join_self_reference_sharing_its_name_with_an_incoming_reference`).
  - `add_reference` / `drop_reference` rebind references to the copy under the last alias name;
    documented as ⚠️ in SPEC.md and left to the explicit alias binding.

- 2026-10-28, five-reviewer pass on !71 (user decisions):
  - **The alias split is removed.** Pairing the references into an RF entered under several
    names with those names by DBF order gave silent wrong answers: JOB 13a/b/c/d written in
    their FROM order, and 12a with `mi_idx` before `mi`, returned 0 rows instead of 1 (main
    raised). `from_dbf` now raises `NotImplementedError` when another relation references an
    aliased RF, and `add_reference` / `drop_reference` refuse a DBF with aliases (their copies
    rebind every reference to the last alias, which turned an aliased self-reference into an
    unwanted self-join). A review of this change showed that `add_join_predicate` and
    `drop_join_predicate` use the same copying, so all four constraint operators refuse
    aliases (the check sits in the shared clone helper). The working route is one relation
    per name, `rf.where(lambda i: True)` for every name including the first (with the
    original RF still in the DBF, a filtered copy's inherited self-reference would point at
    it and form an edge), plus `add_reference`; it already gave the SQL answer for 12a on
    main. This matches !79, where one RF under several names is ambiguous as well.
  - **The guard is removed.** A self-reference is no join request (the Decision above), so
    `{t: title}` is a plain one-relation DBF and `join` passes its tuples through, the same as
    SQL's `SELECT … FROM title`. The guard assumed a self-join was meant, which is the "every
    foreign key is a join" habit the Decision rejects; it prevented no wrong answer. With it go
    `JoinGraph.self_referencing`, `isolated_by_self_reference()` and the box-executor TODO.
    This supersedes the contract above and the second and fifth bullets of the previous
    note.
  - The rule changes results for the 28 of 113 JOB queries that have an unfiltered `title` in
    the DBF; a filtered `t` points at the base `title`, never at itself. The ~110 holds only
    together with !79.
  - The `.where`-on-hub tests (with the strict xfail) moved to !79, which fixes them.

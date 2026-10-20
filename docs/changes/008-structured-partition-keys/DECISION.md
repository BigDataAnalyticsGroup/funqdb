# 008 — Structured partition keys for partition and group_by
**Date:** 2026-10
**MR:** https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/merge_requests/88
---
## Context

The goal is FQL plans that a backend can translate to SQL. After 007, partitioning functions of `partition`
(and `partition_by_aggregate`, built on it) are still opaque lambdas. Real partition lambdas in tests and
docs: a tuple of attributes (what `group_by` does), a comparison (`yob < 1980`), a case distinction and
arithmetic. Two defects exist today: `group_by` raises `AttributeError` on a missing grouping attribute
(004 deferred: own group vs dropped), and a `Predicate` passed as partitioning function or ranking key
already looks structured in the plan but is evaluated on the item instead of its value, so every tuple
lands in `False` (or keeps input order when ranking). This MR also addresses 007's deferred item
"a Predicate as partition key".

## Decision

- **Missing attribute: the tuple is dropped** (user decision), as for every other reader in 004. No
  `UNDEFINED` group.
- **`Composite(*components)`** (new expression): evaluates each component and returns a Python tuple;
  `UNDEFINED` if any component is `UNDEFINED`. Components must be expressions (`Attr`, `Key`, `Composite`)
  or `Predicate`s; anything else (e.g. a lambda or a bare string) and zero components are rejected at
  construction. Name `Composite` chosen over `Tuple`, which clashes with FDM's tuple functions.
- **One evaluation rule** (private helper in the expressions module): a `Predicate` is evaluated on
  `item.value` (as in `filter_values`), anything else on the item. Used by `Composite`, `partition` and the
  shared sorting helper of `rank_by` / `subset` / `items_sorted_by`; this also fixes bare `Predicate`
  ranking keys (now `False` sorts before `True`).
- **`partition`**: a key of `UNDEFINED` drops the tuple; lambdas are unchanged.
- **`Predicate` as key** (user decision): always yields a bool, so tuples lacking the attribute land in the
  `False` partition (predicates are two-valued, 004). This keeps
  `partition(P) == {True: filter_values(P), False: filter_values(Not(P))}` (absent partitions where empty).
  Mixed: `Composite(Attr("a"), Gt("b", 1))` gives `(a, False)` for a tuple without `b`, while
  `Composite(Attr("a"), Attr("b"))` drops it.
- **`group_by`** builds its key from `Attr` (one key) or `Composite` of `Attr`s (several); result keys and
  plan parameters (006) unchanged; `group_by_aggregate` inherits.
- **Composite as ranking key** is official (user decision): lexicographic, one global direction.
- **Plan IR**: `{"type": "expression", "op": "composite", "items": [...]}` with nested expression and
  predicate records. A composite is structured only if it is exactly `Composite` and every expression
  component is serializable; predicate components count as serializable, consistent with how the
  extractor already treats a bare `Predicate`. Malformed outer records raise `ValueError`.
- **`IR_VERSION` 3 → 4**, because existing v3 records change meaning: a `Predicate` as partitioning function
  or ranking key used to be evaluated on the item and is now evaluated on its value.
- **SQL translation** this enables: a partition / grouping attribute becomes `GROUP BY a` plus
  `WHERE a IS NOT NULL` (dropped tuples); a `Predicate` key or component becomes `COALESCE(p, FALSE)`.

## Consequences
**Positive:** `partition` with attribute, composite or predicate keys and `group_by` / `group_by_aggregate`
produce plans without `Opaque`; `group_by` no longer crashes on missing attributes (004 item resolved for
`partition` / `group_by`); predicate keys compute correctly for partitioning and ranking.
**Negative:** Behaviour changes of `group_by` / `group_by_aggregate`:
- a misspelt grouping key, or values that are not attribute functions (scalars, Python dicts), now give an
  empty result silently instead of raising (same rule as `Attr` in 007);
- attribute functions that yield a value only through a default value function without a declared domain
  are dropped instead of grouped under the default;
- grouping keys must be strings and are read as paths (`.` is new; `__` behaved as a path before); invalid
  keys raise `ValueError` — at construction for `group_by`, only when the result is computed for
  `group_by_aggregate` (it builds its `group_by` lazily); integer attribute keys are no longer accepted.
A composite key mixing a stored `None` with numbers in one position raises `TypeError` when sorting.
`Predicate` subclasses serialize under their parent's op (existing gap). Nested predicate records that are
malformed below `op` still raise `KeyError` (existing gap of `Predicate.from_dict`). Plans written with IR
version 3 are rejected. In a SQL backend, a stored `None` and a missing attribute may both become `NULL`, so
a `None` group cannot be distinguished from dropped tuples (same gap as predicates under 004).

## Out of scope & deferred
- Case distinctions (`CASE WHEN`) and arithmetic expressions.
- Direction per ranking key.
- `partition_by_aggregate`'s per-item `aggregation_function`.
- `cogroup` grouping still reads attributes directly and raises on a missing attribute (004 item stays
  open for it).
- How absence is represented in a SQL backend (stored `None` vs missing attribute).
- Existing bug: a partition key value containing `__` (e.g. `"a__b"`) is split as a path when written into
  the result DBF.

## Alternatives rejected
- A missing attribute forms its own `UNDEFINED` group: resembles SQL's NULL group; user chose dropping.
- A new `by=` keyword instead of reusing `partitioning_function`: needless API change.
- A plain Python tuple of expressions as key: not callable, ambiguous in the plan; `Composite` instead.
- A `Predicate` key drops tuples lacking the attribute: needs to know which attributes a predicate reads
  and breaks `partition(P) = filter(P) ∪ filter(Not P)` under 004's two-valued logic.
- Exact-type rule for predicate components: would make `partition(P_subclass)` structured but
  `partition(Composite(P_subclass))` opaque; consistency with the extractor preferred.
- A public `evaluate()` helper: only used internally; kept private.

<!-- notes below: skill never overwrites past this marker -->

## Note 2026-10-20 (POC review)

- The plan field of a composite is called `"components"`, not `"items"` as written in the Decision: in FQL an
  "item" is a key/value pair. Renamed before the format is released, so no further IR version bump is needed.

## Note 2026-10-20 (five-reviewer pass on the POC)

- Python treats `True`, `1` and `1.0` as equal dict keys, so a partition key function returning bools for some
  tuples and numbers for others merges those partitions (also with lambdas; pre-existing, not addressed).
- The private evaluation helper is named `_evaluate_on_item`; `Composite` raises `TypeError` for components of
  the wrong type and `ValueError` for zero components, while `Attr` keeps raising `ValueError` for invalid paths.
- The import of `CompositeForeignObject` in the partition module was already unused before this MR; left as is.

## Note 2026-10-20 (final reviews)

- Reading guide: the Decision section names the plan field `"items"`; the actual field is `"components"` (see
  the POC-review note above).
- Precision: a `Predicate` key yields a bool, never `UNDEFINED`; only a comparison (`Like`, `In`, `Exists`) over
  a missing attribute gives `False` — `Missing(...)` or `Not(...)` give `True`. The identity with
  `filter_values(P)` / `filter_values(Not(P))` holds in every case.
- Malformed nested records inside a composite now raise `ValueError` in every case (errors from
  `Predicate.from_dict` are re-raised), so the `KeyError` gap listed under Consequences no longer applies to
  composites; it remains for bare predicate records.
- The `True` / `1` key merging noted above moved to `TODO.md`. The note on the unused import is irrelevant to
  this change and can be ignored.

## Note 2026-10-20 (final review round 2)

- Corrects the "Precision" bullet of the previous note: a comparison, `Like`, `In` or `Exists` over a missing
  attribute gives `False`; `Missing(...)` or the negation of a comparison gives `True`; `And` / `Or` combine
  their parts. The identity with `filter_values(P)` / `filter_values(Not(P))` holds in every case.
- Corrects the error-handling bullet: nested records inside a composite with missing fields or the wrong shape
  raise `ValueError`; field values of the wrong type (e.g. a numeric `"attr"`) are not validated and only fail
  when the key is evaluated (same as for bare predicate records).

# 010 — Structured aggregates for partition_by_aggregate
**Date:** 2026-10
**MR:** https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/merge_requests/91
---
## Context

The goal is FQL plans that a backend can translate to SQL. `partition_by_aggregate` partitions its input and then
applies `aggregation_function` to each **partition** (an `Item(partition key, RF)` that must map to an `Item`).
Since 008 the partition key can be structured; the aggregation part is a lambda and therefore `Opaque`. Its only
use in the repo counts the tuples per partition. The tutorial already describes the operator as "like
`group_by_aggregate`, but with an arbitrary partitioning function", which the code did not offer. Docstrings,
`TODO.md` and the 006 notes call the aggregation "per item"; it is per partition. The plan was challenged by six
independent reviewers before approval.

## Decision

- **Two forms, exactly one:** either `aggregation_function=<callable>` as before, or built-in aggregation functions
  as keyword arguments, like `group_by_aggregate`:
  `partition_by_aggregate(rf, partitioning_function=Lt("yob", 1980), count=Count(), oldest=Min("yob"))`.
  Neither or both raise `ValueError` at construction (previously, omitting `aggregation_function` raised
  `TypeError`).
- **Guard:** an `Expression` (e.g. a `Record`), a `Predicate` or an `AggregationFunction` passed as
  `aggregation_function` raises `TypeError` at construction with a hint to the keyword form. Such objects would
  look structured in the plan but fail at run time (`Count()` gets an `Item`) or silently yield empty tuples (a
  `Record` evaluated on a partition's RF). This supersedes the 006 note that accepted `aggregation_function=Count()`
  as an unguarded user error.
- **Evaluation:** with keyword aggregates each partition maps to its aggregate tuple, exactly as in
  `group_by_aggregate` (inline in `partition_by_aggregate`; `group_by_aggregate` stays untouched). Absence follows
  004: an aggregate over no mapped values is absent from the partition's tuple; partitions are never empty, so
  `Count()` is at least 1. The partition rule follows 008: an `Attr` / `Composite` / arithmetic key that is
  undefined drops the tuple; a `Predicate` key always yields a bool, so a tuple without the attribute lands in the
  `False` (or, for `Missing` / a negated comparison, the `True`) partition.
- **Plan:** `aggregates` appears as aggregation records (006) and `aggregation_function` as `None`. The plan is free
  of `Opaque` only if the partition key is structured (`Attr`, `Composite`, arithmetic, `Predicate`); a lambda key
  stays `Opaque`. The lambda form now also shows `aggregates: {}`. No `IR_VERSION` bump: no existing record changes
  meaning — an older `partition_by_aggregate` record has no `aggregates` and is read as the lambda form.
- **Reserved names:** an aggregate cannot be named `input_function`, `partitioning_function`,
  `aggregation_function` or `self` (Python raises `TypeError`), as for `group_by_aggregate`.

## Consequences
**Positive:** grouping by an arbitrary structured key with built-in aggregates (SQL: `SELECT key, aggs ... WHERE
<key defined> GROUP BY key`) produces plans without `Opaque`; the operator now matches its tutorial description; a
common mix-up is caught early.
**Negative:** small API change: omitting both forms raises `ValueError` instead of `TypeError`, and structured
objects as `aggregation_function` are now rejected. Plans of the lambda form gain `aggregates: {}`. "No `Opaque`"
is not "translates 1:1" — inherited differences a translator must handle: a stored `None` forms its own group and
is counted by `Count("a")` in FDM but not in SQL; `Max` over a single stored `None` returns `None` while over
`[None, 3]` it raises; `Avg` needs a cast in dialects with integer division; `Median` has no standard SQL
(`PERCENTILE_CONT`) and returns an int for odd counts in FDM; `Min` / `Max` on strings depend on collation; a
predicate key needs `COALESCE` per comparison, not around the whole predicate.

## Out of scope & deferred
- Aggregates as `Record` fields over partitions (mixing item and partition level).
- Re-expressing `group_by_aggregate` via `partition_by_aggregate` (would make its key check eager).
- Hiding the unused form from the plan (`aggregates: {}` / `aggregation_function: None`).

## Alternatives rejected
- A shared helper for both operators: touches `group_by_aggregate` for a three-line lambda.
- Removing the lambda form: user chose to keep both.
- A separate new operator: needless API growth.
- Leaving structured `aggregation_function` values unguarded (006 status quo): misleading plans.

<!-- notes below: skill never overwrites past this marker -->

## Note 2026-10-21 (five-reviewer pass and final review round 1)

- "No `Opaque`" needs a structured partitioning key **and** built-in aggregates (a user-built or invalid
  aggregation stays `Opaque`, per 006); docstring, SPEC and tutorial say so.
- The guard rejects every `AggregationFunction` instance as `aggregation_function`, including user-built ones and
  subclasses, and user subclasses of `Expression` / `Predicate`: none of them takes a partition `Item`. The class
  `Count` itself (not an instance) is a plain callable and passes; it fails at run time like any wrong lambda.
- Additional negative consequence of the keyword form: a misspelt parameter name (e.g. `aggregation_functon=`) is
  silently taken as a keyword aggregate instead of raising `TypeError: unexpected keyword argument`.
- Values given as keyword aggregates are not checked: a predicate or expression there looks structured in the plan
  but is evaluated on the partition RF. The same gap exists in `aggregate` and `group_by_aggregate`; deferred to
  its own MR (TODO.md).

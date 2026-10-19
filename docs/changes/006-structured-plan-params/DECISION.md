# 006 — Remove accidental Opaque markers from plan IR
**Date:** 2026-10
**MR:** https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/merge_requests/85
---
## Context

The plan extractor turns every callable parameter that is not a structured `Predicate` into an `Opaque`
marker. The goal is plans that a backend can translate to SQL; every `Opaque` blocks that.
Many operators produce `Opaque` only *accidentally*: the structured information exists but is lost
on the way into the plan.

- `project` and `group_by` keep their attribute names only inside an internally generated lambda.
- Built-in aggregation functions (`Count`, `Sum`, `Min`, `Max`, `Avg`/`Mean`, `Median`) know their
  function and attribute but are callable, hence opaque. This affects `aggregate`, `group_by_aggregate`
  and `partition_by_aggregate`.
- `output_factory` (and `output_factory_nested` / `output_factory_leaf`) is almost always a lambda.
  Because it is mandatory or defaulted for `union`, `intersect`, `minus`, `cogroup` and `partition`,
  those operators are always opaque, although the factory only chooses the class of the result AF.

Genuine user lambdas (filter lambdas, `transform`, ranking keys, partitioning functions, join
predicates) are a separate problem and not addressed here.

## Decision

- Factory parameters are omitted from the plan: they do not carry query logic.
- `project` exposes its `attributes`; `group_by` exposes its `aggregate_keys` (same name as in
  `group_by_aggregate`, chosen for consistency). The internal lambdas are hidden from the plan.
- Built-in aggregation functions become serializable, mirroring `Predicate`: `to_dict()` produces
  `{"type": "aggregation", "op": ..., "attr": ...}`, `from_dict()` restores via a registry, and a readable
  `__repr__` keeps `explain()` free of memory addresses. Only the exact built-in types are structured;
  `Mean` serializes as `avg` and restores as `Avg`. User subclasses and `AggregationFunction(custom)`
  stay `Opaque`, because their behaviour cannot be inferred from the class.
- `IR_VERSION` is raised to 2, so that an old reader rejects plans containing the new
  `"aggregation"` record instead of silently reading it as a plain dict. Plans without a version
  tag are still accepted as the current version (unchanged behaviour).

## Consequences
**Positive:** `project`, `group_by`, `aggregate`, `group_by_aggregate`, the set operations, `cogroup`
and `partition` with default factory (and filters with a factory) produce plans without `Opaque`,
a precondition for SQL translation. `partition_by_aggregate` with a built-in aggregation becomes
partly structured.
**Negative:** The plan no longer records which result class a factory would produce; a consumer that
wants to rebuild the exact Python pipeline from the plan cannot. Plans written with IR version 1 are
rejected.

## Out of scope & deferred
- Structured alternatives for genuine lambdas: ranking keys (`rank_by`, `subset`), partitioning
  functions, `transform` / `transform_items`, join predicates (`add_join_predicate`).
- Namespacing the bare `"type"` discriminator in the IR (existing `TODO.md` item).
- Synonym operator classes (e.g. `𝜞`) appear under their own class name in the plan.

## Alternatives rejected
- Serialize the factory as its class name: nearly all call sites pass a lambda, so it would not help.
- A dedicated "factory" marker in the IR: an extra IR type without any consumer.
- A common serialization interface shared with `Predicate`: would change `Predicate` code, out of scope.
- Three separate MRs (one per cause): the parts share one goal and are small together.

<!-- notes below: skill never overwrites past this marker -->

## Note 2026-10-19 (POC review)

- Correction: `partition_by_aggregate` does **not** become partly structured. It applies its
  `aggregation_function` per item (via `transform_items`), so a built-in aggregation, which expects a
  whole RF, cannot be used there.
- Additional negative consequence: for `transform_items`, `output_factory=None` means "modify the
  input in place". With factories omitted the plan no longer shows this. Irrelevant for now, because
  `transform_items` is opaque anyway through its user lambda.

## Note 2026-10-19 (final review)

- Refinement of the `partition_by_aggregate` correction: if a user nevertheless passes a built-in such as
  `Count()` as its per-item `aggregation_function`, the plan shows it as a structured aggregation, although
  it would be applied per item (and fail at run time). Accepted as user error; not guarded.
- Built-in aggregations with an invalid attribute (e.g. `Sum(None)`, `Max(5)`) are not serializable and stay
  `Opaque`, so that every written plan can be read back.

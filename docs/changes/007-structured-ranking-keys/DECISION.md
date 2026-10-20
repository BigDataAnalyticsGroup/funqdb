# 007 — Structured ranking keys for rank_by and subset
**Date:** 2026-10
**MR:** https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/merge_requests/87
---
## Context

The goal is FQL plans that a backend can translate to SQL. After 006, the remaining `Opaque` markers come
from genuine user lambdas. Filters and join predicates already have a structured alternative (`Predicate`,
`Ref`); ranking keys of `rank_by` and `subset` (top-k) do not. A survey of tests and docs found that 40 of
43 ranking-key lambdas only read one attribute (`item.value.yob`) or the key (`item.key`). In SQL they
correspond to `ORDER BY` with `LIMIT`/`OFFSET`.

Constraint from 004: a tuple without a mapping for an accessed attribute does not contribute to an operator
that reads it; there is no NULL value and no exception.

## Decision

- Lambdas are replaced by own structured, serializable expression objects (not by introspecting lambdas).
  Lambdas stay allowed and remain `Opaque`.
- A new base class `Expression` mirrors `Predicate`: callable on an `Item`, `to_dict` / `from_dict` with a
  registry, readable `__repr__`. First two expressions: `Attr(attr)` returns the value of an attribute path
  (dot or dunder form) of `item.value`; `Key()` returns `item.key`. Invalid paths are rejected when the
  object is created; `from_dict` raises `ValueError` on malformed records.
- Return contract: an expression may return the public marker `UNDEFINED` (the same object predicates and
  aggregates already use). Every consumer of an expression must handle it.
- Ranking rule: the key is computed once per item; items whose key is `UNDEFINED` are dropped, the rest is
  sorted. This applies to `rank_by`, `subset` (and therefore `top` / `bottom`) and `items_sorted_by`, which
  shares the sorting helper and would otherwise fail on the marker. Lambda keys keep their behaviour
  (a missing attribute still raises). A stored `None` is a real value and is not dropped.
- Plan IR: `{"type": "expression", "op": "attr", "attr": ...}` and `{"type": "expression", "op": "key"}`.
  Only exactly `Attr` and `Key` are structured; user subclasses stay `Opaque` (same rule as 006).
  `IR_VERSION` is raised to 3.
- Names: `Attr` and `Key`, exported only from the new expressions module.

## Consequences
**Positive:** Typical ranking and top-k queries produce plans without `Opaque`, a precondition for
translating them to `ORDER BY` / `LIMIT`. `Expression` is the base for later structured partitioning and
transform expressions.
**Negative:** `Attr` only resolves into attribute functions: on scalar values, or with a misspelt
attribute, every row is dropped silently and the result is empty (same rule as predicates in 004).
`subset` can return fewer than `k` items. Mixing `None` with numbers in a ranking attribute raises a
`TypeError` while sorting, as with `Min`. Plans written with IR version 2 are rejected.

## Out of scope & deferred
- Promoting the definedness check onto the FDM type (`tf.defined(path)` / `tf.get(path, default)`), deferred
  in 004 until a third consumer appears; `Attr` is that third consumer. Re-deferred to its own MR.
- Ranking by several keys with a direction per key (`ORDER BY a, b DESC`).
- Structured partitioning expressions (e.g. a case distinction or a `Predicate` as partition key).
- Structured transform expressions (computed attributes).
- Guarding lambda ranking keys against `AttributeError` (as `where` does).
- Keys such as `len(item.value)`.

## Alternatives rejected
- Reuse `Ref` as ranking key: it resolves on a value, not an item, and has no access to the key.
- Introspect lambdas (syntax tree / bytecode): fragile; rejected by the user.
- Sort undefined attributes first or last (SQL `NULLS FIRST` / `LAST`): contradicts 004 (no NULL semantics).
- Raise an error on an undefined attribute: contradicts 004 (absence never raises).

<!-- notes below: skill never overwrites past this marker -->

## Note 2026-10-19 (five-reviewer pass)

- Wording correction for the first Decision bullet: lambdas are not replaced; ranking-key lambdas get a
  structured, serializable alternative (dedicated expression objects), and lambdas stay allowed.
- `Attr` decides presence with `key in tf`. For an AF with a default value function but no declared domain,
  `af[key]` yields the default while `key in af` is `False`, so `Attr` drops such items although an
  equivalent lambda would rank them. Same behaviour as predicates (004); documented in the rank tutorial and
  noted at the deferred definedness-promotion item in TODO.md.

# 009 — Structured expressions for transform_items
**Date:** 2026-10
**MR:** https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/merge_requests/89
---
## Context

The goal is FQL plans that a backend can translate to SQL. After 008, `transform_items` is still opaque: its
function is a lambda. Its real use is to build a new tuple per item from computed values, e.g.
`{"name": item.value.name, "score": item.value.score * 2}` (SQL: `SELECT name, score * 2 AS score`). Whole-AF
`transform` is mostly used for summaries, which `aggregate` already covers structurally. FQL has no arithmetic
expressions yet. The plan was challenged by five independent reviewers and revised before approval.

## Decision

- **Scope:** only `transform_items` gets a structured function; `transform` stays a lambda operator.
- **`Record(**fields)`** builds a new tuple with exactly the given fields, in the given order (like SQL
  `SELECT`). Fields are flat values: `Attr`, `Key`, an arithmetic expression or a `Predicate` (bool). No
  constants, no nested `Record` / `Composite`. Field names: non-empty strings without `.` or `__` (otherwise
  `Attr` could never read them back). A field whose value is `UNDEFINED` is absent from the result tuple; if
  every field is absent the empty tuple is kept. Result tuples are unfrozen, like those of a lambda.
- **`transform_items`** accepts a `Record` as structured function: each item maps to a new tuple under the
  same key; without an output factory the result is always a new RF (never the in-place mode). Any other
  expression or a bare `Predicate` is rejected at construction. Lambdas are unchanged. `Record` is meant only
  for `transform_items` (documented, not enforced elsewhere).
- **Arithmetic:** `Add`, `Sub`, `Mul`, `Div`, `Mod` (explicit classes sharing one private base) plus the Python
  operators `+ - * / %` on expressions; comparison operators are not overloaded. Operands are `Attr`, `Key`,
  arithmetic expressions or `int` / `float` literals (no `bool`, `str`, `None`, NaN, infinity); anything else is
  rejected at construction. Evaluation: any `UNDEFINED` operand gives `UNDEFINED` (absence wins over other
  errors); division or modulo by zero gives `UNDEFINED`; a non-numeric operand value (e.g. a stored `None` or a
  string) raises `TypeError`; otherwise Python semantics.
- **Python, not SQL, is the reference;** differences are documented with their SQL translation: `/` is true
  division (`CAST(a AS DOUBLE PRECISION) / b`), `%` takes the sign of the divisor (`((a % b) + b) % b`),
  division by zero (`a / NULLIF(b, 0)`); integers do not overflow, floats may give infinity or overflow.
- **Arithmetic expressions also work as ranking and partition keys** (e.g. `partition(..., Attr("n") % 2)`),
  with the existing rule that an `UNDEFINED` key drops the item.
- **Plan IR:** arithmetic `{"type": "expression", "op": "add", "left": ..., "right": ...}`; record
  `{"type": "expression", "op": "record", "fields": [{"name": ..., "value": ...}, ...]}` — an ordered list,
  because JSON output sorts dict keys. Exact-type rule as before; malformed records (including duplicate field
  names) raise `ValueError`. The plan parameter of `transform_items` is named `mapping_function`. No
  `IR_VERSION` bump: only new operations are added, no existing record changes meaning, and an older reader
  rejects them loudly.

## Consequences
**Positive:** typical per-item reshaping (`SELECT expr AS name`) produces plans without `Opaque`; arithmetic
becomes available as structured ranking and partition keys.
**Negative:**
- `Attr("a") + 1` is an expression object, not a number.
- A field that cannot be computed (missing attribute, typo, division by zero) is silently absent.
- An absent field and a stored `None` both map to `NULL` in a SQL result column (008 gap); a stored `None` in
  arithmetic raises locally while SQL would give `NULL`.
- Constraints and references of the input are not carried over to the new RF.
- `partition` with a `Record` key silently puts every tuple into its own partition (documented only).

## Out of scope & deferred
- Case distinctions (`CASE WHEN`), string functions and concatenation.
- Constant (literal) fields, nested records, unary minus.
- Whole-AF `transform`; an "extend the existing tuple" variant.
- `partition_by_aggregate`'s per-item aggregation via `Record`.
- Overloading comparison operators.

## Alternatives rejected
- Introspecting lambdas: fragile; rejected earlier by the user.
- Overloading `==` / `<`: clashes with Python equality; predicates exist.
- Extending the existing tuple instead of building a new one: user chose `SELECT` semantics.
- Dropping the whole item on a missing attribute: user chose field-level absence (as aggregates in 004).
- Any expression as `transform_items` function: creates RFs of plain values and a third absence rule.
- `str` / `bool` / `None` literals: not translatable to SQL arithmetic, or NULL-like.
- Fields as a JSON dict: field order would be lost.
- One generic `BinOp` class: user chose explicit classes.
- Raising on division by zero: not translatable uniformly; absence fits 004.
- `type(input)()` as default container: wrong for DBF inputs.
- An `IR_VERSION` bump: no existing record changes meaning.

<!-- notes below: skill never overwrites past this marker -->

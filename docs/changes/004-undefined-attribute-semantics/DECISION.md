# 004 — Undefined-attribute semantics (no SQL NULL)
**Date:** 2026-09
**MR:** https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/merge_requests/76
---
## Context

The JOB benchmark data has attributes that are absent for some tuples (what SQL would
store as `NULL`, e.g. `cast_info.person_role_id`). In FDM such a value is simply a
missing (key, value) mapping — the attribute function is *undefined* for that key.
"NULL" is SQL terminology; here the phenomenon is *absence*, not a special value.

Several operators read attributes directly (`item.value[attr]`) and therefore raised
`AttributeError` on an undefined attribute instead of doing something meaningful.
Failing tests documented this in aggregates, predicates/`where`, and the join.

The project explicitly does not want to reproduce SQL's relational assumptions, in
particular NULL handling and its three-valued logic.

## Decision

One principle across all operators: **a tuple whose attribute function has no mapping
for the accessed key does not contribute to, and is not selected by, an operator that
reads that key.** Absence propagates; it never becomes a NULL value and never raises.
A stored `None` is a real value (FDM has no NULL), so it participates like any other
value and gets no skip privilege.

Detection is exception-free and path-aware where the accessed path is known: a
per-segment key-membership test (`key in af`) rather than `try/except`. Only an opaque
Python callable (a `where` lambda), whose accessed path cannot be inspected, is guarded
with `try/except AttributeError`.

Concrete semantics:

- **Predicates** are two-valued, not SQL three-valued. A comparison / `Like` / `In`
  over an undefined attribute is `False`; `And`/`Or`/`Not` stay plain boolean, so
  `Not(<undefined>)` is `True`. The distinction "absent" vs "present but non-matching",
  which SQL recovers with three-valued logic, is instead recovered by explicit
  `Exists` / `Missing` predicates, because in FDM definedness is structural and directly
  queryable (e.g. `And(Exists("name"), Not(Eq("name", "Hugo")))`).
- **`where`** drops a tuple when a lambda predicate raises `AttributeError` on a missing
  attribute (structured predicates and kwargs are already exception-free).
- **Aggregates** fold only over mapped values. An empty bag makes the aggregate
  undefined, so its key is left out of the output TF — `Sum` included (absent, not `0`).
  `Count` is the exception: `Count("attr")` counts mapped values (SQL `COUNT(col)`),
  `Count()` counts tuples (`COUNT(*)`); zero is a valid count.
- **Join**: a source tuple with an undefined reference has no edge and is dropped from
  the (inner) join result. Enforced in the semijoin reduction (a tuple with no reference
  cannot match) and defensively in the walk (a missing forward reference yields no row).

## Consequences

**Positive:**
- One coherent, SQL-NULL-free rule shared by predicates, `where`, aggregates and join.
- No three-valued logic: boolean algebra stays ordinary; `Exists`/`Missing` make absence
  a first-class, inspectable condition instead of hiding it inside every comparison.
- Exception-free, path-aware resolution (`_resolve_attr_path`) reused by predicates and
  aggregates; only opaque lambdas need `try/except`.

**Negative:**
- `Not(<undefined>)` selecting the tuple diverges from SQL and can surprise users who
  expect SQL semantics; mitigated by `Exists`/`Missing`.
- The lambda `try/except` in `where` masks a genuine `AttributeError` (e.g. a typo) as
  "no match"; structured predicates avoid this.
- A stored `None` participating means `Min([None, 1972])` raises `TypeError` (wrong type)
  rather than skipping — intentional, but a sharp edge.

## Out of scope & deferred

- **partition / group_by** (`partition.py:90`) reads grouping keys the same way and will
  still raise on a missing key; consistent handling is deferred (open question: a missing
  grouping key forms its own group vs. the tuple is dropped).
- Promoting the path-aware definedness check to a method on the FDM type
  (`tf.defined(path)` / `tf.get(path, default)`) once a third consumer appears; for now
  aggregates import `_resolve_attr_path` from the predicates module.
- Predicate pushdown into the store remains explicitly out of scope.

## Alternatives rejected

- **SQL three-valued logic (UNKNOWN through And/Or/Not, select only on TRUE):** matches
  SQL exactly but reintroduces the NULL semantics the project rejects, and is more code.
- **`try/except AttributeError` everywhere:** simplest, but the user judged it costly and
  inconsistent; kept only for opaque lambdas that cannot be pre-checked.
- **Treating a stored `None` as "missing":** would smuggle SQL-NULL back in; `None` is a
  real value in FDM.
- **`Sum` over an empty bag returning `0`:** defensible as the additive identity, but
  inconsistent with Min/Max/etc.; absence chosen for uniformity.
- **A dedicated `MissingAttributeError(AttributeError)` raised only on true absence:**
  proposed to narrow the catch, but unnecessary once resolution is exception-free.

<!-- notes below: skill never overwrites past this marker -->

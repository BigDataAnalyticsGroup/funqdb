# 002 — key_to_value operator for group identity
**Date:** 2026-08
**MR:** https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/merge_requests/65
---
## Context

After `group_by` / `group_by_aggregate`, the grouping attribute lives *only* in the
result RF's key domain, never in the aggregated TF value:

    group_by_aggregate(customers, "name", count=Count("name"))
    # → RF{ "Tom": TF{count:2}, "John": TF{count:1}, ... }
    #        ^^^^^ "name" is the key, not a value attribute

This is faithful to the FDM (a group's identity *is* its key — the result is a function
from group key to aggregate). But `rank_by` replaces the key domain with ℕ by design
(`rank.py`), so any operator that re-keys discards the group identity entirely. `rank.py`'s
docstring already instructs users to "project the key into the value first" — yet no
operator can do that today: `project()` operates on values, and the grouping attribute is
not a value attribute. MR 65 documents both facets with two `xfail` tests (one about
`project()`, one about `rank_by`).

Constraints in play: funqDB must not become "more SQL-ish"; the group key is the single
source of truth for group identity and must not be silently duplicated.

## Decision

Add a dedicated, explicit operator `key_to_value(input_function, attribute)` that lifts each
item's **key** into its **value** under a caller-named attribute. `group_by_aggregate` is
left unchanged. The key remains the single source of identity; it is copied into the value
only at the explicit moment the caller is about to discard the key domain (e.g. before
`rank_by`):

    group_by_aggregate(customers, "name", count=Count("name")) \
      | key_to_value("name") \
      | rank_by(ranking_key=lambda i: i.value.count)
    # group identity survives ranking, carried in the value

Design specifics:
- Output preserves the **input's concrete type** and key domain (`type(input)()`, generic
  `Operator[IN, IN]`) — it is *not* forced to `RF`. Precedent: `project()`. Forcing `RF`
  would be the SQL-ish move; the key domain is untouched here.
- The input AF is not mutated: each frozen value is copied, unfrozen, augmented, re-frozen.
- Collision is rejected loudly: if `attribute` already exists as a **stored or computed**
  key in the value, raise `ValueError` (never silently shadow — the very failure the branch
  name refers to). A value that is not a `DictionaryAttributeFunction` raises `TypeError`.
- Single-key only for now: a composite tuple key (from multi-attribute `group_by`) is stored
  verbatim under the one attribute; splitting a tuple into several named attributes is
  deferred.
- Implemented by delegating to `transform_items` (mirrors how `group_by_aggregate` delegates
  to `group_by` + `transform_items`), rather than re-implementing the freeze machinery.

The two MR-65 tests are rewritten to assert the resolved semantics, `xfail` removed:
- The `rank_by` test asserts identity survives in the **value** (`item.value.name` and
  `count` present). The key-domain-preservation check is dropped by design — ℕ-rekeying is
  the whole point of `rank_by`.
- The `project()` test asserts the **correct** FDM behaviour: group keys are identity and
  are not removable by `project()` (which trims values); `key_to_value` is the supported way
  to obtain a droppable value attribute. This is a clarified understanding, not a bug fix.

## Consequences

**Positive:** No duplication inside `group_by_aggregate` — group identity has a single
source (the key). Fills the capability `rank.py` already promised but could not deliver.
Faithful to the FDM (key = identity); avoids SQL-style renumbering. Reuses existing
`transform_items` machinery. Partially unblocks the `index_by` TODO.

**Negative:** Callers must add an explicit `key_to_value` step before a domain-replacing
operator — deliberate, so no hidden denormalization happens. New operator surface to
document and maintain. The operation is a controlled denormalization; its docstring must
scope it to "use before an operator such as `rank_by` that replaces the key domain" so it is
not reached for where the key domain is otherwise preserved (there it would be pure
redundancy).

## Out of scope & deferred

- Composite/tuple key split into multiple named value attributes (Tier 2).
- Full `index_by(rf, key_function)` operator — re-key by an arbitrary function (TODO.md:23);
  `key_to_value` is a building block toward it.
- A relational-algebra-style alias/symbol for the operator.
- Any change to `group_by` / `group_by_aggregate` output semantics.

## Alternatives rejected

- Duplicate the grouping attribute into the value *inside* `group_by_aggregate` — redundant,
  two sources of truth for one identity; rejected by the user.
- Renumber `group_by_aggregate`'s output to ℕ keys (MR 65's literal suggestion) — SQL-ish,
  and breaks `aggregates["Tom"]` lookup plus an existing green test.
- Documentation only (show the manual `transform_items` pattern) — the user wants a
  dedicated, discoverable operator.

## Scope-shift 2026-08-13

The collision guard was broadened during implementation. The Decision above scoped it to a
"stored or computed" key; the shipped guard also rejects a **domain-backed default** key
(one that resolves only through a value's `default=` over its `domain=`). It uses the value's
public `__contains__` (which already covers stored ∪ computed ∪ resolvable-domain), so all
three key kinds are refused uniformly. This closes the shadowing hole that gives the branch
its name and is covered by a dedicated test.

## Scope-shift 2026-08-14

The Tier-2 item listed under *Out of scope & deferred* — "Composite/tuple key split into
multiple named value attributes" — is now **in scope** for MR 65. It was raised on the MR by
`lefo00004` (TPC-H Q1 motivation: after `group_by(af, ("l_returnflag", "l_linestatus"))` the
positionally-ordered tuple key must be recoverable as named, droppable value attributes).

`key_to_value`'s `attribute` parameter is widened from `str` to `str | tuple[str, ...]`:

- `str` — unchanged: the **whole** key is stored verbatim under that one name (scalar or
  composite tuple). Backwards-compatible; the verbatim-tuple test stays valid.
- `tuple[str, ...]` — **spread**: component `i` of the key lands under name `i`. Requires the
  key to be a tuple of exactly the same length, else `ValueError`. `("name",)` spreads a
  one-element tuple key. The names tuple must be non-empty and its names unique (both
  `ValueError`), and the per-name shadowing guard is applied to every target name.

On both paths an empty attribute name (`""`) is rejected with `ValueError` (this also tightens
the pre-existing `str` path, which previously accepted it).

Rejected here: the alternative "let `group_by` build an `AttributeFunction` used as the key"
(the MR's second proposal). It touches how `group_by` constructs the key domain (FDM AF-as-key
domain, lookup/equality semantics) rather than `key_to_value`, and is deferred to its own
design pass.

<!-- notes below: skill never overwrites past this marker -->

# 001 — cogroup grouping keys

Allow `cogroup` to specify the grouping attribute(s) per input relation — the capability `group_by` already has,
generalised to the multi-relation case. This turns `cogroup` into a proper equi-join, exactly as its docstring
anticipates ("naturally extended to a classical join operation if the grouping keys can be customized").

## Problem

- `group_by(input_rf, *aggregate_keys)` (`fql/operators/partition.py:72`) lets the caller choose the grouping
  attribute(s) and preserves all matching tuples, because `partition._compute` writes `output[partition_key][item.key]
  = item.value` — keyed by the original `item.key`, so duplicates with the same grouping value coexist losslessly.
- `cogroup(input_dbf, *, output_factory, output_factory_nested)` (`fql/operators/set_operations.py:89`) groups
  *only* by each item's identity key (`item.key`) and writes `output[item.key][af.uuid] = item.value` — a single cell
  per (group, relation). There is no way to pick a grouping attribute, and the single cell cannot hold multiple matches.

A join over differently-named attributes (`users.dept = departments.id`) is impossible today.

## Tier 1 — minimal solution

### Approach

Add an optional mapping **relation-name → grouping attribute(s)**. The map's keys are the keys under which each RF sits
in the input DBF; the value is a `str` (single attribute) or `tuple[str, ...]` (composite key) for that relation. Both a
plain `dict` and an FDM `AttributeFunction` (RF) are accepted — both support `spec[relation_name]` indexing, so the
operator is agnostic (`spec = grouping_keys[relation.key]`). Accepting an AF follows the project rule to prefer a
`DictionaryAttributeFunction` over an ad-hoc dict.

When grouping by attribute, the leaf becomes a **set** (an RF) of matching tuples keyed by their original `item.key`, so
duplicates within one relation survive — mirroring how `group_by` preserves them via `partition`.

The co-group key for relation *i* is `item.value[attr_i]` (or the tuple of values). Equal values across relations land
in the same co-group — i.e. an equi-join on those attributes:

```
users grouped on "dept", departments grouped on "id":
  5 → users       : { 1: {...}, 2: {...} }   # all users with dept == 5
      departments : { 5: {...} }             # department with id == 5
  8 → users       : { 3: {...} }
      departments : { 8: {...} }
```

### Signature

```python
def __init__(self, input_function, grouping_keys=None, *,
             output_factory, output_factory_nested, output_factory_leaf=None):
```

- `grouping_keys`: second positional-or-keyword param, default `None`. Existing callers
  `cogroup(db, output_factory=..., output_factory_nested=...)` keep working unchanged (everything after `input_function`
  is already passed by keyword), so the new positional slot stays `None`.
- `output_factory` / `output_factory_nested` stay keyword-only (anything after `*` / `*args` is). They keep their
  `assert ... is not None` checks.
- `output_factory_leaf`: new **optional** keyword-only factory for the innermost per-(group, relation) set; defaults
  internally to `lambda _: RF(frozen=False)`. **No** `is not None` assert (that would defeat the default).
  `frozen=False` because the operator mutates the leaf after creating it.

### Behaviour (two modes)

- **`grouping_keys is None` (default): byte-identical to current behaviour** — `output[item.key][af.uuid] = item.value`.
  Keep the existing variable names / inline form in this branch to preserve the byte-identical claim and minimise the
  diff (Surgical changes). `output_factory_leaf` is never invoked.
- **`grouping_keys` provided (attribute mode):** for each relation, `spec = grouping_keys[relation.key]`; build the
  group key from `item.value[spec]` (`str`) or `tuple(item.value[a] for a in spec)` (`tuple`); write
  `output[group_key][af.uuid][item.key] = item.value`, where the innermost set comes from `output_factory_leaf`.

### Preconditions (documented; fail-fast, no SQL-NULL semantics)

- Every input relation must have an entry in `grouping_keys`; a missing entry raises `KeyError`.
- Each item value must be a TF supporting attribute indexing; a missing attribute / non-subscriptable value raises the
  natural `KeyError` / `TypeError`.
- The per-relation extracted values must be comparable and of equal arity (composite tuples: same length and order).

Keep the existing `assert len(input_dbf) >= 2`.

### Files to modify

- `fql/operators/set_operations.py` — extend `cogroup.__init__` (add `grouping_keys`, `output_factory_leaf`) and
  `_compute` (resolve per-relation spec, build group key, set-leaf in attribute mode). Default branch untouched.
- `tests/fql/operators/test_set_operators.py` — two **new** tests, each `@pytest.mark.needs_review_new` with per-line
  comments. `test_cogroup` (default mode) stays untouched. Both new tests pass `grouping_keys=` (a kwarg the current
  signature rejects), giving an honest "fails before, passes after" signal:
  1. `test_cogroup_by_attribute` — reuse `_create_testdata`; co-group `users` + `customers` on `name` via
     `{"users": "name", "customers": "name"}`; assert `result["Tom"][customers.uuid].keys() == {1, 2}` (load-bearing
     duplicate preservation — fixture has customers 1 and 2 both `name="Tom"`), and a non-duplicate group (`"John"`) has
     single-key leaves. Pass `grouping_keys` once as a `dict` and once as an `RF` to cover both accepted forms.
  2. `test_cogroup_join_heterogeneous_schema` — small **inline** purpose-built fixture (the existing
     `_create_testdata` cannot express it: `users.department` is a swizzled TF reference, not a scalar comparable to
     `departments` keys). Two RFs with distinct attribute names but comparable scalar values, e.g.
     `left = RF({1: TF({"a": "x"}), 2: TF({"a": "y"})})`,
     `right = RF({10: TF({"b": "x"}), 11: TF({"b": "x"}), 12: TF({"b": "z"})})`;
     `cogroup(DBF({"left": left, "right": right}), {"left": "a", "right": "b"}, ...)`; assert group `"x"` holds
     `left` key `{1}` and `right` keys `{10, 11}` (join + duplicate preservation on the right), `"y"` only `left`,
     `"z"` only `right`.
- `SPEC.md` — extend the `cogroup` line (line 251) to note attribute-based grouping / equi-join.

### Reuse

- `partition` / `group_by` (`fql/operators/partition.py:29,72`) — the single-vs-tuple grouping-key idiom and the
  key-preserving leaf pattern.
- Existing `cogroup._compute` loop structure (`fql/operators/set_operations.py:121`) — default branch unchanged.
- `_create_testdata` (`tests/lib.py:28`) — fixture for the same-name test (customers 1 & 2 share `name="Tom"`).
- `DBF` already accepts scalar and tuple keys (proven by `group_by` writing tuple partition keys into a DBF).

## Tier 2 — optional extensions (separate MRs)

- **(a) Shared grouping-key-function builder** — extract the single-vs-tuple logic so `group_by` and `cogroup`
  deduplicate it. Benefit: DRY. Cost: touches `group_by` (separate concern), small.
- **(b) Freeze cogroup outputs** — align `cogroup` with `partition`/`group_by`, which freeze their outputs. Benefit:
  consistency. Cost: behaviour change to the default mode with its own test obligation; deliberately deferred.
- **(c) Revisit `len(input_dbf) >= 2`** — the existing TODO (`set_operations.py:124`) notes `len == 1` is standard
  grouping; single-relation attribute grouping becomes meaningful with this feature.

## Key tradeoffs

- Two leaf shapes by mode (raw value in default mode, set-RF in attribute mode) instead of one uniform shape →
  maximally backward compatible, slightly asymmetric.
- A third factory parameter (`output_factory_leaf`) widens the API marginally; optional and only relevant in
  attribute mode.
- The map is keyed by DBF relation name and self-documenting; the alternative positional form was rejected (see
  `ALTERNATIVES.md`).

## Out of scope

- Query pushdown (intentionally not implemented in the store).
- Freeze alignment (Tier-2 b).
- The schema-design concern the user raised in passing about co-grouping AFs with mismatched schemas in general.

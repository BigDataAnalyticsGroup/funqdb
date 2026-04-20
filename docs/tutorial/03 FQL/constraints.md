## Constraint Operators

> **Status:** First building block of the revised [join](join.md) pipeline.
> The constraint operators are in place; the flattening `join` operator
> itself follows in a subsequent MR.

FQL treats join specifications as **constraints on a DBF**. Before you run
a join, you assemble a DBF that already carries every reference and
cross-relation predicate the join needs. The `join` operator (to come) then
simply flattens tuple combinations that satisfy those constraints.

> **Heads-up for reduction semantics:** references
> (`ForeignValueConstraint` via `add_reference`) drive the Yannakakis
> reduction done by `semijoin` and `subdatabase`. Predicates
> (`JoinPredicate` via `add_join_predicate`) do **not** — they are
> evaluated with pushdown inside the forthcoming flattening `join`
> operator, as early as all their relations are present in the
> current partial tuple combination. A future extension could lift
> structured equi-predicates into extra semijoin-reduction steps; that
> is sketched in the `JoinPredicate` class docstring but not
> implemented here.
> See [Evaluation model](#evaluation-model--who-consumes-a-joinpredicate-and-who-ignores-it).

Four FQL operators manage that assembly in a pipeline-friendly, plan-
extractable way:

| Operator | Form | Description |
|:---------|:-----|:------------|
| [add_reference](#add_reference) | DBF → DBF | Add a cross-relation reference (FK) |
| [drop_reference](#drop_reference) | DBF → DBF | Remove a reference |
| [add_join_predicate](#add_join_predicate) | DBF → DBF | Add an arbitrary cross-relation predicate |
| [drop_join_predicate](#drop_join_predicate) | DBF → DBF | Remove predicate constraints |

All four operators return a **new DBF with freshly cloned RFs**; the input
DBF is never mutated. Intra-DBF references (`ForeignValueConstraint` and
its reverse) are re-bound to the cloned RFs so the output DBF is
internally consistent.

## add_reference

```python
from fql.operators.constraints import add_reference

augmented: DBF = add_reference(
    dbf, source="users", ref_key="dept", target="departments"
).result
```

Installs a `ForeignValueConstraint` on the cloned source RF and a
`ReverseForeignObjectConstraint` on the cloned target RF — the same effect
as `RF.references()`, but on clones so the input DBF stays intact.

### Pipeline composition

`add_reference` integrates naturally with the rest of FQL:

```python
from fql.operators.semijoins import semijoin

reduced: DBF = semijoin[DBF, DBF](
    add_reference(dbf, source="users", ref_key="dept", target="departments"),
    reduce="departments",
    by="users",
    ref_key="dept",
).result
```

No intermediate `.result` call is necessary — operators compose lazily.

## drop_reference

```python
from fql.operators.constraints import drop_reference

out: DBF = drop_reference(
    dbf, source="users", ref_key="dept", target="departments"
).result
```

Removes the `ForeignValueConstraint` on the named source RF *and* the
matching `ReverseForeignObjectConstraint` on the named target RF (both
scoped to the given `ref_key`). Raises `ValueError` if the DBF does
not contain `source`, does not contain `target`, or does not carry a
matching FVC — silent no-ops on typos are rejected by design.

## add_join_predicate

Arbitrary cross-relation predicates live alongside references as
constraints on the DBF:

```python
from fql.operators.constraints import add_join_predicate
from fql.predicates.predicates import Gt, Ref

# Preferred form — structured predicate: attribute-to-attribute
# comparison using Ref(), survives plan extraction as structured IR.
out: DBF = add_join_predicate(
    dbf,
    "users", "departments",
    predicate=Gt("users.age", Ref("departments.min_age")),
    description="age_gate_v1",
).result

# Fallback — plain lambda. Equivalent result, but the predicate is
# Opaque in the extracted plan.
out: DBF = add_join_predicate(
    dbf,
    "users", "departments",
    predicate=lambda t: t["users"]["age"] > t["departments"]["min_age"],
    description="age_gate_v1",
).result
```

### Structured predicates

Structured predicates from `fql.predicates.predicates` (`Eq`, `Gt`, `Lt`,
`Gte`, `Lte`, `Like`, `In`, `And`, `Or`, `Not`) work directly as join
predicates. Use `Ref(attr_path)` on the right-hand side to compare two
attribute paths instead of a path against a literal. Paths take the form
`relation.attribute` (e.g. `"users.age"`), consistent with the dotted
attribute syntax used in `.where()` and the structured-predicate docs.

The `JoinPredicate` wraps the incoming `{relation_name: TF}`
dict in a frozen TF before calling the predicate, so both styles work —
`tuples["users"]["age"]` for lambdas and `tuples.users.age` / structured
paths for structured predicates.

### description

`description` is an optional human-readable identifier used by
`drop_join_predicate` to match across serialization boundaries (two
deserialized lambdas cannot be identity-compared after a round trip).

### Evaluation model — who consumes a JoinPredicate, and who ignores it

> **Important:** `JoinPredicate`s are **not** consumed by the
> Yannakakis reduction. `semijoin` and `subdatabase` inspect only
> `ForeignValueConstraint` edges when building their reference graph;
> any `JoinPredicate` you register on a DBF is completely ignored
> during reduction. The forthcoming flattening `join` operator (MR 2)
> is the sole consumer — it evaluates each `JoinPredicate` with
> **pushdown during its tuple walk**, as soon as every relation the
> predicate names appears in the current partial combination. The
> pruning effect is therefore early within the join, not a single
> post-materialization pass over fully-assembled combinations.

Consequence: a `JoinPredicate` does **not** prune any tuples during
`subdatabase(…)`. If you need reduction-time filtering, use a
reference (`add_reference`) — references drive Yannakakis; predicates
don't.

Concretely, what each stage does:

| Stage | Reads `ForeignValueConstraint`? | Reads `JoinPredicate`? |
|:------|:--------------------------------|:-----------------------|
| `RF.__setitem__` / `DBF.__setitem__` | yes (FVC blocks if target missing) | **no** — `__call__` returns True |
| `semijoin` (single step) | yes — drives the direction / reduction | **no** |
| `subdatabase` (Yannakakis cascade) | yes — builds the reference graph | **no** |
| `join` (MR 2, flattening) | yes — guides tuple reconstruction | **yes** — pushdown during the walk, fired as soon as all named relations are present |

**Why not also in the reduction?** Arbitrary callables cannot be
introspected into semi-join keys, and the Yannakakis full-reducer
property does not generalise to arbitrary θ-predicates (`<`, `>`, …).
So the reduction stage stays reference-based. A **future extension**
could lift structured equi-predicates (e.g.
``Eq("a.x", Ref("b.y"))`` where neither side is an FDM reference)
into an extra hash-semijoin step that augments the
reference-driven Yannakakis cascade. θ-predicates remain pushdown-
during-walk only. This extension is sketched in the `JoinPredicate`
class docstring but **not implemented** in MR 1 or MR 2.

### How `evaluate` is called

The constraint is **not** evaluated when the DBF is mutated:
`JoinPredicate.__call__` returns `True` unconditionally so that
adding or freezing RFs never invokes the user's predicate.
Evaluation happens through `JoinPredicate.evaluate(tuples)`, which
the join operator (MR 2) will call with a tuple combination it
wants to test.

## drop_join_predicate

Three mutually exclusive matching modes:

```python
from fql.operators.constraints import drop_join_predicate

# 1) by description — preferred when you know the tag
drop_join_predicate(dbf, description="age_gate_v1").result

# 2) by predicate object identity — when you still hold the callable
drop_join_predicate(dbf, predicate=pred_instance).result

# 3) by matcher callable — for bulk or conditional drops
drop_join_predicate(
    dbf, matcher=lambda c: c.relations == ("users", "departments")
).result
```

Exactly one of `description`, `predicate`, or `matcher` must be supplied.

**No-match policy**: `description` and `predicate` modes raise
`ValueError` if no matching `JoinPredicate` exists — this catches typos
and stale handles. `matcher` mode is the explicit *maybe-nothing*
escape hatch: it drops whatever the callable selects, including zero,
and never raises. Reach for matcher mode when you want idempotent
drop semantics.

## Plan extraction

All four operators are plan-extractable via `to_plan()`. Parameters land
as plain JSON-friendly data:

```python
op = add_reference(dbf, source="users", ref_key="dept", target="departments")
op.to_plan().root.params
# → {"source": "users", "ref_key": "dept", "target": "departments"}
```

For `add_join_predicate`, structured predicates are surfaced as
structured IR (same treatment as in `filter_values`). Lambda predicates
are rendered as `Opaque` markers — this is the core reason to prefer
structured predicates whenever the comparison is expressible that way.

## Relationship to RF.references()

`RF.references(ref_key, target_rf)` is the eager counterpart to
`add_reference`: it mutates the source and target RF in place. Use
`references()` when building an RF from scratch; use `add_reference`
when you want to layer constraints on top of an existing DBF without
mutating it — e.g. in a pipeline that branches.

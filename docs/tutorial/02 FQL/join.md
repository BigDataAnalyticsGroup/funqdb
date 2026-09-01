## Join

> **Status:** POC (feature 003 of the join-rework). Reference-based joins on any
> connected graph that is an **undirected tree** in any edge orientation —
> zero, one, or several pure sources. Arbitrary `JoinPredicate` pushdown and
> diamond / non-tree / disconnected / cyclic graphs raise `NotImplementedError`
> with a pointer at the follow-up MR (004).

### Form: DBF → RF

```python
out: RF = join(dbf: DBF).result
```

`join` consumes a constraint-decorated DBF (references assembled via
[`add_reference`](constraints.md) or the eager `RF.references()`) and
returns an RF indexed by row. Each row is a **nested TF** whose
top-level keys are the relation names and whose values are the
original relation TFs, shared across rows by object identity:

```python
out[0] = TF({"users": u1_tf, "departments": d1_tf})
out[1] = TF({"users": u2_tf, "departments": d2_tf})
out[2] = TF({"users": u3_tf, "departments": d1_tf})   # d1_tf shared with out[0]
```

No value denormalization. This is the entire design difference to the
classical SQL `SELECT * FROM a JOIN b ON …`: **SQL copies attributes
per row, FDM preserves object identity**. Two rows whose reference
chain lands on the same department tuple share *exactly one*
department TF in memory, and `out[0]["departments"] is out[2]["departments"]`
holds by design.

### Minimal example

```python
from fdm.attribute_functions import TF, RF, DBF
from fql.operators.constraints import add_reference
from fql.operators.joins import join

departments = RF({"d1": TF({"name": "Dev"}),
                  "d2": TF({"name": "Sales"})}, frozen=True)
users = RF({"u1": TF({"name": "Alice", "dept": departments["d1"]}),
            "u2": TF({"name": "Bob",   "dept": departments["d2"]}),
            "u3": TF({"name": "Carol", "dept": departments["d1"]})},
           frozen=True)

dbf = DBF({"users": users, "departments": departments}, frozen=True)
dbf = add_reference(dbf, source="users", ref_key="dept",
                    target="departments").result

out: RF = join(dbf).result
for row in out:
    print(row.key,
          row.value["users"]["name"],
          "->", row.value["departments"]["name"])
#  0 Alice -> Dev
#  1 Bob   -> Sales
#  2 Carol -> Dev

assert out[0]["departments"] is out[2]["departments"]   # zero-redundancy
```

### Path access on the output

Three equivalent ways to reach a leaf across the nested row:

```python
row["departments"]["name"]       # step-wise __getitem__
row["departments__name"]         # TF's __-path sugar
```

```python
# inside a structured predicate / aggregator (dotted notation):
from fql.predicates.predicates import Eq
Eq("departments.name", "Dev")
```

The last form is what downstream aggregators rely on:
`getattr(row, "departments")` returns the department TF, and
`getattr(departments_tf, "name")` returns the leaf scalar — no
special casing needed, the nested layout falls out naturally.

### Pipeline composition

`join` composes lazily with the other operators — no intermediate
`.result` calls needed:

```python
out: RF = join(
    add_reference(dbf, source="users", ref_key="dept",
                  target="departments")
).result
```

### Glossary: pure source

A **pure source** is a relation with at least one outgoing
`ForeignValueConstraint` and no incoming ones — a node with in-degree 0 and
out-degree ≥ 1 in the directed reference DAG. `join` starts its walk at one
(deterministically `sorted(pure_sources)[0]`, or an explicit `root=`), but the
graph may have **any number** of them: the walk is bidirectional, so a shared
hub referenced by several sources is reached by entering its other edges
backward.

Examples:

```text
  Linear chain (tasks → projects → departments)
      tasks       out=1, in=0   ← pure source
      projects    out=1, in=1
      departments out=0, in=1   (pure sink)

  Single-source star (orders → customers, orders → products)
      orders      out=2, in=0   ← pure source
      customers   out=0, in=1
      products    out=0, in=1

  Multi-source (JOB-style: ci → t, mc → t)
      ci          out=1, in=0   ← pure source
      mc          out=1, in=0   ← pure source
      t           out=0, in=2   (shared hub)
      → TWO pure sources: flattens to the ci × mc fan-in cross product.
```

### How it works: reduce first, reconstruct backlinks second

`join` runs in two clearly separated phases:

1. **Yannakakis reduction.** `join` first runs [`subdatabase`](subdatabase.md)
   to reduce the DBF, so only tuples that participate in the full join survive.
2. **Bidirectional walk.** It then walks the reduced reference tree from the
   start relation. Each edge is followed in one of two ways:
   - **forward** (this tuple is the source): follow the inline pointer
     `source_tf[ref_key]` in O(1) to the single referenced tuple;
   - **backward** (this tuple is the hub): find the source tuples that
     reference it. These *backlinks are not stored anywhere* — they are
     reconstructed on the spot by scanning the reduced source relation and
     matching on **object identity**: `source_tf[ref_key] is hub_tf` (the `is`
     operator, not `==`). This one-to-many step is what produces a fan-in's
     multiple rows.

Two subtleties make the identity scan sound:

- The edge *structure* (which relation references which, via which `ref_key`)
  is read from the join graph of the **original** DBF, not the reduced one:
  `subdatabase` clones RFs with fresh UUIDs, so a graph rebuilt from the reduced
  DBF would drop edges.
- The `is` test works because `subdatabase`'s semijoin preserves each contained
  tuple by object identity — a reduced tuple's `source_tf[ref_key]` is the very
  same instance it was before reduction. (Load-bearing: if that ever became a
  deep copy, the backlink scan would silently find nothing.)

### Scope and deferred follow-ups

In-scope for this MR:

- Zero-edge fallback: a single-RF DBF passes each tuple through as a
  one-entry row.
- Any connected graph that is an **undirected tree** in any edge
  orientation — linear chains, single-source stars, and multi-source
  fan-ins (JOB-style `ci → t, mc → t`) alike, with zero, one, or several
  pure sources. All 2³ = 8 orientations of a given tree flatten to the
  same full join (edge direction only decides which relation is embedded
  where).
- Yannakakis reduction via `subdatabase` (references drive the
  reduction).

Explicitly raises `NotImplementedError` on:

- **`JoinPredicate` on the input DBF** — predicate pushdown is
  deferred. See [constraints.md](constraints.md#evaluation-model--who-consumes-a-joinpredicate-and-who-ignores-it).
- **Diamonds and other non-tree acyclic graphs** — a relation reachable
  via two or more distinct paths (deferred to follow-up 004).
- **Disconnected reference graphs** — that would be a Cartesian product
  across components.
- **Cyclic reference graphs** — rejected by the undirected-tree gate.
- Multi-RF DBFs with zero references at all (also a Cartesian product).

The follow-up MR (004) will add `JoinPredicate` pushdown during the walk
(firing predicates as soon as every participating relation is in the
accumulator) plus diamond / non-tree support via a spanning tree with a
residual-edge consistency check.

### Relationship to the `subdatabase` operator

`subdatabase` and `join` both operate on a constraint-decorated DBF
but return different shapes:

| Operator | Form | What survives |
|:---------|:-----|:--------------|
| [subdatabase](subdatabase.md) | DBF → DBF | Yannakakis-reduced DBF — every relation keeps its own RF, only non-participating tuples are pruned. Nothing materialized per row. |
| [join](join.md) | DBF → RF  | One row per surviving tuple combination across all relations, reached by the bidirectional walk (including fan-in cross products at shared hubs). |

When all you need is the reduced database, stay at `subdatabase` — it
preserves the full normalized structure. Use `join` when a downstream
consumer (e.g. an aggregator like `Min("chn.name")`) wants a
row-indexed view across several relations.

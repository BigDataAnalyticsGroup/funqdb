## Join

> **Status:** Minimal POC (MR 2 of the join-rework). Reference-based
> joins on acyclic graphs with exactly one pure source. Arbitrary
> `JoinPredicate` pushdown and multi-source / non-tree graphs raise
> `NotImplementedError` with a pointer at the follow-up MR.

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

The minimal POC requires the reference graph of the input DBF to have
exactly one **pure source** — a relation with at least one outgoing
`ForeignValueConstraint` and no incoming ones. In graph terms: a node
with in-degree 0 and out-degree ≥ 1 in the directed reference DAG.

Why this matters: FDM references are traversable forward in O(1) via
object identity (`source_tf[ref_key] is target_tf`), but walking
*backward* ("which sources reference this target?") requires scanning
the source RF or building a reverse index. Starting from a pure source
and walking outgoing edges only avoids that cost entirely.

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
      t           out=0, in=2   (shared sink)
      → TWO pure sources: raises NotImplementedError in this MR.
```

Under the hood `join` first runs [`subdatabase`](subdatabase.md) to
Yannakakis-reduce the DBF (so only tuples participating in the full
join survive), then walks from the pure source to materialize one row
per surviving start-tuple.

### Scope and deferred follow-ups

In-scope for this MR:

- Zero-edge fallback: a single-RF DBF passes each tuple through as a
  one-entry row.
- Linear chains, single-source stars, and any tree rooted in a unique
  pure source.
- Yannakakis reduction via `subdatabase` (references drive the
  reduction).

Explicitly raises `NotImplementedError` on:

- **`JoinPredicate` on the input DBF** — predicate pushdown is
  deferred. See [constraints.md](constraints.md#evaluation-model--who-consumes-a-joinpredicate-and-who-ignores-it).
- **Multi-source reference graphs** — e.g. the JOB shape where two
  independent sources share a target.
- **Diamonds and other non-tree acyclic graphs** — same code path.
- Multi-RF DBFs with zero references at all (that would be a
  Cartesian product; also deferred).

The follow-up MR will add `JoinPredicate` pushdown during the walk
(firing predicates as soon as every participating relation is in the
accumulator) plus multi-source / diamond support via a BFS spanning
tree with an on-the-fly reverse index.

### Relationship to the `subdatabase` operator

`subdatabase` and `join` both operate on a constraint-decorated DBF
but return different shapes:

| Operator | Form | What survives |
|:---------|:-----|:--------------|
| [subdatabase](subdatabase.md) | DBF → DBF | Yannakakis-reduced DBF — every relation keeps its own RF, only non-participating tuples are pruned. Nothing materialized per row. |
| [join](join.md) | DBF → RF  | One row per surviving tuple combination of the pure source's tuples and their transitive targets. |

When all you need is the reduced database, stay at `subdatabase` — it
preserves the full normalized structure. Use `join` when a downstream
consumer (e.g. an aggregator like `Min("chn.name")`) wants a
row-indexed view across several relations.

## Flatten

> **Status:** Minimal POC (MR 3 of the join-rework). Converts any RF
> of nested TFs into SQL-style flat rows. Acyclic reference graphs of
> arbitrary depth are supported; cyclic references raise `ValueError`.

### Form: RF → RF

```python
out: RF = flatten(rf: RF).result
```

`flatten` accepts any RF whose row values are TFs with AF-valued
attributes — typically the output of [`join`](join.md) — and returns
a new RF of flat TFs with **dot-separated keys**:

```python
# join output (FDM-native nested shape):
out[0] = TF({"users": TF({"name": "Alice", "dept": TF({"name": "Dev"})}),
             "departments": TF({"name": "Dev"})})

# after flatten:
out[0] = TF({"users.name": "Alice",
             "users.dept.name": "Dev",
             "departments.name": "Dev"})
```

`flatten` deliberately trades FDM's zero-redundancy for SQL-style
convenience: two paths that reach the same leaf value (here
`"users.dept.name"` and `"departments.name"`) both appear in the
output with copies of the scalar. The FDM-native nested shape from
`join` is unchanged and remains the default.

### Minimal example

```python
from fdm.attribute_functions import TF, RF, DBF
from fql.operators.joins import join
from fql.operators.flatten import flatten

departments = RF({"d1": TF({"name": "Dev"}),
                  "d2": TF({"name": "Sales"})}, frozen=True)
users = RF({"u1": TF({"name": "Alice", "dept": departments["d1"]}),
            "u2": TF({"name": "Bob",   "dept": departments["d2"]})},
           frozen=True).references("dept", departments)

dbf = DBF({"users": users, "departments": departments}, frozen=True)

out: RF = flatten(join(dbf)).result
for row in out:
    print(row.value["users.name"], "->", row.value["departments.name"])
#  Alice -> Dev
#  Bob   -> Sales
```

### Key naming

Each output key is a dot-separated path from the top-level attribute
name down to the scalar leaf:

```text
  top-level key  →  "users"          (TF-valued: expanded, not emitted)
  first level    →  "users.name"     (scalar: emitted)
  ref inside TF  →  "users.dept"     (TF-valued: expanded recursively)
  second level   →  "users.dept.name"  (scalar: emitted)
```

Keys whose source values are plain scalars at the top level are emitted
verbatim without any dot prefix:

```python
flat_rf = flatten(RF({0: TF({"x": 1, "y": 2}, frozen=True)}, frozen=True)).result
assert flat_rf[0].keys() == {"x", "y"}
```

### Recursive expansion

AF-valued attributes are expanded recursively to any depth. A linear
chain `tasks → projects → departments` produces keys at all three
levels from a single `flatten` call:

```text
tasks.desc
tasks.project.title
tasks.project.dept.name
projects.title
projects.dept.name
departments.name
```

Computed attributes are materialised as static scalars at flatten time;
domain-backed default attributes are included when the source TF has a
finite domain.

### Pipeline composition

`flatten` accepts any `Operator` producing an RF — no intermediate
`.result` call is needed:

```python
out: RF = flatten(join(dbf)).result
```

The lazy operator chain resolves only when `.result` is accessed.

### Limitations and known edge cases

**Key collisions:** when two distinct paths produce the same
dot-separated key (e.g. both `"users.dept.name"` and
`"departments.name"`), the last path visited wins (depth-first,
left-to-right). This is intentional and documented, not a bug — both
values are the same scalar when the reference graph is consistent.
Attribute names that already contain dots may cause unexpected collisions;
this is a known limitation for this POC.

**Cycle safety:** `flatten` detects reference cycles via a visited-set
guard and raises `ValueError` with the cycle's AF identity and
dot-path. Only acyclic reference graphs are supported:

```python
cyclic_tf = TF({"x": 1}, frozen=False)
cyclic_tf["self"] = cyclic_tf     # cycle
flatten(RF({0: TF({"r": cyclic_tf})}, frozen=True)).result
# → ValueError: flatten: reference cycle detected at AF id=...
```

### When to use `flatten` vs. staying nested

| Situation | Recommendation |
|:----------|:---------------|
| Downstream consumer understands FDM nested rows (aggregators, structured predicates) | Stay with `join` output — object identity and zero-redundancy are preserved. |
| Compatibility with SQL-shaped tools or a pandas DataFrame | Use `flatten(join(dbf))` — accepts the value duplication deliberately. |
| Only one relation in the DBF | `join` + `flatten` still works; output keys are `"relation.attribute"` prefixed. |

### Relationship to the `join` operator

| Operator | Form | Shape |
|:---------|:-----|:------|
| [join](join.md) | DBF → RF | One row per surviving tuple combination; each row is a nested `TF({relation: tf, …})` — FDM-native, zero-redundancy. |
| [flatten](flatten.md) | RF → RF | One flat `TF({"rel.attr": scalar, …})` per row — SQL-style, value duplication accepted. |

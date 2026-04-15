## Set Operations

FQL provides classical set operations on AFs based on their **keys**. All set
operators take a single DBF as input (containing the relations to operate on)
and produce a single output AF.

### intersect

Keeps only items whose key appears in **all** input relations.

```python
from fdm.attribute_functions import TF, RF, DBF
from fql.operators.set_operations import intersect

A = RF({1: TF({"x": "a1"}), 2: TF({"x": "a2"}), 3: TF({"x": "a3"})})
B = RF({2: TF({"x": "b2"}), 3: TF({"x": "b3"}), 4: TF({"x": "b4"})})

result = intersect[DBF, RF](
    DBF({"A": A, "B": B}),
    output_factory=lambda _: RF(),
).result

# result contains only keys present in both A and B:
set(result.keys())  # → {2, 3}
# values come from the first relation (A):
result[2].x         # → "a2"
```

Alias: ``Ʌ`` (visual approximation of ∩).

### minus / difference

Keeps items from the **first** relation whose key does **not** appear in any
subsequent relation.

```python
from fql.operators.set_operations import minus

result = minus[DBF, RF](
    DBF({"A": A, "B": B}),
    output_factory=lambda _: RF(),
).result

set(result.keys())  # → {1}  (only in A, not in B)
result[1].x         # → "a1"
```

``difference`` is an alias for ``minus`` (``except`` is a Python reserved word).

### Relationship to union

``union`` (see [union](union.md)) merges all items from all input relations.
Together, the three operators form the classical set algebra:

| Operator | SQL equivalent | Semantics |
|:---------|:---------------|:----------|
| ``union`` | UNION ALL | All items from all inputs |
| ``intersect`` | INTERSECT | Items in all inputs |
| ``minus`` | EXCEPT | Items in first but not in subsequent inputs |

All three operate on **keys** — values are taken from the first relation
that contributes the key.

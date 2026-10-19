## Plan Extraction and Explain

FQL operators form a tree of transformations. The **plan IR** lets you inspect
this tree without executing it — useful for debugging, optimization, and
backend dispatch.

### explain() — human-readable plan

Every operator has an ``explain()`` method that pretty-prints its operator
subtree:

```python
from fdm.attribute_functions import TF, RF
from fql.operators.filters import filter_values
from fql.predicates import Eq

users = RF({
    1: TF({"name": "Alice", "dept": "eng"}),
    2: TF({"name": "Bob",   "dept": "sales"}),
})

op = filter_values[RF, RF](users, filter_predicate=Eq("dept", "eng"))
print(op.explain())
# - filter_values(filter_predicate=Eq('dept', 'eng'))
#   - leaf RF #<uuid>
```

### to_plan() — structured logical plan

``to_plan()`` returns a ``LogicalPlan`` object — the structured IR that
``explain()`` is built on:

```python
plan = op.to_plan()
plan.root       # → PlanNode(op="filter_values", ...)
plan.root.inputs  # → (LeafRef(kind="af", af_class="RF", ...),)
```

### IR types

The plan IR is defined in ``fql/plan/ir.py``:

| Type | Description |
|:-----|:------------|
| ``LogicalPlan`` | Root wrapper with IR version and JSON serialization |
| ``PlanNode`` | An operator invocation with inputs and parameters |
| ``LeafRef`` | Reference to a concrete AF (by UUID) |
| ``Opaque`` | Marker for non-serializable values (lambdas) |

### JSON serialization

Plans can be serialized to JSON and back:

```python
json_str = plan.to_json()

from fql.plan.ir import LogicalPlan
restored = LogicalPlan.from_json(json_str)
print(restored.explain())  # same output
```

Structured predicates (``Eq``, ``Gt``, etc.) survive the JSON roundtrip as
inspectable dicts. Lambda predicates become ``Opaque`` markers with a
best-effort ``repr``.

### Example plans

The following examples use this setup:

```python
from fdm.attribute_functions import TF, RF
from fql.operators.filters import filter_values
from fql.operators.subsets import subset
from fql.operators.partition import group_by
from fql.operators.aggregates import aggregate, Sum, Avg
from fql.predicates import Eq, Gt, And

users = RF({
    1: TF({"name": "Alice", "dept": "eng",   "salary": 90}),
    2: TF({"name": "Bob",   "dept": "sales", "salary": 80}),
    3: TF({"name": "Carol", "dept": "eng",   "salary": 70}),
})
```

#### 1. Simple filter with structured predicate

```python
op = filter_values[RF, RF](users, filter_predicate=Eq("dept", "eng"))
print(op.explain())
```

```
- filter_values(filter_predicate=Eq('dept', 'eng'))
  - leaf RF #3
```

The predicate is fully inspectable in the JSON (shortened):

```json
{
  "ir_version": 2,
  "root": {
    "type": "node",
    "op": "filter_values",
    "params": {
      "filter_predicate": {
        "type": "predicate",
        "op": "eq",
        "attr": "dept",
        "value": "eng"
      }
    },
    "inputs": [
      { "type": "leaf", "kind": "af", "af_class": "RF", "uuid": 3 }
    ]
  }
}
```

#### 2. Lambda predicate — opaque in the plan

```python
op = filter_values[RF, RF](users, filter_predicate=lambda v: v.salary > 75)
print(op.explain())
```

```
- filter_values(filter_predicate=<opaque lambda>)
  - leaf RF #3
```

The lambda becomes an ``Opaque`` marker in the JSON — a backend cannot
translate it:

```json
"filter_predicate": {
  "type": "opaque",
  "reason": "lambda",
  "repr": "<function <lambda> at 0x...>",
  "py_id": 4341917760
}
```

#### 3. Composed predicate — And(Eq, Gt)

```python
op = filter_values[RF, RF](
    users,
    filter_predicate=And(Eq("dept", "eng"), Gt("salary", 80)),
)
print(op.explain())
```

```
- filter_values(filter_predicate=And(Eq('dept', 'eng'), Gt('salary', 80)))
  - leaf RF #3
```

The composed predicate is fully serializable:

```json
"filter_predicate": {
  "type": "predicate",
  "op": "and",
  "predicates": [
    { "type": "predicate", "op": "eq", "attr": "dept", "value": "eng" },
    { "type": "predicate", "op": "gt", "attr": "salary", "value": 80 }
  ]
}
```

#### 4. Chained operators — filter then top-k

Operator trees are nested in the plan — the inner operator appears as a child:

```python
op = subset[RF, RF](
    filter_values[RF, RF](users, filter_predicate=Eq("dept", "eng")),
    ranking_key=lambda i: i.value.salary,
    k=1,
    reverse=True,
)
print(op.explain())
```

```
- subset(ranking_key=<opaque lambda>, k=1, offset=0, reverse=True, subset_predicate=None)
  - filter_values(filter_predicate=Eq('dept', 'eng'))
    - leaf RF #3
```

Note how the ``ranking_key`` lambda is opaque, but the ``filter_predicate``
inside the nested ``filter_values`` is a structured ``Eq`` — they coexist in
the same plan tree.

#### 5. group_by and aggregate

```python
op = group_by(users, "dept")
print(op.explain())
```

```
- group_by(aggregate_keys=['dept'])
  - leaf RF #3
```

```python
op = aggregate(users, total=Sum("salary"), avg=Avg("salary"))
print(op.explain())
```

```
- aggregate(aggregates={'total': Sum('salary'), 'avg': Avg('salary')})
  - leaf RF #3
```

The built-in aggregation functions (``Count``, ``Sum``, ``Min``, ``Max``,
``Avg``/``Mean``, ``Median``) are structured like predicates and survive the
JSON roundtrip (``Mean`` comes back as ``Avg``). Excerpt of the JSON:

```json
"aggregates": {
  "total": { "type": "aggregation", "op": "sum", "attr": "salary" },
  "avg":   { "type": "aggregation", "op": "avg", "attr": "salary" }
}
```

A self-built ``AggregationFunction(my_function, "salary")`` stays opaque, as do
custom partitioning functions passed to ``partition``. ``group_by`` shows its
grouping keys instead of the internal partitioning function.

Factory parameters such as ``output_factory`` never appear in the plan: they
only choose the class of the result AF, not what the query computes.

### Opaque vs structured

| | Lambda predicate | Structured predicate |
|:---|:---|:---|
| **In plan** | ``<opaque lambda>`` | ``Eq('dept', 'eng')`` |
| **JSON roundtrip** | Lost (``Opaque`` marker) | Preserved |
| **Backend dispatch** | Must execute locally | Can be translated |

This is the primary motivation for [structured predicates](predicates.md).

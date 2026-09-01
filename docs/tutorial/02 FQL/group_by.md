## Group By and Combined Aggregation

The [partition](partition.md) operator accepts an arbitrary partitioning
function. For the most common case — grouping by attribute equality (SQL's
``GROUP BY``) — FQL provides convenience operators.

### group_by — partition by attribute equality

``group_by`` is a specialization of ``partition`` that automatically derives
the partitioning function from one or more grouping keys:

```python
from fdm.attribute_functions import TF, RF
from fql.operators.partition import group_by

employees = RF({
    1: TF({"name": "Alice", "dept": "eng",   "salary": 90}),
    2: TF({"name": "Bob",   "dept": "eng",   "salary": 80}),
    3: TF({"name": "Carol", "dept": "sales", "salary": 70}),
})

grouped = group_by(employees, "dept").result

# Result is a DBF mapping group keys to RFs:
len(grouped["eng"])    # → 2
len(grouped["sales"])  # → 1
```

Multiple grouping keys produce tuple-valued partition keys:

```python
grouped2 = group_by(employees, "dept", "salary").result
# partition key is ("eng", 90), ("eng", 80), ("sales", 70), etc.
```

### group_by_aggregate — group then aggregate in one step

Combines ``group_by`` with ``aggregate`` into a single operator. The result
is an RF mapping each group key to an aggregated TF:

```python
from fql.operators.partition_and_aggregate import group_by_aggregate
from fql.operators.aggregates import Sum, Avg, Count

result = group_by_aggregate(
    employees,
    "dept",
    total_salary=Sum("salary"),
    avg_salary=Avg("salary"),
    headcount=Count("salary"),
).result

result["eng"].total_salary  # → 170
result["eng"].avg_salary    # → 85.0
result["eng"].headcount     # → 2
result["sales"].headcount   # → 1
```

### Accessing and dropping the group key

After ``group_by`` / ``group_by_aggregate`` the group identity is the **RF key**,
not a value attribute. That is deliberate: the key is the single source of truth
for the group. So it stays addressable for free…

```python
result["eng"].headcount   # group identity "eng" is the key
```

…and value-level ``project`` never touches it — projecting the aggregated RF keeps
every group key:

```python
result.project("headcount")["eng"]   # still keyed by "eng"
```

To make the group identity a **droppable** value attribute — or to carry it through
an operator that *replaces* the key domain, such as [`rank_by`](rank.md) (which
re-keys to ℕ) — lift the key into the value first with ``key_to_value``:

```python
from fql.operators.transforms import key_to_value

lifted = key_to_value(result, "dept").result   # copy each group key into its value under "dept"
lifted.project("headcount")["eng"]              # value-level "dept" now dropped by the projection
```

``key_to_value(af, name)`` stores the whole key under ``name``; for a composite
tuple key (from a multi-attribute ``group_by``) pass a tuple of names to spread it
component-wise, e.g. ``key_to_value(result, ("dept", "salary"))``. It refuses to
shadow an attribute that already resolves on a value.

### partition_by_aggregate

Like ``group_by_aggregate``, but takes an arbitrary partitioning function
instead of grouping keys — useful when the grouping criterion is not simple
attribute equality.

### Aggregation functions

FQL provides built-in aggregation function classes in
``fql.operators.aggregates``:

| Class | Description |
|:------|:------------|
| ``Sum(attr)`` | Sum of attribute values |
| ``Avg(attr)`` / ``Mean(attr)`` | Arithmetic mean |
| ``Count(attr)`` | Number of items |
| ``Min(attr)`` | Minimum value |
| ``Max(attr)`` | Maximum value |
| ``Median(attr)`` | Median value |

Each is a callable that receives an RF and returns a scalar. They can be
used standalone or as keyword arguments to ``aggregate`` /
``group_by_aggregate``.

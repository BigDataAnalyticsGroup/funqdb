## Partition

This is the inverse of the [union](union.md) operator.

### Generic Form: AF → AF

```output: AF = partition(input: AF)```

Partitions the items contained in the ```input``` AF and returns a new ```output``` AF, typically of a higher order,
mapping to the different partitions.

### Parameters/Filters

The user has to specify how partitions should be formed. This can be done based on keys, values, or any combination of both.

#### Implemented API

Currently only the **RF → DBF** case below is implemented, with this signature:

```python
partition(input: RF, *, partitioning_function: Callable[[Item], Any] | Predicate,
          output_factory: Callable[..., DBF] = None) -> DBF
```

``partitioning_function`` is **required** (keyword-only): it maps each ``Item`` to
its partition key, and items sharing a key land in the same output RF. For the
common "group by attribute equality" case, use the [group_by](group_by.md)
convenience operator, which derives ``partitioning_function`` for you.

```python
from fql.operators.partition import partition

by_parity = partition(
    RF({1: TF({"n": 10}), 2: TF({"n": 11}), 3: TF({"n": 12})}),
    partitioning_function=lambda item: item.value.n % 2,
).result
# by_parity[0] holds the even-n tuples, by_parity[1] the odd-n ones
```

Instead of a lambda, a structured key keeps the plan free of `Opaque` markers:

```python
from fql.expressions import Attr, Composite
from fql.predicates import Lt

users = RF({
    1: TF({"name": "Ann", "dept": "eng", "city": "Berlin", "yob": 1970}),
    2: TF({"name": "Bob", "dept": "eng", "city": "Paris",  "yob": 1990}),
    3: TF({"name": "Cid",                "city": "Berlin", "yob": 1985}),  # no dept
})

partition(users, partitioning_function=Attr("dept"))                 # by one attribute
partition(users, partitioning_function=Composite(Attr("dept"), Attr("city")))  # tuple keys
partition(users, partitioning_function=Lt("yob", 1980))              # True / False
```

- A tuple without the key attribute (`Attr`, `Composite` of attributes) is **dropped**: FDM has no NULL, so
  there is no "missing" group.
- A `Predicate` is evaluated on the tuple and yields `True` or `False`, never "undefined": a comparison over a
  missing attribute gives `False` (`Missing(...)` or the negation of a comparison give `True`). So `partition(P)` equals
  `{True: filter_values(P), False: filter_values(Not(P))}` (an empty partition is simply absent).
- Mixed: `Composite(Attr("dept"), Lt("yob", 1980))` gives `("eng", False)` for a tuple without `yob`, but
  drops a tuple without `dept`.

### Special cases

#### TF → RF (not yet implemented)

> Split tuples into sub-tuples.

```output: RF = partition(input: TF)```

Partitions the items (e.g. key/value-mappings) mapped to in the ```input``` TF.

*For instance*, this could be used to vertically partition tuples (not the containing relations).
This is often done in the context of a **unpivot** operation, i.e., when converting a pivot table to a flat relational
schema representation.

#### RF → DBF

> Split relation into sub-relations (aka shards, blocks, horizontal and/or vertical partitions, groups, subsets, ... you
> name it).

```output: DBF = partition(input: RF)```

Partitions the items (e.g. key/TF-mappings) mapped to in the ```input``` RF.

*For instance*, this could be used for classical grouping and any form of horizontal partitioning (like in distributed
sorting, distributed databases, distributed query processing, vertical partitioning). Semantically closest to the
GROUP BY clause in SQL, but more general: in FQL, partitions may overlap (replication) and may be formed on arbitrary
conditions, not just equality on key columns.

#### DBF → SDBF (not yet implemented)

> Split databases into sub-databases.

```output: SDBF = partition(input: DBF)```

Partitions the items (e.g. key/RF-mappings) mapped to in the ```input``` DBF.

*For instance*, this could be used to split a database with multiple tenants into separate databases per tenant.
Another example: partitioning a database by geographic region, where each resulting SDBF entry contains only the
relations relevant to that region.

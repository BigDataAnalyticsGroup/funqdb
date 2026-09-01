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
partition(input: RF, *, partitioning_function: Callable[[Item], Any],
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

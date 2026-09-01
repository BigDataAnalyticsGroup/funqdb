## Aggregate

This is the inverse of the [disaggregate](disaggregate.md) operator.

### Generic Form: AF → AF

```output: AF = aggregate(input: AF)```

Aggregates the items contained in the ```input``` AF and returns a new ```output``` AF, typically of smaller order.

### Parameters/Filters

#### Implemented API

Currently only the **RF → TF** case below is implemented, with this signature:

```python
aggregate(input: RF, **aggregates) -> TF   # alias: 𝜞
```

Each keyword becomes an output attribute; its value is an aggregation function
applied to the whole input RF. The built-in aggregation classes live in
``fql.operators.aggregates`` (see the table in [group_by](group_by.md)):

```python
from fql.operators.aggregates import aggregate, Sum, Avg, Count

totals = aggregate(
    RF({1: TF({"salary": 90}), 2: TF({"salary": 80})}),
    total=Sum("salary"), avg=Avg("salary"), n=Count("salary"),
).result
totals.total  # → 170   totals.avg → 85.0   totals.n → 2
```

To aggregate **per group** rather than over the whole RF, combine with
[group_by](group_by.md) via ``group_by_aggregate``.

### Special cases

#### TF → Value (not yet implemented)

> Aggregates a tuple function into an output value.

```output: Any = aggregate(input: TF)```

Computes an aggregated value based on the input TF.

*For instance*, this could be used to determine the length of a TF, i.e. the number of items present in that TF, or
to compute a hash or checksum over all items. This special case is only mentioned here for being conceptually
complete. In Python, ```len(TF)``` returns the same result for the length case.

#### RF → TF

> Aggregates a relation function into an output tuple function.

```output: TF = aggregate(input: RF)```

Computes an aggregated TF based on the input RF. In contrast to other operators, the output is not frozen, as it is a single TF.

*For instance*, this could be used for **classical aggregation** like avg(), mean(), sum(), median(), count() and so
forth that go beyond selecting a subset of the input RF, see [subset](subset.md) for a comparison.

#### DBF → RF (not yet implemented)

> Aggregates a database function into an output relation function.

```output: RF = aggregate(input: DBF)```

Computes an aggregated RF based on the input DBF.

*For instance*, this could be used for **classical aggregation** like avg(), mean(), sum(), median(), count() and so
forth where aggregates are collected for multiple relations **at the same time**, e.g. "give me min, max, sum, avg
values for all these tables". In SQL, you would first have to artificially union those tables (including finding a
common schema and a separating GROUP BY criterion). In FQL, this is not necessary: you can keep all RFs as they are
and compute aggregates as you wish. Another example of this is a **distinct** operation.

#### SDBF → DBF (not yet implemented)

> Aggregates a set of database functions into an output database function.

```output: DBF = aggregate(input: SDBF)```

Computes an aggregated DBF based on the input SDBF.

*For instance*, this could be used to merge two DBFs that are in different versions into one, or to compute
cross-database statistics such as "give me the total number of relations and tuples across all databases in this
server".
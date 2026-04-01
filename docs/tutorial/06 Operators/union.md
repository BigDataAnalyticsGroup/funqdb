## union

This is the inverse of the [partition](partition.md) operator.

### Generic Form: AF1 → AF2

```output: AF2 = union(input: AF1)```

Unions the items contained in the ```input```, typically AF1 is of a higher order than AF2,
i.e. mapping to the different partitions, and returns a new ```output``` AF2.

### Parameters/Filters

The user has to specify how to union data, in particular in the presence of duplicates. What a duplicate is can be
defined based on keys, values, or any combination of both.

#### lambdas

TODO

#### RF → TF

> union tuples into a super-tuple.

```output: RF = union(input: TF)```

Unions the items (e.g. key/value-mappings) mapped to in the ```input``` RF.

*For instance*, this could be used to invert the vertically partitioning of tuples (not the containing relations).
This is often done in the context of a **pivot** operation, i.e., when converting a flat relational schema into a pivot
representation.

#### DBF → RF

> Union relations into super-relations.


```output: RF = union(input: DBF)```

Unions the items (e.g. key/RF-mappings) mapped to in the ```input``` DBF.

*For instance*, this could be used for representing shards (aka blocks, horizontal and/or vertical partitions,
groups, subsets, ... you name it) in a single RF.

#### SDBF → DBF

> Union databases into super-databases.


```output: DBF = union(input: SDBF)```

Union the items (e.g. key/DBF-mappings) mapped to in the ```input``` SDBF.

*For instance*, this could be used to representing shard-databases in a single DBF.


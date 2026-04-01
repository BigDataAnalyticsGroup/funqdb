
## Partition

This is the inverse of the [union](union.md) operator.

### Generic Form: AF → AF

```output: AF = partition(input: AF)```

Partitions the items contained in the ```input``` AF and returns a new ```output``` AF, typically of a higher order,
mapping to the different partitions.
                                                    
### Parameters/Filters

The user has to specify how partitions should be formed. This can be done based on keys, values, or any combination of both.

#### lambdas

TODO


#### TF → RF

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
sorting, distributed databases, distributed query processing, vertical partitioning).

#### DBF → SDBF

> Split databases into sub-databases.


```output: SDBF = partition(input: DBF)```

Partitions the items (e.g. key/RF-mappings) mapped to in the ```input``` DBF.

*For instance*, this could be used to split a database with multiple tenants into separate databases per tenant.


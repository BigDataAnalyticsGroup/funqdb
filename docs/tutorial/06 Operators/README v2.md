# 06 FQL Operators

In this section, we will explain FQL operators.

As each operator works on a generic attribute function (AF), each explanation first describes the principal idea of an
operator. Then we explain important use-cases using tuple functions (TF), relation functions (RF), database functions
(DBF), and sets of databases functions (SDBF).

## Filter

### Generic Form: AF -> AF

```output: AF = filter(input: AF)```

Filters the items contained in the ```input``` AF and returns a new ```output``` AF containing only the qualifying
items.
The filter condition may be phrased against items, keys or values.

### Parameters/Filters

#### lambdas

TODO

#### where-clauses

TODO

### Special cases

#### TF -> TF

> Select the attributes to work on.

```output: TF = filter(input: TF)```

Filters the items (e.g. key/value-mappings) mapped to in the ```input``` TF. Semantically equivalent to the
SELECT-clause in SQL or 𝛑 (projection) in relational algebra (as long as they merely pick the attributes to return
without transforming them). However, note that we provide a separate ```project```-operator for compatibility reasons.

#### RF -> RF

> Select the tuples to work on.


```output: RF = filter(input: RF)```

Filters the items (e.g. key/TF-mappings) mapped to in the ```input``` RF. Semantically equivalent to the WHERE-clause in
SQL
or σ (selection) in relational algebra.

#### DBF -> DBF

> Select the relations to work on.

```output: DBF = filter(input: DBF)```

Filters the items (e.g. key/RF-mappings) mapped to in the ```input``` DBF. Semantically similar to the FROM-clause in
SQL or leaf relations in relational algebra (ignoring its flattening cross product and/or joins and that for FROM you
have to specify a whitelist of relations rather than conditions).

#### SDBF -> SDBF

> Select the databases to work on.

```output: SDBF = filter(input: SDBF)```

Filters the items (e.g. key/DBF-mappings) mapped to in the ```input``` SDBF. In a relational DBMS, semantically
equivalent to selecting **one**
specific database in a database driver, e.g. ```sqlite3.connect("my_fantastic_database.db")```. In SQL, we
select a specific database from a set of databases existing in a particular DBMS. All following queries must then be
restricted to that specific database.

In contrast, in FQL, we may select **multiple** databases and phrase queries across those databases.

# Set Operations

## partition

### Generic Form: AF -> AF

```output: AF = partition(input: AF)```

Partitions the items contained in the ```input``` AF and returns a new ```output``` AF, typically of a higher order,
mapping to the different partitions.

#### TF -> RF

> Split tuples into sub-tuples.


```output: RF = partition(input: TF)```

Partitions the items (e.g. key/value-mappings) mapped to in the ```input``` TF.

*For instance*, this could be used to vertically partition tuples (not the containing relations).

#### RF -> DBF

> Split relation into sub-relations (aka shards, partitions, groups, subsets, ... you name it).


```output: DBF = partition(input: RF)```

Partitions the items (e.g. key/TF-mappings) mapped to in the ```input``` RF.

*For instance*, this could be used for classical grouping and any form of horizontal partitioning (like in distributed
sorting, distributed databases, distributed query processing, vertical partitioning).

#### DBF -> SDBF

> Split databases into sub-databases.


```output: SDBF = partition(input: DBF)```

Partitions the items (e.g. key/RF-mappings) mapped to in the ```input``` DBF.

*For instance*, this could be used to split a database with multiple tenants into separate databases per tenant.

## Transforms

DISCUSS: Inherent to a function call anyway, do we need a separate operator?

## Grouping and Partitioning

## Sub Operators

## (Flattening) Joins

TODO, ongoing...
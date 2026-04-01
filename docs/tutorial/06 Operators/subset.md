## subset (aka top-k)

### Generic Form: AF → AF

```output: AF = subset(input: AF)```

Computes a subset of the items contained in the ```input``` AF and returns a new ```output``` AF.
In contrast to [filter](filter.md), the subset operator computes a condition based on a **global** condition.
In other words:

> **filter operator**: uses a predicate phrased against the **individual** items in the input AF (or just the keys or
> just the values). Such predicate may be evaluated for each item independently. For instance, a condition like
> ```foo==42``` may be computed for each ```foo``` individually without influencing the outcome of other predicates.
>
> **vs**
>
> **subset operator**: uses a predicate phrased against **all** items in the input AF. Such predicate **cannot** be
> evaluated for each item independently. For instance, a condition like 'k-smallest items with respect to their foo
> value' cannot be evaluated independently: the outcome depends on the other items present in the input AF.

### Parameters/Filters

The user has to specify how the subset should be formed. This can be done based on keys, values, or any combination of
both.

#### lambdas

TODO

### Special cases

#### TF → TF

> Select the attributes to work on based on a global condition.

```output: TF = subset(input: TF)```

Computes a subset of the items (e.g. key/value-mappings) mapped to in the ```input``` TF.

*For instance*, this could be used to compute a subset of a tuple, i.e. 'give me the k-smallest items present in that
tuple w.r.t. the condition specified'.

#### RF → RF

> Select the tuples to work on based on a global condition.


```output: RF = subset(input: RF)```

Computes a subset of the items (e.g. key/TF-mappings) mapped to in the ```input``` RF. Semantically equivalent to a
top-k operator in extended relational algebra or simulating the same thing in SQL using ORDER BY and LIMIT.

Note that for k=1, this operation is equivalent to a classical min or max-aggregation (but not mean, avg, median, count
as they compute a new value that does not have to exist in the input RF).

*For instance*, this could be used to compute the subset of tuples of a given relation, i.e. 'give me
the k-smallest tuples based on the condition specified'.

#### DBF → DBF

> Select the relations to work on based on a global condition.

```output: DBF = subset(input: DBF)```

Computes a subset of the items (e.g. key/RF-mappings) mapped to in the ```input``` DBF.

*For instance*, this could be used to compute the subset of relations of a given database, i.e. 'give
me the k-smallest relations based on the condition specified'.

#### SDBF → SDBF

> Select the databases to work on based on a global condition.

```output: SDBF = filter(input: SDBF)```

Filters the items (e.g. key/DBF-mappings) mapped to in the ```input``` SDBF.

*For instance*, this could be used to compute the subset of databases of a given set of databases,
i.e. 'give me the k-smallest databases based on the condition specified'.
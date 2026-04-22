## Disaggregate

This is the inverse of the [aggregate](aggregate.md) operator.

### Generic Form: AF → AF

```output: AF = disaggregate(input: AF)```

Disaggregate the items contained in the ```input``` AF and returns a new ```output``` AF, typically of higher order.

### Parameters/Filters

#### lambdas

TODO

### Special cases

#### Value → TF

> Disaggregates a value into an output tuple function.

```output: TF = disaggregate(input: Any)```

Computes a disaggregated TF based on the input value. This is the inverse of ```output: Any = aggregate(input: TF)```.

*For instance*, this could be used to decompose a scalar value into its constituent parts. Given a single number,
produce a TF representing its properties such as its prime factors, its digits, or its binary representation — each
as a separate key/value mapping. Another example: given a date value, disaggregate it into a TF with keys like
year, month, day, hour, etc.

#### TF → RF

> Disaggregates a tuple function into an output relation function.

```output: RF = disaggregate(input: TF)```

Computes a disaggregated RF based on the input TF. This is the inverse of ```output: TF = aggregate(input: RF)```.
Unlike [partition](partition.md) (which merely redistributes existing values), disaggregate performs transformations
that may produce new values not present in the input.

*For instance*, assume the input represents an integer that we want to factorize into its components, each component
represented by a separate tuple. Another example is fake data generation: given an aggregation result, split it into
sample data such that re-aggregating the fake data returns the original aggregate, e.g. generating plausible
individual salaries from an average salary.

#### RF → DBF

> Disaggregates a relation function into an output database function.

```output: DBF = disaggregate(input: RF)```

Computes a disaggregated DBF based on the input RF. This is the inverse of ```output: RF = aggregate(input: DBF)```.
Unlike [partition](partition.md) (which merely redistributes existing tuples into groups), disaggregate performs
transformations that may produce new tuples or entirely new relation schemas not present in the input.

*For instance*, given a summary relation of aggregated statistics per category, disaggregate it into a DBF where each
RF contains generated detail data consistent with those statistics, e.g. given "department X has 50 employees with
average salary 60k", produce a plausible RF of 50 employee tuples for that department.

#### DBF → SDBF

> Disaggregates a database function into a set of database functions.

```output: SDBF = disaggregate(input: DBF)```

Computes a disaggregated SDBF based on the input DBF. This is the inverse of ```output: DBF = aggregate(input: SDBF)```.

*For instance*, this could be used to split a merged database back into its original per-source databases. Given a DBF
that was produced by aggregating multiple version snapshots, disaggregate it back into per-version DBFs. Another
example: given a DBF containing consolidated data from multiple tenants, disaggregate it into an SDBF where each DBF
represents the data belonging to a single tenant, potentially reconstructing schema variations that were normalized
away during aggregation.

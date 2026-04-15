## Subdatabase

> **Note:** The subdatabase operator is currently being reworked together with
> the [join](join.md) operators. The API and semantics described below may
> change.

### Generic Form: DBF → DBF

```output: DBF = subdatabase(input: DBF)```

Reduces the ```input``` DBF to only those tuples that participate in a match across its relations under a given join
predicate. The output DBF contains the same relations as the input, but each relation is reduced to its qualifying
tuples. This is the FQL equivalent of the classical **Yannakakis reduction** (semi-join reduction).

The subdatabase operator is the foundation for join processing in FQL: rather than immediately flattening results into
a single relation (as SQL joins do), it first computes the reduced database, preserving the original structure. A
separate join operator can then flatten if needed.

### Parameters

#### join_predicate

A callable that receives two items (one from each relation) and returns a boolean. The predicate is treated as a black
box, supporting arbitrary join conditions (not limited to equi-joins).

#### left / right

Names of the two relations in the input DBF to join on. The current implementation is limited to two relations;
generalizing to n relations is future work.

#### create_join_index

If true, the output DBF contains an additional ```join_index``` RF that records all matching (left_key, right_key)
pairs. This index can be used by downstream operators (e.g. a join operator that flattens the result).

### Relationship to other operators

- **vs [filter](filter.md)**: filter reduces a single AF based on a local predicate per item. Subdatabase reduces
  *multiple* relations simultaneously based on a *cross-relation* predicate.
- **vs [partition](partition.md)**: partition splits one relation into groups. Subdatabase keeps the database structure
  but removes non-participating tuples.
- Subdatabase is the basis for **join** operators: ```join = subdatabase + flatten```.

### Variants

#### Inner subdatabase

> Reduce to tuples that have at least one match.

```output: DBF = subdatabase(input: DBF)```

The default variant. Each relation in the output contains only tuples that have at least one matching partner in the
other relation(s) under the join predicate. Equivalent to semi-join reduction.

*For instance*, given a DBF with relations "users" and "customers", and a predicate matching on name equality, the
output DBF contains only those users who are also customers and only those customers who are also users. Users without
a matching customer (and vice versa) are removed.

#### Outer subdatabase (not yet implemented)

> Reduce to matching tuples, plus unmatched tuples from specified relations.

```output: DBF = outer_subdatabase(input: DBF, outer: list[str])```

Like inner subdatabase, but additionally retains all tuples from the relations specified in the ```outer``` parameter,
even if they have no match. This preserves information that would otherwise be lost, similar to outer joins in SQL but
without flattening into a single relation with NULLs.

*For instance*, a left outer subdatabase on "users" would keep all users (even those without matching customers), while
still reducing customers to only those with matching users.

#### Anti subdatabase (not yet implemented)

> Reduce to tuples that have NO match.

```output: DBF = anti_subdatabase(input: DBF, anti: list[str])```

The complement of the inner subdatabase. Returns tuples from the specified relations that do *not* have a match under
the join predicate. Similar to anti-joins in SQL (NOT EXISTS / NOT IN).

*For instance*, given the same users/customers example, an anti subdatabase on "users" would return only those users
who are NOT customers.

#### Grouping set (not yet implemented)

> Partition a DBF along multiple grouping criteria simultaneously.

```output: DBF = grouping_set(input: DBF)```

Applies multiple grouping criteria to the relations in the input DBF at the same time, producing a DBF with the results
of each grouping. Unlike SQL's GROUPING SETS, the results are not hacked into a single output relation with NULLs —
each grouping set produces its own properly typed relation.

*For instance*, given a sales DBF, compute groupings by region, by product, and by (region, product) simultaneously,
each as a separate relation in the output DBF.

#### Cube (not yet implemented)

> Partition a DBF along all combinations of the specified criteria.

```output: DBF = cube(input: DBF)```

A special case of grouping sets that automatically generates all possible combinations of the specified grouping
criteria (the power set). As with grouping sets, results are not forced into a single relation — each combination is a
separate, properly typed relation.

*For instance*, given dimensions (region, product, year), cube produces groupings for every subset: (), (region),
(product), (year), (region, product), (region, year), (product, year), (region, product, year) — each as its own
relation in the output DBF.
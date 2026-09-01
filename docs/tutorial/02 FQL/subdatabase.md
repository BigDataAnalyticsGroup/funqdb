## Subdatabase

> **Note:** The subdatabase operator is currently being reworked together with
> the [join](join.md) operators. The API and semantics described below may
> change.

### Generic Form: DBF → DBF

```output: DBF = subdatabase(input: DBF, *, root=None)```

Reduces the ```input``` DBF to only those tuples that participate in the full join across its relations. The output DBF
contains the same relation names as the input, but each relation is reduced to its qualifying tuples. This is the FQL
equivalent of the classical **Yannakakis reduction** (semi-join reduction).

The subdatabase operator is the foundation for join processing in FQL: rather than immediately flattening results into
a single relation (as SQL joins do), it first computes the reduced database, preserving the original structure. A
separate [join](join.md) operator can then flatten if needed.

The join is **not** given as a black-box predicate. It is derived automatically from the reference structure of the
input: every ```ForeignValueConstraint``` — set up with ```.references(ref_key, target)``` on an RF or with
[`add_reference`](constraints.md) on the DBF — becomes an edge of the join graph. Internally the reduction is
expressed as a cascade of ```semijoin``` operators, so the extracted logical plan (see [plan](plan.md)) can be
shipped to a different backend. The input's constraints are preserved on the output. Only **acyclic** join graphs
are supported.

### Parameters

#### root (optional)

Name of the relation to use as the root of the join tree. When ```None``` (the default) the root is auto-selected as
the relation with no incoming references. Any orientation of the same undirected tree yields the same reduction, so
```root``` only affects the shape of the extracted plan, not the result.

### Minimal example

```python
from fdm.attribute_functions import TF, RF, DBF
from fql.operators.subdatabases import subdatabase

departments = RF({"d1": TF({"name": "Dev"}),
                  "d2": TF({"name": "Sales"}),
                  "d3": TF({"name": "Research"})}, frozen=False)
users = RF({1: TF({"name": "Horst", "dept": departments["d1"]}),
            2: TF({"name": "Tom",   "dept": departments["d1"]}),
            3: TF({"name": "John",  "dept": departments["d2"]})},
           frozen=False).references("dept", departments)  # reference before freezing

users.freeze()
departments.freeze()
dbf = DBF({"departments": departments, "users": users}, frozen=True)

reduced: DBF = subdatabase[DBF, DBF](dbf).result
# d3 ('Research') has no referencing user, so it is dropped:
assert {item.key for item in reduced.departments} == {"d1", "d2"}
assert {item.key for item in reduced.users} == {1, 2, 3}   # all users survive
```

> **Note on `.references()` and freezing:** attach the reference while the RFs are still writable, *then* freeze them.
> `.references()` also installs a reverse constraint on the target, which a frozen target would reject. To add a
> reference to an already-frozen DBF instead, use [`add_reference`](constraints.md).

### Relationship to other operators

- **vs [filter](filter.md)**: filter reduces a single AF based on a local predicate per item. Subdatabase reduces
  *multiple* relations simultaneously based on their *cross-relation* references.
- **vs [partition](partition.md)**: partition splits one relation into groups. Subdatabase keeps the database structure
  but removes non-participating tuples.
- Subdatabase is the basis for **join** operators: ```join = subdatabase + flatten```.

### Variants

#### Inner subdatabase

> Reduce to tuples that participate in the full join.

```output: DBF = subdatabase(input: DBF, *, root=None)```

The default (and currently only implemented) variant, shown above. Each relation in the output contains only tuples
that have a matching partner along every reference edge. Equivalent to semi-join reduction.

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
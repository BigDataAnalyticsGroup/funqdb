## Project

### Generic Form: AF → AF

```output: AF = project(input: AF, *attributes)```

Projects the values contained in the ```input``` AF to the specified ```attributes``` and returns a new ```output```
AF. Each value in the output contains only the keys listed in ```attributes```. Attributes not present in a value
are simply omitted rather than raising an error — in FDM and FQL, missing attributes are not present rather than
being present with a null value.

In contrast to [filter](filter.md), which selects **items** (rows) based on a predicate, project selects
**attributes** (columns) by name.

### Parameters

#### attributes

One or more attribute names to retain in each value:

```python
# as an operator:
from fql.operators.projections import project
result: RF = project(users, "name", "department").result

# as a convenience method:
result: RF = users.project("name", "department")

# relational algebra alias:
result: RF = users.π("name", "department")
```

At least one attribute must be provided.

### Special cases

#### TF → TF

> Select specific key/value-mappings from a tuple.

```output: TF = project(input: TF, *attributes)```

Note: ```project``` on a TF is semantically equivalent to ```filter``` on a TF (both select attributes). The
separate operator exists for compatibility with relational algebra naming conventions.

#### RF → RF

> Select specific attributes from each tuple in a relation.

```output: RF = project(input: RF, *attributes)```

*For instance*, this is semantically equivalent to the SELECT-clause in SQL or 𝛑 (projection) in relational
algebra — selecting which columns to return. Note that unlike SQL, duplicate elimination cannot occur because
each item retains its unique key.

#### DBF → DBF

> Select specific attributes from each tuple in each relation.

```output: DBF = project(input: DBF, *attributes)```

Projects the specified attributes across all relations contained in the database.

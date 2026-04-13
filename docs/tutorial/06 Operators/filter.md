## Filter

### Generic Form: AF → AF

```output: AF = filter(input: AF)```

Filters the items contained in the ```input``` AF and returns a new ```output``` AF containing only the qualifying
items. The filter condition may be phrased against items, keys or values.

In contrast to [subset](subset.md), the filter predicate is evaluated per item independently (local condition),
whereas subset uses a global condition that depends on all items present in the input AF.

### Parameters/Filters

#### lambdas

Filters accept a lambda predicate that is evaluated per item. Depending on the filter variant, the predicate
receives different arguments:

```python
# filter_items: predicate receives an Item (key + value)
filter_items(users, lambda i: i.value.department.name == "Dev")

# filter_values: predicate receives only the value
filter_values(users, lambda v: v.department.name == "Dev")

# filter_keys: predicate receives only the key
filter_keys(db, lambda k: k in ["users", "departments"])
```

All three variants return a new AF of the same type containing only the qualifying items.

#### where-clauses

The convenience method ```where()``` on attribute functions supports two styles of predicates:

**Lambda predicate** — full control, receives an ```Item```:
```python
users.where(lambda i: i.value.department.name == "Dev")
```

**Keyword arguments** — Django ORM-style, evaluated as a conjunct (all must match):
```python
users.where(department__name="Dev")
users.where(yob__gte=1980, yob__lte=2000)
```

The ```__```-syntax resolves **nested attributes** (```department__name``` traverses ```department.name```).
When the last segment is a known lookup operator, it applies a **comparison** instead of traversal.
Available lookups: ```exact```, ```lt```, ```lte```, ```gt```, ```gte```, ```in```, ```contains```,
```icontains```, ```startswith```, ```endswith```, ```isnull```, ```range```.

Plain ```field=value``` is equivalent to ```field__exact=value```.

The relational algebra alias ```𝛔()``` is equivalent to ```where()```.

#### Structured Predicates

Instead of opaque lambdas, filters also accept **structured predicates** from ```fql.predicates```.
These are callable (drop-in replacements for lambdas) *and* serializable — the plan IR represents
them as structured dicts instead of ```Opaque``` markers, enabling backend dispatchers to inspect
filter conditions.

```python
from fql.predicates import Eq, Gt, Like, In, And, Or, Not, Ref

# Simple equality (filter_predicate= is optional, it's a positional arg):
filter_values(users, Eq("name", "Alice"))

# Nested attribute paths (dot or __ notation):
filter_values(users, Eq("department.name", "Dev"))
filter_values(users, Eq("department__name", "Dev"))  # same

# Other comparisons:
filter_values(users, Gt("yob", 1980))
filter_values(users, Like("name", "H%"))
filter_values(users, In("yob", [1972, 1983, 2003]))

# Attribute-to-attribute comparison via Ref:
filter_values(events, Gt("end_year", Ref("start_year")))

# Logical composition:
filter_values(users, And(
    Eq("department.name", "Dev"),
    Gt("yob", 1980),
))
filter_values(users, Or(Eq("name", "Alice"), Eq("name", "Bob")))
filter_values(users, Not(Eq("department.name", "HR")))
```

Structured predicates also work with the ```.where()``` convenience method:

```python
users.where(Eq("department.name", "Dev"))
users.where(And(Eq("department.name", "Dev"), Gt("yob", 1980)))
```

Available predicate classes:

| Predicate | Description | Example |
|:----------|:------------|:--------|
| ```Eq``` | Equality | ```Eq("name", "Alice")``` |
| ```Gt``` | Greater than | ```Gt("yob", 1980)``` |
| ```Lt``` | Less than | ```Lt("yob", 2000)``` |
| ```Gte``` | Greater or equal | ```Gte("yob", 1980)``` |
| ```Lte``` | Less or equal | ```Lte("yob", 2000)``` |
| ```Like``` | SQL-style ```%``` wildcards | ```Like("name", "H%")``` |
| ```In``` | Membership test | ```In("yob", [1972, 1983])``` |
| ```And``` | Conjunction | ```And(pred1, pred2, ...)``` |
| ```Or``` | Disjunction | ```Or(pred1, pred2, ...)``` |
| ```Not``` | Negation | ```Not(pred)``` |
| ```Ref``` | Attribute reference (RHS) | ```Gt("end", Ref("start"))``` |

### Special cases

#### TF → TF

> Select the attributes to work on.

```output: TF = filter(input: TF)```

Filters the items (e.g. key/value-mappings) mapped to in the ```input``` TF.

*For instance*, this is semantically equivalent to the SELECT-clause in SQL or 𝛑 (projection) in relational algebra
(as long as they merely pick the attributes to return without transforming them). However, note that we provide a
separate ```project```-operator for compatibility reasons.

#### RF → RF

> Select the tuples to work on.

```output: RF = filter(input: RF)```

Filters the items (e.g. key/TF-mappings) mapped to in the ```input``` RF.

*For instance*, this is semantically equivalent to the WHERE-clause in SQL or σ (selection) in relational algebra,
e.g. filtering all tuples where ```salary > 50000```.

#### DBF → DBF

> Select the relations to work on.

```output: DBF = filter(input: DBF)```

Filters the items (e.g. key/RF-mappings) mapped to in the ```input``` DBF.

*For instance*, this is semantically similar to the FROM-clause in SQL or leaf relations in relational algebra
(ignoring its flattening cross product and/or joins and that for FROM you have to specify a whitelist of relations
rather than conditions). In FQL, we can filter relations by arbitrary conditions, e.g. 'give me all relations that
have more than 1000 tuples'.

#### SDBF → SDBF

> Select the databases to work on.

```output: SDBF = filter(input: SDBF)```

Filters the items (e.g. key/DBF-mappings) mapped to in the ```input``` SDBF.

*For instance*, in a relational DBMS, this is semantically equivalent to selecting **one** specific database in a
database driver, e.g. ```sqlite3.connect("my_fantastic_database.db")```. In SQL, we select a specific database from a
set of databases existing in a particular DBMS. All following queries must then be restricted to that specific
database. In contrast, in FQL, we may select **multiple** databases and phrase queries across those databases.

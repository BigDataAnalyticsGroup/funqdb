### Composite Keys

In a relational database, a composite primary key is a combination of two or more columns
that uniquely identifies a row. In FDM, the same idea is expressed through the
```CompositeForeignObject```: a key that bundles references to multiple attribute functions
into a single, hashable object.

```python
from fdm.attribute_functions import TF, RF, CompositeForeignObject

user1: TF = TF({"name": "Alice"})
customer1: TF = TF({"name": "ACME Corp"})

# A composite key referencing both a user and a customer:
key: CompositeForeignObject = CompositeForeignObject([user1, customer1])
```

A ```CompositeForeignObject``` supports:

- **```subkey(index)```** — access individual components: ```key.subkey(0)``` returns ```user1```
- **```in```** — containment check: ```user1 in key``` is ```True```
- **```len()```** — number of components: ```len(key)``` is ```2```
- **Hashing** — based on the UUIDs of the constituent AFs, so composite keys can be used as
  dictionary keys (and therefore as AF keys)

***

### RSF: N:M Relationships

The **Relationship Function** (```RSF```) models N:M relationships between attribute functions.
It uses ```CompositeForeignObject``` as keys — each key identifies a specific pairing of
participants — and TF values to hold the relationship's own attributes.

This replaces the traditional **junction table** (or associative table) in a relational database.

```python
from fdm.attribute_functions import TF, RF, RSF, CompositeForeignObject

# Two relations:
users: RF = RF({
    "u1": TF({"name": "Alice"}),
    "u2": TF({"name": "Bob"}),
}, frozen=True)

customers: RF = RF({
    "c1": TF({"name": "ACME Corp"}),
    "c2": TF({"name": "Globex"}),
}, frozen=True)

# An RSF modeling meetings between users and customers:
meetings: RSF = RSF(frozen=False)
meetings[CompositeForeignObject([users["u1"], customers["c1"]])] = TF({"date": "2025-01-01"})
meetings[CompositeForeignObject([users["u2"], customers["c1"]])] = TF({"date": "2025-02-15"})
meetings[CompositeForeignObject([users["u2"], customers["c2"]])] = TF({"date": "2025-03-20"})
```

#### Querying relationships: ```related_values()```

The ```related_values(subkey_index, subkey)``` method finds all composite keys where the
component at ```subkey_index``` matches ```subkey```, and returns those matching components:

```python
# How many meetings does Bob have?
# subkey_index=0 means "look at the user part (position 0) of each composite key"
# subkey=users["u2"] means "keep only keys where position 0 is Bob"
bobs_meetings = list(meetings.related_values(0, users["u2"]))
assert len(bobs_meetings) == 2  # Bob has 2 meetings
```

To find the *other* participants in a relationship, use ```filter_items``` and access
the composite key directly:

```python
from fql.operators.filters import filter_items
from fdm.attribute_functions import RF

# Find all customers that Bob meets with:
bobs_entries = filter_items(meetings,
    lambda i: i.key.subkey(0) == users["u2"],
    output_factory=lambda _: RF(),
).result
bobs_customers = [item.key.subkey(1) for item in bobs_entries]
assert len(bobs_customers) == 2  # customers["c1"] and customers["c2"]
```

***

### Tensor: Multi-Dimensional Attribute Functions

A **Tensor** is a multi-dimensional AF that uses ```CompositeForeignObject``` as coordinates.
It is parameterized by its dimensions and supports element-wise arithmetic.

```python
from fdm.attribute_functions import TF, Tensor, CompositeForeignObject

# A 3x4 matrix (rank-2 tensor):
matrix: Tensor = Tensor([3, 4])
assert matrix.rank() == 2

# Use CompositeForeignObjects as coordinates:
coord: CompositeForeignObject = CompositeForeignObject([TF({"id": 0}), TF({"id": 1})])
matrix[coord] = 42
```

#### Element-wise arithmetic

Tensors of the same dimensions support element-wise ```+```:

```python
t1: Tensor = Tensor([2])
t2: Tensor = Tensor([2])

k: CompositeForeignObject = CompositeForeignObject([TF({"id": 0})])
t1[k] = 10
t2[k] = 3

t_sum: Tensor = t1 + t2    # t_sum[k] == 13
```

> **Note:** ```-``` and ```*``` are defined but currently broken due to the ```dimensions```
> attribute being stored in the same data dict as tensor entries — a known limitation.

***

### Summary

| Concept | Relational Equivalent | FDM Class |
|:--------|:---------------------|:----------|
| Composite primary key | ```PRIMARY KEY (col1, col2)``` | ```CompositeForeignObject``` |
| Junction/associative table | ```CREATE TABLE meetings (user_id, customer_id, ...)``` | ```RSF``` |
| Multi-dimensional array | — | ```Tensor``` |

Both ```RSF``` and ```Tensor``` are subclasses of ```DictionaryAttributeFunction``` with
```CompositeForeignObject``` as their key type, inheriting all standard AF operations
(iteration, freezing, constraints, etc.).

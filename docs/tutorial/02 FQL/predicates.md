## Structured Predicates

FQL filter operators accept lambdas as predicates, but lambdas are **opaque**:
the plan IR cannot inspect or serialize them. Structured predicates are
callable drop-in replacements that are also serializable — enabling backend
dispatchers to translate filter conditions without executing Python code.

### Import

```python
from fql.predicates import Eq, Gt, Lt, Gte, Lte, Like, In, Exists, Missing, And, Or, Not, Ref
```

### Comparison predicates

All comparison predicates take an attribute path and a value:

```python
from fdm.attribute_functions import TF, RF

users = RF({
    1: TF({"name": "Alice", "age": 30, "dept": "eng"}),
    2: TF({"name": "Bob",   "age": 25, "dept": "sales"}),
    3: TF({"name": "Carol", "age": 35, "dept": "eng"}),
})

# Eq — equality:
users.where(Eq("dept", "eng"))      # Alice, Carol

# Gt, Lt, Gte, Lte — comparisons:
users.where(Gt("age", 28))          # Alice, Carol
users.where(Lte("age", 30))         # Alice, Bob

# Like — SQL-style pattern matching (% wildcards):
users.where(Like("name", "A%"))     # Alice
users.where(Like("name", "%o%"))    # Bob, Carol

# In — membership test:
users.where(In("dept", ["eng", "hr"]))  # Alice, Carol
```

### Logical composition

```python
# And — conjunction:
users.where(And(Eq("dept", "eng"), Gt("age", 31)))  # Carol

# Or — disjunction:
users.where(Or(Eq("name", "Alice"), Eq("name", "Bob")))  # Alice, Bob

# Not — negation:
users.where(Not(Eq("dept", "eng")))  # Bob
```

### Attribute-to-attribute comparison with Ref

``Ref`` resolves the right-hand side against the same object as the left-hand
side, enabling attribute-to-attribute comparisons:

```python
projects = RF({
    1: TF({"name": "Alpha", "start_year": 2020, "end_year": 2023}),
    2: TF({"name": "Beta",  "start_year": 2022, "end_year": 2022}),
})

# Projects where end_year > start_year:
projects.where(Gt("end_year", Ref("start_year")))  # Alpha only
```

### Attribute paths

All predicates support nested attribute paths using dot or ``__`` notation:

```python
# These are equivalent:
users.where(Eq("department.name", "Dev"))
users.where(Eq("department__name", "Dev"))
```

### Undefined attributes (missing key/value mappings)

In FDM an attribute a tuple does not map is **undefined** — there is no
(key, value) pair. This is *not* a NULL value; FDM has no NULL and no
three-valued logic. A predicate over an undefined attribute is simply **not
satisfied** (two-valued):

```python
people = RF({
    1: TF({"name": "Hugo"}),
    2: TF({"name": "Egon"}),
    3: TF({}),                 # no "name" mapping at all
})

people.where(Eq("name", "Hugo"))       # Hugo only
```

Comparison / ``Like`` / ``In`` return ``False`` on an undefined attribute, while
``And`` / ``Or`` / ``Not`` stay ordinary boolean — so a negation *selects* a
tuple that lacks the attribute (this is the deliberate difference from SQL):

```python
people.where(Not(Eq("name", "Hugo")))  # Egon AND the name-less tuple 3
```

To distinguish "absent" from "present but different", test definedness directly
with ``Exists`` / ``Missing`` — absence is structural, so it is queryable without
any three-valued logic:

```python
people.where(Exists("name"))           # Hugo, Egon (tuple 3 dropped)
people.where(Missing("name"))          # tuple 3 only

# "name is present and not Hugo":
people.where(And(Exists("name"), Not(Eq("name", "Hugo"))))  # Egon only
```

A stored ``None`` is a real value, so it counts as present:
``Exists("x")`` is ``True`` for ``TF({"x": None})``.

### Why use structured predicates?

Structured predicates produce inspectable plan IR nodes instead of ``Opaque``
markers. This matters for:

- **Plan inspection**: ``explain()`` shows the predicate structure
- **Backend dispatch**: a backend can translate ``Eq("name", "Alice")`` to SQL
  or another query language
- **Serialization**: plans with structured predicates survive JSON roundtrips

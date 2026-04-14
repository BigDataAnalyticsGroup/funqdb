### Schemas as Attribute Functions

In a relational database, schema definitions (``CREATE TABLE``) and data live in
separate worlds: the schema is metadata, the data is payload. In FDM, schemas
and data are **structurally identical** — they are both attribute functions, just
at different levels of abstraction.

#### The core observation

A TF is a function: *attribute name → value*. A schema for a TF describes which
attributes exist and what types they carry — also a function:
*attribute name → type*. Data and schema share the same structure:

| Level    | Data                                        | Schema                                                    |
|:---------|:--------------------------------------------|:----------------------------------------------------------|
| Tuple    | ``TF({"name": "Alice", "yob": 1990})``     | ``TF({"name": str, "yob": int})``                        |
| Relation | ``RF({"u1": tf1, "u2": tf2})``             | a TF describing the common structure of the relation's values |
| Database | ``DBF({"users": rf1, "departments": rf2})`` | ``RF({"users": schema_tf1, "departments": schema_tf2})`` |

A database schema is an **RF that maps relation names to schema-TFs** — exactly
the same structure as the DBF itself, just one level higher.

#### Schema as validator

The primary role of a schema in FDM is **validation**: ensuring that every value
inserted into a relation conforms to the declared structure. The ``Schema`` class
(``fdm.schema``) already serves this purpose — it inherits from both
``DictionaryAttributeFunction`` and ``AttributeFunctionConstraint``, so it is
simultaneously an AF and a constraint that can be attached to any relation:

```python
from fdm.attribute_functions import TF, RF
from fdm.schema import Schema

departments: RF = RF({
    "d1": TF({"name": "Dev", "budget": "11M"}),
    "d2": TF({"name": "Consulting", "budget": "22M"}),
})
departments.add_values_constraint(Schema({"name": str, "budget": str}))

users: RF = RF({
    1: TF({"name": "Alice", "yob": 1990, "department": departments.d1}),
})
users.add_values_constraint(Schema({"name": str, "yob": int, "department": TF}))
```

Note that attribute functions can be created and populated without any schema at
all — validation is added after the fact, when and where it is needed.
Every subsequent insert or update on ``departments`` or ``users`` is then checked
against the schema. If the value does not match, a ``ConstraintViolationError``
is raised — exactly the same behavior as a ``CHECK`` constraint in SQL, but
expressed as an AF.

#### References: the FDM equivalent of foreign keys

In addition to type-level schemas, ``.references()`` declares that a particular
attribute points to values mapped by another relation — the FDM equivalent of a
traditional foreign key constraint:

```python
users.references("department", departments)
```

This single call does two things (see [Constraints](Constraints.md) for details):

1. Adds a ``ForeignValueConstraint`` to ``users``: every value of
   ``user["department"]`` must be mapped by ``departments``.
2. Adds a ``ReverseForeignObjectConstraint`` to ``departments``: a department
   cannot be deleted while it is still referenced by a user.

Together, ``Schema`` and ``.references()`` form a complete schema definition:
structure (attribute names and types) plus referential integrity (which relations
point where).

#### Concise schema syntax

The ``schema=`` constructor parameter combines both mechanisms into a single
declaration:

```python
departments = RF({
    "d1": TF({"name": "Dev", "budget": "11M"}),
    "d2": TF({"name": "Consulting", "budget": "22M"}),
}, schema={"name": str, "budget": str})

users = RF({
    1: TF({"name": "Alice", "yob": 1990, "department": departments.d1}),
}, schema={"name": str, "yob": int, "department": departments})
```

All three values in the ``users`` schema describe the same concept: **the domain
of an attribute** — which values are allowed. The difference is the kind of
domain:

- ``str``, ``int`` — **open domains**: the set of all Python strings, all
  integers, etc. Validated via ``isinstance``.
- ``departments`` — **closed domain**: exactly the values currently mapped by
  that specific RF. Validated via ``ForeignValueConstraint`` and
  ``ReverseForeignObjectConstraint`` (i.e. full referential integrity, set up
  automatically by the constructor).

The visual distinction between types and AF instances is intentional: you can
immediately see which attributes are scalar and which are references to other
relations.

#### Database-level schemas

Because schemas are AFs, they compose naturally at the database level:

```python
# Schema-TFs: attribute name → type
dept_schema = TF({"name": str, "budget": str})
user_schema = TF({"name": str, "yob": int, "department": TF})

# Database schema: relation name → schema-TF  (an RF, just like a DBF)
db_schema = RF({
    "departments": dept_schema,
    "users": user_schema,
})
```

Because schemas are AFs themselves, FQL operators work on them directly:

- **Schema projection**: ``project(db_schema, "users", "departments")`` —
  extract a sub-schema
- **Schema comparison**: use set operators to find which attributes differ
  between two schemas
- **Schema composition**: union schemas to build a larger one
- **Schema inspection**: schemas are queryable, visualizable, and serializable
  with the same tools as data

This is a direct consequence of the FDM principle that *everything is a
function*: once schemas live inside the same algebra as data, no separate
"metadata language" is needed.

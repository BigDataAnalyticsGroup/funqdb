## The Basics: Functions, Functions, and Functions

### Attribute Functions

The functional data model (FDM) replaces tuples, relations, databases, and sets of databases, and in addition concepts
like tensors )including its 2-dimensional special case *matrices* and its 1-dimensional case *vectors*) with one single
concept: the attribute function (AF). An AF is a function that takes a key as input and produces a value as output.
Thus, a tuple is represented
as a function mapping from attribute names to attribute values, a relation is represented as a function mapping from
tuple identifiers to tuples, and a database is represented as a function mapping from relation names to relations.

For example, let's define three **tuple functions** representing persons:

```python
t1: TF = TF({"name": "Tom", "company": "sample company"})
t2: TF = TF({"name": "Tom", "company": "example inc"}),
t3: TF = TF({"name": "John", "company": "whatever gmbh"}),
```

A **relation function** is a function mapping from tuple identifiers (formerly known as tuple-ids or row-ids) to tuple
functions:

```python
# A relation function representing a set of persons:
persons: RF = RF({"t1": t1, "t2": t2, "t3": t3})
```

A **database function** is a function mapping from relation names to relations (in relational database systems, this
separation does not exist: if you do a `CREATE TABLE foo`, `foo` is at the same time the name of the relation and the
handle to the relation instance).

```python
# A database function representing a set of relations:
db: DBF = DBF({"persons": persons, "customers": ...})
```

<div style="border: 15px;">
<b>Repeated keys?</b> Don't worry about repeated keys when defining tuples, e.g `name`, no one forces us to store that
key with every
instance, all of this may be compressed away, we will discuss that below. All of what is being explained in this
tutorial can be considered **declarative descriptions** of the data. Database people love declarativity, and
declarative descriptions are decoupled from the physical realization. How to map declarative descriptions to physical
realizations is a major topic in database research. What this means for FDM and FQL is that the way we define our data
in the code (here in Python) does **not** necessarily reflect how it is stored on disk in memory, or how it is processed
in the query engine (that is the beauty of SQL, and the same applies to FDM and FQL).
</div>
***

### Frozen (Read-only) Attribute Functions

In order to forbid data to be changed, you may freeze (make read-only) any attribute functions to disallow changes. You
may do this through the initializer:

```python
t1: TF = TF({"name": "Tom", "company": "sample company"}, frozen=True)
# t2, t3, ... accordingly

persons: RF = RF({"t1": t1, "t2": t2, "t3": t3}, frozen=True)
```

If you try to change a frozen attribute function, a `ReadOnlyError` will be raised, e.g.:

```python
persons.t1 = t4 
```

will raise a `ReadOnlyError`.
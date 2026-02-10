# FDM andd FQL Tutorial

Welcome to this tutorial! In this guide, we will walk you through the basics of using our FDM and FQL. For the code
examples, we will use Python syntax. But note, that all these concepts apply to other programming languages as well.
The ideas behind FDM and FQL are not bound to one particular programming language.

## The Basics: Functions, Functions, and Functions

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

Don't worry about repeated keys when defining tuples, e.g ``name``, no one forces us to store that key with every
instance, all of this may be compressed away, we will discuss that below.

### Accessing Data

To access data, we can simply call the functions with the appropriate keys. For example, to retrieve the name of the
person associated with key "t1" in relation *persons*, we can simply use a dot syntax:

```python
name: str = db.persons.t1.name  # This will return the string "Tom"
```
Or, if we define `persons` as a variable:
```python
persons: RF = db.persons  # This will return the relation function representing the set of persons
```
Then, we can write:
```python
name: str = persons.t1.name  # This will also return the string "Tom"
```

Alternatively, we may use the []-syntax:

```python   
name: str = persons["t1"]["name"]  # This will also return the string "Tom"
```

Or the ()-syntax:

```python
name: str = persons("t1")("name")  # This will also return the string "Tom"
```

Or any mix you like (not recommended):

```python
name: str = persons("t1").name  # This will also return "Tom"
```

Note, that in Python, the dot syntax is only available for attributes that are valid python identifiers, e.g. the `name`
is
valid, but the integer `1` is not, so you cannot do `persons.t1.1`, but you can do `persons.t1[1]` or
`persons("t1")(1)`.

In general, I would recommend to stick to the dot syntax for attributes that are valid identifiers, and use the []
-syntax for all other cases. In other words, the dot syntax is more concise and easier to read, but the []-syntax is more flexible
and can handle all cases.

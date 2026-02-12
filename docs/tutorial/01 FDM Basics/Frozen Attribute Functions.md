
### Frozen (Read-only) Attribute Functions

In order to forbid data to be changed, you may freeze (make read-only) any attribute functions to disallow changes. You
may do this through the initializer of the attribute function, e.g.:

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

You may freeze an existing attribute function by calling the `freeze()` method on it:

```python
persons.freeze()
```

You can also unfreeze an attribute function by calling the `unfreeze()` method on it:

```python
persons.unfreeze()
``` 

**Why is freezing even a thing?** In sharp contrast to the relational model and SQL, in FDM, attribute functions may be
referred from multiple attribute functions, e.g. the tuple function `t1` may be referred from multiple relation
functions, e.g. `persons` and `customers', and if we change `t1` in one place, it will change in all places where it is
referred. This has interesting implications for data integrity and constraint checking (see the section on observing
attribute functions), and freezing is a way to prevent accidental changes to data.

Freezing is also a way to enforce
immutability, which can be useful for certain applications, e.g. when you want to
ensure that certain data is not changed after it has been defined. This is also important when returning data from
queries, as it prevents the returned data from being changed by the caller, which can lead to unexpected side effects.
These problems do not occur in the relational model and SQL, because there, tuples are not shared between relations,
and relations are not shared between databases, and query results are just copies of the data, so there is no risk of
accidental changes to data. In FDM, we have to be careful about this, and freezing is one way to do that.


## Constraints

Just like on your good old SQL-tables, you can also define constraints on your data. Constraints are crucial in
maintaining data integrity.
in FDM, constraints restrict what key/value entries may be referenced in an attribute function.

There are two important subclasses of constraints:

1. **Item Constraints**: these constraints restrict individual key/value-pairs (items) represented by an attribute
   function
2. **Attribute Function Constraints**: these constraints make a restriction about the set of all
   key/value-pairs of an attribute function

### 1. Item Constraints

Item constraints include:

1.1. **Schema constraints**, i.e., what kind of keys may be defined for any given item

For instance, we may restrict the set of keys (in relational terminology: the column names), that may be defined on a
particular attribute function:

```python
users: RF = db.users
users.add_items_constraint(attribute_name_schema({"name", "yob", "department"}))
```

This constraint will be checked for every key being inserted into `users`.

1.2. **Type constraints**, i.e., what types are allowed for values mapped to

TODO

1.3. **Domain constraints**, i.e., what kind of instances are allowed for particular values

TODO

### 2. Attribute Function Constraints

Attribute function constraints define a constraint on the **set of items** rather than on individual items. For
instance, you may restrict the number of items to a given max number:

```python
users.add_attribute_function_constraint(max_count(3))
```

This constraint will forbid the attribute function `users`to keep more than 3 items.

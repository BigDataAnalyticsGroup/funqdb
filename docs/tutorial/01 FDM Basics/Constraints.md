## Constraints

Just like on your good old SQL-tables, you can also define constraints on your data. Constraints are crucial in
maintaining data integrity.
in FDM, constraints restrict what key/value entries may be referenced in an attribute function.

There are two important subclasses of constraints:

1. **Value Constraints**: these constraints restrict individual values mapped to by an attribute
   function
2. **Attribute Function Constraints**: these constraints make a restriction about the set of all
   key/value-pairs of an attribute function
3. **Foreign Value Constraints**: these constraints restrict the values of an attribute function to be
   instances of another attribute function. This replaces the relational *foreign key* constraints.

### 1. Value Constraints

Value constraints include:

1.1. **Schema constraints**, i.e., what kind of keys and values may be defined for an attribute function.

For instance, we may restrict the set of keys (in relational terminology: the column names), that may be defined on a
particular attribute function:

```python
users: RF = db.users
user_schema = Schema({"name": str, "yob": int, "department": TF})
users.add_values_constraint(user_schema)
```

The `user_schema` constraint will be checked for every key/value-mapping being inserted into or modified in `users`.

1.2. **Domain constraints**, i.e., what kind of instances are allowed for particular values

TODO



### 2. Attribute Function Constraints

Attribute function constraints define a constraint on the **set of items** rather than on individual items. For
instance, you may restrict the number of items to a given max number:

```python
users.add_attribute_function_constraint(max_count(3))
```

This constraint will forbid the attribute function `users`to keep more than 3 items.

### 3. Foreign Value Constraints

in the relational world known as 'foreign key constraints'. However in FDM, we do not have to refer to by an extra
indirection via foreign keys but refer to instances directly.

TODO: first change code to do this in one call rather than two

```python
users: RF = db.users
departments: RF = db.departments
users.references("department", departments) # adds a foreign value constraint
```
### Computed Attributes

In the relational model, all attribute values are stored. If you need a derived
value — say, a salary computed from an employee's age — you have two choices:
store it redundantly (and keep it in sync), or compute it outside the database
in application code.

In FDM, there is a third option: **computed attributes**. A computed attribute is
a function that is evaluated on every access, just like a stored attribute. It is
**indistinguishable from a stored attribute** when accessed via `[]`, `.`, `()`,
`in`, `len`, or iteration. This is a core FDM principle (paper Section 2.3):
*the boundary between stored and computed data disappears*.

Because computed attributes are implemented on ``DictionaryAttributeFunction`` —
the common base class of **all** AF types — they work uniformly across the entire
hierarchy: TFs, RFs, DBFs, and SDBFs. A computed attribute on an RF might derive
a relation from other relations; a computed attribute on a DBF might provide a
virtual relation that is assembled on the fly.

#### Defining computed attributes

Computed attributes are passed as a ``computed=`` dict alongside the regular data.
Each value is a callable that receives the AF itself as its argument:

```python
from fdm.attribute_functions import TF

employee = TF(
    {"name": "Alice", "age": 30, "base_salary": 50000},
    computed={
        "bonus": lambda tf: tf["base_salary"] * 0.1,
        "total_pay": lambda tf: tf["base_salary"] + tf["bonus"],
    },
)

employee.name          # → "Alice"       (stored)
employee.bonus         # → 5000.0        (computed)
employee.total_pay     # → 55000.0       (computed, references another computed attr)
```

All three attributes — ``name``, ``bonus``, ``total_pay`` — are accessed
identically. Callers cannot tell which are stored and which are computed.

#### Dynamic recomputation

Computed attributes are evaluated on every access (not cached). This means they
always reflect the current state of stored attributes:

```python
employee["base_salary"] = 60000
employee.bonus          # → 6000.0  (recomputed)
employee.total_pay      # → 66000.0 (recomputed)
```

#### Adding computed attributes after construction

Use ``add_computed()`` to attach a computed attribute to an existing AF:

```python
scores = TF({"math": 90, "english": 85})
scores.add_computed("average", lambda tf: (tf["math"] + tf["english"]) / 2)
scores.average  # → 87.5
```

This fails on frozen AFs (``ReadOnlyError``) and if the key already exists as a
stored attribute (``ValueError``).

#### Computed attributes in iteration and operators

Computed attributes appear in iteration alongside stored attributes, making them
visible to FQL operators, schema validation, and any code that iterates over an AF:

```python
for item in employee:
    print(item.key, item.value)
# name Alice
# age 30
# base_salary 50000
# bonus 5000.0
# total_pay 55000.0

len(employee)       # → 5
"bonus" in employee # → True
```

#### Computed attributes survive projection and renaming

When you ``project()`` or ``rename()`` an AF, computed definitions are carried
over to the result — the lambda itself is preserved, not its current value:

```python
from fdm.attribute_functions import RF

staff = RF({1: employee})

# project keeps the computed definition alive:
projected = staff.project("name", "base_salary", "bonus")
projected[1].bonus  # → 5000.0 (still computed, not a frozen snapshot)

# rename changes the key but preserves the computation:
renamed = staff.rename(bonus="yearly_bonus")
renamed[1].yearly_bonus  # → 5000.0
```

> **Note:** If a projection excludes a key that a computed attribute depends on
> (e.g. projecting ``"bonus"`` without ``"base_salary"``), the computed attribute
> will raise ``AttributeError`` when accessed — the same behavior as dropping a
> column that a SQL computed column depends on.

#### Constraints

- A key must be **either stored or computed**, never both. Passing the same key
  in both ``data`` and ``computed`` raises ``ValueError``.
- Computed attributes are **read-only**: assigning to or deleting a computed key
  raises ``ReadOnlyError``.
- Computed attributes are **ephemeral**: they are stripped during pickling
  (serialization) and must be re-attached after deserialization.

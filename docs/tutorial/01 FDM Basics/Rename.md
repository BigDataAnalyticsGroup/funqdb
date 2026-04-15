### rename()

The ``rename()`` method renames keys inside each **value** of an AF — analogous
to the rename operator ρ in relational algebra. It returns a new AF; the
original is not modified.

```python
from fdm.attribute_functions import TF, RF

users = RF({
    1: TF({"name": "Alice", "yob": 1990}),
    2: TF({"name": "Bob",   "yob": 1985}),
})

renamed = users.rename(name="first_name", yob="birth_year")

renamed[1].first_name  # → "Alice"
renamed[1].birth_year  # → 1990
```

#### Key points

- Renames operate **one level deep**: on an RF, they rename attributes inside
  each TF, not the tuple keys of the RF itself.
- Keys not mentioned in the rename mapping are kept as-is.
- Computed attribute definitions are preserved under the new key name.
- The relational algebra alias ``ρ()`` is equivalent to ``rename()``.

#### Example with computed attributes

```python
emp = RF({
    1: TF({"age": 30}, computed={"salary": lambda tf: tf["age"] * 1000}),
})

renamed = emp.rename(salary="pay")
renamed[1].pay  # → 30000 (computed definition preserved under new key)
```

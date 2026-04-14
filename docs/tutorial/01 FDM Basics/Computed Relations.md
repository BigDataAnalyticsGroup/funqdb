### Computed Relations

The ``computed=`` dict (see [Computed Attributes](Computed%20Attributes.md))
handles a **finite, named** set of computed entries — each key is explicitly
listed. But what if an RF should generate values for *any* key, not just
pre-listed ones? This is the concept of **computed relations** (paper
Section 2.6): an RF that returns computed TFs on the fly for keys not explicitly
stored.

#### The ``default=`` parameter

The ``default=`` constructor parameter provides a **fallback function** that
receives the requested key as its argument:

```python
from fdm.attribute_functions import TF, RF

# Paper example R4: stored tuples for keys 1 and 2,
# generated tuples for any other integer key.
R4 = RF(
    {1: TF({"name": "Alice", "age": 12}),
     2: TF({"name": "Bob",   "age": 25})},
    default=lambda key: TF({"name": f"Generated-{key}", "age": 42 * key}),
)

R4[1].name     # → "Alice"        (stored)
R4[10].age     # → 420            (generated)
R4[10]("age")  # → 420            (function-call syntax)
```

Like ``computed=``, the ``default=`` parameter is implemented on
``DictionaryAttributeFunction`` and therefore available on all AF types
(TFs, RFs, DBFs, SDBFs).

#### Difference from ``computed=``

| | ``computed=`` | ``default=`` |
|:---|:---|:---|
| **Keys** | Finite, explicitly listed in the dict | Open — any key the caller requests |
| **Lambda argument** | Receives ``self`` (the AF) | Receives the **key** |
| **Visible in iteration** | Yes (``len``, ``__iter__``, ``keys``, ``values``) | No (potentially infinite domain) |
| **``in`` operator** | Returns ``True`` | Returns ``False`` (not enumerable) |

The key difference: ``computed=`` declares known attributes whose values depend
on other attributes of the *same* AF. ``default=`` is a catch-all for an
*open domain* — the set of valid keys is not known upfront and may be infinite.

#### Stored values take precedence

If a key exists in both ``data`` and the default covers it, the stored value
wins:

```python
R4[10] = TF({"name": "Stored-10", "age": 99})
R4[10].name  # → "Stored-10" (stored takes precedence)
```

#### ``add_default()``

Like ``add_computed()``, you can attach a default function after construction:

```python
users = RF({1: TF({"name": "Alice"})})
users.add_default(lambda key: TF({"name": f"User-{key}"}))
users[999].name  # → "User-999"
```

#### Constraints

- Fails on frozen AFs (``ReadOnlyError``).
- The default is **ephemeral** (stripped on pickling) and must be re-attached
  after deserialization.
- The default is **not propagated** by ``where()``, ``project()``, or
  ``rename()`` — their results are materialized subsets without a default.

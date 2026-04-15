### Computed Attribute Functions

[Computed Attribute Values](Computed%20Attribute%20Values.md) (``computed=``)
handle a **finite, named** set of computed entries — each key is explicitly
listed. But what if an AF should generate values for *any* key, not just
pre-listed ones? This is the concept of **computed attribute functions** (paper
Section 2.6): the AF *as a function* computes values on the fly for keys not
explicitly stored.

Because ``default=`` is implemented on ``DictionaryAttributeFunction``, it works
uniformly across all AF types — TFs, RFs, DBFs, and SDBFs.

> **See also:** the comparison table in
> [Computed Attribute Values](Computed%20Attribute%20Values.md) for a quick
> overview of the differences between ``computed=`` and ``default=``.

#### The ``default=`` parameter

The ``default=`` constructor parameter provides a **fallback function** that
receives the requested key as its argument:

```python
from fdm.attribute_functions import TF, RF, DBF

# RF example (paper example R4): stored tuples for keys 1 and 2,
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

#### Examples across AF types

**TF — config with defaults:**

```python
config = TF(
    {"host": "localhost", "port": 8080},
    default=lambda key: f"default_{key}",
)

config.host    # → "localhost"     (stored)
config.timeout # → "default_timeout" (generated)
```

**DBF — database that generates empty relations on demand:**

```python
db = DBF(
    {"users": RF({1: TF({"name": "Alice"})})},
    default=lambda name: RF(),
)

db["users"][1].name  # → "Alice"  (stored relation)
len(db["logs"])      # → 0        (generated empty RF)
```

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

#### Scoping the default with ``domain=``

Without a domain, the default function handles **any** key not in data or computed.
The ``domain=`` parameter restricts which keys the default covers, making
the AF **enumerable**:

```python
R = RF(
    default=lambda key: TF({"val": key * 10}),
    domain=range(1, 6),
)

len(R)     # → 5
list(R)    # → [Item(1, TF(...)), ..., Item(5, TF(...))]
1 in R     # → True
99 in R    # → False
R[99]      # → AttributeError (outside domain)
```

The domain only scopes the default — it does **not** restrict stored or computed
keys. You can freely store keys outside the domain:

```python
R[99] = TF({"val": "extra"})  # works fine
len(R)  # → 6 (5 domain + 1 stored)
```

Deleting a stored key that is also in the domain causes it to fall back to the
default:

```python
R[1] = TF({"val": "override"})
R[1].val   # → "override" (stored)
del R[1]
R[1].val   # → 10 (back to default)
```

Enumeration reflects the **union** of stored, computed, and resolvable domain
keys (i.e. domain keys backed by a default). ``len``, ``__iter__``, ``keys()``,
and ``values()`` all follow this union semantics.

> **Note:** Passing ``domain=`` without ``default=`` emits a warning — domain
> keys are only resolvable when a default function is set.

**TF with domain — finite config schema:**

```python
settings = TF(
    {"theme": "dark"},
    default=lambda key: "unset",
    domain={"theme", "language", "timezone"},
)

len(settings)         # → 3
"language" in settings # → True
settings.language     # → "unset"
```

#### Constraints

- Fails on frozen AFs (``ReadOnlyError``).
- The default and domain are **ephemeral** (stripped on pickling) and must be
  re-attached after deserialization.
- The default is **not propagated** by ``where()``, ``project()``, or
  ``rename()`` — their results are materialized subsets without a default.

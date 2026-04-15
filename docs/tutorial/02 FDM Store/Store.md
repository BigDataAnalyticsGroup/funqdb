## Store — Persistence for Attribute Functions

The ``Store`` class (``store/store.py``) provides SQLite-backed persistence
for attribute functions. It uses ``SqliteDict`` as a key/blob store, where
each AF is serialized (pickled) and stored by its UUID.

### Basic usage

```python
from fdm.attribute_functions import TF, RF
from store.store import Store

# Create a store (backed by a SQLite file):
store = Store(file_name="my_store.sqlite")

# Create some AFs:
users = RF({
    1: TF({"name": "Alice"}),
    2: TF({"name": "Bob"}),
})

# Register (persist) the AF:
store.register(users)

# Retrieve by UUID:
loaded = store.get(users.uuid)
loaded[1].name  # → "Alice"
```

### Swizzling and unswizzling

When an AF contains references to other AFs (e.g. an RF whose values are TFs),
these references are **swizzled** during serialization: each nested AF is
replaced by an ``AttributeFunctionSentinel`` containing only its UUID. On
deserialization, sentinels are **unswizzled** back to full AF objects on
first access via the store.

This enables lazy loading: only the AFs you actually access are loaded from
disk.

### Ephemeral state

The following AF state is **stripped during pickling** and must be re-attached
after deserialization:

- ``computed`` — computed attribute value definitions (lambdas)
- ``default`` — default fallback function (lambda)
- ``domain`` — active domain set
- ``store`` — store reference (re-attached automatically on load)

This is by design: lambdas and closures are not safely serializable with
standard ``pickle``.

### Limitations

- **Read-only swizzling**: unswizzling currently works for reads only; writes
  through swizzled references are a known TODO.
- **No query pushdown**: the store treats values as opaque blobs — filtering
  and projection happen in memory after loading.
- **Security**: unpickling untrusted data is a remote code execution vector.
  Only load from trusted sources.

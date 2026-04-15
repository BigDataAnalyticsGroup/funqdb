### Observers

AFs support the **observer pattern**: when an AF is mutated (insert, update,
delete), all registered observers are notified. This enables reactive
constraints, derived values, and cross-AF consistency checks.

#### Registering observers

Any AF can observe another AF by implementing the ``Observer`` interface:

```python
from fdm.attribute_functions import TF, RF
from fdm.util import Observer, ChangeEvent
from fql.util import Item

class Logger(Observer):
    def __init__(self):
        self.log = []

    def receive_notification(self, source, item: Item, event: ChangeEvent):
        self.log.append((event, item.key, item.value))

users = RF({1: TF({"name": "Alice"})})
logger = Logger()
users.add_observer(logger)

users[2] = TF({"name": "Bob"})  # triggers notification
# logger.log → [(ChangeEvent.INSERT, 2, TF({"name": "Bob"}))]
```

#### Change events

The ``ChangeEvent`` enum distinguishes three mutation types:

| Event | Triggered by |
|:------|:-------------|
| ``ChangeEvent.INSERT`` | Adding a new key |
| ``ChangeEvent.UPDATE`` | Overwriting an existing key |
| ``ChangeEvent.DELETE`` | Deleting a key |

#### Observing item values

With ``observe_items=True``, an AF also registers as observer on its own
``Observable`` values. This means changes to nested AFs (e.g. a TF inside an
RF) propagate upward:

```python
users = RF({1: TF({"name": "Alice"})}, observe_items=True)
users.add_observer(logger)

users[1]["name"] = "Alicia"  # change inside the TF
# logger receives notification about the change
```

#### Constraints

- ``add_observer()`` and ``remove_observer()`` fail on frozen AFs
  (``ReadOnlyError``).
- Observers are **ephemeral**: they are stripped during pickling and must be
  re-attached after deserialization.
- Observer propagation through the store is a known TODO.

## Schema Visualization

funqDB ships a small visualization helper that turns a `DBF` into an
interactive HTML page. It is intended for understanding and discussing
schemas at a glance — relations become nodes, foreign value references
(declared via `.references()`) become labelled edges, and any attached
[`Schema`](Constraints.md) is shown inline on the corresponding node.

The output is a single, standalone HTML file with no Python dependencies
beyond funqDB itself: it loads `Cytoscape.js` from a CDN (pinned with an
SRI hash) and runs entirely in the browser. There is no server, no build
step, and no editing — it is a read-only view of the schema you already
have in memory.

### From Python: `to_html(db)`

`fdm.viz.to_html` takes a `DBF` and returns a complete HTML document as
a string. Write it to disk and open it in a browser:

```python
from pathlib import Path
from fdm.attribute_functions import DBF, RF, TF
from fdm.schema import Schema
from fdm.viz import to_html

departments: RF = RF({
    "d1": TF({"name": "Dev",        "budget": 11_000_000}),
    "d2": TF({"name": "Consulting", "budget": 22_000_000}),
})
departments.add_values_constraint(Schema({"name": str, "budget": int}))

users: RF = RF({
    1: TF({"name": "Horst", "yob": 1972, "department": departments.d1}),
    2: TF({"name": "Tom",   "yob": 1983, "department": departments.d1}),
}).references("department", departments)
users.add_values_constraint(Schema({"name": str, "yob": int, "department": TF}))

db: DBF = DBF({"departments": departments, "users": users})

Path("schema.html").write_text(to_html(db))
```

Open `schema.html` in any browser; you can drag nodes around and zoom
with the scroll wheel.

### From the shell: `funqdb-viz`

For the common case "I have a Python file with a `DBF` definition and
just want to look at it", a small CLI is included:

```sh
scripts/funqdb-viz examples/schema_example.py /tmp/schema.html
```

The script executes the input file as a normal Python script and then
looks for a module-level variable named `db` (or, if there is no `db`,
the unique `DBF` instance at module level) and renders it. The
directory of the input file is added to `sys.path` so neighbouring
imports work just like running `python <input>` directly. See
[`examples/schema_example.py`](../../../examples/schema_example.py) for
a self-contained example with three relations and multiple foreign
value references.

### What is rendered

- **Nodes** — one per `RF` in the `DBF`. The label shows the relation
  name, its current item count, and (if attached) the relation's
  [`Schema`](Constraints.md) as `key: type` lines.
- **Edges** — one per [`ForeignValueConstraint`](Constraints.md), drawn
  from the referencing relation to the referenced relation, labelled
  with the attribute key on which the reference is declared (e.g.
  `users → departments` labelled `department`).
- References to AFs that are not part of the same `DBF` are silently
  skipped, since there is no node to point at.

### What is **not** rendered (yet)

- N:M relationships via `RSF` / `CompositeForeignObject`
- Composite primary keys
- Individual `TF` data inside relations (the visualization is at the
  schema level, not the row level)
- Editing — the page is read-only by design

### Determinism and reproducibility

`to_html` produces a byte-deterministic output for a given `DBF`:
relations are emitted in alphabetical order, edges are ordered by
`(parent, key)`, and edge ids are a monotonic counter. Identical inputs
therefore yield identical HTML, which is convenient for diff-based
review and snapshot testing.

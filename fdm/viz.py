#
#    This is funqDB, a query processing library and system built around FDM and FQL.
#
#    Copyright (C) 2026 Prof. Dr. Jens Dittrich, Saarland University
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#
r"""Schema/constraint visualization for funqDB databases.

This module renders a :class:`~fdm.attribute_functions.DBF` (a funqDB
database) as an interactive HTML page using Cytoscape.js (loaded from a
CDN). Nodes are the :class:`~fdm.attribute_functions.RF`\ s contained in
the database and edges are the foreign-value references between them, as
recorded by :meth:`~fdm.attribute_functions.DictionaryAttributeFunction.references`.

The output is intentionally read-only and has no Python dependencies
beyond funqDB itself. A typical usage is::

    from pathlib import Path
    from fdm.viz import to_html

    Path("schema.html").write_text(to_html(db))

and then opening ``schema.html`` in a browser.
"""

import json
from typing import Any

from fdm.attribute_functions import DBF, RF
from fdm.schema import ForeignValueConstraint, Schema
from fql.util import Item

# Pinned Cytoscape.js version served via unpkg. The SRI hash below is the
# sha384 of the exact file fetched from
# https://unpkg.com/cytoscape@3.30.2/dist/cytoscape.min.js, so that a
# compromised or tampered CDN response will be rejected by the browser.
_CYTOSCAPE_VERSION = "3.30.2"
_CYTOSCAPE_SRI = (
    "sha384-IWROdLKRsN1UuJywMlWl7/blXQ8GEooN2n7dzTxfEPd7ybYIKCUJ2Ol/1Gpf3YV4"
)


def _rf_schema(rf: RF) -> dict[str, str] | None:
    """Return the schema attached to ``rf`` as a ``{key: type_name}`` mapping,
    or ``None`` if no :class:`Schema` is attached.

    funqDB stores schemas as ordinary value constraints, so we inspect
    ``values_constraints`` and pick the first :class:`Schema` instance we
    encounter (in a deterministic order — see :func:`_collect_graph`).
    A relation can in principle carry more than one schema constraint;
    for visualization a single representative one is sufficient.
    """
    # values_constraints is a set, so iterate in a stable order to keep the
    # generated HTML reproducible across runs. Sort by type name (not id),
    # because id(type) varies across Python processes.
    for constraint in sorted(
        rf.__dict__["values_constraints"], key=lambda c: type(c).__name__
    ):
        if isinstance(constraint, Schema):
            # Schema itself is a DictionaryAttributeFunction[key, type]; iterate
            # its items to get the declared attribute → type mapping.
            return {
                str(item.key): getattr(item.value, "__name__", str(item.value))
                for item in constraint
            }
    return None


def _collect_graph(db: DBF) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    r"""Extract Cytoscape-ready node and edge dictionaries from ``db``.

    Nodes correspond to the relations (``RF``\ s) in the database, keyed by
    their name in the ``DBF``. Edges correspond to
    :class:`~fdm.schema.ForeignValueConstraint`\ s on those relations; an
    edge is only emitted if the referenced target AF is itself one of the
    relations contained in ``db`` (references to external AFs are silently
    skipped because we would have no node to point at).

    The returned lists are in deterministic order (by RF name, then by
    ``(target, key)`` for edges) so that :func:`to_html` produces
    reproducible output.
    """
    # Iterate ``db`` only once and materialize its RF items into a list so
    # that both the uuid→name map and the node/edge emission below can reuse
    # it without relying on re-iteration semantics of the underlying AF.
    rf_items: list[Item] = [item for item in db if isinstance(item.value, RF)]
    rf_items.sort(key=lambda item: str(item.key))

    # Map target RF uuid → relation name so we can resolve the target of a
    # ForeignValueConstraint back to a node in our graph. We rely on uuid
    # identity rather than Python's ``is`` to stay consistent with how the
    # rest of funqDB identifies AFs.
    name_by_uuid: dict[int, str] = {item.value.uuid: str(item.key) for item in rf_items}

    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    # Monotonic counter used as edge id, to guarantee uniqueness even when
    # RF names or constraint keys contain characters like ``->`` or ``:``.
    edge_counter = 0

    for item in rf_items:
        rf: RF = item.value
        rf_name = str(item.key)
        schema = _rf_schema(rf)
        nodes.append(
            {
                "data": {
                    "id": rf_name,
                    "label": rf_name,
                    "schema": schema or {},
                    "size": len(rf),
                }
            }
        )

        # Gather this RF's foreign value references, sorted by the resolved
        # (parent_name, key) pair so edge order is stable across runs.
        rf_edges: list[tuple[str, str]] = []
        for constraint in rf.__dict__["values_constraints"]:
            if not isinstance(constraint, ForeignValueConstraint):
                continue
            parent_uuid = constraint.target_attribute_function.uuid
            parent_name = name_by_uuid.get(parent_uuid)
            if parent_name is None:
                # Reference points at something that is not a direct source
                # of this DBF — nothing to draw.
                continue
            rf_edges.append((parent_name, str(constraint.key)))

        rf_edges.sort()
        for parent_name, key in rf_edges:
            edges.append(
                {
                    "data": {
                        "id": f"e{edge_counter}",
                        "source": rf_name,
                        "target": parent_name,
                        "label": key,
                    }
                }
            )
            edge_counter += 1

    return nodes, edges


def _safe_json(payload: object) -> str:
    """Serialize ``payload`` to JSON safe for embedding inside a ``<script>`` tag.

    :func:`json.dumps` does not escape ``<``, ``>``, ``&`` or the line-separator
    characters ``U+2028``/``U+2029``. A relation named ``</script>...`` would
    therefore break out of the surrounding script block and allow arbitrary
    JavaScript execution (XSS). We replace those characters with their
    ``\\uXXXX`` equivalents, which keeps the JSON valid while preventing
    script-tag breakout.
    """
    raw = json.dumps(payload, indent=2)
    return (
        raw.replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


# The HTML template is kept as a module-level constant. Three placeholders
# (``__CY_VERSION__``, ``__CY_SRI__``, ``__ELEMENTS_JSON__``) are substituted
# via ``str.replace`` rather than ``str.format`` so that the embedded
# JavaScript does not need every ``{`` doubled up. The JSON payload is
# replaced last to avoid any token-in-token interference.
_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>funqDB schema</title>
<script src="https://unpkg.com/cytoscape@__CY_VERSION__/dist/cytoscape.min.js"
        integrity="__CY_SRI__"
        crossorigin="anonymous"></script>
<style>
  html, body { margin: 0; padding: 0; height: 100%; font-family: sans-serif; }
  #cy { width: 100vw; height: 100vh; background: #fafafa; }
  #info {
    position: absolute; top: 8px; left: 8px; padding: 6px 10px;
    background: rgba(255,255,255,0.85); border: 1px solid #ccc;
    border-radius: 4px; font-size: 12px;
  }
</style>
</head>
<body>
<div id="info">funqDB schema &mdash; drag nodes, scroll to zoom</div>
<div id="cy"></div>
<script>
  const elements = __ELEMENTS_JSON__;

  // Build a multi-line label for each node that includes its schema, if any.
  for (const n of elements.nodes) {
    const schema = n.data.schema || {};
    const lines = [n.data.label + "  (" + n.data.size + ")"];
    for (const [k, t] of Object.entries(schema)) {
      lines.push(k + ": " + t);
    }
    n.data.displayLabel = lines.join("\\n");
  }

  cytoscape({
    container: document.getElementById("cy"),
    elements: [...elements.nodes, ...elements.edges],
    layout: { name: "cose", animate: false, padding: 40 },
    style: [
      {
        selector: "node",
        style: {
          "shape": "round-rectangle",
          "background-color": "#e8f0fe",
          "border-color": "#4a6fa5",
          "border-width": 1,
          "label": "data(displayLabel)",
          "text-wrap": "wrap",
          "text-valign": "center",
          "text-halign": "center",
          "font-family": "monospace",
          "font-size": 11,
          "padding": "10px",
          "width": "label",
          "height": "label"
        }
      },
      {
        selector: "edge",
        style: {
          "curve-style": "bezier",
          "target-arrow-shape": "triangle",
          "line-color": "#4a6fa5",
          "target-arrow-color": "#4a6fa5",
          "label": "data(label)",
          "font-size": 10,
          "text-background-color": "#fafafa",
          "text-background-opacity": 1,
          "text-background-padding": 2
        }
      }
    ]
  });
</script>
</body>
</html>
"""


def to_html(db: DBF) -> str:
    r"""Render ``db`` as a standalone, interactive HTML page.

    The returned string is a complete HTML document embedding Cytoscape.js
    from a CDN (pinned with an SRI hash). Nodes represent the relations in
    ``db``, each annotated with its attached :class:`~fdm.schema.Schema`
    (if any) and the number of items it contains. Edges represent
    :class:`~fdm.schema.ForeignValueConstraint`\ s between those relations
    and are labelled with the referencing attribute key.

    @param db: The funqDB database to visualize.
    @return: A complete HTML document as a string. Write it to disk and
        open it in a browser to view the schema interactively.
    @raise TypeError: if ``db`` is not a :class:`DBF` instance.
    """
    if not isinstance(db, DBF):
        raise TypeError(f"to_html expects a DBF instance, got {type(db).__name__}")

    nodes, edges = _collect_graph(db)
    elements_json = _safe_json({"nodes": nodes, "edges": edges})
    return (
        _HTML_TEMPLATE.replace("__CY_VERSION__", _CYTOSCAPE_VERSION)
        .replace("__CY_SRI__", _CYTOSCAPE_SRI)
        .replace("__ELEMENTS_JSON__", elements_json)
    )

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
"""Tests for :mod:`fdm.viz`.

These tests cover the standalone HTML rendering of a funqDB ``DBF`` via
Cytoscape.js, the deterministic graph extraction in ``_collect_graph``, and
in particular the XSS-hardening performed by ``_safe_json`` when embedding the
JSON payload inside an inline ``<script>`` tag.
"""

import json
from typing import Any

import pytest

from fdm.attribute_functions import DBF, RF, TF
from fdm.schema import Schema
from fdm.viz import _collect_graph, to_html
from tests.lib import _create_testdata

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_inline_json(html: str) -> Any:
    """Pull the JSON payload out of the inline ``<script>`` block.

    The template embeds it as ``const elements = <json>;``. We slice between
    those literal markers and parse the result, which doubles as a check that
    the embedded JSON is still valid after ``_safe_json`` escaping.
    """
    marker = "const elements = "
    start = html.index(marker) + len(marker)
    end = html.index(";\n", start)
    return json.loads(html[start:end])


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_to_html_basic_contains_rf_names_and_edge_label() -> None:
    """``to_html`` on the canonical test DB returns a complete HTML document
    that mentions every RF name and the single edge label."""
    db: DBF = _create_testdata()
    html: str = to_html(db)

    assert html.startswith("<!DOCTYPE html>")
    # All three relation names appear in the embedded JSON.
    assert "departments" in html
    assert "users" in html
    assert "customers" in html
    # The single foreign-value reference is labelled "department".
    assert "department" in html


def test_to_html_with_schemas_contains_field_names_and_types() -> None:
    """With ``add_schemas=True`` the schema fields and their type names show
    up inside the embedded JSON payload."""
    db: DBF = _create_testdata(add_schemas=True)
    html: str = to_html(db)

    # Field names from each schema.
    assert "name" in html
    assert "yob" in html
    assert "budget" in html
    # Type names — these are produced by ``_rf_schema`` via ``__name__``.
    assert "str" in html
    assert "int" in html
    # The "department" key in users' schema points at TF.
    assert "TF" in html


def test_to_html_contains_sri_and_pinned_cdn() -> None:
    """The Cytoscape ``<script>`` tag must carry the SRI hash, the
    ``crossorigin`` attribute, and a pinned version 3.30.2 URL."""
    html: str = to_html(_create_testdata())

    assert 'integrity="sha384-' in html
    assert 'crossorigin="anonymous"' in html
    assert "cytoscape@3.30.2/dist/cytoscape.min.js" in html


# ---------------------------------------------------------------------------
# _collect_graph
# ---------------------------------------------------------------------------


def test_collect_graph_node_count_and_order() -> None:
    """Nodes are emitted once per RF, in alphabetical order by name."""
    db: DBF = _create_testdata()
    nodes, edges = _collect_graph(db)

    assert [n["data"]["id"] for n in nodes] == ["customers", "departments", "users"]
    # One edge: users.department -> departments.
    assert len(edges) == 1


def test_collect_graph_edge_data() -> None:
    """The single edge in the test DB carries the right source/target/label
    and a monotonic id starting at ``e0``."""
    nodes, edges = _collect_graph(_create_testdata())

    edge = edges[0]
    assert edge["data"]["id"] == "e0"
    assert edge["data"]["source"] == "users"
    assert edge["data"]["target"] == "departments"
    assert edge["data"]["label"] == "department"


def test_collect_graph_node_size_matches_len() -> None:
    """Each node's ``size`` field reflects ``len(rf)`` of the underlying RF."""
    nodes, _ = _collect_graph(_create_testdata())
    sizes: dict[str, int] = {n["data"]["id"]: n["data"]["size"] for n in nodes}

    assert sizes == {"departments": 2, "users": 3, "customers": 5}


def test_collect_graph_with_schemas() -> None:
    """When schemas are attached, each node carries the schema mapping with
    type names rendered as strings."""
    nodes, _ = _collect_graph(_create_testdata(add_schemas=True))
    schemas: dict[str, dict[str, str]] = {
        n["data"]["id"]: n["data"]["schema"] for n in nodes
    }

    assert schemas["departments"] == {"name": "str", "budget": "int"}
    assert schemas["users"] == {"name": "str", "yob": "int", "department": "TF"}
    assert schemas["customers"] == {"name": "str", "company": "str"}


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_to_html_empty_db() -> None:
    """An empty DBF still produces a valid, complete HTML document with an
    empty nodes/edges payload."""
    html: str = to_html(DBF({}))

    assert html.startswith("<!DOCTYPE html>")
    assert html.rstrip().endswith("</html>")
    payload = _extract_inline_json(html)
    assert payload == {"nodes": [], "edges": []}


def test_collect_graph_empty_db() -> None:
    """``_collect_graph`` on an empty DBF returns two empty lists."""
    assert _collect_graph(DBF({})) == ([], [])


def test_collect_graph_single_rf_no_references() -> None:
    """A DBF with a single RF and no references yields one node and zero edges."""
    only: RF = RF({1: TF({"x": 1})})
    nodes, edges = _collect_graph(DBF({"only": only}))

    assert len(nodes) == 1
    assert nodes[0]["data"]["id"] == "only"
    assert edges == []


def test_collect_graph_drops_external_references() -> None:
    """References pointing at an RF that is not in the DBF are silently
    dropped — there is no node to draw an edge to."""
    external: RF = RF({1: TF({"name": "ext"})})
    child: RF = RF({1: TF({"k": external[1]})}).references("k", external)

    db: DBF = DBF({"source": child})
    nodes, edges = _collect_graph(db)

    assert [n["data"]["id"] for n in nodes] == ["source"]
    assert edges == []


def test_collect_graph_edge_id_robust_to_special_chars() -> None:
    """Edge ids are a monotonic counter so RF names / keys containing ``->``
    or ``:`` cannot collide with another edge id."""
    parent: RF = RF({1: TF({"name": "p"})})
    child: RF = RF({1: TF({"a->b:c": parent[1]})}).references("a->b:c", parent)

    db: DBF = DBF({"source": child, "target": parent})
    _, edges = _collect_graph(db)

    assert len(edges) == 1
    assert edges[0]["data"]["id"] == "e0"
    assert edges[0]["data"]["label"] == "a->b:c"

    # And the label survives the round-trip into the rendered HTML.
    html: str = to_html(db)
    assert "a-\\u003eb:c" in html  # ``>`` is escaped to \u003e by _safe_json


def test_to_html_is_deterministic() -> None:
    """Two calls on the same DBF must yield byte-identical output."""
    db: DBF = _create_testdata(add_schemas=True)
    assert to_html(db) == to_html(db)


# ---------------------------------------------------------------------------
# Security: script-tag breakout
# ---------------------------------------------------------------------------


def test_to_html_escapes_script_tag_in_rf_name() -> None:
    """A malicious RF name containing ``</script>`` must not appear verbatim
    inside the rendered HTML — it must be escaped to ``\\u003c/script\\u003e``."""
    payload_name: str = "</script><img src=x onerror=alert(1)>"
    rf: RF = RF({1: TF({"x": 1})})
    db: DBF = DBF({payload_name: rf})

    html: str = to_html(db)

    # The raw payload must NOT be present.
    assert payload_name not in html
    # And the escaped form must be.
    assert "\\u003c/script\\u003e" in html
    # The legitimate template contains exactly two literal "</script>" tags
    # (one closing the CDN script, one closing the inline script).
    assert html.count("</script>") == 2

    # The inline JSON must still be valid and round-trip the original name.
    payload = _extract_inline_json(html)
    assert payload["nodes"][0]["data"]["id"] == payload_name
    assert payload["nodes"][0]["data"]["label"] == payload_name


def test_to_html_escapes_script_tag_in_schema_key() -> None:
    """A malicious schema key must be escaped just like an RF name."""
    bad_key: str = "<script>alert(1)</script>"
    rf: RF = RF({1: TF({bad_key: "v"})})
    rf.add_values_constraint(Schema({bad_key: str}))
    db: DBF = DBF({"r": rf})

    html: str = to_html(db)

    # Raw payload absent, escaped form present.
    assert "<script>alert" not in html
    assert "\\u003cscript\\u003ealert(1)\\u003c/script\\u003e" in html
    # Template still has only its two legitimate </script> tags.
    assert html.count("</script>") == 2

    payload = _extract_inline_json(html)
    assert payload["nodes"][0]["data"]["schema"] == {bad_key: "str"}


def test_to_html_escapes_unicode_line_terminator() -> None:
    """U+2028 is a JS line terminator and would otherwise break the inline
    script. ``_safe_json`` must replace it with the literal six-char escape
    sequence ``\\u2028``."""
    name_with_ls: str = "evil\u2028name"
    rf: RF = RF({1: TF({"x": 1})})
    db: DBF = DBF({name_with_ls: rf})

    html: str = to_html(db)

    # The raw line separator character must be gone.
    assert "\u2028" not in html
    # And the literal escape sequence (six characters) must be present.
    assert "\\u2028" in html

    # JSON still parses and round-trips the original name.
    payload = _extract_inline_json(html)
    assert payload["nodes"][0]["data"]["id"] == name_with_ls


# ---------------------------------------------------------------------------
# Type checking
# ---------------------------------------------------------------------------


def test_to_html_rejects_string() -> None:
    """``to_html`` raises ``TypeError`` when given a non-DBF string."""
    with pytest.raises(TypeError):
        to_html("not a db")  # type: ignore[arg-type]


def test_to_html_rejects_none() -> None:
    """``to_html`` raises ``TypeError`` when given ``None``."""
    with pytest.raises(TypeError):
        to_html(None)  # type: ignore[arg-type]


def test_to_html_rejects_rf() -> None:
    """An RF is not a DBF, so ``to_html`` must reject it."""
    with pytest.raises(TypeError):
        to_html(RF({}))  # type: ignore[arg-type]

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

"""The join_graph dataset: a schema without data for join rendering."""

from collections.abc import Iterator

import pytest

from benchmarks.datasets import (
    DATASETS,
    Dataset,
    LoadedDataset,
    PrimaryKey,
    Reference,
    Schema,
)
from benchmarks.datasets.join_graph.join_graph_dataset import JOIN_GRAPH


@pytest.fixture
def join_graph() -> Iterator[Dataset]:
    """A private open join_graph dataset, closed after the test."""
    dataset = JOIN_GRAPH.open()  # a schema-only dataset
    yield dataset  # keys and references, no data
    dataset.close()  # the fixture owns the dataset it opened


def test_JOIN_GRAPH_open_has_no_data(join_graph):
    """The dataset only renders: it opens without a DuckDB connection."""
    assert JOIN_GRAPH.has_data is False  # declared without a connection builder
    assert not isinstance(join_graph, LoadedDataset)  # so it never executes


def test_JOIN_GRAPH_is_registered():
    """The transpiler finds the dataset by its name."""
    assert DATASETS["join_graph"] is JOIN_GRAPH  # the registry holds this definition


def test_JOIN_GRAPH_open_schema_holds_every_join_shape(join_graph):
    """
    The schema holds every edge of the diagram, each table keyed by `id`, and no
    columns. Each edge shows its `ref_key`:

        ⟲ a_id, a1_id, a2_id
        a ◀─────────────── a_id ─────────────────────┐
        │                                            │
        ├── b_id, renamed (b_r_id), b1_id, b2_id ──▶ b
        │                                            │
        ├── c_id ───────────▶ c ◀─────────── c_id ───┤
        │                     │ d_id                 │
        │                     ▼                      │
        └── d_id ───────────▶ d ◀─────────── d_id ───┘
    """
    schema = join_graph.schema()  # keys and references, no columns
    keys = {table: PrimaryKey.of("id") for table in "abcd"}  # every table by `id`
    expected = {  # written out: an import would repeat any slip in the dataset
        "a": {
            "a_id": Reference("a_id", "a"),  # a self-reference
            "a1_id": Reference("a1_id", "a"),  # a second one
            "a2_id": Reference("a2_id", "a"),  # a third one
            "b_id": Reference("b_id", "b"),  # with b -> a, the cycle
            "b_r_id": Reference("renamed", "b"),  # the column has another `ref_key`
            "b1_id": Reference("b1_id", "b"),  # a parallel edge to `b`
            "b2_id": Reference("b2_id", "b"),  # a second parallel edge
            "c_id": Reference("c_id", "c"),  # the diamond's path through `c`
            "d_id": Reference("d_id", "d"),  # the shortcut a -> d
        },
        "b": {
            "a_id": Reference("a_id", "a"),  # with a -> b, the cycle
            "c_id": Reference("c_id", "c"),  # the long path a -> b -> c -> d
            "d_id": Reference("d_id", "d"),  # the diamond's path through `b`
        },
        "c": {"d_id": Reference("d_id", "d")},  # the end of the path through `c`
    }  # `d` refers to no table, so it has no entry

    assert schema == Schema(references=expected, primary_keys=keys, columns={})  # all

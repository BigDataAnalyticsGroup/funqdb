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
"""The synthetic `join_graph` dataset without data. Each edge shows its `ref_key`:

    ⟲ a_id, a1_id, a2_id
    a ◀─────────────── a_id ─────────────────────┐
    │                                            │
    ├── b_id, renamed (b_r_id), b1_id, b2_id ──▶ b
    │                                            │
    ├── c_id ───────────▶ c ◀─────────── c_id ───┤
    │                     │ d_id                 │
    │                     ▼                      │
    └── d_id ───────────▶ d ◀─────────── d_id ───┘

The shapes: the self-references of `a`, the cycle `a` <-> `b`, the diamond
`a` -> `b` -> `d` / `a` -> `c` -> `d`, the parallel edges from `a` to `b`, and the
column `b_r_id`, whose reference has the other name `renamed`.
"""

from benchmarks.datasets.dataset import DatasetDefinition
from benchmarks.datasets.dataset_types import Reference, References
from benchmarks.datasets.schema import PrimaryKey

JOIN_GRAPH_REFERENCES: References = {
    "a": {
        "a_id": Reference("a_id", "a"),  # A -> A
        "a1_id": Reference("a1_id", "a"),  # A -> A
        "a2_id": Reference("a2_id", "a"),  # A -> A
        "b_id": Reference("b_id", "b"),  # A -> B
        "b_r_id": Reference("renamed", "b"),  # A -> B (renamed)
        "b1_id": Reference("b1_id", "b"),  # A -> B
        "b2_id": Reference("b2_id", "b"),  # A -> B
        "c_id": Reference("c_id", "c"),  # A -> C
        "d_id": Reference("d_id", "d"),  # A -> D
    },
    "b": {
        "a_id": Reference("a_id", "a"),  # B -> A
        "c_id": Reference("c_id", "c"),  # B -> C
        "d_id": Reference("d_id", "d"),  # B -> D
    },
    "c": {"d_id": Reference("d_id", "d")},  # C -> D
}

JOIN_GRAPH_PRIMARY_KEYS: dict[str, PrimaryKey] = {
    table: PrimaryKey.of("id") for table in ("a", "b", "c", "d")
}

JOIN_GRAPH = DatasetDefinition(
    name="join_graph",
    references=JOIN_GRAPH_REFERENCES,
    primary_keys=JOIN_GRAPH_PRIMARY_KEYS,
)

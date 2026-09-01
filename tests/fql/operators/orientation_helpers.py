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

"""Shared helpers for the "all 8 edge orientations" tests.

The tree A-C, B-C, C-D is used by both `test_joins.py` (bidirectional
flattening join) and `test_subdatabases.py` (Yannakakis reduction) to show
that behaviour is edge-direction-independent: every one of the 2**3 = 8
orientations of the three tree edges reduces / flattens the same way. Both
suites need the exact same directed-tree builder and orientation enumeration,
so they live here rather than being duplicated per file.
"""

import itertools

import pytest

from fdm.attribute_functions import TF, RF, DBF


def build_directed_tree(directed_edges: list[tuple[str, str]]) -> DBF:
    """Build a 4-relation DBF (A, B, C, D) whose reference edges follow a given orientation.

    Undirected shape is always the tree A-C, B-C, C-D. Each entry in
    ``directed_edges`` is a (source, target) pair meaning ``source`` references
    ``target`` — the source RF's single tuple embeds the target RF's single
    tuple under a field named after the target (lowercased) and declares
    ``.references()``. Relations are built in dependency order (every target
    before the sources that reference it) so the embedded tuple already exists.

    Each relation holds exactly one tuple and every reference points at that
    single existing tuple, so the graph is fully connected: Yannakakis reduction
    keeps all four tuples and the flattening join yields a single row.

    @param directed_edges: the three edges, each oriented as (source, target).
    @return: a frozen DBF with relations named "A", "B", "C", "D".
    """
    names: list[str] = ["A", "B", "C", "D"]
    # outgoing[name] = relations that `name` references (embeds)
    outgoing: dict[str, list[str]] = {n: [] for n in names}
    for source, target in directed_edges:
        outgoing[source].append(target)

    # Topological order: build a relation only after every relation it
    # references is already built (targets before sources). The graph is a
    # tree, so such an order always exists regardless of orientation.
    order: list[str] = []
    placed: set[str] = set()
    while len(placed) < len(names):
        for n in names:
            if n not in placed and all(t in placed for t in outgoing[n]):
                order.append(n)
                placed.add(n)
                break

    built: dict[str, RF] = {}
    for name in order:
        tuple_key: str = name.lower() + "1"  # e.g. "a1"
        fields: dict[str, object] = {"name": name + "1"}
        # embed one referenced tuple per outgoing edge, keyed by target name
        for target in outgoing[name]:
            fields[target.lower()] = built[target][target.lower() + "1"]
        rf: RF = RF({tuple_key: TF(fields)}, frozen=False)
        # declare the foreign-value constraint for each embedded reference
        for target in outgoing[name]:
            rf = rf.references(target.lower(), built[target])
        built[name] = rf

    for rf in built.values():
        rf.freeze()

    return DBF({n: built[n] for n in names}, frozen=True)


def orientation_params() -> list:
    """One pytest param per orientation of the tree A-C, B-C, C-D.

    Each of the three undirected edges can point either way; the product over
    the three edges yields the eight directed variants. Each param carries a
    human-readable id such as ``"A->C,B->C,C->D"`` and a ``list`` of three
    (source, target) edges ready to hand to `build_directed_tree`.

    @return: a list of 8 pytest params (``pytest.param``), one per orientation.
    """
    undirected: list[tuple[str, str]] = [("A", "C"), ("B", "C"), ("C", "D")]
    params: list = []
    for flips in itertools.product([False, True], repeat=3):
        # flip=True reverses that edge's (source, target)
        edges: list[tuple[str, str]] = [
            (e[1], e[0]) if flip else e for e, flip in zip(undirected, flips)
        ]
        case_id: str = ",".join(f"{s}->{t}" for s, t in edges)
        params.append(pytest.param(edges, id=case_id))
    return params

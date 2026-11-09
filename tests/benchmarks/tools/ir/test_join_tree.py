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

"""The alias graph of a query and the join tree it turns into."""

import pytest

from benchmarks.datasets.dataset_types import Reference
from benchmarks.datasets.schema import Schema
from benchmarks.tools.ir.expressions import (
    BinaryExpression,
    Column,
    ExistsExpression,
    InSubqueryExpression,
    Literal,
    SubqueryExpression,
    TupleIdentityExpression,
    UnaryExpression,
    conjunction,
)
from benchmarks.tools.ir.join_equalities import (
    Edge,
    ForeignKeyEquality,
    ReferenceRead,
)
from benchmarks.tools.ir.join_tree import AliasGraph, Join, JoinTree
from benchmarks.tools.ir.paths import Path
from benchmarks.tools.ir.query import Query

# The stated edges of most tests below. The edges are given by hand, so the schema
# declares no references; the table names only label the aliases.
#
#   mc ── movie_id ──▶ t ◀── movie_id ── mi
#
#   mc: movie_companies, t: title, mi: movie_info
#   t has two incoming edges: it is the hub that joins the roots mc and mi
SCHEMA = Schema(references={}, primary_keys={}, columns={})
MOVIES = {
    "mc": "movie_companies",
    "mi": "movie_info",
    "t": "title",
}  # the three aliases
MOVIE_EDGES = (Edge("mc", "movie_id", "t"), Edge("mi", "movie_id", "t"))  # into the hub


def build(
    tables: dict[str, str],
    edges: tuple[Edge, ...],
    foreign_key_equalities: tuple[ForeignKeyEquality, ...] = (),
    schema: Schema = SCHEMA,
    depth: int = 0,
    reserved: frozenset[str] = frozenset(),
) -> JoinTree:
    """The join tree of the alias graph of `tables` and the stated equalities."""
    graph = AliasGraph(
        depth, schema, tables, edges, foreign_key_equalities, reserved
    )  # checked
    return graph.tree()  # the member most tests below exercise


def path_from(alias: str, root: str, *ref_keys: str) -> Path:
    """The path to `alias` from `root` along `ref_keys`, in the outermost query."""
    return Path(0, alias, root, ref_keys)  # at depth 0, the outermost query


def column_at(alias: str, root: str, name: str, *ref_keys: str) -> Column:
    """The plain column `name` of `alias` as `root` reaches it along `ref_keys`."""
    path = path_from(alias, root, *ref_keys)  # where the column is read
    return Column(path, name)  # a value, not a reference


def reference_at(alias: str, root: str, name: str, *ref_keys: str) -> Column:
    """The reference `name` of `alias` as `root` reaches it along `ref_keys`."""
    path = path_from(alias, root, *ref_keys)  # where the reference is read
    return read(path, name)  # marked as a reference


def tuple_at(alias: str, root: str, *ref_keys: str) -> Column:
    """The tuple of `alias` itself as `root` reaches it along `ref_keys`."""
    path = path_from(alias, root, *ref_keys)  # where the tuple is reached
    return Column.tuple_at(path)  # a column without a name


def read(path: Path, reference: str) -> Column:
    """The reference `reference` read at `path`, marked as a reference."""
    return Column(path, reference, True)  # marked as a reference, not a plain value


def provisional_equality(target_table: str) -> ForeignKeyEquality:
    """`mc.movie_id = mi.movie_id`, two references into `target_table`."""
    mc_read = ReferenceRead("mc", "movie_id")  # mc's reference
    mi_read = ReferenceRead("mi", "movie_id")  # mi's reference
    return ForeignKeyEquality(target_table, mc_read, mi_read)  # equated


def test_alias_graph_post_init_rejects_an_undeclared_alias():
    """An edge may only connect aliases the query declares."""
    edges = (Edge("mc", "movie_id", "t"),)  # `t` is not declared below

    with pytest.raises(ValueError):  # the graph cannot exist
        AliasGraph(0, SCHEMA, {"mc": "movie_companies"}, edges, ())  # only mc


def test_alias_graph_post_init_rejects_an_edge_stated_twice():
    """Edges are deduplicated by the parser, so a repeated edge is a bug."""
    tables = {"mc": "movie_companies", "t": "title"}  # two aliases
    edge = Edge("mc", "movie_id", "t")  # mc.movie_id = t.id

    with pytest.raises(ValueError):  # the graph cannot exist
        AliasGraph(0, SCHEMA, tables, (edge, edge), ())  # the same edge twice


def test_alias_graph_post_init_rejects_a_foreign_key_equated_with_itself():
    """`a.x = a.x` is no equality between two references."""
    read_twice = ReferenceRead("at", "movie_id")  # the same reference on both sides
    equality = ForeignKeyEquality("title", read_twice, read_twice)  # same side

    with pytest.raises(ValueError):  # the graph cannot exist
        AliasGraph(0, SCHEMA, {"at": "aka_title"}, (), (equality,))  # one alias


def test_alias_graph_tree_of_no_alias_is_empty():
    """A query without tables has no root, so nothing to join or to read."""
    tree = build({}, ())  # SELECT 1, no FROM

    assert tree.aliases == ()  # no alias
    assert tree.roots == ()  # no root
    assert tree.join_members == ()  # no join
    assert tree.identities == ()  # nothing to equate
    assert tree.join(None) is None  # fewer than two roots


def test_alias_graph_tree_of_one_alias_is_its_only_root():
    """A query over one table has one root, no join and no identity."""
    tree = build({"t": "title"}, ())  # FROM title t

    assert tree.roots == ("t",)  # nothing references t
    assert tree.join_members == ()  # one root needs no join
    assert tree.identities == ()  # nothing to equate
    assert tree.path_of("t") == path_from("t", "t")  # the root tuple itself


def test_alias_graph_tree_removes_a_self_reference_as_a_cycle():
    """a closes a cycle at once, so a stays a root and tests the identity.

    ⟲ p (removed)
    a
    """
    tree = build({"a": "A"}, (Edge("a", "p", "a"),))  # an alias equated with itself
    a = path_from("a", "a")  # the root
    reference = read(a, "p")  # a.p, the reference that closes the cycle
    itself = Column.tuple_at(a)  # the tuple of a, which no reference read names

    assert tree.roots == ("a",)  # the edge no longer counts
    assert tree.identities == (TupleIdentityExpression(reference, itself),)  # a is a


def test_alias_graph_tree_removes_the_edge_that_closes_a_cycle():
    """The walk from a keeps r, and s becomes an identity with a itself.

    a ── r ──▶ b
    ▲          │
    └─── s ────┘ (removed)
    """
    edges = (Edge("a", "r", "b"), Edge("b", "s", "a"))  # a cycle, so no root at first
    tree = build({"a": "A", "b": "B"}, edges)  # the tree of the cycle
    via_r = path_from("b", "a", "r")  # b reached from a
    reference = read(via_r, "s")  # b.s, the reference that closes the cycle
    itself = tuple_at("a", "a")  # the tuple of a

    assert tree.roots == ("a",)  # b has its incoming edge left
    assert tree.path_of("b") == via_r  # along the kept edge
    assert tree.identities == (TupleIdentityExpression(reference, itself),)  # b to a


def test_alias_graph_tree_breaks_a_cycle_of_three_aliases_at_its_last_edge():
    """The walk from a keeps r and s; t closes the cycle and points back to a.

    a ── r ──▶ b ── s ──▶ c
    ▲                     │
    └───────── t ─────────┘ (removed)
    """
    edges = (
        Edge("a", "r", "b"),  # a.r = b.id
        Edge("b", "s", "c"),  # b.s = c.id
        Edge("c", "t", "a"),  # c.t = a.id, closes the cycle
    )
    tree = build({"a": "A", "b": "B", "c": "C"}, edges)  # no root at first
    via_r_s = path_from("c", "a", "r", "s")  # c reached from a in two steps
    reference = read(via_r_s, "t")  # c.t
    itself = tuple_at("a", "a")  # the tuple of a

    assert tree.roots == ("a",)  # the first declared alias
    assert tree.path_of("c") == via_r_s  # both steps, in walk order
    assert tree.identities == (TupleIdentityExpression(reference, itself),)  # c.t to a


def test_alias_graph_tree_breaks_two_separate_cycles():
    """Each cycle loses its closing edge; each walk starts at its first alias.

    a ── r ──▶ b      c ── u ──▶ d
    ▲          │      ▲          │
    └─── s ────┘      └─── v ────┘
     (removed)         (removed)
    """
    edges = (
        Edge("a", "r", "b"),  # a.r = b.id
        Edge("b", "s", "a"),  # b.s = a.id, closes the first cycle
        Edge("c", "u", "d"),  # c.u = d.id
        Edge("d", "v", "c"),  # d.v = c.id, closes the second cycle
    )
    tree = build({"a": "A", "b": "B", "c": "C", "d": "D"}, edges)  # two cycles
    b_read = reference_at("b", "a", "s", "r")  # b.s, read from a
    a_tuple = tuple_at("a", "a")  # the tuple of a
    d_read = reference_at("d", "c", "v", "u")  # d.v, read from c
    c_tuple = tuple_at("c", "c")  # the tuple of c

    assert tree.roots == ("a", "c")  # one root per cycle
    assert tree.identities == (
        TupleIdentityExpression(b_read, a_tuple),
        TupleIdentityExpression(d_read, c_tuple),
    )  # one identity per cycle, in stated order


def test_alias_graph_tree_breaks_a_cycle_by_walking_from_its_root():
    """x reaches the cycle, so the walk starts at x, not at b, the first declared alias.

    x ── p ──▶ a ── r ──▶ b
               ▲          │
               └─── s ────┘ (removed)
    """
    edges = (
        Edge("x", "p", "a"),  # x.p = a.id
        Edge("a", "r", "b"),  # a.r = b.id
        Edge("b", "s", "a"),  # b.s = a.id, closes the cycle
    )
    tables = {"b": "B", "a": "A", "x": "X"}  # b declared first, the root x last
    tree = build(tables, edges)  # the tree of the cycle below x
    via_p_r = path_from("b", "x", "p", "r")  # b reached from x in two steps
    reference = read(via_p_r, "s")  # b.s
    a_tuple = tuple_at("a", "x", "p")  # the tuple of a, from x

    assert tree.roots == ("x",)  # a starting walk at b would remove a.r instead
    assert tree.path_of("b") == via_p_r  # along p, then r
    assert tree.identities == (TupleIdentityExpression(reference, a_tuple),)  # b.s


def test_alias_graph_tree_skips_a_restated_foreign_key_equality():
    """Both references already lead to t, so the equality adds no alias and no edge."""
    equality = provisional_equality("title")  # mc.movie_id = mi.movie_id

    tree = build(MOVIES, MOVIE_EDGES, (equality,))  # both edges into t are stated too

    assert tree.aliases == ("mc", "mi", "t")  # no provisional alias
    assert tree.identities == ()  # t joins them, nothing to equate


@pytest.mark.parametrize(
    ("left", "right"),
    [("mc", "mi"), ("mi", "mc")],  # the held target is on either side
)
def test_alias_graph_tree_attaches_an_equality_to_the_alias_one_reference_leads_to(
    left, right
):
    """mi's reference joins t too, without a new alias.

    mc ── movie_id ──▶ t ◀── movie_id ── mi
         (stated)           (attached)
    """
    stated = (Edge("mc", "movie_id", "t"),)  # only mc's edge is stated
    left_read = ReferenceRead(left, "movie_id")  # one side of the equality
    right_read = ReferenceRead(right, "movie_id")  # the other side
    equality = ForeignKeyEquality("title", left_read, right_read)  # mi's is to attach
    tree = build(MOVIES, stated, (equality,))  # mi gets an edge into t
    info = column_at("mi", "mi", "info")  # mi.info
    on_info = BinaryExpression("==", info, Literal("x"))  # only mi answers it
    from_mc = column_at("t", "mc", "year", "movie_id")  # t.year read from mc
    on_year = BinaryExpression(">", from_mc, Literal(2000))  # either root answers it
    from_mi = column_at("t", "mi", "year", "movie_id")  # t.year read from mi
    rebased = BinaryExpression(">", from_mi, Literal(2000))  # now read from mi
    condition = conjunction(on_year, on_info)  # both conjuncts, the shared one first
    pushed = conjunction(rebased, on_info)  # both end up in mi, in stated order

    assert tree.aliases == ("mc", "mi", "t")  # no provisional alias
    assert tree.join(condition) == Join({"mi": pushed}, None, ())  # via the new edge


def test_alias_graph_tree_keeps_a_provisional_alias_that_joins_two_roots():
    """No declared alias is the title both references lead to, so title_0 stands for it.

    mc ── movie_id ──▶ title_0 ◀── movie_id ── mi
    """
    tables = {"mc": "movie_companies", "mi": "movie_info"}  # no title declared
    equality = provisional_equality("title")  # mc.movie_id = mi.movie_id

    tree = build(tables, (), (equality,))  # title_0 joins mc and mi

    assert tree.aliases == ("mc", "mi", "title_0")  # the provisional alias is last
    assert tree.join_members == ("mc", "mi", "title_0")  # it joins the two roots
    assert tree.identities == ()  # a hub, not an identity


def test_alias_graph_tree_names_a_provisional_alias_around_a_reserved_name():
    """title_0 is reserved, for example by an enclosing query, so the alias is title_1."""
    tables = {"mc": "movie_companies", "mi": "movie_info"}  # no title declared
    equality = provisional_equality("title")  # mc.movie_id = mi.movie_id
    reserved = frozenset({"title_0"})  # a name the tree must not use

    tree = build(tables, (), (equality,), reserved=reserved)  # title_1 joins them

    assert tree.aliases == ("mc", "mi", "title_1")  # the next free name


def test_alias_graph_tree_names_a_provisional_alias_as_a_python_name():
    """The table `order details` gives the alias order_details_0, which Python reads."""
    tables = {"mc": "movie_companies", "mi": "movie_info"}  # aliases of the equality
    equality = provisional_equality("order details")  # a table name with a space

    tree = build(tables, (), (equality,))  # a provisional alias joins mc and mi

    assert tree.aliases == ("mc", "mi", "order_details_0")  # the space became `_`
    assert tree.table_of("order_details_0") == "order details"  # the table stays


def test_alias_graph_tree_dissolves_a_provisional_alias_that_joins_nothing():
    """One root, so the equality is an identity, and no alias is left.

    a ── x ──▶ title_0
    │             ▲
    └───── y ─────┘

    ==> identity: a.y is a.x
    """
    x_read = ReferenceRead("a", "x")  # a's first reference
    y_read = ReferenceRead("a", "y")  # a's second reference
    equality = ForeignKeyEquality(
        "title", x_read, y_read
    )  # two references of one tuple

    tree = build({"a": "A"}, (), (equality,))  # no other alias
    a = path_from("a", "a")  # the root
    later = read(a, "y")  # the second reference
    first = read(a, "x")  # the first reference
    identity = TupleIdentityExpression(later, first)  # later against first

    assert tree.aliases == ("a",)  # the provisional alias is gone
    assert tree.identities == (identity,)  # the later edge against the first


def test_alias_graph_tree_skips_a_trivial_identity():
    """b and c are one tuple along r, so b.s and c.s lead to one tuple already.

    a ── r ──▶ b ── s ──▶ h
    │                     ▲
    └─ r ──▶ c ── s ──────┘ (removed)
    """
    edges = (
        Edge("a", "r", "b"),  # a.r = b.id
        Edge("a", "r", "c"),  # a.r = c.id, the same reference
        Edge("b", "s", "h"),  # b.s = h.id
        Edge("c", "s", "h"),  # c.s = h.id
    )
    tree = build({"a": "A", "b": "B", "c": "B", "h": "H"}, edges)  # a hub of one root

    assert tree.roots == ("a",)  # one root
    assert tree.path_of("h") == path_from("h", "a", "r", "s")  # two steps, walk order
    assert tree.identities == ()  # (r, s) on both sides


def test_alias_graph_tree_states_an_identity_of_two_tuples_once():
    """b.s and c.t lead to h and to j alike; the second hub restates the first identity.

    a ── r ──▶ b ──┬── s ──▶ h ◀── t ──┬── c
    │              │                   │   ▲
    │              └── s ──▶ j ◀── t ──┘   │
    └──────────────────── r ───────────────┘

    both edges of c.t are removed
    """
    edges = (
        Edge("a", "r", "b"),  # a.r = b.id
        Edge("a", "r", "c"),  # a.r = c.id, the same reference
        Edge("b", "s", "h"),  # b.s = h.id
        Edge("c", "t", "h"),  # c.t = h.id
        Edge("b", "s", "j"),  # b.s = j.id, the same reference again
        Edge("c", "t", "j"),  # c.t = j.id, the same reference again
    )
    tables = {"a": "A", "b": "B", "c": "B", "h": "H", "j": "J"}  # two hubs

    tree = build(tables, edges)  # the tree of both hubs
    b = path_from("b", "a", "r")  # b reached from a
    c = path_from("c", "a", "r")  # c reached from a
    b_read = read(b, "s")  # b.s
    c_read = read(c, "t")  # c.t
    identity = TupleIdentityExpression(c_read, b_read)  # the removed c.t, the kept b.s

    assert tree.identities == (identity,)  # once, not per hub


def test_alias_graph_tree_identity_reads_the_first_kept_edge_when_its_root_reaches_none():
    """t and h are both reached from three roots, so t goes first, as it comes first.
    t joins s1, s2 and s4; at h, s1 and s2 are then one component.
    So h keeps s2's edge, the first of that component, and s3's, and s1's edge is removed.
    s1 reaches neither s2 nor s3, so its identity reads the first kept edge.

    s1 ── t_id ──▶ t ◀── t_id ── s2
    │              ▲             │
    │              │ t_id        │ h_id
    │              s4            ▼
    └──── h_id (removed) ──────▶ h ◀── h_id ── s3
    """
    edges = (
        Edge("s1", "t_id", "t"),  # s1.t_id = t.id
        Edge("s2", "t_id", "t"),  # s2.t_id = t.id
        Edge("s4", "t_id", "t"),  # s4.t_id = t.id, so t joins s1, s2 and s4
        Edge("s2", "h_id", "h"),  # s2.h_id = h.id, kept: first of s1 and s2
        Edge("s3", "h_id", "h"),  # s3.h_id = h.id, kept: h joins s3
        Edge("s1", "h_id", "h"),  # s1.h_id = h.id, removed: s2's edge stays instead
    )
    tables = {
        "s1": "S",
        "s2": "S",
        "s3": "S",
        "s4": "S",
        "t": "T",
        "h": "H",
    }  # four roots, then the hubs t and h, each reached from three roots
    tree = build(tables, edges)  # the tree of both hubs
    s1_read = reference_at("s1", "s1", "h_id")  # the removed edge, read from s1
    s2_read = reference_at("s2", "s2", "h_id")  # the first kept edge in stated order
    identity = TupleIdentityExpression(s1_read, s2_read)  # not s3's edge

    assert tree.join_members == (
        "s1",
        "s2",
        "s3",
        "s4",
        "t",
        "h",
    )  # t and h are join hubs
    assert tree.identities == (identity,)  # the kept edge is s2's, not s3's


def test_alias_graph_tree_pairs_a_removed_edge_with_the_kept_edge_its_root_reaches():
    """The removed edge of c pairs with s's edge, which c reaches, not with a's, the first.

    a ── h_a ──▶ h ◀── h_s ── s ◀── p ── c
                 ▲                       │
                 └──── h_c (removed) ────┘
    """
    edges = (
        Edge("c", "p", "s"),  # c.p = s.id, so c reaches s
        Edge("a", "h_a", "h"),  # a.h_a = h.id, kept: a is a component of its own
        Edge("s", "h_s", "h"),  # s.h_s = h.id, kept: the first of c's component
        Edge("c", "h_c", "h"),  # c.h_c = h.id, removed: c's component has an edge
    )
    tables = {"a": "A", "c": "C", "s": "S", "h": "H"}  # the roots a and c
    tree = build(tables, edges)  # the tree of the one hub h
    removed = reference_at("c", "c", "h_c")  # c.h_c, read from c itself
    kept = reference_at("s", "c", "h_s", "p")  # s.h_s, read along c's path to s
    identity = TupleIdentityExpression(removed, kept)  # not paired with a.h_a

    assert tree.identities == (identity,)  # c's identity reads the edge c reaches


def test_alias_graph_tree_resolves_the_hub_that_covers_the_most_roots_first():
    """h is reached from three roots, t from two, so h goes first and joins all three.
    At t, s1 and s2 are then one component, so t keeps s1's edge only.

    s1 ── t_id ──▶ t ◀── t_id (removed) ── s2
    │                                      │
    │                                      ▼ h_id
    └──────────── h_id ──────────────────▶ h ◀── h_id ── s3

    ==> identity: s2.t_id is s1.t_id
    """
    edges = (
        Edge("s1", "t_id", "t"),  # s1.t_id = t.id, kept: the first into t
        Edge("s2", "t_id", "t"),  # s2.t_id = t.id, removed: h joined s1 and s2
        Edge("s2", "h_id", "h"),  # s2.h_id = h.id
        Edge("s3", "h_id", "h"),  # s3.h_id = h.id
        Edge("s1", "h_id", "h"),  # s1.h_id = h.id, so h is reached from all roots
    )
    tables = {"s1": "S", "s2": "S", "s3": "S", "t": "T", "h": "H"}  # t before h
    tree = build(tables, edges)  # the tree of both hubs
    s2_read = reference_at("s2", "s2", "t_id")  # the removed edge, read from s2
    s1_read = reference_at("s1", "s1", "t_id")  # the kept edge, read from s1
    identity = TupleIdentityExpression(s2_read, s1_read)  # s2's t is s1's t

    assert tree.join_members == ("s1", "s2", "s3", "h")  # t is no join hub
    assert tree.identities == (identity,)  # t's removed edge, not h's
    assert tree.path_of("t") == path_from("t", "s1", "t_id")  # along the kept edge


def test_alias_graph_tree_counts_the_roots_not_the_aliases_that_reach_a_hub():
    """l is reached from four aliases but two roots, h from three roots, so h goes first.
    At l, r1 and r2 are then one component, so l keeps m1's edge only.

    r1 ── m1_id ──▶ m1 ── l_id ──▶ l ◀── l_id (removed) ── m2 ◀── m2_id ── r2
    │                                                                      │
    └──── h_id ────────────────────▶ h ◀────────────────────── h_id ───────┘
                                     ▲
                                     └── h_id ── r3

    ==> identity: r2.m2_id.l_id is r1.m1_id.l_id
    """
    edges = (
        Edge("r1", "m1_id", "m1"),  # r1.m1_id = m1.id
        Edge("r2", "m2_id", "m2"),  # r2.m2_id = m2.id
        Edge("m1", "l_id", "l"),  # m1.l_id = l.id, kept: the first into l
        Edge("m2", "l_id", "l"),  # m2.l_id = l.id, removed: h joined r1 and r2
        Edge("r1", "h_id", "h"),  # r1.h_id = h.id
        Edge("r2", "h_id", "h"),  # r2.h_id = h.id
        Edge("r3", "h_id", "h"),  # r3.h_id = h.id
    )
    tables = {
        "r1": "R",
        "r2": "R",
        "r3": "R",
        "m1": "M",
        "m2": "M",
        "l": "L",
        "h": "H",
    }  # l before h
    tree = build(tables, edges)  # the tree of both hubs
    m2_read = reference_at("m2", "r2", "l_id", "m2_id")  # the removed edge, from r2
    m1_read = reference_at("m1", "r1", "l_id", "m1_id")  # the kept edge, from r1
    identity = TupleIdentityExpression(m2_read, m1_read)  # m2's l is m1's l

    assert tree.join_members == ("r1", "r2", "r3", "h")  # l is no join hub
    assert tree.identities == (identity,)  # l's removed edge, not h's


def test_alias_graph_tree_counts_the_roots_not_the_edges_that_reach_a_hub():
    """a has two incoming edges but three roots, as many as b, and comes first.
    So x and a join all roots; at b, they are then one component, and b keeps r1's edge.

    r1 ── x_id ──▶ x ◀── x_id ── r2
                   │
                   ▼ a_id
                   a ◀── a_id ── r3

    r1, r2, r3 ── b_id ──▶ b      the edges of r2 and r3 are removed

    ==> identities: r2.b_id is r1.b_id, r3.b_id is r1.b_id
    """
    edges = (
        Edge("r1", "x_id", "x"),  # r1.x_id = x.id
        Edge("r2", "x_id", "x"),  # r2.x_id = x.id, so x joins r1 and r2
        Edge("x", "a_id", "a"),  # x.a_id = a.id, reached from r1 and r2
        Edge("r3", "a_id", "a"),  # r3.a_id = a.id, so a joins r3
        Edge("r1", "b_id", "b"),  # r1.b_id = b.id, kept: the first into b
        Edge("r2", "b_id", "b"),  # r2.b_id = b.id, removed
        Edge("r3", "b_id", "b"),  # r3.b_id = b.id, removed
    )
    tables = {
        "r1": "R",
        "r2": "R",
        "r3": "R",
        "x": "X",
        "a": "A",
        "b": "B",
    }  # a before b, so a wins the tie of reach 3
    tree = build(tables, edges)  # the tree of three hubs
    r1_read = reference_at("r1", "r1", "b_id")  # the kept edge, read from r1
    r2_read = reference_at("r2", "r2", "b_id")  # a removed edge, read from r2
    r3_read = reference_at("r3", "r3", "b_id")  # a removed edge, read from r3
    r2_identity = TupleIdentityExpression(r2_read, r1_read)  # r2's b is r1's b
    r3_identity = TupleIdentityExpression(r3_read, r1_read)  # r3's b is r1's b

    assert tree.join_members == ("r1", "r2", "r3", "x", "a")  # b is no join hub
    assert tree.identities == (r2_identity, r3_identity)  # in stated order


def test_alias_graph_tree_resolves_hubs_of_equal_reach_in_topological_order():
    """h and j are both reached from a and b; j is declared first, so it joins them.
    At h, a and b are then one component, so h keeps a's edge only.

    a ── j_id ──▶ j ◀── j_id ── b
    │                           │
    └── h_id ──▶ h ◀── h_id ────┘ (removed)

    ==> identity: b.h_id is a.h_id
    """
    edges = (
        Edge("a", "h_id", "h"),  # a.h_id = h.id, stated first, kept
        Edge("b", "h_id", "h"),  # b.h_id = h.id, removed: j joined a and b
        Edge("a", "j_id", "j"),  # a.j_id = j.id
        Edge("b", "j_id", "j"),  # b.j_id = j.id
    )
    tables = {"a": "A", "b": "B", "j": "J", "h": "H"}  # j before h
    tree = build(tables, edges)  # the tree of both hubs
    b_read = reference_at("b", "b", "h_id")  # the removed edge, read from b
    a_read = reference_at("a", "a", "h_id")  # the kept edge, read from a
    identity = TupleIdentityExpression(b_read, a_read)  # b's h is a's h

    assert tree.join_members == ("a", "b", "j")  # j joins, h does not
    assert tree.identities == (identity,)  # h's removed edge, not j's


def test_alias_graph_tree_resolves_hubs_of_equal_reach_in_topological_not_declaration_order():
    """w, x and z are each reached from two roots. z's roots come first, so z comes first
    in topological order, then w, then x, although w and x are declared before z.
    z joins r1 and r2, w joins r3; at x, r1 and r3 are then one component.

    r1 ── z_id ──▶ z ◀── z_id ── r2 ── w_id ──▶ w ◀── w_id ── r3
    │                                                         │
    └──────── x_id ────────▶ x ◀────── x_id (removed) ────────┘

    ==> identity: r3.x_id is r1.x_id
    """
    edges = (
        Edge("r1", "z_id", "z"),  # r1.z_id = z.id
        Edge("r2", "z_id", "z"),  # r2.z_id = z.id
        Edge("r2", "w_id", "w"),  # r2.w_id = w.id
        Edge("r3", "w_id", "w"),  # r3.w_id = w.id
        Edge("r1", "x_id", "x"),  # r1.x_id = x.id, kept: the first into x
        Edge("r3", "x_id", "x"),  # r3.x_id = x.id, removed: z and w joined r1 and r3
    )
    tables = {
        "w": "W",
        "x": "X",
        "z": "Z",
        "r1": "R",
        "r2": "R",
        "r3": "R",
    }  # the hubs before the roots, z last
    tree = build(tables, edges)  # the tree of three hubs
    r3_read = reference_at("r3", "r3", "x_id")  # the removed edge, read from r3
    r1_read = reference_at("r1", "r1", "x_id")  # the kept edge, read from r1
    identity = TupleIdentityExpression(r3_read, r1_read)  # r3's x is r1's x

    assert tree.join_members == ("w", "z", "r1", "r2", "r3")  # x is no join hub
    assert tree.identities == (identity,)  # x's removed edge, not z's


def test_alias_graph_tree_resolves_a_provisional_hub_with_the_larger_reach_first():
    """company_name_0 is reached from three roots, t from two, so it goes first.
    At t, mc and mi are then one component, so t keeps mc's edge only.

    mc ── movie_id ──▶ t ◀── movie_id (removed) ── mi
    │                                              │
    └── company_id ──▶ company_name_0 ◀── company_id
                             ▲
                             └── company_id ── mk

    ==> identity: mi.movie_id is mc.movie_id
    """
    tables = {
        "mc": "movie_companies",
        "mi": "movie_info",
        "mk": "movie_keyword",
        "t": "title",
    }  # no company_name declared
    edges = (
        Edge("mc", "movie_id", "t"),  # mc.movie_id = t.id, kept
        Edge("mi", "movie_id", "t"),  # mi.movie_id = t.id, removed
    )
    mc_read = ReferenceRead("mc", "company_id")  # mc's company
    mi_read = ReferenceRead("mi", "company_id")  # mi's company
    mk_read = ReferenceRead("mk", "company_id")  # mk's company
    mc_mi = ForeignKeyEquality("company_name", mc_read, mi_read)  # a new alias
    mi_mk = ForeignKeyEquality("company_name", mi_read, mk_read)  # into that alias
    tree = build(tables, edges, (mc_mi, mi_mk))  # company_name_0 joins all three
    mi_movie = reference_at("mi", "mi", "movie_id")  # the removed edge, read from mi
    mc_movie = reference_at("mc", "mc", "movie_id")  # the kept edge, read from mc
    identity = TupleIdentityExpression(mi_movie, mc_movie)  # mi's t is mc's t

    assert tree.aliases == (
        "mc",
        "mi",
        "mk",
        "t",
        "company_name_0",
    )  # the provisional alias stays, last
    assert tree.join_members == (
        "mc",
        "mi",
        "mk",
        "company_name_0",
    )  # t is no join hub
    assert tree.identities == (identity,)  # t's removed edge, not a company edge


def test_alias_graph_tree_dissolves_a_provisional_hub_with_the_smaller_reach():
    """h is reached from three roots, p_0 from two, so h goes first and joins all three.
    At p_0, a and b are then one component, so p_0 joins nothing and is dissolved.

    a ── p_id ──▶ p_0 ◀── p_id ── b
    │                             │
    └── h_id ──▶ h ◀── h_id ──────┘
                 ▲
                 └── h_id ── c

    ==> identity: b.p_id is a.p_id
    """
    edges = (
        Edge("a", "h_id", "h"),  # a.h_id = h.id
        Edge("b", "h_id", "h"),  # b.h_id = h.id
        Edge("c", "h_id", "h"),  # c.h_id = h.id
    )
    a_read = ReferenceRead("a", "p_id")  # a's p
    b_read = ReferenceRead("b", "p_id")  # b's p
    equality = ForeignKeyEquality("p", a_read, b_read)  # no alias of p: a new one
    tables = {"a": "A", "b": "B", "c": "C", "h": "H"}  # no p declared
    tree = build(tables, edges, (equality,))  # p_0 is added, then dissolved
    b_p = reference_at("b", "b", "p_id")  # the later edge into p_0, read from b
    a_p = reference_at("a", "a", "p_id")  # the first edge into p_0, read from a
    identity = TupleIdentityExpression(b_p, a_p)  # b's p is a's p

    assert tree.aliases == ("a", "b", "c", "h")  # p_0 is gone
    assert tree.join_members == ("a", "b", "c", "h")  # only h joins
    assert tree.identities == (identity,)  # the equality as an identity


def test_alias_graph_tree_resolves_a_hub_that_covers_all_roots_before_two_partial_hubs():
    """c is reached from all three roots, a and b from two each, so c goes first.
    Then a and b each find one component and keep their first edge.

    r1 ── a_id ──▶ a ◀── a_id (removed) ── r2 ── b_id ──▶ b ◀── b_id (removed) ── r3
    │                                      │                                     │
    │                                      │ c_id                                │
    │                                      ▼                                     │
    └─────────────── c_id ───────────────▶ c ◀──────────── c_id ─────────────────┘

    ==> identities: r2.a_id is r1.a_id, r3.b_id is r2.b_id
    """
    edges = (
        Edge("r1", "a_id", "a"),  # r1.a_id = a.id, kept
        Edge("r2", "a_id", "a"),  # r2.a_id = a.id, removed: c joined r1 and r2
        Edge("r2", "b_id", "b"),  # r2.b_id = b.id, kept
        Edge("r3", "b_id", "b"),  # r3.b_id = b.id, removed: c joined r2 and r3
        Edge("r1", "c_id", "c"),  # r1.c_id = c.id
        Edge("r2", "c_id", "c"),  # r2.c_id = c.id
        Edge("r3", "c_id", "c"),  # r3.c_id = c.id
    )
    tables = {
        "r1": "R",
        "r2": "R",
        "r3": "R",
        "a": "A",
        "b": "B",
        "c": "C",
    }  # the hubs a, b and c in this order
    tree = build(tables, edges)  # the tree of three hubs
    r2_a = reference_at("r2", "r2", "a_id")  # a's removed edge, read from r2
    r1_a = reference_at("r1", "r1", "a_id")  # a's kept edge, read from r1
    r3_b = reference_at("r3", "r3", "b_id")  # b's removed edge, read from r3
    r2_b = reference_at("r2", "r2", "b_id")  # b's kept edge, read from r2
    a_identity = TupleIdentityExpression(r2_a, r1_a)  # r2's a is r1's a
    b_identity = TupleIdentityExpression(r3_b, r2_b)  # r3's b is r2's b

    assert tree.join_members == ("r1", "r2", "r3", "c")  # only c joins
    assert tree.identities == (a_identity, b_identity)  # in stated order


def test_alias_graph_tree_keeps_an_upstream_hub_that_joins_a_root_the_downstream_hub_needs():
    """h3 is reached from three roots, h1 from two, but h1 is upstream of h3, so h1 goes first.
    h1 joins r1 and r2, then h3 joins r3, so both hubs keep all edges.

    r1 ── h1_id ──▶ h1 ◀── h1_id ── r2
                    │
                    ▼ h3_id
                    h3 ◀── h3_id ── r3
    """
    edges = (
        Edge("r1", "h1_id", "h1"),  # r1.h1_id = h1.id
        Edge("r2", "h1_id", "h1"),  # r2.h1_id = h1.id, joins r2 to the rest
        Edge("h1", "h3_id", "h3"),  # h1.h3_id = h3.id, reached from r1 and r2
        Edge("r3", "h3_id", "h3"),  # r3.h3_id = h3.id
    )
    tables = {"r1": "R", "r2": "R", "r3": "R", "h1": "H", "h3": "H"}  # h1 upstream
    tree = build(tables, edges)  # the tree of the chain of hubs

    assert tree.join_members == ("r1", "r2", "r3", "h1", "h3")  # both hubs join
    assert tree.identities == ()  # no edge is removed


def test_alias_graph_tree_resolves_the_hubs_upstream_of_a_hub_first():
    """d is reached from three roots, u from two, but u is upstream of d, so u goes first.
    u joins a and b. At d, u's edge and b's edge are then from one component,
    so d keeps u's edge, the first, and c's edge, and b's own edge is removed.

    a ── u_id ──▶ u ◀── u_id ── b
                  │             │
                  ▼ d_id        │
                  d ◀── d_id (removed)
                  ▲
                  └── d_id ── c

    ==> identity: b.d_id is b.u_id.d_id
    """
    edges = (
        Edge("b", "u_id", "u"),  # b.u_id = u.id
        Edge("a", "u_id", "u"),  # a.u_id = u.id, so u joins a and b
        Edge("u", "d_id", "d"),  # u.d_id = d.id, kept: reached from a and b
        Edge("b", "d_id", "d"),  # b.d_id = d.id, removed: u already carries b
        Edge("c", "d_id", "d"),  # c.d_id = d.id, kept: d joins c
    )
    tables = {"a": "A", "b": "B", "c": "C", "u": "U", "d": "D"}  # three roots
    tree = build(tables, edges)  # the tree of both hubs
    removed = reference_at("b", "b", "d_id")  # b.d_id, read from b itself
    kept = reference_at("u", "b", "d_id", "u_id")  # u.d_id, read along b's path to u
    identity = TupleIdentityExpression(removed, kept)  # b's d is d along u

    assert tree.join_members == ("a", "b", "c", "u", "d")  # u and d both join
    assert tree.identities == (identity,)  # only b's own edge into d is removed
    assert tree.path_of("d") == path_from("d", "a", "u_id", "d_id")  # one path, via u


def test_alias_graph_tree_joins_with_two_hubs_that_overlap():
    """h1 and h2 are both reached from three roots; h1 comes first and joins r1, r2, r3.
    h2 then joins that component and r4, and keeps r2's edge of the two.

    r1 ── h1_id ──▶ h1 ◀── h1_id ── r2 ── h2_id ──▶ h2 ◀── h2_id ── r4
                    ▲                               ▲
                    └──── h1_id ── r3 ── h2_id ─────┘
                                         (removed)

    ==> identity: r3.h2_id is r2.h2_id
    """
    edges = (
        Edge("r1", "h1_id", "h1"),  # r1.h1_id = h1.id
        Edge("r2", "h1_id", "h1"),  # r2.h1_id = h1.id
        Edge("r3", "h1_id", "h1"),  # r3.h1_id = h1.id
        Edge("r2", "h2_id", "h2"),  # r2.h2_id = h2.id, kept
        Edge("r3", "h2_id", "h2"),  # r3.h2_id = h2.id, removed: h1 joined r2 and r3
        Edge("r4", "h2_id", "h2"),  # r4.h2_id = h2.id, kept: h2 joins r4
    )
    tables = {
        "r1": "R",
        "r2": "R",
        "r3": "R",
        "r4": "R",
        "h1": "H",
        "h2": "H",
    }  # h1 before h2
    tree = build(tables, edges)  # the tree of both hubs
    r3_read = reference_at("r3", "r3", "h2_id")  # the removed edge, read from r3
    r2_read = reference_at("r2", "r2", "h2_id")  # the kept edge, read from r2
    identity = TupleIdentityExpression(r3_read, r2_read)  # r3's h2 is r2's h2

    assert tree.join_members == (
        "r1",
        "r2",
        "r3",
        "r4",
        "h1",
        "h2",
    )  # r1 needs h1 and r4 needs h2, so both join
    assert tree.identities == (identity,)  # h2's removed edge, not h1's


def test_join_tree_join_members_of_roots_no_hub_connects_are_the_roots():
    """No edge connects a and b, but both are roots, so both are members.

    a        b
    """
    tree = build({"a": "A", "b": "B"}, ())  # FROM a, b

    assert tree.join_members == ("a", "b")  # the roots, without a hub


def test_join_tree_path_of_an_alias_of_another_query_is_none():
    """A correlated column names an alias this tree does not hold."""
    tree = build({"t": "title"}, ())  # FROM title t

    assert tree.path_of("outer") is None  # the caller looks in the outer query


def test_join_tree_table_of_an_alias_of_another_query_is_none():
    """Only the aliases of this query have a table here."""
    tree = build({"t": "title"}, ())  # FROM title t

    assert tree.table_of("t") == "title"  # its own alias
    assert tree.table_of("outer") is None  # not of this query


def test_join_tree_join_of_one_root_is_none():
    """A query over one root joins nothing; its condition stays on its tuples."""
    tree = build({"t": "title"}, ())  # FROM title t
    year = column_at("t", "t", "production_year")  # t.production_year
    condition = BinaryExpression(">", year, Literal(2000))  # t.production_year > 2000

    assert tree.join(condition) is None  # no join


def test_join_tree_join_leaves_a_conjunct_across_roots_on_the_join_tuples():
    """A comparison across two roots needs the join."""
    tree = build(MOVIES, MOVIE_EDGES)  # mc -> t <- mi
    note = column_at("mc", "mc", "note")  # mc.note
    info = column_at("mi", "mi", "info")  # mi.info
    across = BinaryExpression("==", note, info)  # reads both roots

    assert tree.join(across) == Join({}, across, ())  # stays on the join tuples


@pytest.mark.parametrize(
    "wrap",
    [
        lambda note, subquery: ExistsExpression(subquery),  # EXISTS (...)
        lambda note, subquery: UnaryExpression(
            "not", ExistsExpression(subquery)
        ),  # NOT EXISTS (...)
        lambda note, subquery: InSubqueryExpression(note, subquery),  # note IN (...)
        lambda note, subquery: BinaryExpression(
            "==", note, SubqueryExpression(subquery)
        ),  # note = (...)
    ],
)
def test_join_tree_join_leaves_a_conjunct_with_a_subquery_on_the_join_tuples(wrap):
    """A subquery may read any alias, so even a conjunct that reads only mc stays."""
    tree = build(MOVIES, MOVIE_EDGES)  # mc -> t <- mi
    note = column_at("mc", "mc", "note")  # mc.note, only mc answers it
    inner_tree = AliasGraph(1, SCHEMA, {"x": "title"}, (), ()).tree()  # depth 1
    subquery = Query(inner_tree, Query(tree))  # nested in the query of `tree`
    conjunct = wrap(note, subquery)  # one form of subquery per row

    assert tree.join(conjunct) == Join({}, conjunct, ())  # not pushed into mc


def test_join_tree_join_pushes_a_conjunct_to_the_root_answering_the_most():
    """t's filter is answered by both roots; mi answers more, so it goes there."""
    tree = build(MOVIES, MOVIE_EDGES)  # mc -> t <- mi
    info = column_at("mi", "mi", "info")  # mi.info
    on_info = BinaryExpression("==", info, Literal("x"))  # only mi answers it
    from_mc = column_at("t", "mc", "year", "movie_id")  # t.year read from mc
    on_year = BinaryExpression(">", from_mc, Literal(2000))  # either root answers it
    condition = conjunction(on_year, on_info)  # the shared conjunct stated first
    from_mi = column_at("t", "mi", "year", "movie_id")  # t.year read from mi
    rebased = BinaryExpression(">", from_mi, Literal(2000))  # now read from mi
    pushed = conjunction(rebased, on_info)  # both pushed into mi, in stated order

    assert tree.join(condition) == Join({"mi": pushed}, None, ())  # all into mi


def test_join_tree_join_prefers_the_earlier_root_on_a_tie():
    """A conjunct only t reads: both roots answer one conjunct each, so the first gets it."""
    tree = build(MOVIES, MOVIE_EDGES)  # mc declared before mi
    from_mi = column_at("t", "mi", "year", "movie_id")  # read from mi
    on_year = BinaryExpression(">", from_mi, Literal(2000))  # either root answers it
    from_mc = column_at("t", "mc", "year", "movie_id")  # read from mc
    rebased = BinaryExpression(">", from_mc, Literal(2000))  # now read from mc

    assert tree.join(on_year) == Join({"mc": rebased}, None, ())  # mc, the first root


def test_join_tree_join_drops_a_schema_reference_between_members_that_is_no_edge():
    """Only mc.info leads to another member without being followed by the join.

                    ⟲ episode_of (self-reference)
    mc ── movie ──▶ t ◀── movie ── mi
    │                              ▲
    ├────── info (dropped) ────────┘
    └── kind (no member) ──▶ kind_type

    only the two movie edges are join edges; the rest are schema references
    """
    schema = Schema(
        references={
            "movie_companies": {
                "movie_id": Reference("movie_id", "title"),  # the join follows it
                "info_id": Reference("info", "movie_info"),  # to a member: dropped
                "kind_id": Reference("kind", "kind_type"),  # to no member
            },
            "title": {"episode_of": Reference("episode_of", "title")},  # to itself
        },
        primary_keys={},
        columns={},
    )  # mc -> t <- mi by the edges, and mc -> mi only by the schema
    tree = build(MOVIES, MOVIE_EDGES, schema=schema)  # the join of mc and mi
    join = tree.join(None)  # the join, no condition

    assert join.dropped_edges == (Edge("mc", "info", "mi"),)  # only that one


def test_join_tree_join_drops_a_schema_reference_between_roots_no_hub_connects():
    """No edge connects a and b, so the join must not follow a's reference to b.

    a ── ref (dropped) ──▶ b

    ref is a schema reference, not a join edge
    """
    schema = Schema(
        references={"A": {"ref_id": Reference("ref", "B")}},  # A references B
        primary_keys={},
        columns={},
    )  # no edge is stated
    tree = build({"a": "A", "b": "B"}, (), schema=schema)  # FROM a, b
    join = tree.join(None)  # the join of a and b, no condition

    assert join.dropped_edges == (Edge("a", "ref", "b"),)  # a.ref is no join edge


def test_join_tree_join_drops_nothing_when_a_table_has_several_members():
    """t1 and t2 are both title, mi1 and mi2 both movie_info: FQL refuses such a join,
    so not even mi1's reference to it, the only info_type member, is dropped.

                ⟲ episode_of                            ⟲ episode_of
    mi1 ── movie ──▶ t1 ◀── movie ── ml ── linked_movie ──▶ t2 ◀── movie ── mi2
    │                                                                        │
    └───────────── info_type ─────────▶ it ◀────────── info_type ────────────┘

    only the four movie edges are join edges; info_type, episode_of and
    ml.episode (into t1 or t2?) are schema references, and none is dropped
    """
    schema = Schema(
        references={
            "movie_link": {
                "movie_id": Reference("movie", "title"),  # followed into t1
                "linked_movie_id": Reference("linked_movie", "title"),  # into t2
                "episode_id": Reference("episode", "title"),  # into t1 or t2
            },
            "movie_info": {
                "movie_id": Reference("movie", "title"),  # followed
                "info_type_id": Reference("info_type", "info_type"),  # to it
            },
            "title": {"episode_of_id": Reference("episode_of", "title")},  # to title
        },
        primary_keys={},
        columns={},
    )  # title and movie_info each stand for two members below
    tables = {
        "ml": "movie_link",
        "mi1": "movie_info",
        "mi2": "movie_info",
        "it": "info_type",
        "t1": "title",
        "t2": "title",
    }  # two members of title, as in JOB 33a; it is a root without an edge
    edges = (
        Edge("ml", "movie", "t1"),  # ml.movie_id = t1.id
        Edge("ml", "linked_movie", "t2"),  # ml.linked_movie_id = t2.id
        Edge("mi1", "movie", "t1"),  # mi1.movie_id = t1.id, so t1 joins ml and mi1
        Edge("mi2", "movie", "t2"),  # mi2.movie_id = t2.id, so t2 joins ml and mi2
    )
    tree = build(tables, edges, schema=schema)  # four roots, two hubs
    join = tree.join(None)  # the join, no condition

    assert tree.join_members == ("ml", "mi1", "mi2", "it", "t1", "t2")  # all six
    assert join.dropped_edges == ()  # not even mi1.info_type into it


def test_join_tree_join_drops_a_reference_the_join_follows_into_a_non_member_only():
    """mc.info leads to i2, which is no member, so the reference to member mi is dropped.

    mc ── movie ──▶ t ◀── movie ── mi
    │                              ▲
    ├────── info (dropped) ────────┘
    └── info ──▶ i2

    mc.info into i2 is an edge, but i2 is no member; mc.info into mi is a schema reference
    """
    schema = Schema(
        references={
            "movie_companies": {
                "movie_id": Reference("movie_id", "title"),  # the join follows it
                "info_id": Reference("info", "movie_info"),  # followed to i2, not mi
            },
        },
        primary_keys={},
        columns={},
    )  # mc -> t <- mi, and mc -> i2 outside the join
    tables = {**MOVIES, "i2": "movie_info"}  # i2 is a second alias of mi's table
    edges = (*MOVIE_EDGES, Edge("mc", "info", "i2"))  # i2 hangs off mc only
    tree = build(tables, edges, schema=schema)  # the join of mc and mi
    join = tree.join(None)  # the join, no condition

    assert tree.join_members == ("mc", "mi", "t")  # i2 is on no way to the hub
    assert join.dropped_edges == (Edge("mc", "info", "mi"),)  # mi is not reached by it


def test_join_tree_join_keeps_a_reference_into_a_filtered_root():
    """FQL's `where` returns a new relation, so no reference leads to a pushed root.

    mc ── movie ──▶ t ◀── movie ── mi (filtered)
    │                              ▲
    └──── info (not dropped) ──────┘

    info is a schema reference, not a join edge
    """
    schema = Schema(
        references={"movie_companies": {"info_id": Reference("info", "movie_info")}},
        primary_keys={},
        columns={},
    )  # mc -> mi only by the schema
    tree = build(MOVIES, MOVIE_EDGES, schema=schema)  # the join of mc and mi
    info = column_at("mi", "mi", "info")  # mi.info
    on_info = BinaryExpression("==", info, Literal("x"))  # pushed into mi
    join = tree.join(on_info)  # the join with mi filtered

    assert join.dropped_edges == ()  # nothing left to drop


def test_join_tree_join_leaves_a_correlated_path_as_it_is():
    """A path of the enclosing query is not rebased, even if the subquery declares the same alias,
    and a path to an alias only the enclosing query has is no error."""
    tree = build(MOVIES, MOVIE_EDGES, depth=1)  # the tree of a subquery
    outer = Column(Path(0, "t", "t", ()), "production_year")  # the outer t
    outer_only = Column(Path(0, "o", "o", ()), "note")  # o: no alias of the subquery
    info = Column(Path(1, "mi", "mi", ()), "info")  # mi.info
    via_mc = Path(1, "t", "mc", ("movie_id",))  # the subquery's own t, via mc
    via_mi = Path(1, "t", "mi", ("movie_id",))  # the subquery's own t, via mi
    from_mc = Column(via_mc, "production_year")  # inner t.production_year
    from_mi = Column(via_mi, "production_year")  # the same, read from mi
    on_info = BinaryExpression("==", info, Literal("x"))  # mi.info = x
    newer_than_outer = BinaryExpression(">", from_mc, outer)  # inner year > outer year
    on_outer = BinaryExpression("==", outer_only, Literal("y"))  # o.note = y
    condition = conjunction(on_info, newer_than_outer, on_outer)  # all into mi
    rebased = BinaryExpression(">", from_mi, outer)  # inner t now read from mi
    pushed = conjunction(on_info, rebased, on_outer)  # outer paths stay as they are
    expected = Join({"mi": pushed}, None, ())  # everything pushed into mi

    assert tree.join(condition) == expected  # t read from mi, outer untouched

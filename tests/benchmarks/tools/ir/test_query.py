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

"""The names a query's result exposes, and what its clauses make of it."""

import pytest

from benchmarks.datasets.schema import Schema
from benchmarks.tools.ir.expressions import (
    AggregateExpression,
    BinaryExpression,
    Column,
    Literal,
    conjunction,
)
from benchmarks.tools.ir.join_equalities import Edge
from benchmarks.tools.ir.join_tree import AliasGraph, Join, JoinTree
from benchmarks.tools.ir.paths import Path
from benchmarks.tools.ir.query import (
    GeneratedColumns,
    Limit,
    OrderKey,
    Output,
    Query,
)

# The edges are given by hand, so the schema declares nothing.
SCHEMA = Schema(references={}, primary_keys={}, columns={})
TITLE = {"t": "title"}  # one alias, so one root
# The tables of the join below:
#
#   mc ── movie ──▶ t ◀── movie ── mi
#
#   mc: movie_companies, t: title, mi: movie_info
#   t has two incoming edges: it is the hub that joins the roots mc and mi
MOVIES = {"mc": "movie_companies", "mi": "movie_info", "t": "title"}
MOVIE_EDGES = (Edge("mc", "movie", "t"), Edge("mi", "movie", "t"))
COUNT_ALL = AggregateExpression("count", None)  # COUNT(*)


def tree_over(
    tables: dict[str, str], edges: tuple[Edge, ...] = (), depth: int = 0
) -> JoinTree:
    """The join tree of `tables` along `edges`, in a query nested `depth` levels deep."""
    graph = AliasGraph(depth, SCHEMA, tables, edges, ())  # no foreign-key equalities
    return graph.tree()  # what a parsed FROM gives a query


def column_of(alias: str, name: str, depth: int = 0) -> Column:
    """The column `name` of the root `alias`, read in a query `depth` levels deep."""
    return Column(Path(depth, alias, alias, ()), name)  # a root reaches itself


T_ID = column_of("t", "id")  # t.id
T_TITLE = column_of("t", "title")  # t.title


def test_generated_columns_column_for_makes_a_column_once_per_expression():
    """A first use makes the column, a later use of the same expression reads it;
    a given name takes no generated number."""
    columns = GeneratedColumns("_computed_")  # nothing generated yet
    year = column_of("t", "production_year")  # t.production_year
    later = BinaryExpression("+", year, Literal(1))  # year + 1
    earlier = BinaryExpression("-", year, Literal(1))  # year - 1
    doubled = BinaryExpression("*", year, Literal(2))  # year * 2

    first = columns.column_for(later)  # made under a generated name
    assert first == Column(None, "_computed_0")  # the first number
    assert columns.column_for(later) is first  # repeated use, the same column
    assert columns.column_for(earlier, "e") == Column(None, "e")  # the given name
    assert columns.column_for(doubled) == Column(None, "_computed_1")  # next number


def test_query_post_init_rejects_a_tree_of_another_depth():
    """A query's tree lies as deep as the query is nested."""
    enclosing = Query(tree_over(TITLE))  # depth 0
    tree = tree_over(TITLE)  # also built at depth 0

    with pytest.raises(ValueError):  # a subquery needs a tree at depth 1
        Query(tree, enclosing)  # nested one level, tree at depth 0


def test_query_condition_is_none_without_identities_or_stated_condition():
    """A tree without removed edges and no WHERE leave nothing to test."""
    query = Query(tree_over(TITLE))  # FROM title t, no WHERE

    assert query.condition is None  # no condition at all


def test_query_condition_puts_the_identities_in_front_of_the_stated_condition():
    """The identities hold before WHERE is read, and the condition follows later changes."""
    #   ⟲ p (removed: closes a cycle)
    #   t
    tree = tree_over(TITLE, (Edge("t", "p", "t"),))  # a self-reference of t
    (identity,) = tree.identities  # t.p is t
    query = Query(tree)  # nothing stated yet
    assert query.condition == identity  # the identity alone

    note = BinaryExpression("==", T_TITLE, Literal("a"))  # t.title = 'a'
    query.stated_condition = note  # WHERE t.title = 'a', set after construction

    assert query.condition == conjunction(identity, note)  # identity first, then WHERE


def test_query_condition_holds_every_identity_in_stated_order():
    """Two removed edges give two identities, and neither is lost.

    ⟲ p, q (both removed)
    t
    """
    edges = (Edge("t", "p", "t"), Edge("t", "q", "t"))  # two self-references of t
    tree = tree_over(TITLE, edges)  # both edges close a cycle at once
    first, second = tree.identities  # t.p is t, then t.q is t
    query = Query(tree)  # no WHERE

    assert query.condition == conjunction(first, second)  # both, in stated order


def test_query_join_pushes_an_identity_into_its_root():
    """The join splits the identities as well, so no caller can lose them."""
    #   ⟲ p (removed)
    #   mc ── movie ──▶ t ◀── movie ── mi
    edges = (*MOVIE_EDGES, Edge("mc", "p", "mc"))  # mc also references itself
    tree = tree_over(MOVIES, edges)  # mc and mi join at t
    (identity,) = tree.identities  # mc.p is mc
    query = Query(tree)  # no WHERE

    assert query.join == Join({"mc": identity}, None, ())  # pushed into mc


def test_query_join_is_none_for_one_root():
    """A single root has nothing to join."""
    query = Query(tree_over(TITLE))  # one root, t

    assert query.join is None  # no join to split a condition over


def test_query_join_splits_the_current_condition():
    """The join is derived from the condition as it stands at each read."""
    query = Query(tree_over(MOVIES, MOVIE_EDGES))  # mc and mi join at t
    note = BinaryExpression("==", column_of("mc", "note"), Literal("a"))  # mc's own
    query.stated_condition = note  # mc.note = 'a', the only conjunct

    assert query.join == Join({"mc": note}, None, ())  # pushed into mc

    title = Column(Path(0, "t", "mc", ("movie",)), "title")  # t.title from mc
    replacement = BinaryExpression("==", title, Literal("x"))  # t.title = 'x'
    query.stated_condition = replacement  # replaced
    assert query.join.pushed_conditions == {"mc": replacement}  # follows it


def test_query_preserves_order_is_false_without_order_by():
    """Without ORDER BY, SQL returns rows in no defined order."""
    query = Query(tree_over(TITLE), outputs=[Output(T_TITLE, "title")])  # no order

    assert not query.preserves_order  # compared as a multiset


def test_query_preserves_order_with_order_by_with_or_without_limit():
    """ORDER BY fixes the row order, also under LIMIT, so the comparison is positional."""
    by_title = [OrderKey(T_TITLE, descending=False)]  # ORDER BY t.title
    ordered = Query(tree_over(TITLE), order_keys=by_title)  # all rows
    top = Query(tree_over(TITLE), order_keys=by_title, limit=Limit(3))  # top 3

    assert ordered.preserves_order  # compared row by row
    assert top.preserves_order  # the top-k order is checked as well


def test_query_copy_sources_map_a_repeated_column_to_its_first_output():
    """Only the later selection copies; `project` reads each column once."""
    title = Output(T_TITLE, "title")  # t.title
    key = Output(T_ID, "id")  # t.id
    copy = Output(T_TITLE, "b")  # t.title AS b, the second selection of t.title
    query = Query(tree_over(TITLE), outputs=[title, key, copy])  # in SELECT order

    assert query.projected_outputs == [title, key]  # each column once
    assert query.copy_sources == {copy: title}  # b copies title, not id


def test_query_copy_sources_follow_a_removed_source():
    """Both members derive from the current outputs, so removing the source promotes the copy."""
    title = Output(T_TITLE, "title")  # the source
    copy = Output(T_TITLE, "b")  # its copy
    query = Query(tree_over(TITLE), outputs=[title, copy])  # a copy of title
    assert query.copy_sources == {copy: title}  # b copies title for now

    query.outputs.remove(title)  # only `b` is left

    assert query.projected_outputs == [copy]  # b now projects t.title itself
    assert query.copy_sources == {}  # nothing is copied any more


def test_query_lookup_output_column_leaves_other_columns_unchanged():
    """A qualified column is already resolved, and a name no output carries has
    nothing to resolve to, so both come back as they are."""
    query = Query(tree_over(TITLE), outputs=[Output(T_TITLE, "name")])  # renamed
    unknown = Column(None, "title")  # the source name, no longer an output name
    t_name = column_of("t", "name")  # qualified, and named like the output

    assert query.lookup_output_column(T_TITLE) is T_TITLE  # qualified: as is
    assert query.lookup_output_column(t_name) is t_name  # not the output `name`
    assert query.lookup_output_column(unknown) is unknown  # no output named `title`
    assert query.lookup_output_column(unknown) is unknown  # repeated call, same answer


def test_query_lookup_output_column_follows_a_later_rename():
    """The lookup reads the current outputs, so a rename recorded
    after an earlier lookup changes the answer."""
    query = Query(tree_over(TITLE), outputs=[Output(T_TITLE, "title")])  # plain
    by_old_name = Column(None, "title")  # the name it carries now
    assert query.lookup_output_column(by_old_name) == T_TITLE  # found

    query.outputs[0] = Output(T_TITLE, "name")  # the column is now exposed as `name`
    by_new_name = Column(None, "name")  # the name it carries after the rename
    assert query.lookup_output_column(by_old_name) is by_old_name  # not found now
    assert query.lookup_output_column(by_new_name) == T_TITLE  # new name


def test_query_owner_of_a_column_deeper_than_any_query_is_the_outermost():
    """A path may name a depth no enclosing query has; the walk ends at the outermost."""
    query = Query(tree_over(TITLE))  # at depth 0
    column = column_of("x", "c", depth=3)  # three levels below it

    assert query.owner_of(column) is query  # no error, and the only query there is


def test_query_owner_of_finds_the_query_a_column_reads():
    """An output name is this query's; a path leads to the query at its depth."""
    outer = Query(tree_over(TITLE))  # the outermost query
    inner = Query(tree_over({"a": "aka_title"}, depth=1), outer)  # nested in it
    inner_column = column_of("a", "id", depth=1)  # the subquery's own
    output_name = Column(None, "x")  # no path to follow

    assert inner.owner_of(T_ID) is outer  # correlated: one query up
    assert inner.owner_of(inner_column) is inner  # its own
    assert inner.owner_of(output_name) is inner  # an output name stays here
    assert outer.owner_of(T_ID) is outer  # the outermost query's own


@pytest.mark.parametrize(
    ("tables", "clauses", "inlinable"),
    [
        (TITLE, {"stated_condition": BinaryExpression("==", T_ID, Literal(1))}, True),
        (TITLE, {"outputs": [Output(T_ID, "id")]}, True),  # the table's own names
        (TITLE, {"outputs": [Output(T_ID, "x")]}, False),  # a renamed column
        ({"mi": "movie_info", "t": "title"}, {}, False),  # two aliases
        (TITLE, {"group_keys": [T_ID]}, False),  # group keys
        (
            TITLE,
            {"aggregates": GeneratedColumns("_a_", {COUNT_ALL: Column(None, "c")})},
            False,
        ),  # an aggregate
        (
            TITLE,
            {
                "computed_columns": GeneratedColumns(
                    "_c_", {BinaryExpression("+", T_ID, Literal(1)): Column(None, "i")}
                )
            },
            False,
        ),  # a computed column
        (TITLE, {"order_keys": [OrderKey(T_ID, descending=False)]}, False),  # order
        (TITLE, {"limit": Limit(1)}, False),  # a limit
    ],
)
def test_query_inlinable_holds_for_a_plain_filter_only(tables, clauses, inlinable):
    """Anything a reader of the table would not get by itself keeps a query apart."""
    query = Query(tree_over(tables), **clauses)  # one reason per row

    assert query.inlinable is inlinable  # only a filter or the own names inline


def test_query_inlinable_is_false_for_one_root_with_two_aliases():
    """mi reaches t, so the query has one root but reads two tables."""
    #   mi ── movie ──▶ t
    edges = (Edge("mi", "movie", "t"),)  # mi.movie = t.id
    tree = tree_over({"mi": "movie_info", "t": "title"}, edges)  # t hangs off mi
    query = Query(tree)  # one root, mi

    assert query.tree.roots == ("mi",)  # one root, so "one root" would allow it
    assert query.inlinable is False  # but two aliases do not inline


def test_query_is_ungrouped_aggregation_needs_an_aggregate_and_no_group():
    """A grouped aggregation has a row per group, a plain query has no aggregate."""
    ungrouped = Query(tree_over(TITLE))  # will aggregate all rows
    ungrouped.aggregates.column_for(COUNT_ALL, "c")  # COUNT(*) AS c
    grouped = Query(tree_over(TITLE), group_keys=[T_ID])  # GROUP BY t.id
    grouped.aggregates.column_for(COUNT_ALL, "c")  # COUNT(*) per group
    plain = Query(tree_over(TITLE), outputs=[Output(T_ID, "id")])  # no aggregate

    assert ungrouped.is_ungrouped_aggregation  # aggregates all rows
    assert not grouped.is_ungrouped_aggregation  # one per group
    assert not plain.is_ungrouped_aggregation  # no aggregate


def test_query_single_value_column_is_none_for_other_queries():
    """Two scalars, or one row per group, are no single value."""
    count = Column(None, "c")  # COUNT(*) AS c
    maximum = Column(None, "m")  # MAX(t.id) AS m
    two_outputs = Query(
        tree_over(TITLE), outputs=[Output(count, "c"), Output(maximum, "m")]
    )  # two scalars
    two_outputs.aggregates.column_for(COUNT_ALL, "c")  # the first aggregate
    two_outputs.aggregates.column_for(AggregateExpression("max", T_ID), "m")  # second
    grouped = Query(
        tree_over(TITLE), outputs=[Output(count, "c")], group_keys=[T_ID]
    )  # GROUP BY t.id
    grouped.aggregates.column_for(COUNT_ALL, "c")  # one count per group

    assert two_outputs.single_value_column is None  # two scalars
    assert grouped.single_value_column is None  # one row per group


def test_query_single_value_column_is_the_output_of_a_single_value_aggregation():
    """It takes the one output, also a value computed over the aggregate."""
    single = Query(
        tree_over(TITLE), outputs=[Output(Column(None, "c"), "c")]
    )  # one output, c
    single.aggregates.column_for(COUNT_ALL, "c")  # SELECT COUNT(*) AS c
    computed = Query(
        tree_over(TITLE), outputs=[Output(Column(None, "d"), "d")]
    )  # one output, d
    aggregate = computed.aggregates.column_for(COUNT_ALL)  # COUNT(*)
    plus_one = BinaryExpression("+", aggregate, Literal(1))  # COUNT(*) + 1
    computed.aggregate_computed_columns.column_for(plus_one, "d")  # ... AS d

    assert single.single_value_column == Column(None, "c")  # the aggregate's column
    assert computed.single_value_column == Column(None, "d")  # the computed output

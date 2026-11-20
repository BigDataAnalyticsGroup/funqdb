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

"""The join-tree step: the aliases of a query and the equalities between them."""

import pytest
import sqlglot
from sqlglot.optimizer.optimizer import optimize

from benchmarks.datasets.dataset_types import Reference
from benchmarks.datasets.schema import PrimaryKey, Schema
from benchmarks.tools.ir.join_equalities import (
    Edge,
    ForeignKeyEquality,
    JoinEquality,
    ReferenceRead,
)
from benchmarks.tools.ir.paths import Path
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.parse.join_tree_parser import (
    SplitConditions,
    build_join_tree,
    read_from,
    split_conditions,
)
from benchmarks.tools.parse.query_parser import parse_query
from benchmarks.tools.sql.optimization import OPTIMIZER_DIALECT, OPTIMIZER_RULES
from tests.benchmarks.tools.framework.parsing import query_from_sql

# The tables of the tests below. A star marks a key column.
#
#                         movie_id (as movie)
#   movie_companies ------------------------------> title <------ movie_info
#        (mc)                                     (t, *id)  (as movie)  (mi)
#          |
#          | note_id (as note)
#          v
#        note (n, no key declared)
SCHEMA = Schema(
    references={
        "movie_companies": {
            "movie_id": Reference("movie", "title"),  # a key into the table t
            "note_id": Reference(
                "note", "note"
            ),  # a second target: a table with no key
        },
        "movie_info": {
            "movie_id": Reference("movie", "title"),  # leads to the same table as mc's
        },
    },
    primary_keys={
        "title": PrimaryKey(("id",))
    },  # the key a foreign key is equated with
    columns={},
)
# the aliases of the queries below, each with its table
TABLES = {"mc": "movie_companies", "mi": "movie_info", "t": "title", "n": "note"}


def equality_of(condition: str) -> JoinEquality | None:
    """The join equality `condition` states between the aliases above; `None` if none."""
    ast = sqlglot.parse_one(condition)  # one condition of a query
    split = split_conditions([ast], TABLES, SCHEMA)  # against the schema
    return split.equalities[0] if split.equalities else None  # at most one


def select_of(sql: str) -> sqlglot.Expr:
    """The AST of the unoptimized query `sql`, whose FROM stays as stated."""
    return sqlglot.parse_one(sql)  # not optimized: FROM stays as stated


@pytest.mark.parametrize(
    "join",
    [
        "LEFT JOIN movie_info AS mi ON mc.id = mi.id",
        "RIGHT JOIN movie_info AS mi ON mc.id = mi.id",
        "FULL JOIN movie_info AS mi ON mc.id = mi.id",
        "NATURAL JOIN movie_info AS mi",
    ],
)
def test_read_from_rejects_a_join_that_keeps_or_matches_otherwise(join):
    """Following a reference drops the tuples an outer join keeps."""
    ast = select_of(f"SELECT mc.id FROM movie_companies AS mc {join}")  # no inner join

    with pytest.raises(UnsupportedSql) as raised:  # the tree would read it as inner
        read_from(ast, {}, frozenset())  # refused while reading FROM

    assert raised.value.limitation is Limitation.OUTER_JOIN  # for that reason


def test_read_from_rejects_a_derived_table_that_no_optimizer_turned_into_a_cte():
    """Only a write statement still holds one, as it skips the optimizer."""
    ast = select_of(
        "SELECT d.id FROM (SELECT t.id FROM title AS t WHERE t.year > 2000) AS d"
    )  # a derived table as the subquery of a DELETE holds it

    with pytest.raises(UnsupportedSql) as raised:  # no table named "" with no filter
        read_from(ast, {}, frozenset())  # refused while reading FROM

    assert raised.value.limitation is Limitation.DERIVED_TABLE_IN_WRITE  # its entry


def test_read_from_rejects_a_grouped_derived_table_for_its_limitation():
    """The optimizer turns it into a CTE with a bare identifier as its alias."""
    ast = select_of(
        "SELECT t.id FROM title AS t, (SELECT mc.id AS w, COUNT(*) AS c "
        "FROM movie_companies AS mc GROUP BY mc.id) AS s WHERE t.id = s.w"
    )  # no filter over one table, so it cannot be inlined
    columns = {"title": {"id": "INT"}, "movie_companies": {"id": "INT"}}  # for qualify
    typed = optimize(
        ast, schema=columns, dialect=OPTIMIZER_DIALECT, rules=OPTIMIZER_RULES
    )  # as the pipeline optimizes a query

    with pytest.raises(UnsupportedSql) as raised:  # no AttributeError on the alias
        parse_query(typed, SCHEMA)  # read_from reaches the CTE

    assert raised.value.limitation is Limitation.CTE_NOT_INLINABLE  # the catalog entry


def test_read_from_reads_aliases_in_order_and_their_on_conjuncts():
    """A comma join, CROSS and INNER all read as one join tree; ON splits into conjuncts."""
    ast = select_of(
        "SELECT t.id FROM title AS t, movie_info AS mi CROSS JOIN note AS n "
        "INNER JOIN movie_companies AS mc ON mc.id = 1 AND mc.note_id = n.id"
    )  # every inner kind

    from_clause = read_from(ast, {}, frozenset())  # the whole FROM

    assert from_clause.tables == {
        "t": "title",
        "mi": "movie_info",
        "n": "note",
        "mc": "movie_companies",
    }  # in declaration order
    assert [condition.sql() for condition in from_clause.conditions] == [
        "mc.id = 1",
        "mc.note_id = n.id",
    ]  # the ON's two conjuncts


@pytest.mark.parametrize(
    "condition",
    [
        "mc.movie_id = mi.id",  # a key of another table than the reference's target
        "mc.movie_id = t.title",  # a column of the target that is not its key
        "n.id = mc.note_id",  # a target without a declared key
    ],
)
def test_split_conditions_rejects_a_foreign_key_equated_with_a_non_key(condition):
    """Such an equality follows no reference, so it translates to no edge."""
    with pytest.raises(UnsupportedSql) as raised:  # no reference leads there
        equality_of(condition)  # refused while reading the condition

    assert raised.value.limitation is Limitation.FOREIGN_KEY_TO_NON_KEY  # by design


@pytest.mark.parametrize(
    "condition",
    [
        "mc.id > mi.id",  # no equality
        "mc.id = 1",  # no column on the right
        "1 = mc.id",  # no column on the left
        "mc.id = mi.id",  # no foreign key on either side
        "mc.movie_id = other.id",  # an alias of another query
        "other.id = mc.movie_id",  # the same on the left
        "mc.movie_id = mc.movie_id",  # a foreign key with itself
        "mc.movie_id = mc.note_id",  # foreign keys into different tables
    ],
)
def test_split_conditions_keeps_any_other_condition_as_a_filter(condition: str):
    """Such a condition stays a filter on the query."""
    ast = sqlglot.parse_one(condition)  # one condition of a query

    split = split_conditions([ast], TABLES, SCHEMA)  # against the schema

    assert split == SplitConditions([], [ast])  # no edge, and the filter is kept


def test_split_conditions_reads_a_foreign_key_and_the_target_key_as_an_edge():
    """The edge leads from the foreign key's alias, whichever side it was written on."""
    expected = Edge("mc", "movie", "t")  # along the reference's name

    assert equality_of("mc.movie_id = t.id") == expected  # written first
    assert equality_of("t.id = mc.movie_id") == expected  # written last


def test_split_conditions_reads_a_key_that_is_also_a_foreign_key_as_an_edge():
    """title.id leads on to another table, but mc.movie_id reads it as title's key.

    movie_companies ---movie_id---> title ---id---> work
         (mc)            (as movie)  (t, *id)  (as work)
    """
    schema = Schema(
        references={
            "movie_companies": {"movie_id": Reference("movie", "title")},
            "title": {"id": Reference("work", "work")},  # the key is a foreign key too
        },
        primary_keys={"title": PrimaryKey(("id",))},
        columns={},
    )  # the schema of the diagram
    written_first = sqlglot.parse_one("mc.movie_id = t.id")  # the reference first
    written_last = sqlglot.parse_one("t.id = mc.movie_id")  # the key first

    first = split_conditions([written_first], TABLES, schema)  # reference first
    last = split_conditions([written_last], TABLES, schema)  # key first

    expected = SplitConditions([Edge("mc", "movie", "t")], [])  # one edge, no filter
    assert first == expected  # no equality of two different references
    assert last == expected  # the same when the key is written first


def test_split_conditions_reads_two_foreign_keys_into_one_table_in_stated_order():
    """Neither alias is the target: the equality names the table they share."""
    equality = equality_of("mc.movie_id = mi.movie_id")  # both into title

    assert equality == ForeignKeyEquality(
        "title", ReferenceRead("mc", "movie"), ReferenceRead("mi", "movie")
    )  # in stated order


def test_split_conditions_keeps_the_conditions_that_join_nothing():
    """Join equalities and other conditions go apart, each in stated order."""
    filtering = sqlglot.parse_one("mc.id > 1")  # a filter
    edge = sqlglot.parse_one("mc.movie_id = t.id")  # an edge
    other = sqlglot.parse_one("mc.id = mi.id")  # no foreign key

    split = split_conditions(
        [filtering, edge, other], TABLES, SCHEMA
    )  # one join equality among three

    assert split.equalities == [Edge("mc", "movie", "t")]  # the edge
    assert split.filters == [filtering, other]  # in stated order


def test_build_join_tree_counts_an_edge_stated_twice_once():
    """`ON` and `WHERE` may state one equality twice without an error."""
    query = query_from_sql(
        "SELECT t.id FROM movie_companies AS mc JOIN title AS t ON mc.movie_id = t.id "
        "WHERE mc.movie_id = t.id",
        SCHEMA,
    )  # the same edge twice

    assert query.tree.roots == ("mc",)  # one edge: t is reached from mc
    assert query.tree.identities == ()  # nothing to equate


def test_build_join_tree_names_a_provisional_alias_outside_the_reserved_names():
    """A foreign-key equality between two roots joins them in a new title alias no other query uses.

    mc ---movie---> title_1 <---movie--- mi
         (title_0 is taken elsewhere)
    """
    tables = {"mc": "movie_companies", "mi": "movie_info"}  # two roots
    equality = ForeignKeyEquality(
        "title", ReferenceRead("mc", "movie"), ReferenceRead("mi", "movie")
    )  # both: a title
    reserved = frozenset({"title_0"})  # an alias elsewhere in the statement

    tree = build_join_tree(tables, [equality], SCHEMA, 1, reserved)  # depth 1

    assert tree.aliases == ("mc", "mi", "title_1")  # title_0 is reserved
    assert tree.join_members == ("mc", "mi", "title_1")  # the alias joins mc and mi
    assert tree.path_of("title_1") == Path(1, "title_1", "mc", ("movie",))  # depth 1

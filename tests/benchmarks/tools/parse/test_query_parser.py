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

"""SQL queries as their IR, and the ones that have none."""

import pytest
import sqlglot

from benchmarks.datasets.dataset_types import Reference
from benchmarks.datasets.schema import PrimaryKey, Schema
from benchmarks.tools.ir.expressions import (
    AggregateExpression,
    BinaryExpression,
    Column,
    Literal,
    SubqueryExpression,
    TupleIdentityExpression,
)
from benchmarks.tools.ir.paths import Path
from benchmarks.tools.ir.query import Limit, OrderKey
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.parse.query_parser import aliases, parse_query, where_conjuncts

# The tables of the tests below. A star marks a key column, an arrow a foreign key.
# No SQL column is declared, so no query is type-checked.
#
#   movie_companies           title                movie_info
#   +----------------+        +-------------+      +----------------+
#   |  movie_id -----+------->| *id         |<-----+----- movie_id  |
#   +----------------+        +-------------+      +----------------+
#
#   both movie_id columns are swizzled into the reference `movie` of title
SCHEMA = Schema(
    references={
        "movie_companies": {"movie_id": Reference("movie", "title")},  # a foreign key
        "movie_info": {"movie_id": Reference("movie", "title")},  # a second one
    },
    primary_keys={"title": PrimaryKey(("id",))},  # the key the foreign keys lead to
    columns={},
)

# the alias t over title, the only root of most queries below
T = Path(0, "t", "t", ())
NAME = Column(T, "name")
YEAR = Column(T, "year")
COUNT_ALL = AggregateExpression("count", None)


def parse(sql: str):
    """The IR of the unoptimized query `sql`, parsed against the schema above."""
    ast = sqlglot.parse_one(sql)  # not optimized: the clauses stay as stated
    return parse_query(ast, SCHEMA)  # the IR, or the rejection under test


@pytest.mark.parametrize(
    ("sql", "limitation"),
    [
        ("SELECT a FROM t UNION SELECT a FROM u", Limitation.SET_OPERATION),  # a union
        ("SELECT 1", Limitation.NO_FROM),  # no table to read
        (
            "SELECT t.id FROM t WHERE EXISTS (VALUES (1))",
            Limitation.NO_FROM,
        ),  # a subquery that is no SELECT
        (
            "SELECT t.*, t.id FROM title AS t",
            Limitation.WILDCARD_WITH_EXPRESSIONS,
        ),  # a star beside an expression
        (
            "SELECT t.id AS a, t.name AS a FROM title AS t",
            Limitation.DUPLICATE_OUTPUT_NAME,
        ),  # two columns under one name
        (
            "SELECT t.id FROM title AS t OFFSET 2",
            Limitation.OFFSET_WITHOUT_LIMIT,
        ),  # a skip with no count
        (
            "SELECT t.id FROM title AS t LIMIT (SELECT 1)",
            Limitation.NON_LITERAL_LIMIT,
        ),  # a count that is no literal
        (
            "SELECT t.id FROM title AS t LIMIT 1 OFFSET (SELECT 1)",
            Limitation.NON_LITERAL_LIMIT,
        ),  # an offset that is no literal
        (
            "SELECT COUNT(*) AS c FROM title AS t HAVING COUNT(*) > 1",
            Limitation.HAVING_WITHOUT_GROUP_BY,
        ),  # an aggregate filtered, no groups
        (
            "SELECT DISTINCT t.name FROM title AS t HAVING t.name = 'x'",
            Limitation.HAVING_WITHOUT_GROUP_BY,
        ),  # DISTINCT's group keys do not admit it
        (
            "SELECT DISTINCT * FROM title AS t",
            Limitation.WILDCARD_WITH_EXPRESSIONS,
        ),  # no columns to group by
        (
            "SELECT t.* FROM movie_companies AS mc, title AS t "
            "WHERE mc.movie_id = t.id",
            Limitation.WILDCARD_WITH_EXPRESSIONS,
        ),  # t is reached from the root mc, whose attributes the star would read
        (
            "SELECT t.* FROM title AS t, movie_info AS mi",
            Limitation.WILDCARD_WITH_EXPRESSIONS,
        ),  # one of two roots, where the star would read the whole join tuple
        (
            "SELECT * FROM movie_companies AS mc, title AS t WHERE mc.movie_id = t.id",
            Limitation.WILDCARD_WITH_EXPRESSIONS,
        ),  # a bare star would read mc's attributes, where SQL also returns t's
        (
            "SELECT t.id FROM title AS t WHERE t.id IN (SELECT u.* FROM "
            "movie_companies AS mc, title AS u WHERE mc.movie_id = u.id)",
            Limitation.WILDCARD_WITH_EXPRESSIONS,
        ),  # IN reads its subquery's output, which would be mc's attributes
        (
            "SELECT DISTINCT mc.movie_id FROM movie_companies AS mc",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # grouping for DISTINCT reads the key value
        (
            "SELECT mc.id FROM movie_companies AS mc GROUP BY mc.movie_id",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # grouped by a column
        (
            "SELECT COUNT(*) AS c FROM movie_companies AS mc GROUP BY mc.movie_id + 1",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # grouped by a computed key
        (
            "SELECT mc.id FROM movie_companies AS mc ORDER BY mc.movie_id",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # ordered by a column
        (
            "SELECT mc.movie_id AS m FROM movie_companies AS mc ORDER BY m",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # by the output name of a foreign key
        (
            "SELECT COUNT(*) + t.year AS c FROM title AS t",
            Limitation.AGGREGATE_MIXED_WITH_COLUMN,
        ),  # a group has no single year
    ],
)
def test_parse_query_rejects_sql_without_an_ir_for_its_limitation(sql, limitation):
    """Each query names the limitation that documents why it has no IR."""
    with pytest.raises(UnsupportedSql) as raised:  # catalogued
        parse(sql)  # parsing is what refuses it

    assert raised.value.limitation is limitation  # the catalog entry of this case


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT t.name, COUNT(*) AS c FROM title AS t",  # an aggregate, no grouping
        "SELECT t.name FROM title AS t GROUP BY t.year",  # grouped by another key
    ],
)
def test_parse_query_rejects_an_output_column_that_is_not_grouped(sql):
    """A group has no single value of such a column, so the SQL is invalid."""
    with pytest.raises(ValueError, match="neither grouped") as raised:  # says which
        parse(sql)  # refused after GROUP BY

    assert not isinstance(raised.value, UnsupportedSql)  # no catalogued limitation


def test_parse_query_rejects_reading_a_cte_that_does_more_than_filter():
    """Only a filter can be inlined; defining such a CTE alone is no error."""
    ast = sqlglot.parse_one(
        "WITH c AS (SELECT COUNT(*) AS n FROM title AS t) SELECT * FROM c"
    )  # the CTE aggregates and the query reads it

    with pytest.raises(UnsupportedSql) as raised:  # the read cannot be inlined
        parse_query(ast, SCHEMA)  # the FROM reaches the CTE

    assert raised.value.limitation is Limitation.CTE_NOT_INLINABLE  # that reason


def test_parse_query_reads_a_star_as_no_outputs():
    """Without outputs, a query reads every attribute of its root's tuples."""
    joined = parse(
        "SELECT mc.* FROM movie_companies AS mc, title AS t WHERE mc.movie_id = t.id"
    )  # the star of the only root, with t joined below it

    assert parse("SELECT * FROM title AS t").outputs == []  # a bare star
    assert parse("SELECT t.* FROM title AS t").outputs == []  # a qualified star
    assert joined.outputs == []  # mc's attributes
    assert joined.tree.roots == ("mc",)  # mc is the only root


def test_parse_query_keeps_any_star_under_exists():
    """EXISTS reads no outputs of its subquery, so its star may span several roots."""
    query = parse(
        "SELECT t.id FROM title AS t WHERE EXISTS (SELECT * FROM "
        "movie_companies AS mc, movie_info AS mi WHERE mc.movie_id = t.id)"
    )  # two roots under EXISTS

    assert query.condition.subquery.tree.roots == ("mc", "mi")  # both kept
    assert query.condition.subquery.outputs == []  # the star adds none


@pytest.mark.parametrize(
    ("sql", "limit"),
    [
        ("SELECT t.id FROM title AS t", None),  # no clause
        ("SELECT t.id FROM title AS t LIMIT 3", Limit(3)),  # a count alone
        ("SELECT t.id FROM title AS t LIMIT 3 OFFSET 1", Limit(3, 1)),  # both
        ("SELECT t.id FROM title AS t LIMIT 1, 3", Limit(3, 1)),  # offset comes first
    ],
)
def test_parse_query_reads_limit_and_offset(sql, limit):
    """The count and the offset are not swapped, whatever their spelling."""
    assert parse(sql).limit == limit  # as SQL means them


def test_parse_query_names_each_output_by_its_alias_or_its_column():
    """A plain column keeps its name; a computed one without alias a generated one."""
    query = parse(
        "SELECT t.name, t.year AS y, t.year + 1 FROM title AS t"
    )  # three kinds

    names = [output.name for output in query.outputs]  # in SELECT order
    columns = [output.column for output in query.outputs]  # what each one reads

    assert names == ["name", "y", "_computed_0"]  # each by its own rule
    assert columns == [NAME, YEAR, Column(None, "_computed_0")]  # the computed by name
    plus = BinaryExpression("+", YEAR, Literal(1))  # t.year + 1
    assert query.computed_columns.by_expression == {plus: columns[2]}  # holds the value


def test_parse_query_keeps_aggregates_and_their_computations_apart():
    """An aggregate gets its own column; one computed from it another kind of column."""
    query = parse("SELECT COUNT(*) AS c, COUNT(*) + 1 AS d FROM title AS t")  # both

    held = query.aggregates.by_expression  # the aggregates
    computed = query.aggregate_computed_columns.by_expression  # computed over them
    plus = BinaryExpression("+", Column(None, "c"), Literal(1))  # c + 1

    assert held == {COUNT_ALL: Column(None, "c")}  # one, used by both outputs
    assert computed == {plus: Column(None, "d")}  # reads the aggregate's column


def test_parse_query_groups_by_a_column_and_by_a_computed_key():
    """A computed key is held in a computed column, which the output reads as well."""
    query = parse(
        "SELECT t.year + 1 AS y, t.name, COUNT(*) AS c FROM title AS t "
        "GROUP BY t.year + 1, t.name"
    )  # one key of each kind

    assert query.group_keys == [Column(None, "y"), NAME]  # in GROUP BY order
    assert query.is_grouped  # derived from the keys


def test_parse_query_reads_having_over_aggregates_and_over_columns():
    """HAVING reads an aggregate's column; without an aggregate it reads plain columns."""
    aggregated = parse(
        "SELECT t.name, COUNT(*) AS c FROM title AS t GROUP BY t.name HAVING COUNT(*) > 1"
    )  # over the aggregate
    plain = parse(
        "SELECT t.name FROM title AS t GROUP BY t.name HAVING t.name = 'x'"
    )  # over the group key

    assert aggregated.having_condition == BinaryExpression(
        ">", Column(None, "c"), Literal(1)
    )  # the output's column, not a second aggregate
    assert plain.having_condition == BinaryExpression("==", NAME, Literal("x"))  # key


def test_parse_query_groups_a_distinct_query_by_its_projected_columns():
    """DISTINCT groups by each selected column once; `b` selects `name` again."""
    query = parse(
        "SELECT DISTINCT t.name, t.name AS b, t.year FROM title AS t"
    )  # a copy

    assert query.group_keys == [NAME, YEAR]  # `b` adds no key
    assert not query.regroups  # the query was not grouped before


def test_parse_query_regroups_a_grouped_distinct_query():
    """A grouped query keeps its keys, and its groups are grouped once more."""
    query = parse(
        "SELECT DISTINCT COUNT(*) AS c FROM title AS t GROUP BY t.name"
    )  # distinct counts

    assert query.group_keys == [NAME]  # still the stated key
    assert query.regroups  # distinct over the groups


def test_parse_query_ignores_distinct_on_an_ungrouped_aggregation():
    """An ungrouped aggregation is one row, which is distinct already."""
    query = parse("SELECT DISTINCT COUNT(*) AS c FROM title AS t")  # one row

    assert query.group_keys == []  # nothing to group by
    assert not query.regroups  # no second grouping either


def test_parse_query_orders_by_a_column_an_output_name_and_an_expression():
    """Each key keeps its own direction, in stated order."""
    query = parse(
        "SELECT t.name AS n, COUNT(*) AS c FROM title AS t GROUP BY t.name "
        "ORDER BY n DESC, t.name, c, COUNT(*) + 1 DESC"
    )  # every kind of key

    assert query.order_keys == [
        OrderKey(NAME, True),
        OrderKey(NAME, False),
        OrderKey(Column(None, "c"), False),
        OrderKey(BinaryExpression("+", Column(None, "c"), Literal(1)), True),
    ]  # the output name n reads t.name, c its aggregate's column
    assert query.preserves_order  # derived from the keys


def test_parse_query_keeps_a_correlated_column_next_to_an_aggregate():
    """An enclosing query's column is one value per evaluation of this query."""
    query = parse(
        "SELECT t.id FROM title AS t WHERE EXISTS "
        "(SELECT t.year, COUNT(*) AS c FROM movie_info AS mi)"
    )  # t.year is no column of the subquery's own

    subquery = query.condition.subquery  # the EXISTS query
    count = Column(None, "c")  # COUNT(*) under its output name

    assert subquery.aggregates.by_expression == {
        COUNT_ALL: count
    }  # parsed, not refused
    assert subquery.outputs[0].column == YEAR  # t.year at depth 0, read as it is


def test_parse_query_reads_a_subquery_value_as_no_aggregate_of_its_own():
    """The MAX aggregates in the subquery, so the outer value is computed per tuple."""
    query = parse(
        "SELECT t.year + (SELECT MAX(mi.id) AS m FROM movie_info AS mi) AS x "
        "FROM title AS t"
    )  # a false AGGREGATE_MIXED_WITH_COLUMN before

    computed = query.computed_columns.by_expression  # the computed values
    ((value, column),) = computed.items()  # exactly one

    assert value.op == "+" and value.left == YEAR  # computed per tuple from t.year
    assert isinstance(value.right, SubqueryExpression)  # plus the MAX subquery
    assert column == Column(None, "x")  # held under the output name
    assert not query.aggregates.by_expression  # this query aggregates nothing
    assert not query.aggregate_computed_columns.by_expression  # no aggregate level


def test_parse_query_groups_a_distinct_subquery_by_its_own_columns_only():
    """A correlated column is one value per evaluation of the subquery, so no key."""
    query = parse(
        "SELECT t.id FROM title AS t WHERE EXISTS "
        "(SELECT DISTINCT t.name, x.note FROM aka_title AS x)"
    )  # t is the enclosing query's

    subquery = query.condition.subquery  # the EXISTS query
    note = Column(Path(1, "x", "x", ()), "note")  # the subquery's own column

    assert subquery.group_keys == [note]  # t.name is correlated


def test_parse_query_puts_the_tree_identities_before_the_where_conjuncts():
    """A hub's identity would otherwise be lost, so the query would return too many rows.

        mc ---movie---> t <---movie--- mi
         |                             |
         +---movie---> u <---movie-----+

    t and u are both hubs of mc and mi. The tree keeps mc's edge and mi's edge into
    one hub; the edge of mi into the other hub becomes the identity `mi.movie is mc.movie`.
    """
    query = parse(
        "SELECT mc.id FROM movie_companies AS mc, movie_info AS mi, title AS t, "
        "title AS u WHERE mc.movie_id = t.id AND mi.movie_id = t.id AND "
        "mc.movie_id = u.id AND mi.movie_id = u.id AND mc.id > 1"
    )  # the aliases of the diagram, and one filter
    mc_movie = Column(
        Path(0, "mc", "mc", ()), "movie", is_reference_column=True
    )  # mc.movie_id as its reference
    mi_movie = Column(
        Path(0, "mi", "mi", ()), "movie", is_reference_column=True
    )  # mi.movie_id as its reference
    identity = TupleIdentityExpression(mi_movie, mc_movie)  # mi's movie is mc's movie
    mc_id = Column(Path(0, "mc", "mc", ()), "id")  # mc.id
    filtered = BinaryExpression(">", mc_id, Literal(1))  # mc.id > 1

    assert query.tree.identities == (identity,)  # the one removed edge
    assert query.condition == BinaryExpression(
        "and", identity, filtered
    )  # the identity first, then the WHERE filter
    assert query.stated_condition == filtered  # the identity is not stated again


def test_parse_query_reads_every_cte_of_a_with_clause():
    """The reading query may read an earlier CTE and a later one side by side."""
    query = parse(
        "WITH c AS (SELECT * FROM title AS t), d AS (SELECT * FROM movie_info AS mi) "
        "SELECT c.id FROM c, d"
    )  # both CTEs are read

    assert query.tree.table_of("c") == "title"  # the earlier CTE is still known
    assert query.tree.table_of("d") == "movie_info"  # and so is the later one


def test_parse_query_chains_a_cte_over_an_earlier_one():
    """A later CTE builds on an earlier one; the reading query gets both conditions."""
    query = parse(
        "WITH c AS (SELECT * FROM title AS t WHERE t.year > 1), "
        "d AS (SELECT * FROM c WHERE c.id > 2) SELECT d.id FROM d"
    )  # d filters c, c filters title

    d = Path(0, "d", "d", ())  # both conditions read the one tuple of d
    over_year = BinaryExpression(">", Column(d, "year"), Literal(1))  # from c
    over_id = BinaryExpression(">", Column(d, "id"), Literal(2))  # from d

    assert query.tree.table_of("d") == "title"  # d stands for title
    assert query.condition == BinaryExpression("and", over_year, over_id)  # c's first


def test_parse_query_keeps_a_cte_for_its_own_statement_only():
    """Parsing a second query with the default CTEs must not see the first one's CTE."""
    defining = "WITH c AS (SELECT * FROM title AS t) SELECT c.id FROM c"  # defines c
    parse(defining)  # the first statement defines c
    later = parse("SELECT c.id FROM c")  # the second declares no CTE: c is a table

    assert later.tree.table_of("c") == "c"  # not the title the first c stood for


def test_parse_query_lets_a_cte_of_a_subquery_read_the_enclosing_query():
    """A CTE declared inside a subquery sees the aliases of the query around it."""
    query = parse(
        "SELECT t.id FROM title AS t WHERE EXISTS (WITH c AS (SELECT * FROM "
        "movie_info AS mi WHERE mi.note = t.name) SELECT * FROM c)"
    )  # the CTE compares with the enclosing t

    subquery = query.condition.subquery  # the EXISTS query, which reads c
    c_note = Column(Path(1, "c", "c", ()), "note")  # the CTE's filter, renamed to c

    assert subquery.condition == BinaryExpression(
        "==", c_note, NAME
    )  # t.name at depth 0


def test_parse_query_reserves_the_statement_aliases_in_a_cte_copy():
    """A subquery of a CTE condition, copied into the query reading the CTE, names
    its provisional alias outside the statement's aliases.

        outer query:  title_0 (the CTE c over title)
        subquery:     mc ---movie---> title_1 <---movie--- mi
    """
    ast = sqlglot.parse_one(
        "WITH c AS (SELECT * FROM title AS t WHERE EXISTS (SELECT * FROM "
        "movie_companies AS mc, movie_info AS mi WHERE mc.movie_id = mi.movie_id)) "
        "SELECT title_0.id FROM c AS title_0"
    )  # the outer query declares title_0

    query = parse_query(ast, SCHEMA)  # c inlined as title_0

    subquery = query.condition.subquery  # the copied EXISTS query
    assert subquery.tree.aliases == ("mc", "mi", "title_1")  # not title_0


def test_aliases_names_every_alias_of_the_whole_statement():
    """Also the aliases of subqueries and CTEs, seen from any node."""
    ast = sqlglot.parse_one(
        "WITH c AS (SELECT * FROM title AS t) SELECT * FROM c AS x "
        "WHERE EXISTS (SELECT * FROM aka_title AS at)"
    )  # three queries
    subquery = ast.find(sqlglot.exp.Exists).this  # the EXISTS query

    assert aliases(ast) == {"t", "x", "at"}  # every declared alias
    assert aliases(subquery) == {"t", "x", "at"}  # the same from inside


def test_where_conjuncts_without_a_where_is_empty():
    """A query that filters nothing has no conjuncts."""
    ast = sqlglot.parse_one("SELECT t.id FROM title AS t")  # no WHERE

    assert where_conjuncts(ast) == []  # none, not a missing value


def test_where_conjuncts_splits_only_the_top_level_and_chain():
    """An OR inside the chain stays one conjunct, without its parentheses."""
    ast = sqlglot.parse_one(
        "SELECT t.id FROM title AS t WHERE t.id > 1 AND (t.id < 5 OR t.id = 9) AND t.x"
    )  # three conjuncts, the second holds an OR

    conjuncts = where_conjuncts(ast)  # the member under test
    texts = [conjunct.sql() for conjunct in conjuncts]  # to compare as text

    assert texts == ["t.id > 1", "t.id < 5 OR t.id = 9", "t.x"]  # parentheses go

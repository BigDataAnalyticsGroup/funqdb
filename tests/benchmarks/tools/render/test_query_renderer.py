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

"""How the renderer turns subqueries and expressions into FQL."""

from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal

import duckdb
import pytest

from benchmarks.datasets.dataset import LoadedDataset
from benchmarks.datasets.dataset_types import Reference
from benchmarks.datasets.fql_runtime import FQL_RUNTIME
from benchmarks.datasets.movies.movies_dataset import MOVIES
from benchmarks.datasets.schema import PrimaryKey, Schema
from benchmarks.datasets.special_names.special_names_dataset import SPECIAL_NAMES
from benchmarks.tools.ir.expressions import (
    TIME_OF_DAY_PARTS,
    BinaryExpression,
    Expression,
    ExtractExpression,
    ExtractPart,
    IntervalExpression,
    IntervalUnit,
    Literal,
)
from benchmarks.tools.ir.query import Query
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.render.config import Config, OperatorResult
from benchmarks.tools.render.query_renderer import QueryRenderer, ValueBase
from tests.benchmarks.tools.framework.parsing import query_from_sql
from tests.benchmarks.tools.framework.rendering import check_fql_reads_as_sql, fql_of

# The tables of the test schema below. A star marks a key column; the other
# columns are not declared.
#
#   movie_companies           title
#   +--------------+          +--------+
#   |  movie_id ---+--------->| *id    |
#   +--------------+          +--------+
#
#   movie_id is swizzled into the reference `movie`, not into `movie_id`
SCHEMA = Schema(
    references={"movie_companies": {"movie_id": Reference("movie", "title")}},
    primary_keys={"title": PrimaryKey(("id",))},
    columns={},
)
CORRELATED_EXISTS = (
    "SELECT t.title FROM title AS t WHERE EXISTS (SELECT * FROM aka_title AS at "
    "WHERE at.title = t.title)"
)
CORRELATED_EXISTS_IN_AGGREGATE = (
    "SELECT SUM(CASE WHEN EXISTS (SELECT * FROM aka_title AS at "
    "WHERE at.production_year = t.production_year) THEN 1 ELSE 0 END) AS s "
    "FROM title AS t"
)
SAMPLE_DATES = [
    date(2000, 12, 31),
    date(2001, 1, 1),
    date(1999, 1, 3),
    date(1, 1, 1),
    date(1996, 2, 29),
]  # year ends, the first date, a leap day
SAMPLE_TIMESTAMPS = [
    datetime(1999, 1, 3, 10, 20, 30, 500000),
    datetime(1969, 12, 31, 23, 59, 59, 999999),
]  # a fraction of a second, a moment before the epoch
# the parser rejects the time-of-day parts for a date
DATE_EXTRACTS = [
    pytest.param(part, value, id=f"{part.name}-{value}")
    for part in ExtractPart
    if part not in TIME_OF_DAY_PARTS
    for value in SAMPLE_DATES
]
TIMESTAMP_EXTRACTS = [
    pytest.param(part, value, id=f"{part.name}-{value}")
    for part in ExtractPart
    for value in SAMPLE_TIMESTAMPS
]

CONFIGS = [
    pytest.param(Config(), id="default"),  # structured predicates, operator calls
    pytest.param(
        Config.from_settings({"call_chaining": "METHOD", "predicate_form": "LAMBDA"}),
        id="method",
    ),  # lambdas and method calls
    pytest.param(
        Config.from_settings(
            {
                "attribute_access": "SUBSCRIPT",
                "operator_naming": "SYMBOLS",
                "operator_result": "CALL",
                "call_chaining": "OPERATOR",
                "predicate_form": "LAMBDA",
            }
        ),
        id="operators",
    ),  # every other style of the other options, with lambdas
    pytest.param(
        Config.from_settings(
            {
                "attribute_access": "PATH",
                "predicate_form": "STRUCTURED",
                "call_chaining": "METHOD",
            }
        ),
        id="paths",
    ),  # path access, structured predicates under method calls
]
# Queries on `movies` whose result is the same under SQL and FQL. A foreign key is
# never read as a value, and a computed column keeps its source column: both are
# limitations the transpiler catalogs.
MOVIES_QUERIES = [
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.production_year > 2000", id="filter"
    ),  # one comparison
    pytest.param(
        "SELECT t.title FROM title AS t "
        "WHERE t.production_year > 2000 OR t.title = 'The Matrix'",
        id="or",
    ),  # a disjunction
    pytest.param(
        "SELECT t.title FROM title AS t WHERE NOT (t.production_year > 2000)",
        id="not",
    ),  # a negation
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.title IN ('The Matrix', 'Heat') "
        "AND t.production_year NOT BETWEEN 2001 AND 2002",
        id="in-list-and-not-between",
    ),  # a list membership and a negated range
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.production_year NOT IN (1999, 2003)",
        id="not-in-list",
    ),  # a negated list membership
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.imdb_index IS NULL", id="is-null"
    ),  # an unmapped attribute
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.imdb_index IS NOT NULL",
        id="is-not-null",
    ),  # a mapped attribute
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.title LIKE 'The%'", id="like-prefix"
    ),  # startswith
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.title LIKE '%ed'", id="like-suffix"
    ),  # endswith
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.title NOT LIKE '%Matrix%'",
        id="not-like-contains",
    ),  # a negated substring test
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.title LIKE 'The M_trix%'",
        id="like-regex",
    ),  # `_` needs a regular expression
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.title LIKE '%M_trix'",
        id="like-regex-whole-value",
    ),  # "The Matrix" matches; "The Matrix Reloaded" only begins with a match
    pytest.param(
        "SELECT t.title, t.production_year FROM title AS t "
        "ORDER BY t.production_year DESC, t.title LIMIT 3",
        id="order-limit",
    ),  # two order keys and a top 3
    pytest.param(
        "SELECT t.title, t.production_year, t.id FROM title AS t "
        "ORDER BY t.production_year DESC, t.title, t.id DESC LIMIT 4",
        id="order-three-directions",
    ),  # three sort runs: the innermost sorts by the last key
    pytest.param(
        "SELECT t.title FROM title AS t ORDER BY t.title LIMIT 2 OFFSET 1",
        id="offset",
    ),  # skips the first row
    pytest.param("SELECT COUNT(*) AS c FROM title AS t", id="count"),  # one scalar
    pytest.param(
        "SELECT MIN(t.production_year) AS lo, MAX(t.production_year) AS hi "
        "FROM title AS t",
        id="two-aggregates",
    ),  # two scalars of one aggregation
    pytest.param(
        "SELECT COUNT(*) + 1 AS c FROM title AS t", id="computed-aggregate"
    ),  # a value over an aggregate
    pytest.param(
        "SELECT COUNT(DISTINCT t.production_year) AS c FROM title AS t",
        id="count-distinct",
    ),  # counts each year once
    pytest.param(
        "SELECT t.production_year, AVG(t.id) AS a, MIN(t.title) AS m "
        "FROM title AS t GROUP BY t.production_year",
        id="group-by",
    ),  # aggregates per group
    pytest.param(
        "SELECT t.production_year, COUNT(*) AS c FROM title AS t "
        "GROUP BY t.production_year HAVING COUNT(*) > 1",
        id="having",
    ),  # a filter on the groups
    pytest.param(
        'SELECT t.production_year, COUNT(*) AS "title count" FROM title AS t '
        "GROUP BY t.production_year",
        id="aggregate-name-with-a-space",
    ),  # no keyword argument can carry the name
    pytest.param(
        "SELECT t.title, MAX(t.production_year) AS m FROM title AS t "
        "GROUP BY t.title ORDER BY m DESC, t.title",
        id="order-by-aggregate",
    ),  # orders the groups by an aggregate
    pytest.param(
        "SELECT DISTINCT t.production_year FROM title AS t", id="distinct"
    ),  # each year once
    pytest.param(
        "SELECT DISTINCT COUNT(*) AS c FROM title AS t GROUP BY t.production_year",
        id="distinct-groups",
    ),  # the groups grouped once more
    pytest.param(
        "SELECT t.production_year, t.production_year + 1 AS y FROM title AS t",
        id="computed",
    ),  # a value per tuple
    pytest.param(
        "SELECT t.production_year, CASE WHEN t.production_year > 2000 "
        "THEN 'new' ELSE 'old' END AS age FROM title AS t",
        id="case",
    ),  # a conditional value
    pytest.param(
        "SELECT t.production_year, CASE WHEN t.production_year > 2000 "
        "THEN 'new' END AS age FROM title AS t",
        id="case-without-else",
    ),  # None where no branch holds, as SQL's NULL
    pytest.param(
        "SELECT t.title, SUBSTRING(t.title, 1, 3) AS s FROM title AS t",
        id="substring",
    ),  # a slice of a string
    pytest.param(
        "SELECT t.title, SUBSTRING(t.title, 0, 3) AS s FROM title AS t",
        id="substring-from-zero",
    ),  # start 0 is before the first character: two characters, not three
    pytest.param(
        "SELECT EXTRACT(YEAR FROM DATE '2001-02-03') AS y FROM title AS t",
        id="extract",
    ),  # a constant value per tuple
    pytest.param(
        "SELECT t.title, t.title AS again FROM title AS t", id="copy"
    ),  # one column selected twice
    pytest.param("SELECT t.title AS name FROM title AS t", id="rename"),  # an alias
    pytest.param(
        "SELECT mc.note, cn.name FROM movie_companies AS mc, company_name AS cn "
        "WHERE mc.company_id = cn.id",
        id="reference",
    ),  # reads along one reference
    pytest.param(
        "SELECT t.title, cn.name FROM title AS t, movie_companies AS mc, "
        "company_name AS cn WHERE mc.movie_id = t.id AND mc.company_id = cn.id "
        "AND cn.country_code = '[us]'",
        id="reference-chain",
    ),  # two references, filtered at the end
    pytest.param(
        "SELECT MIN(t.title) AS m FROM title AS t, movie_keyword AS mk, "
        "keyword AS k, movie_companies AS mc WHERE mk.movie_id = t.id "
        "AND mk.keyword_id = k.id AND mc.movie_id = t.id",
        id="join",
    ),  # two roots join at the hub `t`
    pytest.param(
        "SELECT akt.title, t.production_year FROM aka_title AS akt, title AS t "
        "WHERE akt.movie_id = t.id AND akt.production_year = t.production_year",
        id="reference-and-filter",
    ),  # a filter that compares both ends of a reference
    pytest.param(
        "SELECT a.title FROM title AS a, title AS z WHERE a.episode_of_id = z.id "
        "AND (a.production_year > 1900 OR z.production_year > 2000)",
        id="reference-read-in-one-branch-of-an-or",
    ),  # the first branch holds without reading the reference, which must exist
    pytest.param(
        "SELECT a.title FROM title AS a, title AS z WHERE a.episode_of_id = z.id "
        "AND (a.production_year > 1900 OR z.production_year > 2000) IS TRUE",
        id="reference-read-in-an-or-under-is-true",
    ),  # IS TRUE compares, yet its OR holds without z, which must still exist
    pytest.param(
        "SELECT a.title FROM title AS a, title AS z WHERE a.episode_of_id = z.id "
        "AND CASE WHEN a.production_year > 1900 THEN 1 ELSE z.production_year END = 1",
        id="reference-read-in-a-case-branch",
    ),  # the THEN branch holds without reading z, which must still exist
    pytest.param(
        "SELECT a.title FROM title AS a, title AS z "
        "WHERE a.episode_of_id = z.id AND z.imdb_index IS NULL",
        id="null-test-through-a-reference",
    ),  # true for a title without an episode_of_id, which the join drops
    pytest.param(
        "SELECT mc.note FROM title AS t, movie_companies AS mc, movie_info AS mi, "
        "title AS z WHERE mc.movie_id = t.id AND mi.movie_id = t.id "
        "AND t.episode_of_id = z.id AND (mc.note IS NULL OR z.title LIKE 'A%')",
        id="reference-off-the-join-read-in-one-branch-of-an-or",
    ),  # t.episode_of_id leads out of the join of mc and mi
    pytest.param(
        "SELECT akt.title, mk.id FROM title AS t, aka_title AS akt, "
        "movie_keyword AS mk WHERE akt.episode_of_id = t.id AND mk.movie_id = t.id",
        id="join-drops-a-reference",
    ),  # akt.movie_id also leads to title, so the join drops it to stay a tree
    pytest.param(
        "SELECT t.production_year + 1 AS y FROM title AS t "
        "GROUP BY t.production_year + 1 HAVING t.production_year + 1 > 2000",
        id="computed-group-key-after-grouping",
    ),  # HAVING reads the computed key as the group holds it
    pytest.param(
        "SELECT t.title FROM title AS t, movie_keyword AS mk, keyword AS k "
        "WHERE mk.movie_id = t.id AND mk.keyword_id = k.id AND k.keyword = 'dream'",
        id="filter-on-a-join-root",
    ),  # the filter on k is pushed into the root mk
    pytest.param(
        "SELECT t.title FROM title AS t, movie_link AS ml "
        "WHERE ml.movie_id = t.id AND ml.linked_movie_id = t.id",
        id="tuple-identity",
    ),  # both references of ml lead to the same title
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.production_year > 1999.5",
        id="decimal-literal",
    ),  # an exact literal with a fraction
    pytest.param(
        "SELECT t.title FROM title AS t WHERE EXISTS "
        "(SELECT * FROM aka_title AS akt WHERE akt.title = t.title)",
        id="exists",
    ),  # a correlated EXISTS
    pytest.param(
        "SELECT t.title FROM title AS t WHERE NOT EXISTS "
        "(SELECT * FROM aka_title AS akt WHERE akt.title = t.title)",
        id="not-exists",
    ),  # a negated correlated EXISTS
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.production_year IN "
        "(SELECT akt.production_year FROM aka_title AS akt)",
        id="in-subquery",
    ),  # a membership in a subquery's column
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.production_year IN "
        "(SELECT akt.production_year FROM aka_title AS akt WHERE akt.title IN "
        "(SELECT t2.title FROM title AS t2 WHERE t2.production_year = t.production_year))",
        id="nested-in-subquery",
    ),  # an IN subquery in an IN subquery, correlated two levels out
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.production_year = "
        "(SELECT MAX(t2.production_year) FROM title AS t2)",
        id="scalar-subquery",
    ),  # an uncorrelated scalar
    pytest.param(
        "SELECT t.title FROM title AS t WHERE t.production_year > "
        "(SELECT MIN(akt.production_year) FROM aka_title AS akt "
        "WHERE akt.title = t.title)",
        id="correlated-scalar-subquery",
    ),  # a scalar per outer tuple
    pytest.param(
        "SELECT t.production_year, COUNT(*) AS c FROM movie_keyword AS mk, title AS t "
        "WHERE mk.movie_id = t.id GROUP BY t.production_year "
        "HAVING COUNT(*) > (SELECT COUNT(*) FROM title AS t2 "
        "WHERE t2.production_year = t.production_year)",
        id="correlated-subquery-reads-a-group-key",
    ),  # the group key, read along a reference, as the group holds it
    pytest.param(
        "SELECT t.production_year, COUNT(*) AS c FROM movie_keyword AS mk, title AS t "
        "WHERE mk.movie_id = t.id GROUP BY t.production_year "
        "HAVING EXISTS (SELECT * FROM aka_title AS akt "
        "WHERE akt.production_year = t.production_year)",
        id="correlated-exists-reads-a-group-key",
    ),  # the same read inside EXISTS
    pytest.param(
        "WITH recent AS (SELECT * FROM title AS t WHERE t.production_year > 2000) "
        "SELECT r.title FROM recent AS r",
        id="cte",
    ),  # an inlined CTE
]
# Queries on `movies` that the renderer rejects, each with the limitation it raises.
REJECTED_QUERIES = [
    pytest.param(
        "SELECT t.title FROM title AS t WHERE EXISTS "
        "(SELECT MAX(akt.production_year) FROM aka_title AS akt)",
        Limitation.EXISTS_OVER_AGGREGATE,
        id="exists-over-aggregate",
    ),  # one row even over no rows, so always true
    pytest.param(
        "SELECT t.title FROM title AS t WHERE (t.production_year + 1) IS NULL",
        Limitation.NULL_IN_PREDICATE,
        id="is-null-of-a-computed-value",
    ),  # FQL tests only whether an attribute is mapped
    pytest.param(
        "SELECT akt.title AS a, t.title AS b FROM aka_title AS akt, title AS t "
        "WHERE akt.movie_id = t.id",
        Limitation.PROJECTED_NAME_COLLISION,
        id="projected-name-collision",
    ),  # both paths end in `title` before the rename
    pytest.param(
        'SELECT t.title AS "a__b" FROM title AS t',
        Limitation.PATH_SEPARATOR_NAME,
        id="output-name-with-a-path-separator",
    ),  # FDM reads `__` as a path
    pytest.param(
        "SELECT COUNT(*) AS input_function FROM title AS t",
        Limitation.OPERATOR_PARAMETER_NAME,
        id="aggregate-named-as-a-parameter",
    ),  # aggregate(input_function=...) would bind the relation twice
    pytest.param(
        "SELECT t.production_year, COUNT(*) AS input_function FROM title AS t "
        "GROUP BY t.production_year",
        Limitation.OPERATOR_PARAMETER_NAME,
        id="grouped-aggregate-named-as-a-parameter",
    ),  # the same for group_by_aggregate
    pytest.param(
        "SELECT t.production_year + 1 AS mk FROM title AS t, movie_keyword AS mk, "
        "movie_companies AS mc WHERE mk.movie_id = t.id AND mc.movie_id = t.id",
        Limitation.COMPUTED_ALIAS_SHADOWS_COLUMN,
        id="computed-output-named-as-a-join-alias",
    ),  # the join tuple stores mk already
    pytest.param(
        "SELECT COUNT(*) AS self FROM title AS t",
        Limitation.OPERATOR_PARAMETER_NAME,
        id="aggregate-named-self",
    ),  # aggregate.__init__ already binds self
    pytest.param(
        "SELECT t.production_year, COUNT(*) AS self FROM title AS t "
        "GROUP BY t.production_year",
        Limitation.OPERATOR_PARAMETER_NAME,
        id="grouped-aggregate-named-self",
    ),  # the same for group_by_aggregate
]
# Queries on `special_names`, whose names need quotes in SQL and care in FQL.
SPECIAL_NAMES_QUERIES = [
    pytest.param(
        'SELECT "first name", "class", "it\'s" FROM "order details" ORDER BY id',
        id="quoted-names",
    ),  # a space, a keyword and a quote in the names
    pytest.param('SELECT * FROM "order details"', id="star"),  # every column
    pytest.param(
        'SELECT o."first name" FROM "order details" AS o '
        'WHERE o."class" > 2 AND o."it\'s" < 30',
        id="quoted-filter",
    ),  # filters on quoted names
    pytest.param(
        "SELECT o.id FROM \"order details\" AS o WHERE o.project LIKE 'back%line'",
        id="like-across-a-line-break",
    ),  # `%` spans the line break of row 3, as in SQL
]


@pytest.fixture(scope="module")
def movies() -> Iterator[LoadedDataset]:
    """The `movies` dataset, shared by the tests below that only read it."""
    dataset = MOVIES.open()  # the same rows on both sides
    yield dataset  # read, never written
    dataset.close()  # releases the DuckDB connection


@pytest.fixture(scope="module")
def special_names() -> Iterator[LoadedDataset]:
    """The `special_names` dataset, shared by the tests below that only read it."""
    dataset = SPECIAL_NAMES.open()  # the same rows on both sides
    yield dataset  # read, never written
    dataset.close()  # releases the DuckDB connection


def test_QueryRenderer_render_expression_rejects_a_correlated_column_outside_its_query():
    """A correlated column reads its enclosing tuple, which only a rendered query binds."""
    query = parse(CORRELATED_EXISTS)  # a correlated EXISTS
    subquery = query.condition.subquery  # the EXISTS subquery
    renderer = QueryRenderer(Config())  # the default style
    base = ValueBase("i1", False)  # the subquery's own binding only

    with pytest.raises(
        ValueError, match="outside the query that encloses it"
    ):  # t.title
        renderer.render_expression(subquery.condition, subquery, base, False)  # unbound


@pytest.mark.parametrize(("sql", "limitation"), REJECTED_QUERIES)
def test_QueryRenderer_render_query_rejects_what_FQL_reads_otherwise(
    sql, limitation, movies
):
    """A query whose FQL would read other rows raises the limitation it hits."""
    with pytest.raises(UnsupportedSql) as rejection:  # nothing is rendered
        fql_of(sql, movies.schema())  # parses, then renders

    assert rejection.value.limitation is limitation  # the cataloged reason


@pytest.mark.parametrize("part, value", DATE_EXTRACTS + TIMESTAMP_EXTRACTS)
def test_QueryRenderer_render_expression_EXTRACT_computes_what_duckdb_extracts(
    part, value
):
    """Each rendered part agrees with DuckDB on dates and timestamps."""
    literal = Literal(value)  # the date or timestamp to extract from
    extract = ExtractExpression(part, literal)  # EXTRACT(part FROM value)

    rendered = evaluate_in_sandbox(extract)  # FQL's value
    expected = duckdb_value(f"SELECT EXTRACT({part.name} FROM ?)", value)  # SQL's

    assert rendered == expected  # equal value


@pytest.mark.parametrize("unit", list(IntervalUnit), ids=lambda unit: unit.name)
def test_QueryRenderer_render_expression_INTERVAL_shifts_a_timestamp_as_duckdb_does(
    unit,
):
    """`count` units shift a timestamp to where DuckDB's INTERVAL does."""
    start = datetime(1996, 2, 29, 10, 20, 30, 500000)  # a leap day, mid-second
    interval = IntervalExpression(unit, 2)  # two units
    shift = BinaryExpression("+", Literal(start), interval)  # start + 2 units

    rendered = evaluate_in_sandbox(shift)  # FQL's shifted timestamp
    expected = duckdb_value(f"SELECT ? + INTERVAL 2 {unit.name}", start)  # SQL's

    assert rendered == expected  # the same moment


def test_QueryRenderer_render_expression_INTERVAL_shifts_by_fractional_seconds_as_duckdb_does():
    """A fraction of a second shifts to the microsecond DuckDB shifts to."""
    start = datetime(1996, 2, 29, 23, 59, 59, 750000)  # a quarter second before March
    seconds = IntervalExpression(IntervalUnit.SECOND, Decimal("1.5"))  # 1.5 s
    shift = BinaryExpression("+", Literal(start), seconds)  # start + 1.5 s

    rendered = evaluate_in_sandbox(shift)  # FQL's shifted timestamp
    expected = duckdb_value("SELECT ? + INTERVAL '1.5' SECOND", start)  # SQL's

    assert rendered == expected  # the same moment


def test_QueryRenderer_render_query_forgets_the_enclosing_binding_afterwards():
    """An enclosing binding is forgotten once its subquery is rendered."""
    query = parse(CORRELATED_EXISTS)  # a correlated EXISTS
    subquery = query.condition.subquery  # the EXISTS subquery
    renderer = QueryRenderer(Config())  # one renderer, reused
    base = ValueBase("i1", False)  # the subquery's own binding only
    renderer.render_query(query)  # binds the enclosing tuple while it renders

    with pytest.raises(
        ValueError, match="outside the query that encloses it"
    ):  # a kept binding would let t.title render
        renderer.render_expression(subquery.condition, subquery, base, False)  # t.title


def test_QueryRenderer_render_query_computes_each_literal_of_another_type():
    """Equal literals of other types are separate outputs, not copies of the first."""
    query = parse("SELECT 1 AS one, TRUE AS yes, 1e0 AS x FROM title AS t")  # 1 == True

    fql = QueryRenderer(Config()).render_query(query)  # default style

    assert "lambda tf: True" in fql  # yes computes TRUE
    assert "lambda tf: 1.0" in fql  # x computes the float
    assert "tf.one" not in fql  # neither copies the column `one`


def test_QueryRenderer_render_query_twice_gives_the_same_fql():
    """A reused renderer renders a correlated subquery the same way again."""
    query = parse(CORRELATED_EXISTS_IN_AGGREGATE)  # EXISTS inside SUM(CASE ...)
    renderer = QueryRenderer(Config())  # one renderer, reused

    first = renderer.render_query(query)  # first render
    second = renderer.render_query(query)  # second render, same instance

    assert first == second  # identical output


FILTERED = "SELECT t.title FROM title t WHERE t.production_year > 1990"
GROUPED_RANKED = (
    "SELECT t.production_year, COUNT(t.id) AS c FROM title t WHERE t.id > 1 "
    "GROUP BY t.production_year ORDER BY t.production_year LIMIT 2"
)
MULTI_ROOT_JOIN = (
    "SELECT t.title, at.id FROM title t, aka_title at "
    "WHERE t.production_year = at.production_year AND t.id > 3"
)


@pytest.mark.parametrize(
    ("sql", "config", "expected"),
    [
        pytest.param(
            FILTERED,
            Config(),
            "project(filter_values(dbf.title, Gt('production_year', 1990)),'title').result",
            id="filter-property",
        ),  # the filter is project's input, so only project's result is taken
        pytest.param(
            FILTERED,
            Config(operator_result=OperatorResult.CALL),
            "project(filter_values(dbf.title, Gt('production_year', 1990)),'title')()",
            id="filter-call",
        ),  # the same in the call style
        pytest.param(
            GROUPED_RANKED,
            Config(),
            "project(subset(key_to_value(group_by_aggregate(filter_values(dbf.title, "
            "Gt('id', 1)),'production_year',c=Count('id')),'production_year'), "
            "ranking_key=lambda i: i.value.production_year, k=2),'production_year','c')"
            ".result",
            id="grouped-ranked-property",
        ),  # five nested operators, each a bare input of the next
        pytest.param(
            GROUPED_RANKED,
            Config(operator_result=OperatorResult.CALL),
            "project(subset(key_to_value(group_by_aggregate(filter_values(dbf.title, "
            "Gt('id', 1)),'production_year',c=Count('id')),'production_year'), "
            "ranking_key=lambda i: i.value.production_year, k=2),'production_year','c')"
            "()",
            id="grouped-ranked-call",
        ),  # the same in the call style
        pytest.param(
            MULTI_ROOT_JOIN,
            Config(),
            "project(filter_values(join(DBF({'t': filter_values(dbf.title, Gt('id', 3))"
            ".result,'at': dbf.aka_title})), Eq('at__production_year', "
            "Ref('t__production_year'))),'t.title','at.id').result",
            id="join",
        ),  # join is the filter's input; a DBF entry is no operator input
    ],
)
def test_QueryRenderer_render_query_takes_no_result_of_an_operator_input(
    sql, config, expected, movies
):
    """An operator resolves an operator input itself, so its result is not taken."""
    fql = fql_of(sql, movies.schema(), config)  # the rendered query

    assert fql == expected  # every inner operator stays a bare call


@pytest.mark.parametrize(
    ("sql", "taken"),
    [
        pytest.param(
            "SELECT t.title AS movie FROM title t",
            "project(dbf.title,'title').result.rename(",
            id="method-receiver",
        ),  # an operator has no rename method
        pytest.param(
            "SELECT t.title FROM title t WHERE EXISTS (SELECT * FROM aka_title at "
            "WHERE at.production_year = t.production_year)",
            "tf.production_year))).result)",
            id="exists",
        ),  # an operator is always true, its result is true only when not empty
        pytest.param(
            "SELECT t.title FROM title t WHERE t.production_year IN "
            "(SELECT at.production_year FROM aka_title at WHERE at.id > 3)",
            "for x in filter_values(dbf.aka_title, Gt('id', 3)).result]",
            id="in-subquery",
        ),  # an operator cannot be iterated
        pytest.param(
            "SELECT t.title FROM title t WHERE t.id = (SELECT MIN(t2.id) FROM title t2)",
            "aggregate(dbf.title,_col_0=Min('id')).result._col_0",
            id="scalar-subquery",
        ),  # an operator has no aggregate attribute
        pytest.param(
            MULTI_ROOT_JOIN,
            "'t': filter_values(dbf.title, Gt('id', 3)).result,",
            id="dbf-entry",
        ),  # join reads each DBF entry as a relation, never as an operator
        pytest.param(
            "SELECT t.title FROM title t",
            "project(dbf.title,'title').result",
            id="query-result",
        ),  # the query evaluates to its rows, not to an operator
    ],
)
def test_QueryRenderer_render_query_takes_the_result_where_a_value_is_read(
    sql, taken, movies
):
    """An operator call that is not an operator input has its result taken."""
    fql = fql_of(sql, movies.schema())  # the rendered query, in the default style

    assert taken in fql  # the result call where the value is read


@pytest.mark.parametrize("config", CONFIGS)
@pytest.mark.parametrize("sql", MOVIES_QUERIES)
def test_QueryRenderer_render_query_reads_the_rows_duckdb_reads_on_movies(
    sql, config, movies
):
    """Each query reads the same rows in FQL as in DuckDB, in every style."""
    check_fql_reads_as_sql(sql, movies, config)  # raises on other rows


@pytest.mark.parametrize("config", CONFIGS)
@pytest.mark.parametrize("sql", SPECIAL_NAMES_QUERIES)
def test_QueryRenderer_render_query_reads_the_rows_duckdb_reads_on_special_names(
    sql, config, special_names
):
    """Names that need quotes or escapes read the same rows, in every style."""
    check_fql_reads_as_sql(sql, special_names, config)  # raises on other rows


def parse(sql: str) -> Query:
    """`sql` parsed over the test schema."""
    return query_from_sql(sql, SCHEMA)  # the parsed query over the test schema


def evaluate_in_sandbox(expression: Expression) -> object:
    """`expression` rendered, then evaluated with the FQL runtime only."""
    base = ValueBase("", True)  # no tuple to read from
    renderer = QueryRenderer(Config())  # the default style
    rendered = renderer.render_expression(
        expression, None, base, False
    )  # FQL source text
    return eval(
        rendered, {"__builtins__": {}, **FQL_RUNTIME}
    )  # only the runtime names resolve


def duckdb_value(sql: str, value: object) -> object:
    """The value DuckDB computes for `sql` with `value` as its one parameter."""
    with duckdb.connect() as connection:  # a fresh in-memory database, closed after use
        return connection.execute(sql, [value]).fetchone()[
            0
        ]  # the single cell DuckDB computes

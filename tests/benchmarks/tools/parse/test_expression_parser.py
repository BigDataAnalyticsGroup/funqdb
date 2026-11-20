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

"""SQL expressions as their IR, and the ones that have none."""

from datetime import date, datetime
from decimal import Decimal

import pytest
import sqlglot
from sqlglot.optimizer.optimizer import optimize

from benchmarks.datasets.dataset_types import Reference
from benchmarks.datasets.schema import PrimaryKey, Schema
from benchmarks.tools.ir.expressions import (
    AggregateExpression,
    BetweenExpression,
    BinaryExpression,
    CaseBranch,
    CaseWhenExpression,
    CastExpression,
    Column,
    ExistsExpression,
    ExtractExpression,
    ExtractPart,
    InExpression,
    InSubqueryExpression,
    IntervalExpression,
    IntervalUnit,
    IsBooleanExpression,
    IsNullExpression,
    LikeExpression,
    Literal,
    ParenExpression,
    SubqueryExpression,
    SubstringExpression,
    TupleIdentityExpression,
    UnaryExpression,
)
from benchmarks.tools.ir.paths import Path
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.parse.expression_parser import (
    parse_value_expression,
    resolve_column,
)
from benchmarks.tools.parse.parse_state import ParseState
from benchmarks.tools.parse.query_parser import parse_query
from benchmarks.tools.sql.optimization import OPTIMIZER_DIALECT, OPTIMIZER_RULES
from tests.benchmarks.tools.framework.parsing import query_from_sql

# The tables of the tests below. A star marks a key column, an arrow a foreign key.
#
#   mc (key: id)               title (key: id)             company
#   +-----------------+        +--------------------+      +-------------+
#   | *id       INT   |        | *id     INT        |      | no columns  |
#   | movie_id  INT --+------->|  name   VARCHAR    |      | declared    |
#   | sequel_id INT --+------->|  year   INT        |      +-------------+
#   | other_id  INT --+---+    |  made   DATE       |             ^
#   | note   VARCHAR  |   |    |  shown  TIMESTAMP  |             |
#   +-----------------+   |    |  ok     BOOLEAN    |             |
#                         |    +--------------------+             |
#                         +---------------------------------------+
#
#   mc.movie_id is swizzled into the reference `movie`, mc.sequel_id into `sequel`,
#   mc.other_id into `other`
SCHEMA = Schema(
    references={
        "mc": {
            "movie_id": Reference("movie", "title"),
            "sequel_id": Reference("sequel", "title"),
            "other_id": Reference("other", "company"),
        }
    },
    primary_keys={"title": PrimaryKey(("id",)), "mc": PrimaryKey(("id",))},
    columns={
        "title": {
            "id": "INT",  # the key, a number to aggregate
            "name": "VARCHAR",  # text for LIKE, SUBSTRING and concatenation
            "year": "INT",  # a number to compute and compare with
            "made": "DATE",  # shifted by intervals and extracted from
            "shown": "TIMESTAMP",  # has a time of day to extract
            "ok": "BOOLEAN",  # a truth value for IS TRUE
        },
        "mc": {
            "id": "INT",  # the key, a number to aggregate
            "movie_id": "INT",  # a foreign key: a reference, not a value
            "sequel_id": "INT",  # a second foreign key, into the same table
            "other_id": "INT",  # a second foreign key, into another table
            "note": "VARCHAR",  # a plain text column beside the foreign keys
        },
    },
)
# the alias t over title, and the alias m over mc, the roots of the queries below
T = Path(0, "t", "t", ())
M = Path(0, "m", "m", ())
NAME = Column(T, "name")
YEAR = Column(T, "year")
MADE = Column(T, "made")
MOVIE = Column(M, "movie", True)
SEQUEL = Column(M, "sequel", True)
OTHER = Column(M, "other", True)
SUBSTITUTE = Column(None, "_computed_0")


def title_state() -> ParseState:
    """A parse state over the query `FROM title`, with no CTEs and no reserved aliases."""
    query = query_from_sql("SELECT 1 FROM title")  # any query holding the alias title
    return ParseState(query, {}, frozenset())  # no CTEs, no reserved aliases


def parsed(sql: str):
    """The IR of the unoptimized query `sql`, parsed against the schema above."""
    ast = sqlglot.parse_one(sql)  # untyped: no type check can refuse it
    return parse_query(ast, SCHEMA)  # the IR, or the rejection under test


def optimized(sql: str):
    """The query of `sql` as the optimizer types it, which the type checks read."""
    ast = sqlglot.parse_one(sql)  # not typed yet
    typed_ast = optimize(
        ast, schema=SCHEMA.columns, dialect=OPTIMIZER_DIALECT, rules=OPTIMIZER_RULES
    )  # as the pipeline optimizes a query
    return parse_query(typed_ast, SCHEMA)  # the IR, or the rejection under test


def computed(sql: str):
    """The only expression `sql` selects as a computed column."""
    query = parsed(sql)  # selects one computed value
    (expression,) = query.computed_columns.by_expression  # fails if there are more
    return expression  # the IR of that value


@pytest.mark.parametrize(
    ("condition", "limitation"),
    [
        ("t.name LIKE t.name", Limitation.UNSUPPORTED_EXPRESSION),  # no literal pattern
        ("t.name LIKE 1", Limitation.UNSUPPORTED_EXPRESSION),  # a literal, but no text
        ("t.year IS 5", Limitation.UNSUPPORTED_EXPRESSION),  # neither NULL nor a truth
        ("t.year IS (1)", Limitation.UNSUPPORTED_EXPRESSION),  # no literal at all
        ("t.year IN ()", Limitation.UNSUPPORTED_EXPRESSION),  # nothing to be in
        ("t.year = NULL", Limitation.NULL_LITERAL_COMPARISON),  # a plain comparison
        ("(NULL) <> t.year", Limitation.NULL_LITERAL_COMPARISON),  # in parentheses
        ("t.year BETWEEN NULL AND 2", Limitation.NULL_LITERAL_COMPARISON),  # a bound
        ("t.year IN (1, NULL)", Limitation.NULL_LITERAL_COMPARISON),  # a list option
        ("COUNT(*) > 1", Limitation.AGGREGATE_MIXED_WITH_COLUMN),  # column level
        (
            "t.year = (SELECT m.id FROM mc AS m)",
            Limitation.SCALAR_SUBQUERY_ROWS,
        ),  # rows, no single value
        (
            "t.year IN (SELECT m.movie_id FROM mc AS m)",
            Limitation.FOREIGN_KEY_AGAINST_SCALAR,
        ),  # a reference selected, a number tested
    ],
)
def test_parse_where_expression_rejects_a_condition_for_its_limitation(
    condition, limitation
):
    """Each condition names the limitation that documents why it has no IR."""
    with pytest.raises(UnsupportedSql) as raised:  # catalogued
        parsed(f"SELECT t.id FROM title AS t WHERE {condition}")  # refused in WHERE

    assert raised.value.limitation is limitation  # the catalog entry of this case


@pytest.mark.parametrize(
    ("condition", "limitation"),
    [
        ("m.movie_id = 1", Limitation.FOREIGN_KEY_AGAINST_SCALAR),  # a scalar
        ("1 = (m.movie_id)", Limitation.FOREIGN_KEY_AGAINST_SCALAR),  # either side
        ("m.movie_id IN (1, 2)", Limitation.FOREIGN_KEY_AGAINST_SCALAR),  # a list
        (
            "m.movie_id IN (m.other_id, 2)",
            Limitation.FOREIGN_KEY_AGAINST_SCALAR,
        ),  # two references do not make every option one
        ("m.movie_id < m.other_id", Limitation.FOREIGN_KEY_ORDERED),  # no order
        ("m.movie_id BETWEEN 1 AND 2", Limitation.FOREIGN_KEY_ORDERED),  # a range
        ("m.movie_id LIKE 'y'", Limitation.FOREIGN_KEY_AS_VALUE),  # needs the value
        ("m.movie_id + 1 > 2", Limitation.FOREIGN_KEY_AS_VALUE),  # computed with
        (
            "m.movie_id = m.other_id",
            Limitation.FOREIGN_KEYS_COMPARED_AS_REFERENCES,
        ),  # into title and into company: SQL may find equal ids
        (
            "m.movie_id <> m.other_id",
            Limitation.FOREIGN_KEYS_COMPARED_AS_REFERENCES,
        ),  # the inequality would always hold
        (
            "(m.other_id) = m.movie_id",
            Limitation.FOREIGN_KEYS_COMPARED_AS_REFERENCES,
        ),  # parentheses and side do not hide either reference
        (
            "m.movie_id IN (m.sequel_id, m.other_id)",
            Limitation.FOREIGN_KEYS_COMPARED_AS_REFERENCES,
        ),  # one option into another table is enough
        (
            "m.movie_id IN (SELECT m2.other_id FROM mc AS m2)",
            Limitation.FOREIGN_KEYS_COMPARED_AS_REFERENCES,
        ),  # the selected reference is read in the subquery
        (
            "EXISTS (SELECT m2.id FROM mc AS m2 WHERE m2.other_id = m.movie_id)",
            Limitation.FOREIGN_KEYS_COMPARED_AS_REFERENCES,
        ),  # the correlated reference is read in the enclosing query
    ],
)
def test_parse_where_expression_rejects_a_foreign_key_read_as_a_value(
    condition, limitation
):
    """A foreign key is a reference: it has no order and compares with references only."""
    with pytest.raises(UnsupportedSql) as raised:  # by design of the FDM
        parsed(f"SELECT m.id FROM mc AS m WHERE {condition}")  # refused in WHERE

    assert raised.value.limitation is limitation  # the catalog entry of this case


@pytest.mark.parametrize(
    "condition",
    [
        "m.movie_id = t.name",  # a column of the target table that is not its key
        "m.other_id = t.id",  # the key of another table than the reference's target
        "m.movie_id = t.id + 0",  # the key computed with, no longer the tuple's key
    ],
)
def test_parse_where_expression_rejects_a_foreign_key_against_a_correlated_non_key(
    condition: str,
):
    """Only the key of its target table names the tuple a reference leads to."""
    with pytest.raises(UnsupportedSql) as raised:  # a reference is no key value
        parsed(
            f"SELECT t.id FROM title AS t WHERE EXISTS "
            f"(SELECT m.id FROM mc AS m WHERE {condition})"
        )  # t is the enclosing query's

    assert raised.value.limitation is Limitation.FOREIGN_KEY_AGAINST_SCALAR  # by design


def test_parse_where_expression_rejects_in_a_subquery_of_two_columns():
    """IN compares with one column, so the second has no reading in SQL either."""
    sql = "SELECT t.id FROM title AS t WHERE t.year IN (SELECT m.id, m.note FROM mc AS m)"  # two columns selected

    with pytest.raises(ValueError, match="one column") as raised:  # invalid SQL
        parsed(sql)  # refused in WHERE

    assert not isinstance(raised.value, UnsupportedSql)  # no catalogued limitation


@pytest.mark.parametrize(
    ("condition", "expected"),
    [
        (
            "NOT t.year > 1",
            UnaryExpression("not", BinaryExpression(">", YEAR, Literal(1))),
        ),  # NOT binds the whole comparison
        (
            "(t.year > 1)",
            ParenExpression(BinaryExpression(">", YEAR, Literal(1))),
        ),  # parentheses stay in the IR
        (
            "t.year > 1 OR t.year < 0",
            BinaryExpression(
                "or",
                BinaryExpression(">", YEAR, Literal(1)),
                BinaryExpression("<", YEAR, Literal(0)),
            ),
        ),  # a logical operator over two comparisons
        (
            "t.year BETWEEN 1 AND 2",
            BetweenExpression(YEAR, Literal(1), Literal(2)),
        ),  # lower bound before upper
        (
            "t.year IN (1, 2)",
            InExpression(YEAR, (Literal(1), Literal(2))),
        ),  # options in stated order
        ("t.name LIKE 'x%'", LikeExpression(NAME, "x%")),  # a literal pattern
        (
            "t.name NOT LIKE 'x%'",
            UnaryExpression("not", LikeExpression(NAME, "x%")),
        ),  # NOT LIKE negates the match
        ("t.year IS NULL", IsNullExpression(YEAR)),  # a definedness test
        (
            "t.year IS NOT NULL",
            UnaryExpression("not", IsNullExpression(YEAR)),
        ),  # its negation
        ("t.ok IS TRUE", IsBooleanExpression(Column(T, "ok"), True)),  # a truth test
        ("t.ok IS FALSE", IsBooleanExpression(Column(T, "ok"), False)),  # the other
    ],
)
def test_parse_where_expression_reads_each_kind_of_condition(condition, expected):
    """Each condition becomes its own IR node, its operands in stated order."""
    query = parsed(f"SELECT t.id FROM title AS t WHERE {condition}")  # one condition

    assert query.condition == expected  # the exact node


def test_parse_where_expression_tests_a_foreign_key_for_null_and_membership():
    """A definedness test or a list of references reads no key value."""
    is_null = parsed("SELECT m.id FROM mc AS m WHERE m.movie_id IS NULL")  # allowed
    within = parsed(
        "SELECT m.id FROM mc AS m WHERE m.movie_id IN (m.sequel_id)"
    )  # both into title

    assert is_null.condition == IsNullExpression(MOVIE)  # the reference tested
    assert within.condition == InExpression(MOVIE, (SEQUEL,))  # references only


def test_parse_where_expression_compares_foreign_keys_by_identity():
    """Foreign keys into one table compare by the tuple they refer to, where no join equality takes them."""
    equal = parsed(
        "SELECT m.id FROM mc AS m WHERE m.movie_id = m.sequel_id OR m.id = 1"
    )  # both into title; under OR, so no join equality
    different = parsed(
        "SELECT m.id FROM mc AS m WHERE m.movie_id <> m.sequel_id"
    )  # both into title
    wrapped = parsed(
        "SELECT m.id FROM mc AS m WHERE (m.movie_id) <> m.sequel_id"
    )  # parens
    correlated = parsed(
        "SELECT m.id FROM mc AS m WHERE EXISTS "
        "(SELECT m2.id FROM mc AS m2 WHERE m2.sequel_id <> m.movie_id)"
    )  # the enclosing reference also into title

    identity = BinaryExpression("is", MOVIE, SEQUEL)  # the same tuple
    other_key = BinaryExpression("==", Column(M, "id"), Literal(1))  # the plain branch
    assert equal.condition == BinaryExpression("or", identity, other_key)  # kept
    assert different.condition == BinaryExpression(
        "is not", MOVIE, SEQUEL
    )  # other tuple
    assert wrapped.condition == BinaryExpression(
        "is not", ParenExpression(MOVIE), SEQUEL
    )  # parentheses do not hide the reference
    correlated_sequel = Column(
        Path(1, "m2", "m2", ()), "sequel", is_reference_column=True
    )  # m2.sequel_id, one level down
    assert correlated.condition.subquery.condition == BinaryExpression(
        "is not", correlated_sequel, MOVIE
    )  # the enclosing reference read at depth 0


@pytest.mark.parametrize(
    ("condition", "negated"),
    [
        ("m.movie_id = t.id", False),  # the reference first
        ("t.id = m.movie_id", False),  # the key first
        ("(m.movie_id) = (t.id)", False),  # in parentheses
        ("m.movie_id <> t.id", True),  # another tuple
    ],
)
def test_parse_where_expression_reads_a_foreign_key_against_its_target_key_as_an_identity(
    condition: str, negated: bool
):
    """The shape of TPC-H Q4: the subquery's reference leads to the enclosing tuple.

    t (title, *id)  <---movie---  m (mc), one level down
    """
    query = parsed(
        f"SELECT t.id FROM title AS t WHERE EXISTS "
        f"(SELECT m.id FROM mc AS m WHERE {condition})"
    )  # t is the enclosing query's
    movie = Column(Path(1, "m", "m", ()), "movie", is_reference_column=True)  # m.movie
    identity = TupleIdentityExpression(movie, Column.tuple_at(T))  # leads to t's tuple
    negation = UnaryExpression("not", identity)  # leads to another tuple

    expected = negation if negated else identity  # by the operator
    assert (
        query.condition.subquery.condition == expected
    )  # no FOREIGN_KEY_AGAINST_SCALAR


def test_parse_where_expression_reads_subqueries():
    """EXISTS and IN hold their query; IN one aggregate value is the equality with it."""
    exists = parsed(
        "SELECT t.id FROM title AS t WHERE EXISTS (SELECT m.id FROM mc AS m)"
    ).condition  # any row
    within = parsed(
        "SELECT t.id FROM title AS t WHERE t.year IN (SELECT m.id FROM mc AS m)"
    ).condition  # many values
    single = parsed(
        "SELECT t.id FROM title AS t WHERE t.year IN (SELECT MAX(m.id) AS x FROM mc AS m)"
    ).condition  # one value
    scalar = parsed(
        "SELECT t.id FROM title AS t WHERE t.year = (SELECT MAX(m.id) AS x FROM mc AS m)"
    ).condition  # one value, as an operand

    m_id = Column(Path(1, "m", "m", ()), "id")  # m.id, one level down
    assert isinstance(exists, ExistsExpression)  # holds the query
    assert exists.subquery.outputs[0].column == m_id  # the subquery's own column
    assert isinstance(within, InSubqueryExpression)  # a membership test
    assert within.value == YEAR  # of the stated value
    assert within.subquery.outputs[0].column == m_id  # against m.id
    assert isinstance(single, BinaryExpression) and single.op == "=="  # an equality
    assert single.left == YEAR  # of the stated value
    assert isinstance(single.right, SubqueryExpression)  # with the query's value
    assert scalar.op == "==" and scalar.left == YEAR  # read the same way
    assert isinstance(scalar.right, SubqueryExpression)  # with the query's value
    assert scalar.right.subquery.depth == 1  # nested one level below the query


def test_parse_where_expression_lets_a_subquery_read_a_cte_of_the_enclosing_query():
    """A CTE is visible to every subquery of the query declaring it."""
    query = parsed(
        "WITH c AS (SELECT * FROM title AS t WHERE t.year > 1) "
        "SELECT m.id FROM mc AS m WHERE EXISTS (SELECT * FROM c)"
    )  # c is read only inside the EXISTS

    subquery = query.condition.subquery  # the EXISTS query, one level down
    c_year = Column(Path(1, "c", "c", ()), "year")  # c.year as the subquery reads it

    assert subquery.tree.table_of("c") == "title"  # c stands for title, not a table c
    assert subquery.condition == BinaryExpression(">", c_year, Literal(1))  # c's filter


def test_parse_where_expression_keeps_a_date_shift_against_a_date():
    """The optimizer widens `date + INTERVAL` to a timestamp, but FQL keeps the date."""
    query = optimized(
        "SELECT t.id FROM title AS t WHERE t.made + INTERVAL '1' DAY = t.made"
    )  # rendered as a date on both sides

    shift = IntervalExpression(IntervalUnit.DAY, 1)  # INTERVAL '1' DAY
    plus = BinaryExpression("+", MADE, shift)  # the widening CAST is gone
    expected = BinaryExpression("==", MADE, plus)  # as the optimizer sorts
    assert query.condition == expected  # the date, not a timestamp


def test_parse_where_expression_keeps_a_date_shift_on_either_side_of_the_interval():
    """Interval first or last, the widening is the optimizer's and is not kept."""
    query = optimized(
        "SELECT t.id FROM title AS t WHERE INTERVAL '1' DAY + t.made = t.made"
    )  # the interval on the left

    shift = IntervalExpression(IntervalUnit.DAY, 1)  # INTERVAL '1' DAY
    plus = BinaryExpression("+", shift, MADE)  # in the stated order
    expected = BinaryExpression("==", MADE, plus)  # as the optimizer sorts
    assert query.condition == expected  # the date, not a timestamp


def test_parse_where_expression_keeps_an_in_subquery_of_a_widened_date_shift():
    """A subquery is judged by what its column renders as: here a date, not a timestamp."""
    query = optimized(
        "SELECT t.id FROM title AS t WHERE t.made IN "
        "(SELECT x.made - INTERVAL '1' DAY AS s FROM title AS x)"
    )  # the optimizer widens the shift to a timestamp

    assert isinstance(query.condition, InSubqueryExpression)  # not rejected
    assert query.condition.value == MADE  # the outer date, not the subquery's


@pytest.mark.parametrize(
    ("sql", "limitation"),
    [
        (
            "SELECT t.year AS y, COUNT(*) + t.year AS x FROM title AS t",
            Limitation.AGGREGATE_MIXED_WITH_COLUMN,
        ),  # a column beside an aggregate in one value
        (
            "SELECT UPPER(t.name) AS x FROM title AS t",
            Limitation.UNSUPPORTED_EXPRESSION,
        ),  # a function with no IR
        (
            "SELECT (SELECT m.id FROM mc AS m) AS x FROM title AS t",
            Limitation.SCALAR_SUBQUERY_ROWS,
        ),  # a subquery selecting rows
        (
            "SELECT SUBSTRING(t.name FROM t.year) AS x FROM title AS t",
            Limitation.SUBSTRING_BOUNDS,
        ),  # a start that is no literal
        (
            "SELECT SUBSTRING(t.name FROM '2') AS x FROM title AS t",
            Limitation.SUBSTRING_BOUNDS,
        ),  # a start that is text
        (
            "SELECT SUBSTRING(t.name FROM 1.5) AS x FROM title AS t",
            Limitation.SUBSTRING_BOUNDS,
        ),  # a start that is no whole number
        (
            "SELECT SUBSTRING(t.name FROM 1 FOR t.year) AS x FROM title AS t",
            Limitation.SUBSTRING_BOUNDS,
        ),  # a length that is no literal
        (
            "SELECT EXTRACT(TIMEZONE_HOUR FROM t.made) AS x FROM title AS t",
            Limitation.EXTRACT_TIMEZONE,
        ),  # a field a Python date lacks
        (
            "SELECT CAST(t.year AS INT) AS x FROM title AS t",
            Limitation.INEXACT_CAST,
        ),  # a target other than DOUBLE
        (
            "SELECT DATE '2020-1-1' AS x FROM title AS t",
            Limitation.TEMPORAL_LITERAL,
        ),  # a date Python reads differently
        (
            "SELECT TIMESTAMP '2020-13-01 00:00:00' AS x FROM title AS t",
            Limitation.TEMPORAL_LITERAL,
        ),  # a datetime that is no datetime
        (
            "SELECT TIME '10:00:00' AS x FROM title AS t",
            Limitation.TEMPORAL_LITERAL,
        ),  # a time of day without a date
        (
            "SELECT CAST('2020-01-01 00:00:00+01' AS TIMESTAMPTZ) AS x FROM title AS t",
            Limitation.TEMPORAL_LITERAL,
        ),  # a datetime with a time zone
        (
            "SELECT CAST('10:00:00+01' AS TIMETZ) AS x FROM title AS t",
            Limitation.TEMPORAL_LITERAL,
        ),  # a time of day with a time zone
        (
            "SELECT CAST('2020-01-01 00:00:00' AS TIMESTAMPLTZ) AS x FROM title AS t",
            Limitation.TEMPORAL_LITERAL,
        ),  # a datetime in the local time zone
        (
            "SELECT CAST(t.shown AS TIME) AS x FROM title AS t",
            Limitation.TEMPORAL_CAST,
        ),  # a stored datetime converted to a time of day
        (
            "SELECT INTERVAL '1' DAY TO HOUR AS x FROM title AS t",
            Limitation.UNSUPPORTED_EXPRESSION,
        ),  # a span of fields
        (
            "SELECT INTERVAL '1.5' SECOND AS x FROM title AS t",
            Limitation.UNSUPPORTED_EXPRESSION,
        ),  # timedelta refuses a Decimal
        (
            "SELECT -m.movie_id AS x FROM mc AS m",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # negated
        (
            "SELECT m.movie_id || 'x' AS x FROM mc AS m",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # concatenated
        (
            "SELECT CAST(m.movie_id AS DOUBLE) AS x FROM mc AS m",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # cast
        (
            "SELECT EXTRACT(YEAR FROM m.movie_id) AS x FROM mc AS m",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # extracted from
        (
            "SELECT SUBSTRING(m.movie_id FROM 1) AS x FROM mc AS m",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # cut
        (
            "SELECT CASE WHEN m.id > 1 THEN m.movie_id END AS x FROM mc AS m",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # a result of a branch
        (
            "SELECT CASE WHEN m.id > 1 THEN 1 ELSE m.movie_id END AS x FROM mc AS m",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # the default result
    ],
)
def test_parse_value_expression_rejects_a_value_for_its_limitation(sql, limitation):
    """Each selected value names the limitation that documents why it has no IR."""
    with pytest.raises(UnsupportedSql) as raised:  # catalogued
        parsed(sql)  # refused in the SELECT list

    assert raised.value.limitation is limitation  # the catalog entry of this case


def test_parse_value_expression_rejects_a_foreign_key_base_of_a_case_against_a_scalar():
    """The desugared comparison is checked like any other."""
    with pytest.raises(UnsupportedSql) as raised:  # a reference is no key value
        parsed("SELECT CASE m.movie_id WHEN 1 THEN 'a' END AS x FROM mc AS m")  # = 1

    assert raised.value.limitation is Limitation.FOREIGN_KEY_AGAINST_SCALAR  # by design


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("-t.year", UnaryExpression("-", YEAR)),  # a sign
        ("(t.year)", ParenExpression(YEAR)),  # parentheses stay in the IR
        ("t.year + 1", BinaryExpression("+", YEAR, Literal(1))),  # sum
        ("t.year - 1", BinaryExpression("-", YEAR, Literal(1))),  # difference
        ("t.year * 2", BinaryExpression("*", YEAR, Literal(2))),  # product
        ("t.year / 2", BinaryExpression("/", YEAR, Literal(2))),  # quotient
        ("t.name || 'x'", BinaryExpression("+", NAME, Literal("x"))),  # Python's +
        ("TRUE", Literal(True)),  # a truth value
        ("FALSE", Literal(False)),  # the other one, not read as TRUE
        ("NULL", Literal(None)),  # SQL's NULL
        ("'x'", Literal("x")),  # text
        ("1.5", Literal(Decimal("1.5"))),  # an exact number
        ("SUBSTRING(t.name FROM 2)", SubstringExpression(NAME, 2)),  # to the end
        ("SUBSTRING(t.name FROM 2 FOR 3)", SubstringExpression(NAME, 2, 3)),  # a length
        (
            "EXTRACT(YEAR FROM t.made)",
            ExtractExpression(ExtractPart.YEAR, MADE),
        ),  # a field of a date
        (
            "EXTRACT(HOUR FROM t.shown)",
            ExtractExpression(ExtractPart.HOUR, Column(T, "shown")),
        ),  # a field of a datetime
        ("CAST(t.year AS DOUBLE)", CastExpression(YEAR)),  # the one exact cast
        ("DATE '2020-01-02'", Literal(date(2020, 1, 2))),  # a date literal
        ("CAST('2020-01-02' AS DATE)", Literal(date(2020, 1, 2))),  # its other spelling
        (
            "TIMESTAMP '2020-01-02 03:04:05'",
            Literal(datetime(2020, 1, 2, 3, 4, 5)),
        ),  # a datetime literal
        ("CAST(t.made AS DATE)", MADE),  # already of that type on a typed AST
        ("INTERVAL '2' MONTH", IntervalExpression(IntervalUnit.MONTH, 2)),  # one field
        (
            "t.made + INTERVAL '2' YEAR",
            BinaryExpression("+", MADE, IntervalExpression(IntervalUnit.YEAR, 2)),
        ),  # a date shifted by a whole unit
        ("t.year IS NULL", IsNullExpression(YEAR)),  # a test read as a value
        (
            "NOT t.year IS NULL",
            UnaryExpression("not", IsNullExpression(YEAR)),
        ),  # its negation read as a value
    ],
)
def test_parse_value_expression_reads_each_kind_of_value(value, expected):
    """Each value becomes its own IR node, its operands in stated order."""
    expression = computed(f"SELECT {value} AS x FROM title AS t")  # one value

    assert expression == expected  # the exact node


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("24", 24),  # whole numbers are ints
        ("0.07", Decimal("0.07")),  # a fraction is exact
        (".5", Decimal("0.5")),  # a fraction without integer digits
        ("1.5E0", 1.5),  # an exponent makes a float
        ("2e-3", 0.002),  # a lower-case negative exponent
    ],
)
def test_parse_value_expression_types_a_number_as_iso_sql_does(text, expected):
    """A number without an exponent is exact, one with an exponent is a float."""
    literal = computed(f"SELECT {text} AS x FROM title AS t")  # the selected number

    assert literal == Literal(expected)  # the value SQL reads
    assert type(literal.value) is type(expected)  # exact stays exact, a float stays one


def test_parse_value_expression_reads_every_branch_of_a_case_and_the_default():
    """Conditions and results stay paired; without ELSE there is no default."""
    with_else = computed(
        "SELECT CASE WHEN t.year > 1 THEN 'a' WHEN t.year < 0 THEN 'b' ELSE 'c' END AS x "
        "FROM title AS t"
    )  # two branches and a default
    without_else = computed(
        "SELECT CASE WHEN t.year > 1 THEN 'a' END AS x FROM title AS t"
    )  # no default

    assert with_else == CaseWhenExpression(
        (
            CaseBranch(BinaryExpression(">", YEAR, Literal(1)), Literal("a")),
            CaseBranch(BinaryExpression("<", YEAR, Literal(0)), Literal("b")),
        ),
        Literal("c"),
    )  # in stated order
    assert without_else.default is None  # the absent ELSE


def test_parse_value_expression_desugars_the_base_value_of_a_case_into_comparisons():
    """`CASE x WHEN v` compares x with each v, so each branch is checked as one."""
    expression = computed(
        "SELECT CASE t.year WHEN 1 THEN 'a' WHEN 2 THEN 'b' END AS x FROM title AS t"
    )  # with a base value

    assert expression == CaseWhenExpression(
        (
            CaseBranch(BinaryExpression("==", YEAR, Literal(1)), Literal("a")),
            CaseBranch(BinaryExpression("==", YEAR, Literal(2)), Literal("b")),
        )
    )  # the base value in every comparison


def test_parse_value_expression_leaves_a_case_with_a_base_value_unchanged():
    """The base value is desugared on a copy, so parsing twice gives the same IR."""
    simple = sqlglot.parse_one(
        "CASE a WHEN 1 THEN 'x' WHEN 2 THEN 'y' ELSE 'z' END"
    )  # with a base value
    original = simple.sql()  # the SQL before parsing
    state = title_state()  # the query FROM title

    first = parse_value_expression(simple, state)  # into the IR
    second = parse_value_expression(simple, state)  # once more

    assert second == first  # parsing again gives the same IR
    assert simple.sql() == original  # the parsed AST keeps its base value


def test_resolve_column_rejects_an_alias_no_query_declares():
    """Neither the query nor any query enclosing it declares `x`."""
    state = title_state()  # SELECT 1 FROM title
    unknown = sqlglot.parse_one("x.id")  # x is declared nowhere

    with pytest.raises(ValueError, match="Unknown table 'x'") as raised:  # invalid SQL
        resolve_column(unknown, state)  # walks up to the outermost query

    assert not isinstance(raised.value, UnsupportedSql)  # not a catalogued limitation


def test_resolve_column_without_a_table_is_read_by_name():
    """An unqualified column has no alias to resolve: it names an output."""
    state = title_state()  # SELECT 1 FROM title
    unqualified = sqlglot.parse_one("x")  # no table

    assert resolve_column(unqualified, state) == Column(None, "x")  # by name, no path


def test_parse_value_expression_reads_a_subquery_in_further_parentheses():
    """The extra parentheses change nothing in SQL, so they change nothing here."""
    query = parsed(
        "SELECT ((SELECT MAX(m.id) AS x FROM mc AS m)) AS y FROM title AS t"
    )  # a subquery in two pairs of parentheses
    (value,) = query.computed_columns.by_expression  # the one computed value
    inner = value.unparenthesized()  # without the parentheses
    m_id = Column(Path(1, "m", "m", ()), "id")  # m.id, one level down

    assert isinstance(inner, SubqueryExpression)  # its query is read
    assert inner.subquery.aggregates.by_expression == {
        AggregateExpression("max", m_id): Column(None, "x")
    }  # MAX(m.id) under its output name


def test_parse_aggregate_expression_rejects_an_aggregate_of_the_enclosing_query():
    """SQL aggregates `t.year` in the outer query, one row; the IR would give one per tuple."""
    sql = "SELECT (SELECT SUM(t.year) AS x FROM mc AS m) AS y FROM title AS t"  # t only

    with pytest.raises(
        UnsupportedSql
    ) as raised:  # no IR for an aggregate read from below
        parsed(sql)  # refused in the subquery

    assert (
        raised.value.limitation is Limitation.UNSUPPORTED_EXPRESSION
    )  # the catalog entry


def test_parse_aggregate_expression_keeps_an_aggregate_mixing_own_and_enclosing_columns():
    """One column of its own query makes it an aggregate of that query, in SQL too."""
    query = parsed(
        "SELECT (SELECT SUM(m.id + t.year) AS x FROM mc AS m) AS y FROM title AS t"
    )  # m is the subquery's own
    (value,) = query.computed_columns.by_expression  # the outer value
    m_id = Column(Path(1, "m", "m", ()), "id")  # m.id, one level down
    summed = BinaryExpression("+", m_id, YEAR)  # m.id + t.year, t at depth 0

    assert isinstance(value, SubqueryExpression)  # the aggregate stays in the subquery
    assert list(value.subquery.computed_columns.by_expression) == [
        summed
    ]  # the argument computed in the subquery, reading the enclosing t.year


def test_resolve_column_swizzles_a_foreign_key_into_its_reference():
    """The column is read under the reference's name, marked as a reference."""
    query = parsed("SELECT m.movie_id, m.note FROM mc AS m")  # one of each
    foreign_key, plain = query.outputs  # in SELECT order

    assert foreign_key.column == MOVIE  # the reference, not movie_id
    assert foreign_key.name == "movie_id"  # the output keeps the SQL name
    assert plain.column == Column(M, "note")  # a plain column stays as it is


def test_resolve_column_reads_a_correlated_alias_in_the_enclosing_query():
    """A column of the enclosing query is read at its path, also two levels up."""
    query = parsed(
        "SELECT t.id FROM title AS t WHERE EXISTS (SELECT m.id FROM mc AS m WHERE EXISTS "
        "(SELECT x.id FROM title AS x WHERE x.year = t.year AND x.name = m.note))"
    )  # the innermost query reads both enclosing queries

    middle = query.condition.subquery  # one level down
    inner = middle.condition.subquery  # two levels down
    over_t, over_m = inner.condition.conjuncts()  # the two comparisons

    assert over_t.right == YEAR  # at depth 0
    assert over_m.right == Column(Path(1, "m", "m", ()), "note")  # at depth 1
    assert over_t.left == Column(Path(2, "x", "x", ()), "year")  # its own at depth 2


@pytest.mark.parametrize(
    "argument",
    ["SUM(*)", "COUNT(DISTINCT *)"],  # a star for another function, a star told apart
)
def test_parse_aggregate_expression_rejects_a_star_only_count_reads(argument):
    """Only COUNT(*) reads a star, and it counts rows, so it cannot be DISTINCT."""
    with pytest.raises(ValueError, match="only COUNT") as raised:  # invalid SQL
        parsed(f"SELECT {argument} AS x FROM mc AS m")  # in the SELECT list

    assert not isinstance(raised.value, UnsupportedSql)  # no catalogued limitation


def test_parse_aggregate_expression_rejects_a_star_of_one_alias():
    """`COUNT(m.*)` is no ISO SQL; only some dialects read it."""
    with pytest.raises(UnsupportedSql) as raised:  # no reading in FDM
        parsed("SELECT COUNT(m.*) AS x FROM mc AS m")  # in the SELECT list

    assert raised.value.limitation is Limitation.DIALECT_SPECIFIC  # the catalog entry


@pytest.mark.parametrize(
    ("argument", "limitation"),
    [
        ("SUM(m.movie_id)", Limitation.FOREIGN_KEY_AS_VALUE),  # summed
        ("SUM((m.movie_id))", Limitation.FOREIGN_KEY_AS_VALUE),  # in parentheses
        ("COUNT(DISTINCT m.movie_id)", Limitation.FOREIGN_KEY_AS_VALUE),  # told apart
        ("COUNT(DISTINCT (m.movie_id))", Limitation.FOREIGN_KEY_AS_VALUE),  # the same
    ],
)
def test_parse_aggregate_expression_rejects_reading_a_foreign_key_value(
    argument, limitation
):
    """Aggregating or telling references apart needs the key value a reference lacks."""
    with pytest.raises(UnsupportedSql) as raised:  # a reference has no value
        parsed(f"SELECT {argument} AS x FROM mc AS m")  # in the SELECT list

    assert raised.value.limitation is limitation  # the catalog entry of this case


@pytest.mark.parametrize(
    ("argument", "expected"),
    [
        ("COUNT(*)", AggregateExpression("count", None)),  # rows, no argument
        ("COUNT(m.id)", AggregateExpression("count", Column(M, "id"))),  # values
        (
            "COUNT(DISTINCT m.id)",
            AggregateExpression("count", Column(M, "id"), True),
        ),  # told apart
        ("SUM(m.id)", AggregateExpression("sum", Column(M, "id"))),  # added up
        ("AVG(m.id)", AggregateExpression("avg", Column(M, "id"))),  # averaged
        ("COUNT(m.movie_id)", AggregateExpression("count", MOVIE)),  # counts values
        (
            "COUNT((m.movie_id))",
            AggregateExpression("count", SUBSTITUTE),
        ),  # computed first
    ],
)
def test_parse_aggregate_expression_reads_each_kind_of_argument(argument, expected):
    """COUNT reads whether a value is there, which a reference tells as well."""
    query = parsed(f"SELECT {argument} AS x FROM mc AS m")  # one aggregate

    assert query.aggregates.by_expression == {expected: Column(None, "x")}  # held in x


def test_parse_aggregate_expression_computes_an_argument_first():
    """An argument that is no column goes through a computed column, also for DISTINCT."""
    query = parsed(
        "SELECT SUM(m.id + 1) AS s, COUNT(DISTINCT m.id + 2) AS d FROM mc AS m"
    )  # two computed arguments
    plus_one = BinaryExpression("+", Column(M, "id"), Literal(1))  # m.id + 1
    plus_two = BinaryExpression("+", Column(M, "id"), Literal(2))  # m.id + 2

    assert query.computed_columns.by_expression == {
        plus_one: Column(None, "_computed_0"),
        plus_two: Column(None, "_computed_1"),
    }  # each in its own generated column
    assert query.aggregates.by_expression == {
        AggregateExpression("sum", Column(None, "_computed_0")): Column(None, "s"),
        AggregateExpression("count", Column(None, "_computed_1"), True): Column(
            None, "d"
        ),
    }  # the aggregates read those columns

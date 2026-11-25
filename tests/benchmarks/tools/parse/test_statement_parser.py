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

"""Write and DDL statements as their IR, and the ones that have none."""

from datetime import date, datetime
from decimal import Decimal

import pytest
import sqlglot
from sqlglot.errors import OptimizeError

from benchmarks.datasets.dataset_types import Reference
from benchmarks.datasets.schema import PrimaryKey, Schema
from benchmarks.tools.ir.expressions import (
    BinaryExpression,
    Column,
    Literal,
    ParenExpression,
    ReferencedTupleExpression,
    SubqueryExpression,
    UnaryExpression,
)
from benchmarks.tools.ir.paths import Path
from benchmarks.tools.ir.query import Query
from benchmarks.tools.ir.statements import (
    Assignment,
    CreateTable,
    Delete,
    DropTable,
    Insert,
    Update,
)
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.parse.sql_baseline import reject_non_baseline_sql
from benchmarks.tools.parse.statement_parser import parse_statement

MOVIE = Reference("movie", "title")
# The tables of the tests below. A star marks a key column, an arrow a foreign key.
#
#   mc (key: id)              title (key: id)            pair (key: a, b)
#   +-----------------+       +--------------------+     +------------------+
#   | *id       INT   |       | *id      INT       |     | no SQL columns   |
#   | movie_id  INT --+------>|  name    VARCHAR   |     | declared         |
#   | note   VARCHAR  |       |  year    INT       |     +------------------+
#   +-----------------+       |  price   DOUBLE    |
#                             |  made    DATE      |
#   mc.movie_id is swizzled   |  shown   TIMESTAMP |
#   into the reference        |  ok      BOOLEAN   |
#   `movie` of title          |  exact   DECIMAL   |
#                             |  single  FLOAT     |
#                             +--------------------+
SCHEMA = Schema(
    references={"mc": {"movie_id": MOVIE}},
    primary_keys={
        "title": PrimaryKey(("id",)),
        "mc": PrimaryKey(("id",)),
        "pair": PrimaryKey(("a", "b")),  # a composite key: no column alone names a row
    },
    columns={
        "title": {
            "id": "INT",  # the key; the integer family
            "name": "VARCHAR",  # the text family
            "year": "INT",  # an integer column that numbers of other types convert into
            "price": "DOUBLE",  # a Python float is a DOUBLE
            "made": "DATE",  # a date, which Python's datetime is also an instance of
            "shown": "TIMESTAMP",  # a datetime, told apart from a date exactly
            "ok": "BOOLEAN",  # a bool, which is an int in Python but not in SQL
            "exact": "DECIMAL(10, 2)",  # holds a decimal up to its scale
            "single": "FLOAT",  # single precision: it rounds a Python float
        },
        "mc": {
            "id": "INT",  # the key
            "movie_id": "INT",  # the foreign key into title
            "note": "VARCHAR",  # a plain column beside the foreign key
        },
    },
)
# the same keys and references, without declared SQL columns
UNTYPED = Schema(SCHEMA.references, SCHEMA.primary_keys, {})
# the columns of `title` in declared order, as an Insert aligns its rows
TITLE_COLUMNS = (
    "id",
    "name",
    "year",
    "price",
    "made",
    "shown",
    "ok",
    "exact",
    "single",
)


def parsed(sql: str, schema: Schema = SCHEMA):
    """The IR of `sql`, which the baseline check admits, parsed against `schema`."""
    ast = sqlglot.parse_one(sql)  # the statement as written
    reject_non_baseline_sql(ast)  # only SQL the baseline admits reaches the parser
    return parse_statement(ast, schema)  # the member under test


def parsed_bypassing_baseline(sql: str, schema: Schema = SCHEMA):
    """The IR of `sql` with the baseline check skipped, for SQL it does not admit."""
    ast = sqlglot.parse_one(sql)  # the statement as written
    return parse_statement(ast, schema)  # the member under test, fed directly


def inserted_row(sql: str, schema: Schema = SCHEMA) -> dict:
    """The only row `sql` inserts, by column."""
    insert = parsed(sql, schema)  # one INSERT
    (row,) = insert.rows  # fails if `sql` wrote more than one row
    return dict(zip(insert.columns, row))  # each value under its column


@pytest.mark.parametrize(
    ("sql", "limitation"),
    [
        ("INSERT INTO title VALUES (1)", Limitation.INSERT_WITHOUT_COLUMNS),  # no list
        (
            "INSERT INTO title (id) SELECT id FROM mc",
            Limitation.INSERT_FROM_SELECT,
        ),  # a query
        (
            "INSERT INTO nope (id) VALUES (1)",
            Limitation.TABLE_WITHOUT_PRIMARY_KEY,
        ),  # no key
        ("UPDATE title SET id = 1", Limitation.UPDATE_PRIMARY_KEY),  # the key
        (
            "UPDATE pair SET b = 1",
            Limitation.UPDATE_PRIMARY_KEY,
        ),  # not the first key column
        (
            "UPDATE title SET year = DEFAULT",
            Limitation.UNSUPPORTED_EXPRESSION,
        ),  # keyword
        ("UPDATE mc SET note = movie_id", Limitation.FOREIGN_KEY_AS_VALUE),  # read
        ("UPDATE mc SET movie_id = id + 1", Limitation.FOREIGN_KEY_AS_VALUE),  # written
        ("SELECT mc.movie_id FROM mc", Limitation.FOREIGN_KEY_AS_VALUE),  # selected
        (
            "SELECT mc.note, mc.movie_id AS m FROM mc",
            Limitation.FOREIGN_KEY_AS_VALUE,
        ),  # selected under an alias, after a plain column
        ("CREATE VIEW v AS SELECT 1", Limitation.NON_TABLE_OBJECT),  # created
        ("DROP VIEW v", Limitation.NON_TABLE_OBJECT),  # dropped: the other call site
        ("CREATE TABLE g", Limitation.TABLE_WITHOUT_PRIMARY_KEY),  # no column list
        ("CREATE TABLE g (id INT)", Limitation.TABLE_WITHOUT_PRIMARY_KEY),  # no key
        (
            "CREATE GLOBAL TEMPORARY TABLE g (id INT PRIMARY KEY)",
            Limitation.DML_MODIFIER,
        ),  # a modifier of CREATE
        ("DROP TABLE t CASCADE", Limitation.DML_MODIFIER),  # a modifier of DROP
        (
            "CREATE TABLE g (id INT PRIMARY KEY, CHECK (id > 0))",
            Limitation.UNENFORCED_CONSTRAINT,
        ),  # not a column definition
        (
            "CREATE TABLE g (id INT PRIMARY KEY, a INT NOT NULL)",
            Limitation.UNENFORCED_CONSTRAINT,
        ),  # NOT NULL only holds on a key column
        (
            "CREATE TABLE g (id INT PRIMARY KEY, a INT DEFAULT 1)",
            Limitation.UNENFORCED_CONSTRAINT,
        ),  # only DEFAULT NULL is what an unmapped attribute reads as
        (
            "CREATE TABLE g (id INT PRIMARY KEY, a INT UNIQUE)",
            Limitation.UNENFORCED_CONSTRAINT,
        ),  # a constraint kind the relation never enforces
        (
            "DELETE FROM title WHERE year = 'x'",
            Limitation.DIALECT_SPECIFIC,
        ),  # the WHERE of a write is typed as a query's is
        (
            "DELETE FROM title WHERE title.id IN (SELECT d.id FROM "
            "(SELECT t.id FROM title AS t WHERE t.year > 2000) AS d)",
            Limitation.DERIVED_TABLE_IN_WRITE,
        ),  # no optimizer turns the derived table into a CTE, so its filter would be lost
        (
            "UPDATE title SET year = " + " + ".join(["1"] * 2000),
            Limitation.EXPRESSION_TOO_DEEP,
        ),  # one Python call per `+`, beyond the recursion limit
    ],
)
def test_parse_statement_rejects_sql_without_an_ir_for_its_limitation(sql, limitation):
    """Each statement names the limitation that documents why it has no IR."""
    with pytest.raises(UnsupportedSql) as raised:  # catalogued, not plain invalid SQL
        parsed(sql)  # parsing is what refuses it

    assert raised.value.limitation is limitation  # the catalog entry of this case


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM title WHERE zzz = 1",  # no such column
        'UPDATE title SET year = "DEFAULT"',  # quoted, it is a column, not the keyword
        "UPDATE title SET year = title.default",  # qualified, the same
        # an output name is no alias to the WHERE of its own SELECT
        "UPDATE title SET year = (SELECT m.id AS x FROM mc AS m WHERE x = 1)",
        "INSERT INTO title (id, name) VALUES (1, (SELECT MAX(zzz) FROM mc))",  # the same
        "INSERT INTO title (id, name) VALUES (1, (SELECT MAX(m.note) FROM mc AS m "
        "WHERE m.id = title.id))",  # the row it would correlate into does not exist yet
    ],
)
def test_parse_statement_raises_for_a_name_as_the_optimizer_does_in_a_query(sql):
    """The pipeline reports a query's unknown name from the optimizer, and a write's alike."""
    with pytest.raises(OptimizeError):  # sqlglot's own error for an unresolved name
        parsed(sql)  # qualifying is what finds it


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        (
            "INSERT INTO title (id, id) VALUES (1, 2)",
            "names a column twice",
        ),  # listed twice
        ("INSERT INTO title (name) VALUES ('x')", "key column.*id"),  # no key at all
        ("INSERT INTO pair (a) VALUES (1)", "key column.*b"),  # part of a composite key
        (
            "INSERT INTO title (id, nope) VALUES (1, 2)",
            "does not have: nope",
        ),  # unknown
        (
            "INSERT INTO title (id, name) VALUES (1)",
            "lists 2.*supplies 1",
        ),  # a short row
        (
            "INSERT INTO title (id, year) VALUES (1, id)",
            "cannot read a column",
        ),  # no row yet
        (
            "INSERT INTO title (id) VALUES (NULL)",
            "NULL into the key column 'title.id'",
        ),  # bare
        (
            "INSERT INTO title (id) VALUES ((NULL))",
            "NULL into the key column",
        ),  # parens
        ("INSERT INTO title (id) VALUES (1), (1)", "one key twice"),  # a later row wins
        (
            "INSERT INTO title (id) VALUES (1), ((1))",
            "one key twice",
        ),  # the same key in parentheses
        (
            "INSERT INTO title (id) VALUES (-1), (-(1))",
            "one key twice",
        ),  # the same key with the sign outside the parentheses
        (
            "UPDATE title SET (year) = (1)",
            "must target a column",
        ),  # a parenthesized target
        ("UPDATE title SET nope = 1", "column it lacks: nope"),  # as INSERT refuses it
        ("UPDATE title SET mc.year = 1", "assigns 'mc.year'"),  # another table's column
        (
            "UPDATE title SET year = 1, year = 2",
            "assigns 'year' twice",
        ),  # one column twice
        (
            "CREATE TABLE g (a INT PRIMARY KEY, b INT PRIMARY KEY)",
            "two primary keys",
        ),  # two
    ],
)
def test_parse_statement_rejects_invalid_sql_as_a_plain_value_error(sql, message):
    """Invalid SQL is no limitation of the transpiler, so the catalog does not name it."""
    with pytest.raises(ValueError, match=message) as raised:  # says what is wrong
        parsed(sql)  # parsing is what refuses it

    assert not isinstance(raised.value, UnsupportedSql)  # no catalogued limitation


def test_parse_statement_rejects_a_statement_kind_without_an_ir():
    """ALTER reaches the parser only past the baseline, which refuses it first."""
    with pytest.raises(
        UnsupportedSql
    ) as raised:  # a tuple function has no schema to alter
        parsed_bypassing_baseline("ALTER TABLE t ADD COLUMN c INT")  # no baseline check

    assert raised.value.limitation is Limitation.UNSUPPORTED_STATEMENT  # as documented


def test_parse_statement_create_rejects_a_table_key_naming_an_unknown_column():
    """A key over a column the table does not declare is invalid SQL."""
    with pytest.raises(
        ValueError, match="unknown column"
    ) as raised:  # says what is wrong
        parsed_bypassing_baseline("CREATE TABLE g (id INT, PRIMARY KEY (nope))")  # nope

    assert not isinstance(raised.value, UnsupportedSql)  # no catalogued limitation


def test_parse_statement_create_rejects_a_constraint_holding_more_than_a_key():
    """The CHECK beside the key would be dropped silently, so the key is not read."""
    sql = "CREATE TABLE g (id INT, CONSTRAINT pk PRIMARY KEY (id) CHECK (id > 0))"  # two parts

    with pytest.raises(UnsupportedSql) as raised:  # no key is declared
        parsed_bypassing_baseline(sql)  # one constraint name for both parts

    assert raised.value.limitation is Limitation.TABLE_WITHOUT_PRIMARY_KEY  # no key


@pytest.mark.parametrize(
    ("value", "limitation"),
    [
        ("2.5", Limitation.LITERAL_TYPE_COERCION),  # SQL rounds it into an INT
        ("-2.5", Limitation.LITERAL_TYPE_COERCION),  # a sign is read through
        ("(2.5)", Limitation.LITERAL_TYPE_COERCION),  # parentheses are read through
        ("-(2.5)", Limitation.LITERAL_TYPE_COERCION),  # parentheses inside a sign
        (
            "1E1",
            Limitation.LITERAL_TYPE_COERCION,
        ),  # a float, not a decimal, into an INT
        ("'x'", Limitation.DIALECT_SPECIFIC),  # text into a number
        ("TRUE", Limitation.DIALECT_SPECIFIC),  # a bool is an int in Python, not in SQL
    ],
)
def test_parse_statement_rejects_a_literal_sql_converts_for_its_int_column(
    value, limitation
):
    """FQL stores a literal as written, so one SQL converts or refuses is rejected."""
    with pytest.raises(UnsupportedSql) as raised:  # the two results would differ
        parsed(f"INSERT INTO title (id, year) VALUES (1, {value})")  # year is an INT

    assert raised.value.limitation is limitation  # which of the two it is


@pytest.mark.parametrize(
    ("column", "value", "limitation"),
    [
        ("name", "5", Limitation.DIALECT_SPECIFIC),  # a number into a VARCHAR
        ("price", "2.5", Limitation.LITERAL_TYPE_COERCION),  # a DECIMAL into a DOUBLE
        ("price", "2", Limitation.LITERAL_TYPE_COERCION),  # an int is no DOUBLE
        ("exact", "2", Limitation.LITERAL_TYPE_COERCION),  # an int is no DECIMAL
        ("single", "2", Limitation.LITERAL_TYPE_COERCION),  # an int is no FLOAT
        (
            "exact",
            "1.505",
            Limitation.LITERAL_TYPE_COERCION,
        ),  # beyond the column's scale
        ("single", "1.5", Limitation.LITERAL_TYPE_COERCION),  # a FLOAT rounds it
        ("ok", "1", Limitation.DIALECT_SPECIFIC),  # an int is no BOOLEAN
        (
            "made",
            "TIMESTAMP '2020-01-01 10:00:00'",
            Limitation.DIALECT_SPECIFIC,
        ),  # a date
        ("shown", "DATE '2020-01-01'", Limitation.DIALECT_SPECIFIC),  # a datetime
    ],
)
def test_parse_statement_rejects_a_literal_of_another_type_per_column_type(
    column, value, limitation
):
    """The stored types are compared exactly per family of SQL column types."""
    sql = f"INSERT INTO title (id, {column}) VALUES (1, {value})"  # one literal written

    with pytest.raises(UnsupportedSql) as raised:  # not of the column's own type
        parsed(sql)  # parsing is what refuses it

    assert raised.value.limitation is limitation  # the reason this family gives


@pytest.mark.parametrize(
    "value",
    [
        "2 + 3",  # arithmetic
        "-(2 + 3)",  # a sign over a value that is no literal
        "CAST(5 AS DOUBLE)",  # a literal wrapped in a cast is no literal either
    ],
)
def test_parse_statement_rejects_a_foreign_key_value_that_is_no_key_literal(value):
    """Only a literal names the referenced tuple; a negated non-number is none either."""
    sql = f"INSERT INTO mc (id, movie_id) VALUES (1, {value})"  # no literal key

    with pytest.raises(UnsupportedSql) as raised:  # no literal names a tuple
        parsed(sql, UNTYPED)  # untyped: no type check runs before the key is read

    assert raised.value.limitation is Limitation.FOREIGN_KEY_AS_VALUE  # as documented


@pytest.mark.parametrize(
    ("column", "value", "limitation"),
    [
        ("year", "price", Limitation.LITERAL_TYPE_COERCION),  # a DOUBLE into an INT
        ("price", "year", Limitation.LITERAL_TYPE_COERCION),  # an INT into a DOUBLE
        ("single", "price", Limitation.LITERAL_TYPE_COERCION),  # a DOUBLE into a FLOAT
        ("name", "year + 1", Limitation.DIALECT_SPECIFIC),  # a number into text
        ("made", "shown", Limitation.DIALECT_SPECIFIC),  # a datetime into a DATE
        ("shown", "made + INTERVAL '1' DAY", Limitation.DIALECT_SPECIFIC),  # a date
        ("ok", "year", Limitation.DIALECT_SPECIFIC),  # a number into a BOOLEAN
        (
            "made",
            "CASE WHEN ok THEN made ELSE shown END",
            Limitation.DIALECT_SPECIFIC,
        ),  # typed DATE by its first result, yet a datetime for rows without ok
    ],
)
def test_parse_statement_update_rejects_a_computed_value_sql_converts(
    column, value, limitation
):
    """FQL stores a value as it is, so one of another type would differ from SQL's."""
    with pytest.raises(UnsupportedSql) as raised:  # the two results would differ
        parsed(f"UPDATE title SET {column} = {value}")  # a computed value

    assert raised.value.limitation is limitation  # which of the two it is


@pytest.mark.parametrize(
    ("subquery", "limitation"),
    [
        (
            "(SELECT MAX(m.id) AS x FROM mc AS m WHERE m.note = 1)",
            Limitation.DIALECT_SPECIFIC,
        ),  # text against a number
        (
            "(SELECT MAX(title.made + INTERVAL '1' HOUR) AS x FROM title)",
            Limitation.INEXACT_INTERVAL,
        ),  # a sub-day shift of a date
    ],
)
def test_parse_statement_insert_checks_the_types_inside_a_subquery(
    subquery: str, limitation: Limitation
):
    """A subquery in VALUES is qualified and typed as a query is, so the checks hold."""
    with pytest.raises(UnsupportedSql) as raised:  # the types do not fit
        parsed(f"INSERT INTO title (id, year) VALUES (1, {subquery})")  # in VALUES

    assert raised.value.limitation is limitation  # the catalog entry of the check


def test_parse_statement_insert_rejects_an_ill_typed_value_outside_a_subquery():
    """The row values are typed too, not only the subqueries inside them."""
    with pytest.raises(UnsupportedSql) as raised:  # ISO SQL multiplies only numbers
        parsed(
            "INSERT INTO title (id, name) VALUES (1, 'a' * 2)"
        )  # text times a number

    assert raised.value.limitation is Limitation.DIALECT_SPECIFIC  # the catalog entry


def test_parse_statement_insert_rejects_a_converted_value_in_a_row():
    """A computed value is checked against its column, as in UPDATE."""
    sql = "INSERT INTO title (id, year) VALUES (1, CAST(1.5 AS DOUBLE))"  # no literal

    with pytest.raises(UnsupportedSql) as raised:  # SQL rounds it into the INT
        parsed(sql)  # parsing is what refuses it

    assert raised.value.limitation is Limitation.LITERAL_TYPE_COERCION  # the reason


def test_parse_statement_delete_applies_the_foreign_key_rules_to_its_where():
    """An unqualified foreign key is recognised as one, as in a query."""
    with pytest.raises(UnsupportedSql) as raised:  # a reference is no key value
        parsed("DELETE FROM mc WHERE movie_id = 1")  # unqualified, against a scalar

    assert raised.value.limitation is Limitation.FOREIGN_KEY_AGAINST_SCALAR  # in WHERE


@pytest.mark.parametrize(
    "value",
    ["exact * 1.123", "exact / 3"],  # both have more digits than the column's scale
)
def test_parse_statement_update_rejects_a_decimal_sql_would_round(value):
    """SQL rounds into DECIMAL(10, 2), while FQL would store every digit."""
    with pytest.raises(UnsupportedSql) as raised:  # the two results would differ
        parsed(f"UPDATE title SET exact = {value}")  # same type, more digits

    assert raised.value.limitation is Limitation.LITERAL_TYPE_COERCION  # the reason


def test_parse_statement_update_keeps_a_decimal_sum():
    """A sum of a column and an integer keeps the column's scale, so nothing rounds."""
    update = parsed("UPDATE title SET exact = exact + 1")  # same type, same digits
    exact = Column(Path(0, "title", "title", ()), "exact")  # title.exact
    plus_one = BinaryExpression("+", exact, Literal(1))  # exact + 1

    assert update.assignments == (Assignment("exact", plus_one),)  # the sum, unrounded


def test_parse_statement_keeps_a_select_as_a_query():
    """A query is no write: it needs no key and comes back as its own IR."""
    statement = parsed("SELECT id FROM nokey")  # no key declared for nokey

    assert isinstance(statement, Query)  # parse_query's result
    assert statement.tree.table_of("nokey") == "nokey"  # of the named table


def test_parse_statement_keeps_a_foreign_key_an_in_subquery_selects():
    """IN compares the subquery's foreign key as a reference, so only the result refuses one."""
    statement = parsed(
        "SELECT mc.id FROM mc WHERE mc.movie_id IN (SELECT other.movie_id FROM mc AS other)"
    )  # a reference tested against the references the subquery selects

    subquery = statement.condition.subquery  # the IN subquery
    assert subquery.outputs[0].column.is_reference_column  # kept as the reference


def test_parse_statement_drop_names_its_table():
    """DROP holds the table's name only."""
    assert parsed("DROP TABLE t") == DropTable("t")  # the table to remove


@pytest.mark.parametrize(
    ("sql", "key"),
    [
        ("CREATE TABLE g (id INT PRIMARY KEY, a INT)", ("id",)),  # on the column
        ("CREATE TABLE g (a INT, id INT PRIMARY KEY)", ("id",)),  # not the first column
        (
            "CREATE TABLE g (id INT NOT NULL PRIMARY KEY)",
            ("id",),
        ),  # NOT NULL on the key
        (
            "CREATE TABLE g (id INT PRIMARY KEY, a INT DEFAULT NULL)",
            ("id",),
        ),  # no value
        (
            "CREATE TABLE g (id INT PRIMARY KEY, a INT DEFAULT (NULL))",
            ("id",),
        ),  # parens
    ],
)
def test_parse_statement_create_reads_the_key_declared_on_a_column(sql, key):
    """The key is all the relation keeps of the declaration."""
    create = parsed(sql)  # any way to declare a key on a column

    assert create == CreateTable("g", PrimaryKey(key))  # the table and its key


@pytest.mark.parametrize(
    ("sql", "key"),
    [
        (
            "CREATE TABLE g (id INT, b INT, PRIMARY KEY (b, id))",
            ("b", "id"),
        ),  # composite
        ("CREATE TABLE g (id INT, CONSTRAINT pk PRIMARY KEY (id))", ("id",)),  # named
    ],
)
def test_parse_statement_create_reads_the_key_declared_on_the_table(sql, key):
    """A table-level key is read in its stated column order, named or not."""
    create = parsed_bypassing_baseline(sql)  # a table-level key, the parser alone

    assert create == CreateTable("g", PrimaryKey(key))  # the table and its key


def test_parse_statement_reads_names_of_a_write_as_sql_does():
    """Unquoted names are case-insensitive, so the checks by name still hold."""
    with pytest.raises(UnsupportedSql) as raised:  # ID is the key id
        parsed("UPDATE TITLE SET ID = 5")  # would bypass the key check by spelling

    insert = parsed("INSERT INTO TITLE (ID, YEAR) VALUES (1, 2)")  # the same table

    assert raised.value.limitation is Limitation.UPDATE_PRIMARY_KEY  # the key
    assert insert.written_table == "title"  # the declared name
    year_value = insert.rows[0][TITLE_COLUMNS.index("year")]  # the value under year
    assert year_value == Literal(2)  # the declared column


def test_parse_statement_delete_reads_the_rows_of_its_where():
    """DELETE holds the query reading the rows to delete."""
    every_row = parsed("DELETE FROM title")  # no WHERE
    some_rows = parsed("DELETE FROM title WHERE year = 1")  # a WHERE
    year = Column(Path(0, "title", "title", ()), "year")  # title.year

    assert isinstance(every_row, Delete)  # the write's IR
    assert every_row.query.condition is None  # nothing filtered
    filtered = BinaryExpression("==", year, Literal(1))  # year = 1
    assert some_rows.query.condition == filtered  # only the rows the WHERE keeps


# a table and a column whose names need quotes, as in the `special_names` dataset:
#
#   order details
#   +-----------------+
#   | *id       INT   |
#   |  "quoted" INT   |
#   +-----------------+
QUOTED = Schema(
    references={},
    primary_keys={"order details": PrimaryKey(("id",))},
    columns={"order details": {"id": "INT", '"quoted"': "INT"}},
)


@pytest.mark.parametrize(
    "sql",
    [
        'DELETE FROM "order details" WHERE """quoted""" = 1',  # a delete
        'UPDATE "order details" SET """quoted""" = 2 WHERE """quoted""" = 1',  # an update
    ],
)
def test_parse_statement_reads_a_quoted_column_in_the_where_of_a_write(sql):
    """A write qualifies its WHERE against the schema with each name quoted as written."""
    statement = parsed(sql, QUOTED)  # unquoted, sqlglot could not resolve "quoted"
    details = Path(0, "order details", "order details", ())  # the only alias
    quoted = Column(details, '"quoted"')  # the column, its quotes kept

    filtered = BinaryExpression("==", quoted, Literal(1))  # "quoted" = 1
    assert statement.query.condition == filtered  # the rows the WHERE keeps


def test_parse_statement_insert_aligns_a_row_with_the_declared_columns():
    """The listed order does not matter, and an unlisted column holds NULL."""
    insert = parsed("INSERT INTO title (year, id) VALUES (1999, 7)")  # listed backwards

    assert isinstance(insert, Insert)  # the write's IR
    assert insert.written_table == "title"  # into title
    assert insert.primary_key == PrimaryKey(("id",))  # title's key
    assert insert.columns == TITLE_COLUMNS  # declared order, not listed order
    assert insert.rows == (
        (
            Literal(7),  # id
            Literal(None),  # name: unlisted
            Literal(1999),  # year
            Literal(None),  # price: unlisted
            Literal(None),  # made: unlisted
            Literal(None),  # shown: unlisted
            Literal(None),  # ok: unlisted
            Literal(None),  # exact: unlisted
            Literal(None),  # single: unlisted
        ),
    )  # each value under its own column


def test_parse_statement_insert_without_declared_columns_keeps_the_listed_ones():
    """Without declared SQL columns the list is all that is known, in its own order."""
    sql = "INSERT INTO title (name, id) VALUES ('Heat', 7)"  # listed backwards

    insert = parsed(sql, UNTYPED)  # no types

    assert insert.columns == ("name", "id")  # as listed
    assert insert.rows == ((Literal("Heat"), Literal(7)),)  # aligned with the list


def test_parse_statement_insert_writes_every_row():
    """VALUES may hold several rows, each parsed on its own."""
    insert = parsed("INSERT INTO mc (id, note) VALUES (1, 'a'), (2, 'b')")  # two rows
    columns = insert.columns  # id, movie_id, note
    ids = [row[columns.index("id")] for row in insert.rows]  # one id per row
    notes = [row[columns.index("note")] for row in insert.rows]  # one note per row

    assert ids == [Literal(1), Literal(2)]  # in VALUES order
    assert notes == [Literal("a"), Literal("b")]  # each row keeps its own note


def test_parse_statement_insert_parses_a_composite_key():
    """Every key column written, the statement carries the whole key."""
    insert = parsed(
        "INSERT INTO pair (b, a) VALUES (1, 2)", UNTYPED
    )  # both key columns

    assert insert.primary_key == PrimaryKey(
        ("a", "b")
    )  # in key order, not listed order
    assert insert.columns == ("b", "a")  # as listed, without declared SQL columns
    assert insert.rows == ((Literal(1), Literal(2)),)  # as listed: b is 1, a is 2


def test_parse_statement_insert_writes_rows_that_share_only_part_of_a_composite_key():
    """Two rows differ in b, so they have two keys although a is the same."""
    insert = parsed(
        "INSERT INTO pair (a, b) VALUES (1, 2), (1, 3)", UNTYPED
    )  # (1, 2) and (1, 3)

    assert insert.rows == (
        (Literal(1), Literal(2)),
        (Literal(1), Literal(3)),
    )  # both rows, none refused as a repeated key


@pytest.mark.parametrize(
    ("column", "value", "stored"),
    [
        ("year", "1999", 1999),  # an int into an INT
        ("price", "1E1", 10.0),  # a float into a DOUBLE
        ("name", "'x'", "x"),  # text into a VARCHAR
        ("made", "DATE '2020-01-02'", date(2020, 1, 2)),  # a date into a DATE
        (
            "shown",
            "TIMESTAMP '2020-01-02 10:00:00'",
            datetime(2020, 1, 2, 10),
        ),  # a datetime
        ("ok", "TRUE", True),  # a bool into a BOOLEAN
        ("exact", "1.50", Decimal("1.50")),  # a decimal of the column's scale
    ],
)
def test_parse_statement_insert_stores_a_literal_of_the_column_type(
    column, value, stored
):
    """A literal of the column's own family is stored as written."""
    sql = f"INSERT INTO title (id, {column}) VALUES (1, {value})"  # one literal
    row = inserted_row(sql)  # the one row

    assert row[column] == Literal(stored)  # the value as written
    assert type(row[column].value) is type(stored)  # 2 stays an int, not a Decimal


def test_parse_statement_insert_stores_a_negated_number_of_the_column_type():
    """The check reads the number below the sign, and the sign stays on the value."""
    row = inserted_row("INSERT INTO title (id, year) VALUES (1, -1999)")  # an int

    assert row["year"] == UnaryExpression("-", Literal(1999))  # stored as written


@pytest.mark.parametrize("value", ["NULL", "(NULL)"])  # the second: in parentheses
def test_parse_statement_insert_stores_null_into_a_foreign_key_as_it_is(value):
    """NULL refers to no tuple, so no reference is rendered for it."""
    row = inserted_row(f"INSERT INTO mc (id, movie_id) VALUES (1, {value})")  # no tuple

    assert row["movie_id"].is_null_literal()  # NULL stays NULL


@pytest.mark.parametrize(
    ("value", "key"),
    [
        ("5", 5),  # the key as written
        ("-5", -5),  # negated into the key's own value
        ("(5)", 5),  # the parentheses are no part of the key
        ("-(5)", -5),  # both
        ("'k'", "k"),  # a text key, on the untyped schema below
    ],
)
def test_parse_statement_insert_writes_a_foreign_key_as_the_tuple_it_refers_to(
    value, key
):
    """A literal key names the referenced tuple along the column's reference."""
    sql = f"INSERT INTO mc (id, movie_id) VALUES (1, {value})"  # a literal key

    row = inserted_row(sql, UNTYPED)  # untyped: a text key needs no type check

    expected = ReferencedTupleExpression(MOVIE, Literal(key))  # the tuple of the key
    assert row["movie_id"] == expected  # not the key value itself


@pytest.mark.parametrize(
    "subquery",
    [
        "(SELECT MAX(m.id) AS x FROM mc AS m)",  # reads a column of its own table
        "(SELECT COUNT(m.id) AS x FROM mc AS m WHERE m.id > 1)",  # in its WHERE too
        "(SELECT MAX(m.id) AS x FROM mc AS m WHERE EXISTS (SELECT * FROM mc AS n))",  # nested
    ],
)
def test_parse_statement_insert_lets_a_subquery_read_its_own_columns(subquery):
    """Only a column outside any subquery would read the tuple being created."""
    sql = f"INSERT INTO title (id, year) VALUES (1, {subquery})"  # a scalar subquery
    row = inserted_row(sql)  # the one row

    assert isinstance(row["year"], SubqueryExpression)  # parsed, not refused


def test_parse_statement_update_assigns_a_foreign_key_under_its_reference_name():
    """The attribute written is the reference, and the literal is the tuple it names."""
    update = parsed(
        "UPDATE mc SET movie_id = 7, note = 'x' WHERE id = 1"
    )  # two columns
    mc = Column(Path(0, "mc", "mc", ()), "id")  # the WHERE's column

    assert isinstance(update, Update)  # the write's IR
    assert update.assignments == (
        Assignment(
            "movie", ReferencedTupleExpression(MOVIE, Literal(7))
        ),  # under its reference
        Assignment("note", Literal("x")),  # a plain column under its own name
    )  # in SET order
    where = BinaryExpression("==", mc, Literal(1))  # id = 1
    assert update.query.condition == where  # the rows to write


def test_parse_statement_update_reads_an_unqualified_column_of_the_written_table():
    """A SET value reads the row it replaces, qualified as a query's column is."""
    update = parsed("UPDATE title SET year = year + 1")  # no WHERE
    year = Column(Path(0, "title", "title", ()), "year")  # title.year
    (assignment,) = update.assignments  # the one SET

    assert assignment == Assignment(
        "year", BinaryExpression("+", year, Literal(1))
    )  # read + 1
    assert update.query.condition is None  # every row


def test_parse_statement_update_reads_an_unqualified_column_of_an_untyped_table():
    """Without declared SQL columns an unqualified column is read as the table's own."""
    update = parsed("UPDATE title SET year = id WHERE year = 1", UNTYPED)  # no types
    path = Path(0, "title", "title", ())  # the only alias
    (assignment,) = update.assignments  # the one SET

    assert assignment == Assignment("year", Column(path, "id"))  # SET reads title.id
    where = BinaryExpression("==", Column(path, "year"), Literal(1))  # year = 1
    assert update.query.condition == where  # the WHERE reads title.year


def test_parse_statement_update_reads_every_attribute_of_the_rows_it_writes():
    """The SET values are no outputs of the query that reads the rows to write."""
    update = parsed("UPDATE title SET year = year + 1")  # a computed value

    assert update.query.outputs == []  # the tuples are read whole
    assert not update.query.computed_columns.by_expression  # SET's value is no column


def test_parse_statement_update_keeps_a_parenthesized_value():
    """Parentheses do not stop the type check, and they stay on the stored value."""
    update = parsed("UPDATE title SET year = (2)")  # an int into an INT
    (assignment,) = update.assignments  # the one SET

    assert assignment == Assignment(
        "year", ParenExpression(Literal(2))
    )  # stored as written


def test_parse_statement_update_of_a_table_without_a_key_assigns_any_column():
    """Without a declared key no column is protected from being written."""
    update = parsed("UPDATE nokey SET c = 1", UNTYPED)  # no primary key declared

    assert update.assignments == (
        Assignment("c", Literal(1)),
    )  # written as any other column

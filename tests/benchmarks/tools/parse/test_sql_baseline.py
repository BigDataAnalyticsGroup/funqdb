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

"""Which SQL the allow-list admits before the optimizer: ISO SQL-92, what the JOB and
TPC-H queries add, and the extensions accepted by decision."""

from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlglot
from sqlglot.optimizer.optimizer import optimize

from benchmarks.datasets import LoadedDataset
from benchmarks.datasets.tpch.tpch_dataset import TPCH
from benchmarks.job.job_queries import job_query_files
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.parse.sql_baseline import reject_non_baseline_sql
from benchmarks.tools.sql.optimization import OPTIMIZER_DIALECT, OPTIMIZER_RULES
from tests.benchmarks.datasets.tpch.extension import open_or_skip

# the one TPC-H query DuckDB ships that the transpiler stops at, and why
TPCH_STOPPED_QUERIES = {13: Limitation.ALIAS_COLUMN_LIST}


def check(sql: str, dialect: str | None = None) -> None:
    """Checks every statement of `sql`, read in `dialect`, against the baseline."""
    for statement in sqlglot.parse(sql, read=dialect):  # `sql` may hold several
        reject_non_baseline_sql(statement)  # raises for SQL outside the baseline


def rejection(sql: str, dialect: str | None = None) -> Limitation:
    """The limitation the baseline check raises for `sql`."""
    with pytest.raises(UnsupportedSql) as raised:  # the baseline must refuse `sql`
        check(sql, dialect)  # raises the rejection under test
    return raised.value.limitation  # the catalog entry of the rejection


@pytest.fixture(scope="module")
def tpch() -> Iterator[LoadedDataset]:
    """The TPC-H dataset; skipped where DuckDB's extension is unavailable."""
    dataset = open_or_skip(TPCH)  # skips outside CI without the tpch extension
    yield dataset  # its queries and SQL columns
    dataset.close()  # frees the DuckDB and FDM databases


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT t.id FROM other.title AS t",  # in FROM
        "SELECT t.id FROM db.other.title AS t",  # with a catalog
        "SELECT t.id FROM title AS t JOIN other.note AS n ON n.id = t.id",  # in a JOIN
        "SELECT zzz.title.id FROM title",  # a column read with its schema
        "DELETE FROM title WHERE sch.title.id = 1",  # the same in a WHERE
        "DROP TABLE other.t",  # dropped
        "CREATE TABLE other.g (id INT PRIMARY KEY)",  # created
        "INSERT INTO other.title (id) VALUES (1)",  # written
        "UPDATE other.title SET year = 1",  # written
        "DELETE FROM other.title",  # written
    ],
)
def test_reject_non_baseline_sql_rejects_a_name_with_a_schema(sql: str):
    """A dataset has no schemas, so the qualifier would be ignored and read other data."""
    assert rejection(sql) is Limitation.SCHEMA_QUALIFIED_TABLE  # before any other stage


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT t.a FROM title AS t(a, b)",  # on a table
        "SELECT t.id FROM title AS t JOIN note AS n(a) ON n.a = t.id",  # in a JOIN
        "WITH c(a) AS (SELECT * FROM title) SELECT c.a FROM c",  # on a CTE
        "SELECT t.a FROM (SELECT m.x FROM m) AS t(a)",  # on a derived table
        "WITH c AS (SELECT m.x FROM m) SELECT t.a FROM c AS t(a)",  # on a CTE's reader
    ],
)
def test_reject_non_baseline_sql_rejects_an_alias_with_a_column_list(sql: str):
    """It names columns by position, which is open work; the optimizer would resolve
    some of these away, so the check stops them first."""
    assert rejection(sql) is Limitation.ALIAS_COLUMN_LIST  # before the optimizer runs


@pytest.mark.parametrize(
    "sql",
    [
        "ALTER TABLE t ADD COLUMN c INT",  # a tuple function has no schema to alter
        "GRANT SELECT ON t TO u",  # no privileges
        "REVOKE SELECT ON t FROM u",  # no privileges
        "COMMIT",  # no transactions
        "ROLLBACK",  # no transactions
    ],
)
def test_reject_non_baseline_sql_rejects_a_sql92_statement_without_fdm_counterpart(
    sql: str,
):
    """These SQL-92 statements pass as not dialect-specific; their contents are not checked."""
    limitation = rejection(sql)  # a statement kind the transpiler never translates

    assert limitation is Limitation.UNSUPPORTED_STATEMENT  # rejected as a whole


@pytest.mark.parametrize(
    ("sql", "dialect"),
    [
        ("SELECT t.a FROM t ASOF JOIN u ON t.a >= u.a", None),  # DuckDB
        ("SELECT t.a FROM t ANTI JOIN u ON t.a = u.a", None),  # DuckDB
        ("SELECT t.a FROM t SEMI JOIN u ON t.a = u.a", None),  # DuckDB
        ("SELECT t.a FROM t OUTER JOIN u ON t.a = u.a", None),  # OUTER without side
        ("SELECT a FROM t UNION BY NAME SELECT a FROM u", None),  # DuckDB
        ("SELECT a FROM t INTERSECT BY NAME SELECT a FROM u", None),  # its own check
        ("SELECT a FROM t EXCEPT BY NAME SELECT a FROM u", None),  # its own check
        ("SELECT a FROM t WHERE a ILIKE 'x%'", None),  # PostgreSQL
        ("SELECT a FROM t WHERE a GLOB 'x*'", None),  # SQLite
        ("SELECT a FROM t ORDER BY ALL", None),  # DuckDB; a column named ALL
        ("TRUNCATE TABLE t RESTART IDENTITY", None),  # SQL:2008, PostgreSQL
        ("SELECT * REPLACE (1 AS a) FROM t", None),  # DuckDB
        ("SELECT MEDIAN(a) AS m FROM t", None),  # DuckDB, Oracle
        ("CREATE TABLE g (id INT PRIMARY KEY, label TEXT)", None),  # TEXT
        ("CREATE TABLE g (id BIGINT PRIMARY KEY)", None),  # SQL:2003
        ("INSERT INTO t (a) VALUES (1) RETURNING a", None),  # PostgreSQL
        ("UPDATE t SET a = 1 RETURNING a", None),  # PostgreSQL
        ("DELETE FROM t RETURNING a", None),  # PostgreSQL
        ("WITH x AS (SELECT 1) INSERT INTO t (a) VALUES (1)", None),  # WITH on DML
        ("WITH x AS (SELECT 1) UPDATE t SET a = 1", None),  # WITH on DML
        ("WITH x AS (SELECT 1) DELETE FROM t", None),  # WITH on DML
        ("SELECT a FROM t FETCH FIRST 3 ROWS ONLY", None),  # SQL:2008
        ("SELECT a FROM t ORDER BY a LIMIT 3 WITH TIES", None),  # LIMIT option
        ("SELECT DISTINCT ON (a) a FROM t", None),  # PostgreSQL
        ("SELECT a FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY a) = 1", None),
        ("SELECT ROW_NUMBER() OVER (ORDER BY a) AS r FROM t", None),  # SQL:2003
        ("SELECT a FROM t GROUP BY ROLLUP (a)", None),  # SQL:1999
        ("WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r", None),  # SQL:1999
        ("SELECT d + INTERVAL '2' WEEK AS w FROM t", None),  # no SQL-92 unit
        ("SELECT d + INTERVAL '2' MONTHS AS w FROM t", None),  # plural
        ("SELECT ABS(a) AS b, a % 2 AS c FROM t", None),  # SQL:1999 / dialects
        ("SELECT a FROM t WHERE a BETWEEN SYMMETRIC 1 AND 2", None),  # SQL:1999
        ("INSERT IGNORE INTO t (a) VALUES (1)", "mysql"),  # MySQL
        ("INSERT OVERWRITE TABLE t SELECT * FROM t", "hive"),  # Hive
        ("UPDATE t SET a = 1 ORDER BY b", "mysql"),  # MySQL
        ("DELETE FROM t LIMIT 1", "mysql"),  # MySQL
        ("DROP TEMPORARY TABLE t", "mysql"),  # MySQL
        ("CREATE INDEX i ON t (a)", None),  # no SQL-92 index
        ("BEGIN", None),  # SQL-92 starts transactions implicitly
        ("CREATE LOCAL TEMPORARY TABLE g (id INT PRIMARY KEY)", None),  # a command
        ("CREATE TEMPORARY TABLE g (id INT PRIMARY KEY)", None),  # not GLOBAL TEMPORARY
        ("CREATE TABLE g AS SELECT a FROM t", None),  # only a view has a query
        ("PRAGMA table_info(t)", None),  # SQLite
        ("SELECT t.a FROM u JOIN t TABLESAMPLE (2 ROWS) ON u.a = t.a", None),  # join
        ("SELECT t.a FROM t USING SAMPLE 2", None),  # DuckDB's spelling
        ("DROP INDEX i", None),  # a DROP kind outside SQL-92
        ("DROP TABLE a, b", None),  # SQL-92 drops one object per statement
        ("CREATE TABLE g (a INT REFERENCES u (x) MATCH SIMPLE)", None),  # SQL:1999
        ("SELECT a FROM t ORDER BY a LIMIT 2.5", None),  # DuckDB rounds the count
        ("SELECT a FROM t ORDER BY a LIMIT 2 OFFSET 1.5", None),  # DuckDB rounds
        ("SELECT a FROM t ORDER BY a LIMIT -1", None),  # SQLite reads it as no limit
        ("SELECT a FROM t ORDER BY a LIMIT NULL", None),  # DuckDB reads it as no limit
        ("SELECT a FROM t ORDER BY a LIMIT '3'", None),  # DuckDB converts the text
        ("SELECT a FROM t ORDER BY a LIMIT 2 OFFSET -1", None),  # negative OFFSET
        ("SELECT a FROM t ORDER BY a LIMIT (-1)", None),  # the same in parentheses
        ("SELECT a FROM t ORDER BY a LIMIT -(1)", None),  # the sign outside them
        ("SELECT EXTRACT(QUARTER FROM d) AS q FROM t", None),  # no SQL-92 field
        ("SELECT SUBSTRING(a, -2) AS s FROM t", None),  # DuckDB counts from the end
        ("SELECT SUBSTRING(a, 1, -1) AS s FROM t", None),  # DuckDB reads it as empty
    ],
)
def test_reject_non_baseline_sql_rejects_sql_outside_the_baseline_as_dialect_specific(
    sql: str, dialect: str | None
):
    """Anything neither SQL-92 nor used by JOB / TPC-H nor accepted by decision is
    rejected as dialect-specific, however sqlglot represents it."""
    limitation = rejection(sql, dialect)  # read in a dialect that can spell it

    assert limitation is Limitation.DIALECT_SPECIFIC  # rejected, not open work


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT a FROM t ORDER BY a NULLS LAST",  # sqlglot's default dialect: first
        "SELECT a FROM t ORDER BY a DESC NULLS FIRST",  # DESC: last there
        "SELECT DATE '1995-03-01T10:00' AS d FROM t",  # ISO 8601's T
        "SELECT TIMESTAMP '1995-03-01' AS d FROM t",  # no time of day
        "SELECT d + INTERVAL (1 + 1) DAY AS e FROM t",  # a count that is no string
        "SELECT FOO(a) AS f FROM t",  # an unknown function
        "SELECT OCTET_LENGTH(a, b) AS o FROM t",  # a known one, two arguments
        "SELECT d + INTERVAL '1.5' DAY AS e FROM t",  # a fraction, not of SECOND
        "SELECT d + INTERVAL 'x' DAY AS e FROM t",  # no count at all
        "SELECT a FROM t GROUP BY ()",  # SQL:1999's empty grouping set
        "SELECT t.a FROM t CROSS JOIN u ON t.a = u.a",  # CROSS takes no condition
        "SELECT t.a FROM t NATURAL JOIN u USING (a)",  # NATURAL takes none either
        "SELECT t.a FROM t INNER JOIN u",  # INNER needs ON or USING
        "SELECT t.a FROM t LEFT JOIN u",  # so does an outer join
        "SELECT d + INTERVAL '1' AS e FROM t",  # no unit
        "SELECT COUNT(DISTINCT a, b) AS c FROM t",  # two expressions
        "SELECT DISTINCT a FROM t ORDER BY b",  # the key is not selected
        "SELECT DISTINCT a AS x FROM t ORDER BY a + 1",  # nor an item
        "SELECT DISTINCT a FROM t ORDER BY t.a",  # another spelling of the item
        "UPDATE t AS x SET a = 1",  # SQL-92 names a written table without alias
        "INSERT INTO t AS x (a) VALUES (1)",  # the same for INSERT's target
    ],
)
def test_reject_non_baseline_sql_rejects_a_foreign_value_in_a_baseline_node(sql: str):
    """Arguments that name a choice are checked too: a NULL ordering away from the
    default, datetime text not in SQL-92's format, an INTERVAL count that is no
    string literal, an unknown function name."""
    limitation = rejection(sql)  # a baseline node holding a foreign value

    assert limitation is Limitation.DIALECT_SPECIFIC  # rejected


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT a FROM t WHERE a IN (SELECT b FROM u ORDER BY b)",  # ORDER BY
        "SELECT a FROM t WHERE EXISTS (SELECT b FROM u ORDER BY b LIMIT 1)",  # LIMIT
        "SELECT d.b FROM (SELECT b FROM u ORDER BY b) AS d",  # in a derived table
        "WITH x AS (SELECT b FROM u ORDER BY b LIMIT 1) SELECT x.b FROM x",  # in a CTE
        "SELECT a FROM t WHERE a IN (WITH x AS (SELECT 1 AS b) SELECT b FROM x)",
        "SELECT a FROM t UNION (SELECT a FROM u ORDER BY a LIMIT 2)",  # set operand
        "INSERT INTO t (a) SELECT b FROM u ORDER BY b",  # INSERT's query
    ],
)
def test_reject_non_baseline_sql_rejects_order_limit_and_with_below_the_statement(
    sql: str,
):
    """ORDER BY, LIMIT and WITH below the statement root are dialect-specific."""
    limitation = rejection(sql)  # the clause sits below the statement

    assert limitation is Limitation.DIALECT_SPECIFIC  # not in the baseline


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT a FROM t ORDER BY a LIMIT 1, 2",  # the AST of LIMIT 2 OFFSET 1
        "SELECT TRUE AS y, CAST(a AS BOOLEAN) AS b FROM t WHERE a IS NOT FALSE",
        "SELECT a::INT AS b FROM t",  # read as CAST
        "SELECT d + INTERVAL 3 DAY AS e FROM t",  # read as INTERVAL '3' DAY
        "SELECT CAST(a AS DOUBLE) AS d, CAST(a AS INT4) AS i FROM t",  # SQL-92 types
        "SELECT a FROM t ORDER BY a NULLS FIRST, b DESC NULLS LAST",  # the defaults
        "SELECT a FROM t ORDER BY a LIMIT 0 OFFSET 0",  # zero counts no rows
        "SELECT LENGTH(a) AS n FROM t",  # read as CHAR_LENGTH
        "SELECT TRIM(a) AS r FROM t",  # no position set, so no value to check
    ],
)
def test_reject_non_baseline_sql_admits_extensions_and_spellings_of_baseline_nodes(
    sql: str,
):
    """Accepted extensions and every spelling that parses into a baseline node pass."""
    check(sql)  # raises for anything outside the baseline


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT t.a FROM t LEFT OUTER JOIN u ON t.a = u.a RIGHT JOIN v ON t.a = v.a",
        "SELECT t.a FROM t FULL JOIN u ON t.a = u.a CROSS JOIN v",
        "SELECT t.a FROM t NATURAL JOIN u NATURAL LEFT JOIN v",  # NATURAL, plain and sided
        "SELECT EXTRACT(YEAR FROM d) AS y FROM t",  # a SQL-92 field, as TPC-H uses
        "SELECT a FROM t UNION ALL SELECT a FROM u EXCEPT SELECT a FROM v",
        "SELECT a FROM t INTERSECT SELECT a FROM u ORDER BY a",  # ORDER BY on the root
        "SELECT a FROM t UNION CORRESPONDING SELECT a FROM u",  # SQL-92's by-name
        "SELECT a FROM t UNION CORRESPONDING BY (a) SELECT a FROM u",
        "SELECT COALESCE(a, b) AS c, NULLIF(a, b) AS n FROM t",  # SQL-92 functions
        "SELECT UPPER(a) AS u, LOWER(a) AS l, TRIM(LEADING 'x' FROM a) AS r FROM t",
        "SELECT CHAR_LENGTH(a) AS c, OCTET_LENGTH(a) AS o, POSITION('x' IN a) AS p FROM t",
        "SELECT CURRENT_DATE AS d, CURRENT_TIME AS t, CURRENT_TIMESTAMP AS s FROM t",
        "SELECT CURRENT_USER AS u, SESSION_USER AS s FROM t",
        "SELECT a FROM t WHERE a = ANY (SELECT b FROM u) AND a > ALL (SELECT b FROM u)",
        "SELECT a FROM t WHERE (a, b) = (1, 2) AND a LIKE 'x!%' ESCAPE '!'",
        "SELECT a COLLATE x AS c FROM t",  # a collation name
        "SELECT N'x' AS n FROM t",  # a national character literal
        "SELECT CAST(a AS NUMERIC(3, 1)) AS n, CAST(a AS CHARACTER VARYING(3)) AS v FROM t",
        "SELECT CAST(a AS TIME) AS m, CAST(a AS TIMESTAMP WITH TIME ZONE) AS z FROM t",
        "SELECT DATE '1995-3-1' AS d, TIMESTAMP '1995-03-01 10:00:00.5' AS s FROM t",
        "SELECT d + INTERVAL '-3' DAY AS e, d + INTERVAL '1.5' SECOND AS f FROM t",
        "SELECT SUBSTRING(a FROM -b) AS n FROM t",  # a negated column, no literal
        "SELECT CAST(a AS INTERVAL DAY) AS i FROM t",  # an interval type has no count
        "SELECT CAST(a AS INTERVAL DAY TO HOUR) AS i FROM t",  # a span of them
        "SELECT d + INTERVAL '1' DAY TO HOUR AS e FROM t",  # a span with a count
        "SELECT DISTINCT a FROM t",  # no ORDER BY to check
        "SELECT a FROM t ORDER BY b",  # no DISTINCT to check it against
        "SELECT DISTINCT a AS x FROM t ORDER BY x",  # by the selected name
        "SELECT DISTINCT t.a FROM t ORDER BY t.a",  # by the selected item
        "SELECT DISTINCT a + 1 AS x FROM t ORDER BY a + 1",  # by a computed item
        "INSERT INTO t (a, b) VALUES (1, DEFAULT), (2, NULL)",  # DEFAULT in VALUES
        "INSERT INTO t DEFAULT VALUES",  # SQL-92's empty row
        "CREATE SCHEMA s",  # SQL-92, rejected later
        "CREATE TABLE g (a INT, b INT, PRIMARY KEY (a, b))",  # a table-level key
        "CREATE VIEW v AS SELECT a FROM t",  # a view is defined by its query
        "CREATE GLOBAL TEMPORARY TABLE g (id INT PRIMARY KEY)",  # SQL-92's only kind
        "CREATE TABLE g (a INT CONSTRAINT pk PRIMARY KEY, b VARCHAR(3) UNIQUE NOT NULL,"
        " c INT DEFAULT 1 REFERENCES u (x) ON DELETE CASCADE, CHECK (a > 0),"
        " CONSTRAINT f FOREIGN KEY (c) REFERENCES u ON UPDATE SET NULL, UNIQUE (b, c))",
        "DROP VIEW v RESTRICT",
        'SELECT "all".a FROM t AS "all" ORDER BY "all".a',  # quoted keyword
    ],
)
def test_reject_non_baseline_sql_admits_sql92(sql: str):
    """SQL-92 passes, whether translated or not: untranslated SQL-92 is rejected
    later, for its specific limitation."""
    check(sql)  # raises for anything outside the baseline


def test_reject_non_baseline_sql_admits_a_keyword_as_a_name_only_as_the_corpora_or_set_default_use_it():
    """Unquoted sqlglot keywords pass as names only as JOB / TPC-H use them, or as
    SQL-92's `SET c = DEFAULT`; elsewhere DEFAULT is a keyword misread as a name."""
    check("SELECT t.title AS character FROM title AS t")  # JOB's corpus name
    check("UPDATE t SET a = DEFAULT")  # SQL-92's DEFAULT, read as a column

    limitation = rejection("SELECT DEFAULT AS d FROM t")  # DEFAULT as a value
    assert limitation is Limitation.DIALECT_SPECIFIC  # no SQL-92 slot for it


@pytest.mark.parametrize("path", job_query_files(), ids=lambda path: path.stem)
def test_reject_non_baseline_sql_admits_each_job_query(path: Path):
    """The baseline promises to admit every JOB query, and the optimizer to take it."""
    sql = path.read_text(encoding="utf-8")  # one SELECT per file
    ast = sqlglot.parse_one(sql)  # as the pipeline reads it

    reject_non_baseline_sql(ast)  # raises for anything outside the baseline
    optimize(
        ast, dialect=OPTIMIZER_DIALECT, rules=OPTIMIZER_RULES
    )  # raises if a kept rule or the type checks refuse it


@pytest.mark.parametrize("number", range(1, 23))
def test_reject_non_baseline_sql_admits_each_tpch_query_but_q13(
    tpch: LoadedDataset, number: int
):
    """Each TPC-H query passes the baseline and the optimizer, but Q13 stops at open work."""
    found = tpch.execute_sql(
        f"SELECT query FROM tpch_queries() WHERE query_nr = {number}"
    )  # DuckDB's text of the query, as one row
    [(sql,)] = found.rows  # exactly one query per number
    ast = sqlglot.parse_one(sql)  # one SELECT
    stopped_by = TPCH_STOPPED_QUERIES.get(number)  # None for an admitted query

    if stopped_by is not None:  # Q13 renames its derived table's columns by position
        assert rejection(sql) is stopped_by  # the catalogued reason
        return  # nothing to optimize
    reject_non_baseline_sql(ast)  # raises for anything outside the baseline
    schema = tpch.schema()  # the SQL columns of the TPC-H tables
    optimize(
        ast,
        schema=schema.columns,
        dialect=OPTIMIZER_DIALECT,
        rules=OPTIMIZER_RULES,
    )  # the unqualified columns resolve against the tables; raises if refused

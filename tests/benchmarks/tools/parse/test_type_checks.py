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

"""What SQL's types make FQL read otherwise, checked on a statement as written."""

import pytest
import sqlglot
from sqlglot import Expr
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify

from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.parse.type_checks import has_other_type, reject_ill_typed_sql
from benchmarks.tools.sql.optimization import OPTIMIZER_DIALECT

# one column per type family the checks tell apart:
#
#   t
#   +----------------------+
#   | id     INT           |
#   | ratio  DOUBLE        |
#   | price  DECIMAL(10,2) |
#   | name   VARCHAR       |
#   | made   DATE          |
#   | shown  TIMESTAMP     |
#   | clock  TIME          |
#   | zoned  TIMESTAMPTZ   |
#   | ok     BOOLEAN       |
#   +----------------------+
COLUMNS = {
    "t": {
        "id": "INT",  # a number
        "ratio": "DOUBLE",  # a float, which Python compares exactly with a Decimal
        "price": "DECIMAL(10, 2)",  # a Decimal, the same the other way round
        "name": "VARCHAR",  # text
        "made": "DATE",  # a date
        "shown": "TIMESTAMP",  # a datetime
        "clock": "TIME",  # a time, its own family
        "zoned": "TIMESTAMPTZ",  # a zoned datetime, its own family
        "ok": "BOOLEAN",  # a truth value
    }
}


def typed(sql: str, columns: dict[str, dict[str, str]] = COLUMNS) -> Expr:
    """`sql` qualified and typed, as the optimizer rule hands it to the checks."""
    ast = sqlglot.parse_one(sql)  # as written
    qualified = qualify(
        ast, schema=columns, dialect=OPTIMIZER_DIALECT, infer_schema=True
    )  # every column knows its table
    return annotate_types(qualified, schema=columns, dialect=OPTIMIZER_DIALECT)  # typed


def selected(value: str) -> str:
    """`value` in a SELECT list."""
    return f"SELECT {value} AS x FROM t"  # one output


def filtered(condition: str) -> str:
    """`condition` in a WHERE clause."""
    return f"SELECT t.id FROM t WHERE {condition}"  # one conjunct


@pytest.mark.parametrize(
    "sql",
    [
        filtered("t.id = '1999'"),  # SQL converts the text, Python finds them unequal
        filtered("t.ok = 1"),  # a truth value against a number
        filtered("t.made = '2020-01-01'"),  # text the optimizer would read as a date
        filtered("t.made = t.shown"),  # Python never equates a date with a datetime
        filtered("t.shown = t.made"),  # the same with the sides swapped
        filtered("t.made = t.clock"),  # a date against a time
        filtered("t.shown = t.zoned"),  # a datetime against a zoned one
        filtered("t.id BETWEEN 1 AND 'x'"),  # the upper bound
        filtered("t.id BETWEEN 'x' AND 1"),  # the lower bound
        filtered("t.id IN (1, 'x')"),  # the second option
        filtered(
            "t.id IN (SELECT y.name FROM t AS y)"
        ),  # the column a subquery selects
        filtered("t.id = (SELECT MAX(y.name) FROM t AS y)"),  # a scalar subquery
        selected("CASE t.id WHEN 'x' THEN 1 END"),  # a base value against its WHEN
    ],
)
def test_reject_ill_typed_sql_rejects_as_dialect_specific(sql: str):
    """FQL would read these otherwise than SQL, without an error."""
    with pytest.raises(UnsupportedSql) as raised:  # the results would differ
        reject_ill_typed_sql(typed(sql))  # on the statement as written

    assert raised.value.limitation is Limitation.DIALECT_SPECIFIC  # the catalog entry


@pytest.mark.parametrize(
    "condition",
    [
        "t.ratio = 0.1",  # SQL converts the literal, Python finds 0.1 != Decimal('0.1')
        "0.1 = t.ratio",  # the literal first
        "t.ratio BETWEEN 0.1 AND 0.2",  # a bound
        "t.ratio IN (0.1)",  # an option
        "t.price = 1.1e0",  # the reverse: a float literal against a DECIMAL column
        "t.ratio = -0.1",  # sqlglot reads a minus sign in front of the literal
        "t.ratio BETWEEN -0.2 AND -0.1",  # negative bounds
        "t.ratio IN (-0.1)",  # a negative option
        "t.ratio = -(0.1)",  # the literal in parentheses under the sign
    ],
)
def test_reject_ill_typed_sql_rejects_a_decimal_against_a_float(condition: str):
    """SQL converts the literal to the column's type; Python compares the exact values."""
    with pytest.raises(UnsupportedSql) as raised:  # the row would be lost
        reject_ill_typed_sql(typed(filtered(condition)))  # valid SQL, another result

    assert (
        raised.value.limitation is Limitation.LITERAL_TYPE_COERCION
    )  # the catalog entry


@pytest.mark.parametrize(
    "value",
    [
        "CAST(t.made AS TIMESTAMP)",  # a stored date to a datetime
        "CAST(t.made AS TIMESTAMP) + INTERVAL '1' HOUR",  # FQL would keep the date
        "CAST(t.id AS DATE)",  # a number to a date
        "CAST(1 AS DATE)",  # a number literal is no text literal
    ],
)
def test_reject_ill_typed_sql_rejects_a_cast_that_converts_a_value(value: str):
    """Nothing converts a value, so only a text literal or one of the target type may pass."""
    with pytest.raises(UnsupportedSql) as raised:  # no conversion exists in FQL
        reject_ill_typed_sql(typed(selected(value)))  # the cast as the user wrote it

    assert raised.value.limitation is Limitation.TEMPORAL_CAST  # the catalog entry


@pytest.mark.parametrize(
    "value",
    [
        "INTERVAL '1' HOUR",  # alone it shifts nothing yet, a date may follow later
        "t.made + INTERVAL '1' HOUR",  # Python drops a sub-day shift of a date
        "INTERVAL '1' MINUTE + t.made",  # with the interval first
        "t.made + INTERVAL '1' HOUR * 2",  # not directly the shift
        "t.made + INTERVAL '1' MONTH / 2",  # Python drops the fraction
        "t.made + INTERVAL '1' DAY / 2",  # the same with a whole day
        "t.shown + INTERVAL '1' MONTH * 0.5",  # a fractional factor drops it too
        "0.5 * INTERVAL '1' MONTH + t.shown",  # with the factor first
        "INTERVAL '1' MONTH * t.ratio",  # a float column as the factor
    ],
)
def test_reject_ill_typed_sql_rejects_an_interval_python_shifts_otherwise(value: str):
    """relativedelta drops a fraction and a sub-day shift of a date without an error."""
    with pytest.raises(UnsupportedSql) as raised:  # the shift would be lost
        reject_ill_typed_sql(typed(selected(value)))  # the interval as written

    assert raised.value.limitation is Limitation.INEXACT_INTERVAL  # the catalog entry


@pytest.mark.parametrize(
    "sql",
    [
        filtered("t.id = 1.5"),  # an integer column reads a decimal literal exactly
        filtered("t.price = 1.5"),  # a decimal against a decimal
        filtered("t.ratio = 1e0"),  # a float against a float
        filtered("t.ratio = 1"),  # an integer literal is exact in both
        filtered("t.price > t.ratio"),  # no literal: the check reads literals only
        filtered("t.name = 'x'"),  # two kinds of text
        filtered("t.made = DATE '2020-01-01'"),  # two dates
        filtered("t.ok = TRUE"),  # two truth values
        filtered(
            "t.made + INTERVAL '1' DAY = t.made"
        ),  # a whole-day shift stays a date
        filtered("t.made IN (SELECT y.made - INTERVAL '1' DAY FROM t AS y)"),  # dates
        selected("CASE t.id WHEN 1 THEN 'a' ELSE 'b' END"),  # one family per comparison
        selected("CASE WHEN t.ok THEN 1 END"),  # no base value to compare
        selected("t.made + INTERVAL '1' MONTH"),  # a date by a whole unit
        selected("t.shown + INTERVAL '1' HOUR"),  # a datetime by a sub-day unit
        selected("INTERVAL '1' HOUR + t.shown"),  # with the interval first
        selected("INTERVAL '1' DAY * 2"),  # scaled by a whole number, nothing is lost
        selected("t.id * INTERVAL '1' MONTH"),  # an integer column, factor first
        selected("t.id * 0.5"),  # no interval: numbers multiply
        selected("t.id / 2"),  # numbers divide
        selected("INTERVAL '1' DAY TO HOUR"),  # its own limitation refuses a span
        selected("CAST(t.made AS DATE)"),  # already of that type
        selected("CAST('2020-01-01' AS DATE)"),  # the literal form of a date
    ],
)
def test_reject_ill_typed_sql_keeps_what_fql_reads_as_sql_does(sql: str):
    """Each of these has one meaning in SQL and in Python."""
    reject_ill_typed_sql(typed(sql))  # raises if a check refused it


@pytest.mark.parametrize(
    "sql",
    [
        filtered("1 = t.a"),  # a typed value against no known type
        filtered("0.1 = t.a"),  # a literal against no known type
        selected("CAST(t.a AS DATE)"),  # a cast
        selected("t.a + INTERVAL '1' HOUR"),  # a sub-day shift
        selected("INTERVAL '1' MONTH * t.a"),  # a factor of no known type
    ],
)
def test_reject_ill_typed_sql_trusts_a_value_of_unknown_type(sql: str):
    """A dataset without an SQL schema types nothing, so no type check can refuse."""
    reject_ill_typed_sql(typed(sql, {}))  # raises if a check refused an untyped value


def test_has_other_type_of_an_untyped_node_is_false():
    """Only a known type can be another one."""
    node = sqlglot.parse_one("a")  # no type annotated

    assert not has_other_type(node, sqlglot.exp.DataType.Type.INT)  # trusted


def test_has_other_type_compares_with_every_expected_type():
    """One matching type is enough."""
    node = typed(selected("t.id")).selects[0].unalias()  # an INT
    types = sqlglot.exp.DataType.Type  # the names below

    assert has_other_type(node, types.VARCHAR)  # no match
    assert not has_other_type(node, types.VARCHAR, types.INT)  # the second matches

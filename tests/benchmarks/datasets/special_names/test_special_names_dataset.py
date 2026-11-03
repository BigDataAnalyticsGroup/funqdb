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

"""The special_names dataset: names that SQL must quote."""

from collections.abc import Iterator

import duckdb
import pytest

from benchmarks.datasets import DATASETS, LoadedDataset
from benchmarks.datasets.special_names.special_names_dataset import SPECIAL_NAMES


@pytest.fixture
def special_names() -> Iterator[LoadedDataset]:
    """A private open special_names dataset, closed after the test."""
    dataset = SPECIAL_NAMES.open()  # a private copy: a write must not leak
    yield dataset  # one test writes, so each test gets its own
    dataset.close()  # the fixture owns the dataset it opened


def test_SPECIAL_NAMES_is_registered():
    """The transpiler finds the dataset by its name."""
    assert DATASETS["special_names"] is SPECIAL_NAMES  # this definition


def test_SPECIAL_NAMES_open_duplicate_key_write_fails_in_sql(special_names):
    """The reference database enforces the key on the quoted table name."""
    insert = "INSERT INTO \"order details\" VALUES (1, 'Alan', 'Enigma', 1, 40)"  # id 1

    with pytest.raises(duckdb.ConstraintException):  # id 1 exists
        special_names.execute_sql(insert)  # a duplicate key


def test_SPECIAL_NAMES_open_reads_quoted_names_on_both_sides(special_names):
    """Names with a space or a keyword hold the same rows on both sides."""
    sql = 'SELECT "first name", "class" FROM "order details" ORDER BY id'  # quoted
    fql = "dbf['order details'].project('first name', 'class')"  # by name

    sql_result = special_names.execute_sql(sql)  # DuckDB's rows
    fql_result = special_names.execute_fql(fql)  # the FDM's rows

    expected = (("Ada", 2), ("Grace", 3), ("O'Brien", 4))  # every row, in id order
    assert sql_result.rows == expected  # the quote in the value survives
    sql_result.check_matches(fql_result)  # passes: both sides hold the same rows


def test_SPECIAL_NAMES_open_reads_column_that_shadows_method(special_names):
    """Attribute access finds the FDM method `project`, a subscript the column."""
    by_subscript = "dbf['order details'][1]['project']"  # the column
    by_attribute = "dbf['order details'][1].project == 'Apollo'"  # the method

    subscript_result = special_names.execute_fql(by_subscript)  # the FDM's value
    attribute_result = special_names.execute_fql(by_attribute)  # a method is no value

    assert subscript_result.rows == (("Apollo",),)  # the column, not the method
    assert attribute_result.rows == ((False,),)  # not the column: the name is shadowed


def test_SPECIAL_NAMES_open_reads_column_with_quote_in_its_name(special_names):
    """A name with a quote, rendered with `repr`, reads the same rows on both sides."""
    column = "it's"  # would end an FQL string built by string formatting
    sql = f'SELECT "{column}" FROM "order details" ORDER BY id'  # SQL's quotes
    fql = f"dbf['order details'].project({column!r})"  # `repr` escapes the quote

    sql_result = special_names.execute_sql(sql)  # DuckDB's rows
    fql_result = special_names.execute_fql(fql)  # the FDM's rows

    assert sql_result.rows == ((10,), (20,), (30,))  # the values of the row inserts
    sql_result.check_matches(fql_result)  # passes: both sides hold the same rows


def test_SPECIAL_NAMES_open_keeps_values_that_need_escaping(special_names):
    """A backslash and a line break arrive unchanged on both sides."""
    sql = 'SELECT project FROM "order details" WHERE id = 3'  # DuckDB's value
    fql = "dbf['order details'][3]['project']"  # the FDM's value

    sql_result = special_names.execute_sql(sql)  # one row
    fql_result = special_names.execute_fql(fql)  # one scalar

    assert sql_result.rows == (("back\\slash\nline",),)  # unchanged in DuckDB
    sql_result.check_matches(fql_result)  # the same value in the FDM

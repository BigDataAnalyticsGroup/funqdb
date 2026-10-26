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

"""The hand-written movies dataset: the same rows in DuckDB and in the FDM."""

from collections.abc import Iterator

import duckdb
import pytest

from benchmarks.datasets import LoadedDataset
from benchmarks.datasets.imdb.schema import IMDB_REFERENCES
from benchmarks.datasets.movies.movies_data import MOVIES_DATA
from benchmarks.datasets.movies.movies_dataset import MOVIES, _table_columns


@pytest.fixture(scope="module")
def movies() -> Iterator[LoadedDataset]:
    """This module's own read-only movies dataset."""
    dataset = MOVIES.open()  # a private copy, not the app's instance
    yield dataset  # shared read-only by the tests below
    dataset.close()  # the module owns the dataset it opened


def test_MOVIES_open_duplicate_key_write_fails_in_sql_and_overwrites_in_fdm():
    """A duplicate key fails in DuckDB and replaces the tuple in the FDM."""
    dataset = MOVIES.open()  # a private copy: the write must not leak
    sql = "INSERT INTO keyword (id, keyword) VALUES (1, 'noir')"  # id 1 exists
    values = "{'id': 1, 'keyword': 'noir', 'phonetic_code': None}"  # under id 1
    tuple_ = f"TF({values}, frozen=False)"  # as the transpiler writes a tuple

    with pytest.raises(duckdb.ConstraintException):  # refused by the key
        dataset.execute_sql(sql)  # the SQL write
    dataset.execute_fql(f"dbf.keyword[1] = {tuple_}")  # the FQL write
    keyword = dataset.execute_fql("dbf.keyword[1].keyword")  # what id 1 holds now
    dataset.close()  # the test owns this dataset

    assert keyword.rows == (("noir",),)  # the FDM replaced the tuple under id 1


def test_MOVIES_open_holds_every_row_on_both_sides(movies):
    """Both sides hold every table, each with as many rows as its data."""
    information_schema = "SELECT table_name FROM information_schema.tables"  # SQL
    every_relation = f"set(dbf.keys()) == {set(MOVIES_DATA)!r}"  # FQL, a bool

    sql_tables = movies.execute_sql(information_schema).rows  # DuckDB's tables
    fql_tables = movies.execute_fql(every_relation).rows  # the FDM's relations

    assert {row[0] for row in sql_tables} == set(MOVIES_DATA)  # DuckDB: every table
    assert fql_tables == ((True,),)  # FDM: every table
    for table, rows in MOVIES_DATA.items():  # one table at a time
        count = f'SELECT COUNT(*) FROM "{table}"'  # the SQL count
        sql_count = movies.execute_sql(count).rows  # DuckDB's
        fql_count = movies.execute_fql(f"len(dbf[{table!r}])").rows  # the FDM's
        assert sql_count == ((len(rows),),), table  # DuckDB holds every row
        assert fql_count == ((len(rows),),), table  # and so does the FDM


def test_MOVIES_open_gives_both_sides_the_same_columns(movies):
    """The SQL columns equal the FDM attributes, less the NULL columns."""
    for table, rows in MOVIES_DATA.items():  # one table at a time
        first_row = rows[0]  # its NULL columns decide the FDM attributes
        source_columns = set(first_row)  # the columns the data declares
        references = IMDB_REFERENCES.get(table, {})  # movies uses the IMDB FKs
        absent = {
            foreign_key.reference_name
            for fk, foreign_key in references.items()
            if first_row[fk] is None
        }  # a NULL fk swizzles into no attribute
        nulls = {column for column, value in first_row.items() if value is None}
        absent |= nulls  # the FDM has no NULL: a NULL column is no attribute either
        sql = f'SELECT * FROM "{table}" LIMIT 0'  # the columns, without rows
        attributes = f"set(dbf[{table!r}][{first_row['id']!r}].keys())"  # FQL side
        expected = source_columns - absent  # every other column survives
        same_attributes = f"{attributes} == {expected!r}"  # FQL, a bool

        sql_columns = movies.execute_sql(sql).columns  # DuckDB's columns
        fql_same = movies.execute_fql(same_attributes).rows  # the FDM tuple's

        assert set(sql_columns) == source_columns, table  # DuckDB has every column
        assert fql_same == ((True,),), table  # the FDM tuple of that row


def test_MOVIES_open_swizzles_foreign_keys_into_references(movies):
    """Every non-NULL FK is the referenced tuple; a NULL FK is no attribute."""
    for table, mapping in IMDB_REFERENCES.items():  # movies' FKs: the IMDB schema
        for row in MOVIES_DATA[table]:  # every row of the table
            tf = f"dbf[{table!r}][{row['id']!r}]"  # that row's FDM tuple
            for fk_column, foreign_key in mapping.items():  # each FK
                reference = foreign_key.reference_name  # the swizzled attribute
                target = foreign_key.target_table  # the referenced relation
                if row[fk_column] is None:  # a NULL key references nothing
                    absent = f"{reference!r} not in {tf}"  # FQL: no attribute
                    message = f"{table} {row['id']}: {reference} stays"
                    assert movies.execute_fql(absent).rows == ((True,),), message
                    continue  # nothing to resolve
                referenced = f"dbf[{target!r}][{row[fk_column]!r}]"  # target tuple
                same = f"{tf}[{reference!r}] is {referenced}"  # FQL: identity
                message = f"{table} {row['id']}: {reference} is not the target"
                assert movies.execute_fql(same).rows == ((True,),), message


def test_table_columns_rows_with_other_columns_raise_ValueError():
    """Rows that do not share one column set cannot be inserted together."""
    rows = [{"id": 1}, {"id": 2, "extra": 3}]  # the second row has another key set

    with pytest.raises(ValueError, match="Inconsistent columns in 't'"):  # named
        _table_columns("t", rows)  # refuses

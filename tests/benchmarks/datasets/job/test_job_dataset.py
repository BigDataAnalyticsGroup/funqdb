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

"""The JOB data folders, each as a dataset of its own."""

from pathlib import Path

import duckdb
import pytest

from benchmarks.datasets import DATASETS, LoadedDataset
from benchmarks.datasets.imdb.schema import IMDB_PRIMARY_KEYS, IMDB_REFERENCES
from benchmarks.datasets.job import job_dataset
from benchmarks.datasets.dataset import read_sql_schema
from benchmarks.datasets.job.job_dataset import (
    JOB_DATA_FOLDERS,
    JOB_SCHEMA_SOURCE,
    build_job_connection,
    job_dataset_name,
    job_definition,
    load_job_csvs,
    sql_path_literal,
)
from tests.benchmarks.datasets.job.imdb_folder import IMDB_ROWS

TITLE_SCHEMA = "CREATE TABLE title (id INTEGER, title VARCHAR);"  # one JOB table


def _committed_columns() -> dict[str, dict[str, str]]:
    """Per table of the committed JOB schema, its columns and SQL types."""
    connection = duckdb.connect()
    connection.execute(JOB_SCHEMA_SOURCE.read_text())
    columns = read_sql_schema(connection)
    connection.close()
    return columns


def test_build_job_connection_loads_every_csv_row(tmp_path):
    """JOB CSVs have no header row, so the first line is data too."""
    schema = tmp_path / "schema.sql"  # the schema, outside the data folder
    schema.write_text(TITLE_SCHEMA)  # one table
    folder = tmp_path / "data"  # the data folder
    folder.mkdir()  # holds the CSV only
    (folder / "title.csv").write_text("1,Nocturne\n2,Heat\n")  # no header

    connection = build_job_connection(folder, schema)  # creates and fills the table
    rows = connection.execute("SELECT * FROM title ORDER BY id").fetchall()  # all
    connection.close()  # the test owns this connection

    assert rows == [(1, "Nocturne"), (2, "Heat")]  # the first line was not dropped


def test_load_job_csvs_missing_csv_raises_FileNotFoundError(tmp_path):
    """A table of the schema without its CSV file is an incomplete folder, not an
    empty table."""
    schema = tmp_path / "schema.sql"  # two tables
    schema.write_text(TITLE_SCHEMA + " CREATE TABLE kind_type (id INTEGER);")  # 2
    (tmp_path / "title.csv").write_text("1,Nocturne\n")  # kind_type.csv is missing
    connection = duckdb.connect()  # an empty database

    with pytest.raises(FileNotFoundError, match="kind_type.csv"):  # names the file
        load_job_csvs(connection, tmp_path, schema)  # stops at the missing table
    connection.close()  # the test owns this connection


def test_load_job_csvs_leaves_tables_it_did_not_create_alone(tmp_path):
    """Only the tables that the schema creates are loaded: a table the connection
    had before needs no CSV file."""
    schema = tmp_path / "schema.sql"  # one table
    schema.write_text(TITLE_SCHEMA)  # title only
    (tmp_path / "title.csv").write_text("1,Nocturne\n")  # its CSV
    connection = duckdb.connect()  # a database that already has a table
    connection.execute("CREATE TABLE notes (id INTEGER)")  # not from the schema

    load_job_csvs(connection, tmp_path, schema)  # no notes.csv needed
    titles = connection.execute("SELECT COUNT(*) FROM title").fetchone()[0]  # rows
    connection.close()  # the test owns this connection

    assert titles == 1  # the schema's table was loaded


def test_build_job_connection_runs_the_committed_schema_not_the_folders(tmp_path):
    """Without a schema argument, the committed job/schematext.sql runs; a
    schematext.sql in the data folder is never executed."""
    columns = _committed_columns()  # the 21 tables of the committed schema
    for table, types in columns.items():  # one valid row per table
        values = ["1" if "INT" in sql_type else "x" for sql_type in types.values()]
        (tmp_path / f"{table}.csv").write_text(",".join(values) + "\n")  # no header
    (tmp_path / "schematext.sql").write_text("CREATE TABLE evil (id INTEGER);")

    connection = build_job_connection(tmp_path)  # the default schema
    tables = read_sql_schema(connection)  # what was created
    connection.close()  # the test owns this connection

    assert set(tables) == set(columns)  # the committed tables
    assert "evil" not in tables  # the folder's file did not run


def test_build_job_connection_failed_load_closes_connection(tmp_path, monkeypatch):
    """A folder that cannot be loaded does not leave its connection open."""
    schema = tmp_path / "schema.sql"  # one table
    schema.write_text(TITLE_SCHEMA)  # whose CSV is missing
    opened = []  # the connections that build_job_connection opens
    connect = duckdb.connect  # the real function

    def recorded_connect() -> duckdb.DuckDBPyConnection:
        """`duckdb.connect()`, remembering the connection it returns."""
        connection = connect()  # a fresh in-memory database
        opened.append(connection)  # for the check below
        return connection  # for build_job_connection to use

    monkeypatch.setattr(job_dataset.duckdb, "connect", recorded_connect)  # record

    with pytest.raises(FileNotFoundError):  # title.csv is missing
        build_job_connection(tmp_path, schema)  # fails while loading

    with pytest.raises(duckdb.ConnectionException):  # the connection is closed
        opened[0].execute("SELECT 1")  # unusable after the failed load


def test_job_dataset_name_names_and_registers_every_folder():
    """`.data` is `job`, `.data_seeds` is `job_seeds`; both are registered."""
    names = [job_dataset_name(folder) for folder in JOB_DATA_FOLDERS]  # one each

    assert names == ["job", "job_seeds"]  # the full dump, then the seeds
    assert all(name in DATASETS for name in names)  # both registered


def test_job_definition_missing_folder_has_no_data(tmp_path):
    """A missing folder gives the IMDB schema without data, and no error."""
    definition = job_definition(tmp_path / ".data_missing")  # absent

    opened = definition.open()  # no connection to open, no rows to read

    assert definition.has_data is False  # nothing to execute on
    assert not isinstance(opened, LoadedDataset)  # a dataset without data
    assert opened.schema().primary_keys == IMDB_PRIMARY_KEYS  # the IMDB keys


@pytest.mark.parametrize("folder", list(JOB_DATA_FOLDERS))
def test_job_definition_registered_folders_use_the_IMDB_keys(folder):
    """Every registered JOB dataset declares the IMDB keys and references."""
    definition = DATASETS[job_dataset_name(folder)]  # the registered dataset

    assert definition.references == IMDB_REFERENCES  # the one IMDB FK mapping
    assert definition.primary_keys == IMDB_PRIMARY_KEYS  # and the same keys


def test_job_definition_existing_folder_opens_its_own_data(imdb_folder, imdb_schema):
    """A present folder opens every IMDB table from that folder on both sides."""
    # the folder is named .data but is not the real JOB_FULL_DATA_DIR, so it counts
    # as a generated folder with data:
    definition = job_definition(imdb_folder, imdb_schema)  # the synthetic folder
    every_table = f"set(dbf.keys()) == {set(IMDB_PRIMARY_KEYS)!r}"  # FQL, a bool

    dataset = definition.open()  # reads the CSVs into DuckDB and the FDM
    tables = dataset.execute_fql(every_table).rows  # the FDM side's relations
    titles = dataset.execute_sql("SELECT COUNT(*) FROM title").rows  # DuckDB's
    fdm_titles = dataset.execute_fql("len(dbf.title)").rows  # the FDM's
    dataset.close()  # the test owns this dataset

    assert definition.has_data is True  # the folder exists
    assert tables == ((True,),)  # all 21 IMDB tables
    assert titles == ((IMDB_ROWS,),)  # every CSV row in DuckDB
    assert fdm_titles == titles  # and the same rows in the FDM


def test_job_definition_full_dump_has_no_data_even_when_folder_exists(
    imdb_folder, monkeypatch
):
    """The full dump has dangling foreign keys, so it is schema-only."""
    monkeypatch.setattr(job_dataset, "JOB_FULL_DATA_DIR", imdb_folder)  # this is .data

    definition = job_definition(imdb_folder)  # the folder exists

    assert definition.has_data is False  # nothing to open: no 74 M row load
    assert definition.open().schema().primary_keys == IMDB_PRIMARY_KEYS  # keys remain


def test_sql_path_literal_doubles_single_quotes():
    """A quote in a path cannot end the SQL string early."""
    literal = sql_path_literal(Path("a/it's"))  # a folder name with a quote

    assert literal == "'a/it''s'"  # the quote is doubled, the literal is closed once


def test_build_job_connection_path_with_quote_loads(tmp_path):
    """A folder whose name holds a quote and a semicolon still loads its CSVs."""
    schema = tmp_path / "schema.sql"  # one table
    schema.write_text("CREATE TABLE title (id INTEGER);")  # a single column
    folder = tmp_path / "it's; DROP TABLE title"  # hostile but legal on a filesystem
    folder.mkdir()  # the data folder
    (folder / "title.csv").write_text("1\n2\n")  # no header

    connection = build_job_connection(folder, schema)  # COPY ... FROM the quoted path
    count = connection.execute("SELECT COUNT(*) FROM title").fetchone()[0]  # rows
    connection.close()  # the test owns this connection

    assert count == 2  # the path was one literal: both rows were read

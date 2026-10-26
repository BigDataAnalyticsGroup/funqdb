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
"""The real IMDB data of the Join Order Benchmark: the full dump, schema-only, and
the seeds that `setup/downscale_job_data.py` writes as a dataset of their own."""

from functools import partial
from pathlib import Path

import duckdb

from benchmarks.datasets.dataset import DatasetDefinition, read_sql_schema
from benchmarks.datasets.imdb.schema import IMDB_PRIMARY_KEYS, IMDB_REFERENCES

JOB_DATA_DIR = Path(__file__).resolve().parent
# the full dump that `setup/download_job_data.py` fetches; not in git
JOB_FULL_DATA_DIR = JOB_DATA_DIR / ".data"
# the seeds that `setup/downscale_job_data.py` writes; not in git
JOB_SEEDS_DIR = JOB_DATA_DIR / ".data_seeds"
# the schema in git, the one schema that ever runs: never one from a data folder
JOB_SCHEMA_SOURCE = JOB_DATA_DIR / "schematext.sql"
# the downloaded archive in `JOB_FULL_DATA_DIR`, deleted once it is fully extracted
JOB_ARCHIVE_NAME = "imdb.tgz"
JOB_CSV_OPTIONS = "FORMAT CSV, DELIMITER ',', HEADER FALSE, ESCAPE '\\'"


def load_job_csvs(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    schema: Path = JOB_SCHEMA_SOURCE,
) -> None:
    """Create the tables of `schema` in `connection` and copy `<table>.csv` of the
    folder `path` into each table that `schema` creates; tables that `connection`
    had before are left alone. Raises `FileNotFoundError` naming the file when a
    table has no CSV file, so a folder that lacks a table never loads as complete."""
    existing = set(read_sql_schema(connection))
    connection.execute(schema.read_text())
    for table in read_sql_schema(connection):
        if table in existing:
            continue

        csv = path / f"{table}.csv"
        if not csv.is_file():
            raise FileNotFoundError(f"{csv} is missing: {path} is incomplete")

        connection.execute(
            f'COPY "{table}" FROM {sql_path_literal(csv)} ({JOB_CSV_OPTIONS});'
        )


def sql_path_literal(path: Path) -> str:
    """`path` as an SQL string literal, its quotes escaped."""
    escaped = path.as_posix().replace("'", "''")
    return f"'{escaped}'"


def build_job_connection(
    path: Path, schema: Path = JOB_SCHEMA_SOURCE
) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB connection holding the JOB data of the folder `path`; the
    connection is closed when loading fails."""
    connection = duckdb.connect()
    try:
        load_job_csvs(connection, path, schema)
    except Exception:
        connection.close()
        raise
    return connection


def job_dataset_name(folder: str) -> str:
    """`job` for the full dump `.data`, `job_<suffix>` for `.data_<suffix>`."""
    return "job" + folder.removeprefix(".data")


def _has_written_data(path: Path) -> bool:
    """True when `path` exists and is not the full dump `JOB_FULL_DATA_DIR`. The
    downscaler gives its folder that name only once the folder passed every check."""
    return path.exists() and path != JOB_FULL_DATA_DIR


def job_definition(path: Path, schema: Path = JOB_SCHEMA_SOURCE) -> DatasetDefinition:
    """The JOB dataset on the data folder `path`, named after it: schema-only,
    rendering FQL without executing it, where that folder was never generated.
    The full dump `JOB_FULL_DATA_DIR` is always schema-only: it is the source of the
    seeds, and 93 of its `aka_title.movie_id` values refer to no `title` row.
    Whether a folder exists is decided when the definition is made, i.e. when
    `benchmarks.datasets` is imported."""
    name = job_dataset_name(path.name)
    return DatasetDefinition(
        name=name,
        references=IMDB_REFERENCES,
        primary_keys=IMDB_PRIMARY_KEYS,
        build_connection=(
            partial(build_job_connection, path, schema)
            if _has_written_data(path)
            else None
        ),
    )


# the full dump, then the seeds written from it
JOB_DATA_FOLDERS = (JOB_FULL_DATA_DIR.name, JOB_SEEDS_DIR.name)

JOB_DATASETS: tuple[DatasetDefinition, ...] = tuple(
    job_definition(JOB_DATA_DIR / folder) for folder in JOB_DATA_FOLDERS
)

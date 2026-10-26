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
"""The small hand-written `movies` dataset in the IMDB schema."""

from typing import Any

import duckdb

from benchmarks.datasets.dataset import DatasetDefinition, declare_primary_keys
from benchmarks.datasets.imdb.schema import IMDB_PRIMARY_KEYS, IMDB_REFERENCES
from benchmarks.datasets.movies.movies_data import (
    MOVIES_DATA,
    MOVIES_SCHEMA_SQL,
)


def build_movies_connection() -> duckdb.DuckDBPyConnection:
    """Create and populate `data`'s tables in a fresh in-memory DuckDB connection,
    the reference engine FQL output is compared against. It enforces the keys the
    FDM stores the rows under, so a duplicate-key write fails on both sides."""
    connection = duckdb.connect()
    connection.execute(MOVIES_SCHEMA_SQL)
    declare_primary_keys(connection, IMDB_PRIMARY_KEYS)
    for table, rows in MOVIES_DATA.items():
        columns = _table_columns(table, rows)
        placeholders = ", ".join("?" for _ in columns)
        connection.executemany(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            [tuple(row[column] for column in columns) for row in rows],
        )

    return connection


MOVIES = DatasetDefinition(
    name="movies",
    references=IMDB_REFERENCES,
    primary_keys=IMDB_PRIMARY_KEYS,
    build_connection=build_movies_connection,
)


def _table_columns(table: str, rows: list[dict[str, Any]]) -> list[str]:
    """The column order of `table`, verifying every row shares that exact key set."""
    columns = list(rows[0].keys())
    for row in rows:
        if list(row.keys()) != columns:
            raise ValueError(
                f"Inconsistent columns in '{table}': {list(row.keys())} != {columns}"
            )
    return columns

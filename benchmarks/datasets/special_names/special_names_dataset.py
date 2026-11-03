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
"""A table and columns whose names need quoting in SQL or FQL or clash with Python."""

import duckdb

from benchmarks.datasets.dataset import DatasetDefinition, declare_primary_keys
from benchmarks.datasets.dataset_types import DatasetRowData
from benchmarks.datasets.schema import PrimaryKey

SPECIAL_NAMES_TABLE = "order details"
SPECIAL_NAMES_PRIMARY_KEYS: dict[str, PrimaryKey] = {
    SPECIAL_NAMES_TABLE: PrimaryKey.of("id")
}
# `project` shadows the FDM method of that name, so FQL must read it by subscript;
# `it's` ends an FQL string that holds it unless it is escaped;
# the last row holds values that break FQL or SQL unless they are escaped
SPECIAL_NAMES_DATA: DatasetRowData = {
    SPECIAL_NAMES_TABLE: [
        {"id": 1, "first name": "Ada", "project": "Apollo", "class": 2, "it's": 10},
        {"id": 2, "first name": "Grace", "project": "Gemini", "class": 3, "it's": 20},
        {
            "id": 3,
            "first name": "O'Brien",
            "project": "back\\slash\nline",
            "class": 4,
            "it's": 30,
        },
    ]
}


def build_special_names_connection() -> duckdb.DuckDBPyConnection:
    """A fresh DuckDB connection holding `SPECIAL_NAMES_DATA`."""
    connection = duckdb.connect()
    connection.execute(
        f'CREATE TABLE "{SPECIAL_NAMES_TABLE}" '
        '(id INTEGER, "first name" TEXT, "project" TEXT, "class" INTEGER, '
        '"it\'s" INTEGER)'
    )
    declare_primary_keys(connection, SPECIAL_NAMES_PRIMARY_KEYS)
    for row in SPECIAL_NAMES_DATA[SPECIAL_NAMES_TABLE]:
        columns = ", ".join(f'"{column}"' for column in row)
        placeholders = ", ".join("?" for _ in row)
        connection.execute(
            f'INSERT INTO "{SPECIAL_NAMES_TABLE}" ({columns}) VALUES ({placeholders})',
            list(row.values()),
        )
    return connection


SPECIAL_NAMES = DatasetDefinition(
    name="special_names",
    primary_keys=SPECIAL_NAMES_PRIMARY_KEYS,
    build_connection=build_special_names_connection,
)

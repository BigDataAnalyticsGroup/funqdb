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

"""A synthetic JOB data folder: every IMDB table, but only its id and its foreign
key columns."""

from pathlib import Path

from benchmarks.datasets import Reference
from benchmarks.datasets.imdb.schema import IMDB_PRIMARY_KEYS, IMDB_REFERENCES

IMDB_ROWS = 20  # rows per table


def write_imdb_folder(folder: Path, schema: Path) -> None:
    """Write one CSV of `IMDB_ROWS` rows per IMDB table into `folder`, and the
    matching `CREATE TABLE` statements into `schema`."""
    folder.mkdir()
    tables = []
    for table in IMDB_PRIMARY_KEYS:
        foreign_keys = IMDB_REFERENCES.get(table, {})
        columns = ["id INTEGER", *(f"{column} INTEGER" for column in foreign_keys)]
        tables.append(f"CREATE TABLE {table} ({', '.join(columns)});")
        lines = [_csv_line(table, foreign_keys, row) for row in range(1, IMDB_ROWS + 1)]
        (folder / f"{table}.csv").write_text("".join(lines))

    schema.write_text("\n".join(tables))


def _csv_line(table: str, foreign_keys: dict[str, Reference], row: int) -> str:
    """One CSV line, row `row`: it references row `row` of every other table; of its
    own table, the row before it when `row` is even, else nothing (NULL)."""
    values = [str(row)]
    self_reference = str(row - 1) if row % 2 == 0 else ""
    for foreign_key in foreign_keys.values():
        referenced = foreign_key.target_table
        values.append(self_reference if referenced == table else str(row))
    return ",".join(values) + "\n"

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
"""One-time setup: the seeds of the full dump `.data` as the folder `.data_seeds`, up
to `SEED_ROWS_PER_QUERY` rows of the join of every JOB query plus every row they
reference. Run from the repository root, after `download_job_data`:

    python -m benchmarks.datasets.job.setup.downscale_job_data
"""

import re
import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb

from benchmarks.datasets.imdb.schema import IMDB_PRIMARY_KEYS, IMDB_REFERENCES
from benchmarks.datasets.job.job_dataset import (
    JOB_ARCHIVE_NAME,
    JOB_CSV_OPTIONS,
    JOB_FULL_DATA_DIR,
    JOB_SCHEMA_SOURCE,
    JOB_SEEDS_DIR,
    load_job_csvs,
    sql_path_literal,
)
from benchmarks.job import job_query_files

DATABASE_PATH = JOB_FULL_DATA_DIR / "job.duckdb"
SEED_ROWS_PER_QUERY = 3

FROM_CLAUSE = re.compile(r"\bFROM\b(.*?)(?:\bWHERE\b|;|\Z)", re.IGNORECASE | re.DOTALL)
QUOTED_LITERAL = re.compile(r"('(?:[^']|'')*')")
TABLE_ALIAS = re.compile(r"(\w+)\s+AS\s+(\w+)", re.IGNORECASE)


class GuaranteeViolated(Exception):
    """Written seeds on which a JOB query loses its rows or a foreign key dangles."""


class IncompleteDump(Exception):
    """A full dump whose extraction did not finish."""


@dataclass(frozen=True)
class TableAlias:
    """A table of a JOB query's `FROM` clause under the alias the query gives it."""

    table: str
    alias: str


def main() -> None:
    """Write the seeds of the full dump to `JOB_SEEDS_DIR`."""
    connection = _full_database()
    seeds, full_empty = _harvest_seeds(connection)
    print(f"writing {JOB_SEEDS_DIR}")
    _write_checked(connection, seeds, JOB_SEEDS_DIR, full_empty)
    connection.close()


def _full_database() -> duckdb.DuckDBPyConnection:
    """The full `.data` as a DuckDB file, loaded once and reused on later runs."""
    if not DATABASE_PATH.exists():
        _load_full_database()

    return duckdb.connect(str(DATABASE_PATH))


def _load_full_database() -> None:
    """Load the CSV files of `.data` into `DATABASE_PATH`. Raises `IncompleteDump`
    while the archive is still there: the downloader deletes it only after a
    complete extraction, and a truncated CSV must not end up in `DATABASE_PATH`."""
    archive = JOB_FULL_DATA_DIR / JOB_ARCHIVE_NAME
    if archive.exists():
        raise IncompleteDump(
            f"{archive} still exists: its extraction did not finish;"
            " run download_job_data again"
        )

    # an interrupted load leaves only the .part file, which the next run replaces,
    # so a partial database is never taken for a complete one
    partial = DATABASE_PATH.with_name(DATABASE_PATH.name + ".part")
    # DuckDB's write-ahead log of that file: a stale one would be replayed into the
    # fresh load, so it goes too
    partial_wal = partial.with_name(partial.name + ".wal")
    partial.unlink(missing_ok=True)
    partial_wal.unlink(missing_ok=True)

    print(f"loading {JOB_FULL_DATA_DIR} into {DATABASE_PATH} ...")
    connection = duckdb.connect(str(partial))
    load_job_csvs(connection, JOB_FULL_DATA_DIR, JOB_SCHEMA_SOURCE)
    connection.close()
    partial.rename(DATABASE_PATH)


def _aliased_tables(query: str) -> list[TableAlias]:
    """The tables of the `FROM` clause of `query`, in query order."""
    from_clause = FROM_CLAUSE.search(query).group(1)
    return [TableAlias(*match) for match in TABLE_ALIAS.findall(from_clause)]


def _seed_query(query: str) -> str:
    """`query` from `FROM` on, but selecting the id of every aliased table instead of
    the aggregates: the first `SEED_ROWS_PER_QUERY` rows of its join, in id order.
    Every alias is quoted, so that JOB's `aka_title AS AT` with `at.movie_id`
    survives DuckDB, where `at` is a keyword."""
    aliases = [aliased.alias for aliased in _aliased_tables(query)]
    from_start = re.search(r"\bFROM\b", query, re.IGNORECASE).start()
    body = _quote_aliases(query[from_start:], aliases)

    positions = range(len(aliases))
    projection = ", ".join(
        f'"{alias}".id AS c{i}' for i, alias in zip(positions, aliases)
    )
    order = ", ".join(f"c{position}" for position in positions)
    # no DISTINCT: the ids of all aliases already make every row of the join unique
    return (
        f"SELECT {projection} {body.rstrip().rstrip(';')}"
        f" ORDER BY {order} LIMIT {SEED_ROWS_PER_QUERY}"
    )


def _quote_aliases(body: str, aliases: list[str]) -> str:
    """`body` with every alias quoted, in any case, string literals left as they are."""
    # with a group in the pattern, split puts the literals at the odd indexes, so the
    # even parts are the text outside them
    parts = QUOTED_LITERAL.split(body)
    for index in range(0, len(parts), 2):
        for alias in aliases:
            parts[index] = re.sub(
                rf"\bAS\s+{alias}\b", f'AS "{alias}"', parts[index], flags=re.IGNORECASE
            )
            parts[index] = re.sub(
                rf"\b{alias}\.", f'"{alias}".', parts[index], flags=re.IGNORECASE
            )
    return "".join(parts)


def _id_list(ids: set[int] | list[int]) -> str:
    """`ids` as the body of an SQL `IN` list."""
    return ", ".join(map(str, ids))


def _close_seeds(
    connection: duckdb.DuckDBPyConnection, seeds: dict[str, set[int]]
) -> None:
    """Pull every row a seed row references into the seeds, until nothing grows."""
    growing = True
    while growing:
        growing = False
        for table, references in IMDB_REFERENCES.items():
            if not seeds[table]:
                continue

            identifiers = _id_list(seeds[table])
            for column, foreign_key in references.items():
                referenced = foreign_key.target_table
                targets = connection.execute(
                    f"SELECT DISTINCT {column} FROM {table}"
                    f" WHERE id IN ({identifiers}) AND {column} IS NOT NULL"
                ).fetchall()
                before = len(seeds[referenced])
                seeds[referenced].update(target for (target,) in targets)
                growing |= len(seeds[referenced]) > before


def _harvest_seeds(
    connection: duckdb.DuckDBPyConnection,
) -> tuple[dict[str, list[int]], set[str]]:
    """Per table, the sorted ids of up to `SEED_ROWS_PER_QUERY` rows of the join of
    every JOB query plus every row they reference; and the file names of the queries
    whose join finds no rows on `connection`."""
    seeds: dict[str, set[int]] = {table: set() for table in IMDB_PRIMARY_KEYS}
    empty = set()
    for path in job_query_files():
        query = path.read_text()
        tables = [aliased.table for aliased in _aliased_tables(query)]
        print(f"seeding from {path.name} ...")
        rows = connection.execute(_seed_query(query)).fetchall()
        if not rows:
            empty.add(path.name)
        for row in rows:
            for table, identifier in zip(tables, row):
                seeds[table].add(identifier)

    _close_seeds(connection, seeds)
    return {table: sorted(ids) for table, ids in seeds.items()}, empty


def _write_checked(
    connection: duckdb.DuckDBPyConnection,
    seeds: dict[str, list[int]],
    target: Path,
    full_empty: set[str],
) -> None:
    """Write the seeds to `target.part`, check them, and only then replace `target`
    with that folder: seeds that fail the check never get the name `target`, and an
    earlier `target` stays as it was until new seeds pass."""
    partial = target.with_name(target.name + ".part")
    if partial.exists():  # left by an earlier run that failed or was interrupted
        shutil.rmtree(partial)
    _write(connection, seeds, partial)
    _check_guarantees(partial, full_empty)
    if target.exists():  # the seeds of an earlier run, replaced only now
        shutil.rmtree(target)
    partial.rename(target)


def _write(
    connection: duckdb.DuckDBPyConnection, seeds: dict[str, list[int]], target: Path
) -> None:
    """Write the seed rows of every table as one CSV per table into `target`."""
    target.mkdir(parents=True)
    for table in IMDB_PRIMARY_KEYS:
        # `IN ()` is a syntax error, and `IN (NULL)` matches no row
        seeded = _id_list(seeds[table]) or "NULL"
        csv_path = sql_path_literal(target / f"{table}.csv")
        connection.execute(
            f"COPY (SELECT * FROM {table} WHERE id IN ({seeded}))"
            f" TO {csv_path} ({JOB_CSV_OPTIONS});"
        )
        print(f"  {table}: {len(seeds[table])} rows")


def _check_guarantees(target: Path, full_empty: set[str]) -> None:
    """Raise `GuaranteeViolated` unless every JOB query whose join finds rows on the
    full dump finds rows on `target`, and no foreign key of `target` dangles."""
    connection = duckdb.connect()
    load_job_csvs(connection, target, JOB_SCHEMA_SOURCE)
    empty = _empty_queries(connection)
    dangling = _dangling_references(connection)
    connection.close()

    lost = sorted(empty - full_empty)
    if lost:
        raise GuaranteeViolated(f"{target.name} loses the rows of {lost}")

    if dangling:
        raise GuaranteeViolated(f"{target.name} keeps dangling references {dangling}")


def _empty_queries(connection: duckdb.DuckDBPyConnection) -> set[str]:
    """The file names of the JOB queries whose join finds no rows on `connection`."""
    # the JOB queries are MIN() aggregates, which return one row even over an empty
    # join, so it is the seed query (the join itself) that is run
    return {
        path.name
        for path in job_query_files()
        if not connection.execute(_seed_query(path.read_text())).fetchall()
    }


def _dangling_references(connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Per `table.column`, the number of rows whose non-NULL foreign key refers to no
    row."""
    dangling = {}
    for table, references in IMDB_REFERENCES.items():
        for column, foreign_key in references.items():
            count = connection.execute(
                f"SELECT count(*) FROM {table} WHERE {column} IS NOT NULL"
                f" AND {column} NOT IN (SELECT id FROM {foreign_key.target_table})"
            ).fetchone()[0]
            if count:
                dangling[f"{table}.{column}"] = count
    return dangling


if __name__ == "__main__":
    main()

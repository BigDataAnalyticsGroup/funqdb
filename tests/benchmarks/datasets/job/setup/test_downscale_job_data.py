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

"""The one-time JOB downscaler, run without the full data."""

import csv
import re
from pathlib import Path

import duckdb
import pytest

from benchmarks.datasets.imdb.schema import IMDB_PRIMARY_KEYS, IMDB_REFERENCES
from benchmarks.datasets.job.job_dataset import JOB_SCHEMA_SOURCE, build_job_connection
from benchmarks.datasets.job.setup import downscale_job_data
from benchmarks.datasets.job.setup.downscale_job_data import (
    GuaranteeViolated,
    _aliased_tables,
    _check_guarantees,
    _close_seeds,
    _full_database,
    IncompleteDump,
    _harvest_seeds,
    _seed_query,
)
from benchmarks.job import job_queries, job_query_files
from tests.benchmarks.datasets.job.imdb_folder import IMDB_ROWS

# JOB's own spelling in 15a to 15d: the alias in upper case, its uses in lower case,
# and `at` is a keyword in DuckDB
KEYWORD_ALIAS_QUERY = "SELECT MIN(at.id) FROM aka_title AS AT WHERE at.id = 1;"
# the same spelling in a join, as in 15a
KEYWORD_ALIAS_JOIN = (
    "SELECT MIN(at.id) FROM aka_title AS AT, title AS t WHERE at.movie_id = t.id;"
)
TITLE_QUERY = (
    "SELECT MIN(t.id) AS m FROM title AS t, movie_info AS mi"
    " WHERE mi.movie_id = t.id AND t.id = 2;"
)  # one title joined with its movie_info row


def _use_synthetic_dump(monkeypatch, folder: Path, schema: Path, tmp_path: Path):
    """Point the downscaler at the synthetic dump `folder`, its `schema`, and a seeds
    folder and database under `tmp_path`."""
    monkeypatch.setattr(downscale_job_data, "JOB_FULL_DATA_DIR", folder)  # the dump
    monkeypatch.setattr(downscale_job_data, "JOB_SCHEMA_SOURCE", schema)  # its schema
    monkeypatch.setattr(
        downscale_job_data, "JOB_SEEDS_DIR", tmp_path / ".data_seeds"
    )  # the folder main() writes
    monkeypatch.setattr(
        downscale_job_data, "DATABASE_PATH", tmp_path / "job.duckdb"
    )  # the loaded dump, outside the real .data


def _row_1_of_every_table() -> dict[str, str]:
    """One query per IMDB table, each finding row 1 of it: no table stays without
    seeds, and an empty CSV file could not be loaded."""
    return {
        f"{table}.sql": f"SELECT MIN(x.id) FROM {table} AS x WHERE x.id = 1;"
        for table in IMDB_PRIMARY_KEYS
    }


def _use_queries(monkeypatch, folder: Path, queries: dict[str, str]) -> None:
    """Make `queries` (file name to SQL) the JOB queries, written into `folder`."""
    folder.mkdir()
    for name, sql in queries.items():
        (folder / name).write_text(sql)
    monkeypatch.setattr(job_queries, "JOB_QUERY_PATH", folder)


def _written_ids(folder: Path) -> dict[str, set[int]]:
    """The ids of the rows written per table of a seeds folder."""
    return {table: set(_column(folder, table, "id")) for table in IMDB_PRIMARY_KEYS}


def _check_references_kept(folder: Path) -> None:
    """Fail when a written foreign key references a row the folder did not keep."""
    kept = _written_ids(folder)
    for table, foreign_keys in IMDB_REFERENCES.items():
        for column, foreign_key in foreign_keys.items():
            values = {value for value in _column(folder, table, column) if value}
            referenced = foreign_key.target_table
            assert values <= kept[referenced], f"{folder.name}: {table}.{column}"


def _column(folder: Path, table: str, column: str) -> list[int | None]:
    """One column of a written CSV, `None` for an empty field (a NULL)."""
    position = ["id", *IMDB_REFERENCES.get(table, {})].index(column)
    with (folder / f"{table}.csv").open(newline="") as file:
        rows = list(csv.reader(file))
    return [int(row[position]) if row[position] else None for row in rows]


def test_main_writes_referentially_intact_seeds(
    imdb_folder, imdb_schema, tmp_path, monkeypatch
):
    """The seeds keep a row of every query, and every foreign key they write is kept
    too; a query that is empty on the full dump may stay empty; the folder gets its
    name only after the checks."""
    queries = _row_1_of_every_table()  # row 1 of every table
    queries["join.sql"] = TITLE_QUERY  # seeds title 2 and movie_info 2
    queries["keyword_alias.sql"] = KEYWORD_ALIAS_QUERY  # JOB's AS AT with at.
    queries["empty.sql"] = "SELECT MIN(t.id) FROM title AS t WHERE t.id = 99;"  # none
    _use_queries(monkeypatch, tmp_path / "queries", queries)  # the corpus
    _use_synthetic_dump(monkeypatch, imdb_folder, imdb_schema, tmp_path)  # the dump

    downscale_job_data.main()  # load, harvest, write, check, rename
    seeds = _written_ids(tmp_path / ".data_seeds")  # ids per table

    assert seeds["title"] == {1, 2}  # row 1, and title 2 of the join
    assert seeds["movie_info"] == {1, 2}  # row 1, and the joined row
    assert seeds["kind_type"] == {1, 2}  # referenced by both seeded titles
    assert seeds["name"] == {1}  # row 1 only
    assert not (tmp_path / ".data_seeds.part").exists()  # renamed after the checks
    _check_references_kept(tmp_path / ".data_seeds")  # no dangling foreign key

    # every query that finds rows on the dump still finds rows on the seeds:
    seeds_connection = build_job_connection(tmp_path / ".data_seeds", imdb_schema)
    for path in job_query_files():  # the corpus, as main() reads it
        if path.name == "empty.sql":  # empty on the full dump too
            continue
        seed_sql = _seed_query(path.read_text())  # its join
        rows = seeds_connection.execute(seed_sql).fetchall()  # on the seeds
        assert rows, f"{path.name} lost its rows"  # the core guarantee
    seeds_connection.close()  # the test owns this connection


def test_main_replaces_earlier_seeds_and_a_leftover_part(
    imdb_folder, imdb_schema, tmp_path, monkeypatch
):
    """A run that passes the checks replaces the seeds of an earlier run, and clears
    the .part folder that a failed run left."""
    _use_queries(monkeypatch, tmp_path / "queries", _row_1_of_every_table())
    _use_synthetic_dump(monkeypatch, imdb_folder, imdb_schema, tmp_path)  # the dump
    (tmp_path / ".data_seeds").mkdir()  # the seeds of an earlier run
    (tmp_path / ".data_seeds" / "old.csv").write_text("1\n")  # a file of them
    (tmp_path / ".data_seeds.part").mkdir()  # left by a failed run
    (tmp_path / ".data_seeds.part" / "stale.csv").write_text("1\n")  # a file of it

    downscale_job_data.main()  # writes, checks, replaces

    assert not (tmp_path / ".data_seeds" / "old.csv").exists()  # replaced
    assert (tmp_path / ".data_seeds" / "title.csv").exists()  # by the new seeds
    assert not (tmp_path / ".data_seeds.part").exists()  # nothing left over


def test_main_seeds_that_lose_a_query_raise_and_leave_no_folder(
    imdb_folder, imdb_schema, tmp_path, monkeypatch
):
    """When the written seeds fail the check, main() stops; the unchecked seeds
    never get the name `.data_seeds`, and the seeds of an earlier run stay."""
    queries = {"q.sql": "SELECT MIN(t.id) FROM title AS t WHERE t.id = 2;"}  # title 2
    _use_queries(monkeypatch, tmp_path / "queries", queries)  # the corpus
    _use_synthetic_dump(monkeypatch, imdb_folder, imdb_schema, tmp_path)  # the dump
    seeds = {table: [1] for table in IMDB_PRIMARY_KEYS}  # row 1 everywhere: no title 2
    monkeypatch.setattr(
        downscale_job_data, "_harvest_seeds", lambda connection: (seeds, set())
    )  # seeds that miss the query's row, which the full dump has
    (tmp_path / ".data_seeds").mkdir()  # the checked seeds of an earlier run
    (tmp_path / ".data_seeds" / "old.csv").write_text("1\n")  # a file of them

    with pytest.raises(GuaranteeViolated, match="q.sql"):  # the lost query is named
        downscale_job_data.main()  # writes, checks, stops

    assert (tmp_path / ".data_seeds" / "old.csv").exists()  # the earlier seeds stay
    assert not (tmp_path / ".data_seeds" / "title.csv").exists()  # not replaced


def test_full_database_replaces_partial_load(
    imdb_folder, imdb_schema, tmp_path, monkeypatch
):
    """A load that a crash interrupted is done again, not reused as complete."""
    _use_synthetic_dump(monkeypatch, imdb_folder, imdb_schema, tmp_path)  # the dump
    partial = tmp_path / "job.duckdb.part"  # what an interrupted load leaves
    partial.write_text("not a database")  # garbage from the crashed run
    wal = tmp_path / "job.duckdb.part.wal"  # and its write-ahead log
    wal.write_text("not a log")  # garbage that must not be replayed

    connection = _full_database()  # loads afresh: no complete database exists
    count = connection.execute("SELECT count(*) FROM title").fetchone()[0]  # rows
    connection.close()  # the test owns the connection

    assert count == IMDB_ROWS  # the whole title CSV was loaded
    assert (tmp_path / "job.duckdb").exists()  # the complete load took the name
    assert not partial.exists()  # and the garbage .part file is gone
    assert not wal.exists()  # as is its log


def test_full_database_reuses_complete_database(
    imdb_folder, imdb_schema, tmp_path, monkeypatch
):
    """A complete job.duckdb from an earlier run is opened, not loaded again."""
    _use_synthetic_dump(monkeypatch, imdb_folder, imdb_schema, tmp_path)  # the dump
    _full_database().close()  # the first run loads and keeps the database
    monkeypatch.setattr(
        downscale_job_data,
        "_load_full_database",
        lambda: pytest.fail("loaded a database that was complete"),
    )  # a second load would fail the test

    connection = _full_database()  # the second run
    count = connection.execute("SELECT count(*) FROM title").fetchone()[0]  # rows
    connection.close()  # the test owns this connection

    assert count == IMDB_ROWS  # the kept database holds the rows


def test_full_database_refuses_to_load_while_the_archive_remains(
    imdb_folder, imdb_schema, tmp_path, monkeypatch
):
    """The downloader deletes imdb.tgz only after a complete extraction, so while it
    is there, a CSV may be truncated: nothing is loaded."""
    _use_synthetic_dump(monkeypatch, imdb_folder, imdb_schema, tmp_path)  # the dump
    (imdb_folder / "imdb.tgz").write_bytes(b"archive")  # extraction interrupted

    with pytest.raises(IncompleteDump, match="download_job_data"):  # what to run
        _full_database()  # would load a possibly truncated dump

    assert not (tmp_path / "job.duckdb").exists()  # nothing kept for later runs


def test_harvest_seeds_assigns_each_id_to_its_table(
    imdb_folder, imdb_schema, tmp_path, monkeypatch
):
    """A join whose aliases bring different ids puts each id into its own table:
    title 4 is an episode of title 3, the movie of aka_title 3."""
    query = (
        "SELECT MIN(at.id) FROM aka_title AS at, title AS t"
        " WHERE at.movie_id = t.episode_of_id AND t.id = 4;"
    )  # its join row is (aka_title 3, title 4)
    _use_queries(monkeypatch, tmp_path / "queries", {"q.sql": query})  # the corpus
    connection = build_job_connection(imdb_folder, imdb_schema)  # the synthetic dump

    seeds, _ = _harvest_seeds(connection)  # one query, one join row
    connection.close()  # the test owns this connection

    assert seeds["aka_title"] == [3]  # aka_title gets 3, not 4
    assert seeds["title"] == [3, 4]  # title 4, and title 3 that both refer to


def test_harvest_seeds_without_query_files_raises_FileNotFoundError(
    tmp_path, monkeypatch
):
    """Without queries, the harvest stops instead of returning empty seeds."""
    monkeypatch.setattr(job_queries, "JOB_QUERY_PATH", tmp_path)  # holds no .sql
    connection = duckdb.connect()  # never queried: the corpus is checked first

    with pytest.raises(FileNotFoundError, match="No JOB queries"):  # visible stop
        _harvest_seeds(connection)  # would otherwise return {table: []} for all
    connection.close()  # the test owns the connection


def test_harvest_seeds_names_the_queries_whose_join_is_empty(
    imdb_folder, imdb_schema, tmp_path, monkeypatch
):
    """A query whose join finds no rows seeds nothing and is reported as empty."""
    queries = {
        "found.sql": "SELECT MIN(t.id) FROM title AS t WHERE t.id = 3;",  # title 3
        "empty.sql": "SELECT MIN(t.id) FROM title AS t WHERE t.id = 99;",  # none
    }
    _use_queries(monkeypatch, tmp_path / "queries", queries)  # the corpus
    connection = build_job_connection(imdb_folder, imdb_schema)  # the synthetic dump

    seeds, empty = _harvest_seeds(connection)  # one run over both queries
    connection.close()  # the test owns this connection

    assert seeds["title"] == [3]  # title 3 is no episode: no other title is added
    assert empty == {"empty.sql"}  # only the query without rows


def test_close_seeds_without_seeds_adds_nothing(imdb_folder, imdb_schema):
    """Tables without seeds are skipped, and no seed appears out of nothing."""
    connection = build_job_connection(imdb_folder, imdb_schema)  # the synthetic dump
    seeds = {table: set() for table in IMDB_PRIMARY_KEYS}  # nothing seeded

    _close_seeds(connection, seeds)  # nothing to follow
    connection.close()  # the test owns this connection

    assert all(not ids for ids in seeds.values())  # still empty


def test_close_seeds_follows_references_until_nothing_grows(imdb_folder, imdb_schema):
    """A row reached in one pass is followed in the next: title 4 is an episode of
    title 3, whose kind only the second pass adds."""
    connection = build_job_connection(imdb_folder, imdb_schema)  # the synthetic dump
    seeds = {table: set() for table in IMDB_PRIMARY_KEYS}  # nothing seeded
    seeds["title"] = {4}  # one title, an episode

    _close_seeds(connection, seeds)  # follows title 4, then title 3
    connection.close()  # the test owns this connection

    assert seeds["title"] == {3, 4}  # the episode and its series
    assert seeds["kind_type"] == {3, 4}  # kind 3 only through the series
    assert all(not seeds[t] for t in seeds if t not in {"title", "kind_type"})  # rest


def test_seed_query_keeps_string_literals_and_works_without_WHERE():
    """Aliases are quoted outside literals only; a query needs no WHERE clause."""
    with_literal = (  # a literal that looks like an alias and a column reference
        "SELECT MIN(t.id) FROM title AS t WHERE t.title = 't.x AS t';"
    )
    without_where = "SELECT COUNT(*) FROM title AS t;"  # no WHERE clause

    literal_sql = _seed_query(with_literal)  # the alias is quoted around the literal
    plain_sql = _seed_query(without_where)  # no WHERE to find

    assert "'t.x AS t'" in literal_sql  # the literal is untouched
    assert '"t".title' in literal_sql  # the column reference is quoted
    assert 'FROM title AS "t"' in plain_sql  # the alias is quoted
    assert "ORDER BY c0 LIMIT 3" in plain_sql  # the same seed rows on every run


def test_seed_query_upper_case_alias_with_lower_case_uses_runs_in_DuckDB(
    imdb_folder, imdb_schema
):
    """JOB's `aka_title AS AT` with `at.movie_id` runs once every use of the alias
    is quoted, whatever its case: unquoted, `at` is a keyword in DuckDB."""
    connection = build_job_connection(imdb_folder, imdb_schema)  # the synthetic dump
    seed_sql = _seed_query(KEYWORD_ALIAS_JOIN)  # AS "AT", "AT".movie_id

    rows = connection.execute(seed_sql).fetchall()  # would be a parser error
    connection.close()  # the test owns this connection

    assert rows == [(1, 1), (2, 2), (3, 3)]  # aka_title i joins title i, 3 rows
    assert '"AT".movie_id' in seed_sql  # the lower-case use is quoted too


def test_seed_query_of_every_JOB_query_runs_on_the_committed_schema():
    """Every one of the 113 JOB queries is understood by the downscaler: its tables
    are IMDB tables, and its seed query parses and runs on the committed schema."""
    connection = duckdb.connect()  # empty tables are enough to parse and bind
    connection.execute(JOB_SCHEMA_SOURCE.read_text())  # the schema in git

    for path in job_query_files():  # the real queries
        query = path.read_text()  # e.g. 15a.sql with AS AT and at.movie_id
        tables = [aliased.table for aliased in _aliased_tables(query)]  # FROM list
        cut = re.split(r"\bFROM\b", query, maxsplit=1, flags=re.I)  # not from_company
        from_list = re.split(r"\bWHERE\b", cut[1], flags=re.I)[0]  # the tables, raw
        assert len(tables) == from_list.count(",") + 1, path.name  # every table
        assert set(tables) <= set(IMDB_PRIMARY_KEYS), path.name  # IMDB tables
        rows = connection.execute(_seed_query(query)).fetchall()  # parses and binds
        assert rows == [], path.name  # no rows in empty tables
    connection.close()  # the test owns this connection


def test_check_guarantees_lost_query_raises_GuaranteeViolated(
    imdb_folder, imdb_schema, tmp_path, monkeypatch
):
    """Seeds on which a query that the full dump answers is empty are rejected."""
    queries = {"q.sql": "SELECT MIN(x.id) FROM title AS x WHERE x.id = 99;"}  # none
    _use_queries(monkeypatch, tmp_path / "queries", queries)  # the corpus
    _use_synthetic_dump(monkeypatch, imdb_folder, imdb_schema, tmp_path)  # schema

    with pytest.raises(GuaranteeViolated, match="q.sql"):  # nobody has title 99
        _check_guarantees(imdb_folder, full_empty=set())  # the full dump answered it


def test_check_guarantees_query_empty_on_full_dump_too_passes(
    imdb_folder, imdb_schema, tmp_path, monkeypatch
):
    """A query that finds no rows on the full dump may find none on the seeds."""
    queries = {"q.sql": "SELECT MIN(x.id) FROM title AS x WHERE x.id = 99;"}  # none
    _use_queries(monkeypatch, tmp_path / "queries", queries)  # the corpus
    _use_synthetic_dump(monkeypatch, imdb_folder, imdb_schema, tmp_path)  # schema

    _check_guarantees(imdb_folder, full_empty={"q.sql"})  # no error: nothing lost


def test_check_guarantees_dangling_reference_raises_GuaranteeViolated(
    imdb_folder, imdb_schema, tmp_path, monkeypatch
):
    """Seeds that keep a foreign key to a missing row are rejected."""
    queries = {"q.sql": "SELECT MIN(x.id) FROM title AS x WHERE x.id = 1;"}  # found
    _use_queries(monkeypatch, tmp_path / "queries", queries)  # the corpus
    _use_synthetic_dump(monkeypatch, imdb_folder, imdb_schema, tmp_path)  # schema
    (imdb_folder / "movie_info.csv").write_text("1,999,1\n")  # title 999 is missing

    with pytest.raises(GuaranteeViolated, match=r"movie_info\.movie_id"):  # named
        _check_guarantees(imdb_folder, full_empty=set())  # the dangling key is found

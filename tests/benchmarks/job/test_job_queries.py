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

"""The Join Order Benchmark query files."""

import hashlib
from pathlib import Path

import pytest

from benchmarks.job import job_queries, job_query_files

# how many variants (a, b, ...) each of the 33 JOB queries has, e.g. 4 for 1a to 1d;
# 113 queries in all
_VARIANTS_OF_QUERIES_1_TO_17 = (4, 4, 3, 3, 3, 6, 3, 4, 4, 3, 4, 3, 4, 3, 4, 4, 6)
_VARIANTS_OF_QUERIES_18_TO_33 = (3, 4, 3, 3, 4, 3, 2, 3, 3, 3, 3, 3, 3, 3, 2, 3)
JOB_VARIANT_COUNTS = _VARIANTS_OF_QUERIES_1_TO_17 + _VARIANTS_OF_QUERIES_18_TO_33

# `_corpus_digest` of the 113 query files 1a.sql to 33c.sql of viktorleis/job at
# commit fa6f033, sorted by name (without its schema.sql and fkindexes.sql); the
# files here are copied from there unchanged
JOB_CORPUS_SHA256 = "cfabb408106320aabaeaa3b374b4989b4983eb56d4a1ccbe9395ec58de19295f"


def _corpus_digest(queries: list[Path]) -> str:
    """SHA-256 over the name and the bytes of every file of `queries`, in order."""
    digest = hashlib.sha256()
    for query in queries:
        # a zero byte after each name and each file's bytes keeps the files apart:
        # text moved from the end of one file to the start of the next still counts
        digest.update(query.name.encode() + b"\0" + query.read_bytes() + b"\0")
    return digest.hexdigest()


def test_job_query_files_without_queries_raises_FileNotFoundError(
    tmp_path, monkeypatch
):
    """A folder without a visible `.sql` file raises instead of returning an empty
    list."""
    (tmp_path / "notes.md").write_text("not a query")  # not a .sql file
    (tmp_path / "._1a.sql").write_text("binary")  # hidden, so no query either
    monkeypatch.setattr(job_queries, "JOB_QUERY_PATH", tmp_path)  # point it there

    with pytest.raises(FileNotFoundError, match="No JOB queries"):  # not []
        job_query_files()  # finds no *.sql there


def test_job_query_files_leaves_out_hidden_files_and_folders(tmp_path, monkeypatch):
    """Only visible files count: not a macOS resource file and not a folder that
    is named like a query."""
    (tmp_path / "1a.sql").write_text("SELECT 1;")  # a query
    (tmp_path / "._1a.sql").write_text("binary")  # a macOS resource file
    (tmp_path / "2a.sql").mkdir()  # a folder, not a file
    monkeypatch.setattr(job_queries, "JOB_QUERY_PATH", tmp_path)  # point it there

    queries = job_query_files()  # what a caller gets

    assert [query.name for query in queries] == ["1a.sql"]  # only the query


def test_job_query_files_returns_the_113_job_queries_in_text_order():
    """The corpus is exactly the JOB queries 1a to 33c, sorted by file name, so
    `10a.sql` comes before `1a.sql`; README.md, LICENSE and `SQL vs FQL.md` in the
    same folder are left out."""
    names = [query.name for query in job_query_files()]  # e.g. 1a.sql
    expected = {
        f"{number}{variant}.sql"
        for number, count in enumerate(JOB_VARIANT_COUNTS, start=1)
        for variant in "abcdef"[:count]
    }  # 1a.sql to 33c.sql

    assert names == sorted(expected)  # every query once, no other file, in order
    assert len(names) == 113  # the full benchmark
    assert (names[0], names[-1]) == ("10a.sql", "9d.sql")  # text, not numeric order


def test_job_query_files_are_unchanged_from_upstream():
    """The query files are byte for byte those of viktorleis/job at commit fa6f033:
    any edit, even of whitespace, fails here."""
    digest = _corpus_digest(job_query_files())  # over names and bytes, in order

    assert digest == JOB_CORPUS_SHA256, (
        "The query files differ from viktorleis/job at commit fa6f033; "
        "`git diff -- benchmarks/job/queries` shows how. Do not edit them: adapt "
        "a query in the code that runs it (e.g. quote the alias AT for DuckDB). "
        "After copying a newer upstream commit on purpose, set JOB_CORPUS_SHA256 "
        "to the digest below and name that commit in benchmarks/job/queries/README.md."
    )  # the upstream files, unchanged

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

"""The Join Order Benchmark queries: one `.sql` file per query."""

from pathlib import Path

JOB_QUERY_PATH = Path(__file__).resolve().parent / "queries"


def job_query_files() -> list[Path]:
    """The `.sql` files in `JOB_QUERY_PATH`, sorted by file name, so in text order
    (`10a.sql` before `1a.sql`). Hidden files and anything that is not a file, such
    as a folder or a broken link, are left out; a link to a file counts as the file.
    Raises `FileNotFoundError` when there is none."""
    # unlike the shell, pathlib's glob also matches names that start with a dot,
    # such as the macOS resource file ._1a.sql or the editor lock file .#1a.sql
    queries = sorted(
        path
        for path in JOB_QUERY_PATH.glob("*.sql")
        if path.is_file() and not path.name.startswith(".")
    )
    if not queries:
        raise FileNotFoundError(f"No JOB queries (*.sql) in {JOB_QUERY_PATH}.")
    return queries

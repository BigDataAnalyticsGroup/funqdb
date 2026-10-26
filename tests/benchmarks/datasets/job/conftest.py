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

"""Fixtures for the JOB tests: a synthetic IMDB data folder and its schema."""

from pathlib import Path

import pytest

from tests.benchmarks.datasets.job.imdb_folder import write_imdb_folder


@pytest.fixture
def imdb_folder(tmp_path) -> Path:
    """A synthetic JOB data folder of every IMDB table, named like the full dump;
    its schema is written beside it (see `imdb_schema`)."""
    folder = tmp_path / ".data"
    write_imdb_folder(folder, tmp_path / "imdb_schema.sql")
    return folder


@pytest.fixture
def imdb_schema(imdb_folder) -> Path:
    """The schema of the synthetic folder: every IMDB table, with its id and its
    foreign key columns only. It lies outside the folder, as the committed one does."""
    return imdb_folder.parent / "imdb_schema.sql"

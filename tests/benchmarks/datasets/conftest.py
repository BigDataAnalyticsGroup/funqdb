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

from collections.abc import Iterator

import duckdb
import pytest

from benchmarks.datasets import DatasetDefinition, LoadedDataset, PrimaryKey


@pytest.fixture
def genre() -> DatasetDefinition:
    """A definition with data: one table `genre` holding (1, 'noir')."""
    return DatasetDefinition(
        name="genre",
        primary_keys={"genre": PrimaryKey.of("id")},
        build_connection=_genre_connection,
    )


@pytest.fixture
def dataset(genre: DatasetDefinition) -> Iterator[LoadedDataset]:
    """A private open dataset of `genre`, closed after the test."""
    opened = genre.open()
    yield opened
    opened.close()


@pytest.fixture
def flat() -> DatasetDefinition:
    """A definition without data."""
    return DatasetDefinition(name="flat")


@pytest.fixture(params=["flat", "genre"], ids=["no data", "data"])
def any_definition(request) -> DatasetDefinition:
    """Each kind of definition in turn: without and with data."""
    return request.getfixturevalue(request.param)


def _genre_connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("CREATE TABLE genre (id INTEGER, label VARCHAR)")
    connection.execute("INSERT INTO genre VALUES (1, 'noir')")
    return connection

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

"""The no_tables dataset: a schema without tables."""

from collections.abc import Iterator

import pytest

from benchmarks.datasets import DATASETS, Dataset, Schema
from benchmarks.datasets.no_tables.no_tables_dataset import NO_TABLES


@pytest.fixture
def no_tables() -> Iterator[Dataset]:
    """A private open no_tables dataset, closed after the test."""
    dataset = NO_TABLES.open()  # a schema-only dataset
    yield dataset  # nothing is declared
    dataset.close()  # the fixture owns the dataset it opened


def test_NO_TABLES_is_registered():
    """The transpiler finds the dataset by its name."""
    assert DATASETS["no_tables"] is NO_TABLES  # the registry holds this definition


def test_NO_TABLES_open_schema_is_empty(no_tables):
    """The dataset has no data, no keys, no references and no columns."""
    schema = no_tables.schema()  # nothing is declared

    assert NO_TABLES.has_data is False  # declared without a connection builder
    assert schema == Schema(references={}, primary_keys={}, columns={})  # empty

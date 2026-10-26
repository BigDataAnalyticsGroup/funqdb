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

"""Opening a dataset that needs a DuckDB extension, which DuckDB installs from its repository."""

import os

import duckdb
import pytest

from benchmarks.datasets import DatasetDefinition, LoadedDataset


def open_or_skip(definition: DatasetDefinition) -> LoadedDataset:
    """The opened `definition`, a definition with data. Where opening raises a
    `duckdb.IOException`, as when DuckDB cannot install or load its extension, the
    test is skipped; in CI (environment variable `CI` set) it fails instead, so a
    CI run never loses these tests unnoticed. Any other error, such as a duplicate
    key, fails the test."""
    try:
        dataset = definition.open()
    except duckdb.IOException as error:  # includes duckdb.HTTPException
        message = f"the DuckDB extension is unavailable: {error}"
        if os.environ.get("CI"):
            pytest.fail(message)
        pytest.skip(message)

    assert isinstance(dataset, LoadedDataset), "the definition has no data"
    return dataset

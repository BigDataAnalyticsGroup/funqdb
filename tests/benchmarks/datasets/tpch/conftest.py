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

"""TPC-H needs DuckDB's `tpch` extension."""

from collections.abc import Iterator

import pytest

from benchmarks.datasets import LoadedDataset
from benchmarks.datasets.tpch.tpch_dataset import TPCH
from tests.benchmarks.datasets.tpch.extension import open_or_skip


@pytest.fixture(scope="module")
def tpch() -> Iterator[LoadedDataset]:
    """This module's own TPC-H dataset; outside CI, the tests are skipped where
    DuckDB cannot install or load the extension, in CI they fail (see
    `open_or_skip`)."""
    dataset = open_or_skip(TPCH)
    yield dataset
    dataset.close()

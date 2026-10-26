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

"""The benchmark datasets: each opened as an FDM database and a DuckDB reference."""

from benchmarks.datasets.dataset import (
    Dataset,
    DatasetClosed,
    DatasetDefinition,
    LoadedDataset,
)
from benchmarks.datasets.fql_runtime import FQL_RUNTIME
from benchmarks.datasets.query_result import (
    FQLExecutionFailed,
    NotATable,
    NotExecuted,
    NotTabular,
    QueryResult,
    ReferenceValue,
    ResultMismatch,
)
from benchmarks.datasets.registry import DATASETS
from benchmarks.datasets.schema import PrimaryKey, Schema
from benchmarks.datasets.dataset_types import References, Reference, SqlSchema

__all__ = [
    "DATASETS",
    "Dataset",
    "DatasetClosed",
    "DatasetDefinition",
    "FQLExecutionFailed",
    "FQL_RUNTIME",
    "References",
    "Reference",
    "LoadedDataset",
    "NotATable",
    "NotExecuted",
    "NotTabular",
    "PrimaryKey",
    "QueryResult",
    "ReferenceValue",
    "ResultMismatch",
    "Schema",
    "SqlSchema",
]

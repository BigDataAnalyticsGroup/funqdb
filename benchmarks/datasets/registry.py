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
"""The registry of every dataset, and the open datasets in use."""

from benchmarks.datasets.dataset import Dataset, DatasetDefinition
from benchmarks.datasets.job.job_dataset import JOB_DATASETS
from benchmarks.datasets.join_graph.join_graph_dataset import JOIN_GRAPH
from benchmarks.datasets.movies.movies_dataset import MOVIES
from benchmarks.datasets.no_tables.no_tables_dataset import NO_TABLES
from benchmarks.datasets.special_names.special_names_dataset import (
    SPECIAL_NAMES,
)
from benchmarks.datasets.tpch.tpch_dataset import TPCH

DATASETS: dict[str, DatasetDefinition] = {
    definition.name: definition
    for definition in (
        MOVIES,
        *JOB_DATASETS,
        TPCH,
        SPECIAL_NAMES,
        NO_TABLES,
        JOIN_GRAPH,
    )
}


class LiveDatasets:
    """The open datasets in use, one per key of `definitions`: each is opened on
    first use and reused until it is discarded. A caller discards a dataset a
    script changed, so the next `get` returns a fresh copy. Not thread-safe: a
    caller that shares it between threads holds a lock around every call."""

    def __init__(self, definitions: dict[str, DatasetDefinition]) -> None:
        self._definitions = definitions
        self._opened: dict[str, Dataset] = {}

    def get(self, key: str) -> Dataset:
        """The open dataset of `key`. A caller that closes it closes it for every
        caller until `discard`. Raises `KeyError` for a key outside the
        definitions: naming a dataset that does not exist is a caller bug."""
        if key not in self._opened:
            self._opened[key] = self._definitions[key].open()
        return self._opened[key]

    def discard(self, key: str) -> bool:
        """Drop and close the dataset of `key`; ``True`` when there was one, also
        one that a caller closed. ``False`` (and nothing happens) when `key` holds no
        dataset: it was never fetched with `get`, or it was discarded."""
        dropped = self._opened.pop(key, None)
        if dropped is None:
            return False

        dropped.close()
        return True

    def close(self) -> None:
        """Discard every open dataset, also after a failed close, then raise the
        first error. A later `get` opens afresh."""
        first_error: Exception | None = None
        for key in list(self._opened):
            try:
                self.discard(key)
            except Exception as error:
                first_error = first_error or error
        if first_error is not None:
            raise first_error

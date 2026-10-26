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

"""Skipping a dataset whose DuckDB extension cannot be loaded, for example offline,
and failing instead in CI."""

import duckdb
import pytest

from benchmarks.datasets import DatasetDefinition
from benchmarks.datasets.tpch.tpch_dataset import build_tpch_connection
from tests.benchmarks.datasets.tpch.extension import open_or_skip


def _duplicate_key() -> duckdb.DuckDBPyConnection:
    """A connection builder that fails as a primary key over duplicates does."""
    raise duckdb.ConstraintException("duplicate key")


def _offline_tpch(extension_directory: str) -> DatasetDefinition:
    """TPC-H that DuckDB must download into the empty `extension_directory` from a
    repository that cannot be reached, as without network access."""
    offline = {
        "extension_directory": extension_directory,
        "custom_extension_repository": "http://127.0.0.1:9",
    }
    return DatasetDefinition(
        name="offline", build_connection=lambda: build_tpch_connection(offline)
    )


def test_open_or_skip_failed_download_skips_the_test(tmp_path, monkeypatch):
    """Outside CI, a failed extension download is a skip, not a failure. DuckDB
    really tries the download here."""
    monkeypatch.delenv("CI", raising=False)  # as on a developer's machine
    definition = _offline_tpch(str(tmp_path))  # tpch cannot be installed

    with pytest.raises(pytest.skip.Exception, match="unavailable"):  # skipped
        open_or_skip(definition)  # INSTALL tpch raises an IOException


def test_open_or_skip_failed_download_fails_the_test_in_CI(tmp_path, monkeypatch):
    """In CI, a failed extension download fails the test, so the TPC-H tests are
    never skipped unnoticed."""
    monkeypatch.setenv("CI", "true")  # as GitLab CI sets it
    definition = _offline_tpch(str(tmp_path))  # tpch cannot be installed

    try:  # a skip escapes pytest.raises below
        with pytest.raises(pytest.fail.Exception, match="unavailable"):  # a failure
            open_or_skip(definition)  # INSTALL tpch raises an IOException
    except pytest.skip.Exception:  # a skip is no failure: raises() lets it pass
        pytest.fail("in CI, the download error became a skip")  # so fail here


def test_open_or_skip_other_duckdb_error_fails_the_test(monkeypatch):
    """Only an I/O error is a skip: any other DuckDB error is raised."""
    monkeypatch.delenv("CI", raising=False)  # where an I/O error would be a skip
    definition = DatasetDefinition(
        name="broken", build_connection=_duplicate_key
    )  # opening raises a constraint error

    with pytest.raises(duckdb.ConstraintException):  # raised as it is
        try:  # catches a skip before pytest.raises would let it pass
            open_or_skip(definition)  # the open raises a constraint error
        except pytest.skip.Exception:  # a skip is no Exception: it would pass by
            pytest.fail("a constraint error was turned into a skip")  # so fail


def test_open_or_skip_available_dataset_is_returned(genre):
    """Without an error, the opened dataset is returned."""
    dataset = open_or_skip(genre)  # opens normally

    try:  # close the dataset whatever the assert does
        assert dataset.schema().primary_keys == genre.primary_keys  # genre's keys
    finally:
        dataset.close()  # the test owns this dataset, also when the assert fails

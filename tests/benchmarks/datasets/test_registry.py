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

"""The registered datasets and the open datasets in use."""

from collections.abc import Iterator
from typing import NoReturn

import pytest

from benchmarks.datasets import (
    Dataset,
    DatasetClosed,
    DatasetDefinition,
    LiveDatasets,
)


@pytest.fixture
def live(genre: DatasetDefinition, flat: DatasetDefinition) -> Iterator[LiveDatasets]:
    """Open datasets of `genre` (data) and `flat` (no data), closed after the test."""
    datasets = LiveDatasets({"genre": genre, "flat": flat})  # one of each kind
    yield datasets  # nothing is open yet
    datasets.close()  # the fixture owns every dataset the test opened


def _is_closed(dataset: Dataset) -> bool:
    """True when `dataset` refuses a call because it is closed."""
    try:
        dataset.schema()  # every call but `close` raises on a closed dataset
    except DatasetClosed:
        return True  # the dataset refused the call
    return False  # the call worked, so the dataset is open


def test_LiveDatasets_get_unknown_key_raises_KeyError_and_opens_nothing(live):
    """An undefined key is a caller bug: `get` raises and opens nothing."""
    with pytest.raises(KeyError):  # an undefined key is refused
        live.get("no_such_dataset")  # a typo, say
    dropped = live.discard("no_such_dataset")  # was anything opened for it?

    assert dropped is False  # the failed get left no dataset behind


def test_LiveDatasets_get_returns_dataset_a_caller_closed_until_discard(live):
    """A caller that closes a shared dataset closes it for all; `discard` reopens it."""
    shared = live.get("genre")  # one caller's dataset
    shared.close()  # that caller closes the shared dataset

    closed = live.get("genre")  # another caller gets the same object
    dropped = live.discard("genre")  # drops the closed dataset
    reopened = live.get("genre")  # opened afresh

    assert closed is shared  # get does not replace the closed dataset
    assert dropped is True  # the closed dataset is still held
    assert _is_closed(closed)  # still closed: get does not reopen it
    assert not _is_closed(reopened)  # discard lets the next get open a new one


def test_LiveDatasets_get_reuses_dataset_until_it_is_discarded(live):
    """`get` reuses one dataset; after `discard` it opens a fresh one."""
    first = live.get("genre")  # opened on first use
    again = live.get("genre")  # reused
    dropped = live.discard("genre")  # e.g. after a script wrote to it
    reopened = live.get("genre")  # opened afresh

    assert again is first  # no reopen while it is in use
    assert dropped is True  # there was a dataset to drop
    assert _is_closed(first)  # and it was closed on discard
    assert reopened is not first  # the next caller gets a clean dataset


def test_LiveDatasets_discard_unopened_key_returns_False_and_keeps_others(live):
    """Discarding a dataset that is not open is a no-op, and says so."""
    opened = live.get("genre")  # only genre is open

    dropped = live.discard("flat")  # flat was never opened
    kept = live.get("genre")  # genre is untouched by that

    assert dropped is False  # nothing was there to drop
    assert kept is opened  # the other dataset survived
    assert not _is_closed(kept)  # and still open


def test_LiveDatasets_discard_closes_dataset_without_data_and_keeps_others(live):
    """A discarded dataset is closed, also without a connection; others stay open."""
    kept = live.get("flat")  # a caller keeps a reference
    other = live.get("genre")  # open next to it

    dropped = live.discard("flat")  # e.g. after a script wrote to it

    assert dropped is True  # the dataset was dropped
    assert _is_closed(kept)  # and the kept reference is closed
    assert not _is_closed(other)  # the other dataset is still open


def test_LiveDatasets_close_failing_dataset_closes_others_and_raises(live, monkeypatch):
    """A failed close does not keep the other datasets open; the error still shows."""
    failing = live.get("genre")  # opened first, so closed first
    other = live.get("flat")  # would stay open if the loop stopped
    close_for_real = failing.close  # the fixture cannot release it any more

    def fail() -> NoReturn:
        """Raise as a failed close does."""
        raise OSError("disk gone")  # stands for any failed close

    monkeypatch.setattr(failing, "close", fail)  # only this dataset fails

    with pytest.raises(OSError, match="disk gone"):  # the first error is raised
        live.close()  # closes both
    close_for_real()  # the test owns the dataset whose close failed

    assert _is_closed(other)  # the loop went on after the error
    assert live.discard("genre") is False  # the failed dataset is gone as well


def test_LiveDatasets_close_two_failing_datasets_raises_first_error(live, monkeypatch):
    """With two failed closes, the error of the first one is raised."""
    first = live.get("genre")  # opened first, so closed first
    second = live.get("flat")  # fails as well
    close_first = first.close  # the fixture cannot release it any more

    def fail_first() -> NoReturn:
        """Raise the error of the first close."""
        raise OSError("first")  # the error the caller must see

    def fail_second() -> NoReturn:
        """Raise the error of the second close."""
        raise OSError("second")  # a later error must not replace it

    monkeypatch.setattr(first, "close", fail_first)  # the first close fails
    monkeypatch.setattr(second, "close", fail_second)  # and the second

    with pytest.raises(OSError, match="first"):  # not "second"
        live.close()  # tries both
    close_first()  # the test owns the dataset whose close failed


def test_LiveDatasets_close_closes_every_dataset_and_can_repeat(live):
    """`close` closes every dataset; it can repeat, and `get` works after it."""
    with_data = live.get("genre")  # a dataset with data
    without_data = live.get("flat")  # and one without

    live.close()  # discards both
    live.close()  # nothing left: a no-op
    after = live.get("genre")  # usable again after close

    assert _is_closed(with_data)  # the first close closed the dataset with data
    assert _is_closed(without_data)  # and the one without
    assert after is not with_data  # a fresh dataset after close
    assert not _is_closed(after)  # open until the fixture closes it

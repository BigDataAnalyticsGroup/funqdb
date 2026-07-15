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

import pytest

from fdm.attribute_functions import RF, DBF
from fql.operators.subsets import subset
from fql.util import Item
from tests.lib import _create_testdata


def test_subset_top_k_ascending():
    """Verify top-k returns the k items with the smallest ranking values."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # top-2 youngest users (smallest yob = oldest, so ascending order)
    result: RF = subset(
        users,
        ranking_key=lambda item: item.value.yob,
        k=2,
    ).result

    assert len(result) == 2
    yobs: set[int] = {item.value.yob for item in result}
    # the two smallest yob values are 1972 (Horst) and 1983 (Tom)
    assert yobs == {1972, 1983}


def test_subset_top_k_descending():
    """Verify top-k with reverse=True returns the k items with the largest ranking values."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # top-2 youngest users (largest yob)
    result: RF = subset(
        users,
        ranking_key=lambda item: item.value.yob,
        k=2,
        reverse=True,
    ).result

    assert len(result) == 2
    yobs: set[int] = {item.value.yob for item in result}
    # the two largest yob values are 2003 (John) and 1983 (Tom)
    assert yobs == {1983, 2003}


def test_subset_top_1_is_min():
    """Verify that top-1 ascending is equivalent to min."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    result: RF = subset(
        users,
        ranking_key=lambda item: item.value.yob,
        k=1,
    ).result

    assert len(result) == 1
    only_item: Item = next(iter(result))
    assert only_item.value.name == "Horst"
    assert only_item.value.yob == 1972


def test_subset_top_1_reverse_is_max():
    """Verify that top-1 descending is equivalent to max."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    result: RF = subset(
        users,
        ranking_key=lambda item: item.value.yob,
        k=1,
        reverse=True,
    ).result

    assert len(result) == 1
    only_item: Item = next(iter(result))
    assert only_item.value.name == "John"
    assert only_item.value.yob == 2003


def test_subset_k_larger_than_input():
    """Verify that k > len(input) returns all items."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    result: RF = subset(
        users,
        ranking_key=lambda item: item.value.yob,
        k=100,
    ).result

    assert len(result) == len(users)


def test_subset_with_output_factory():
    """Verify that the output_factory parameter is used when provided."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    result: RF = subset(
        users,
        ranking_key=lambda item: item.value.yob,
        k=2,
        output_factory=lambda _: RF(),
    ).result

    assert type(result) is RF
    assert len(result) == 2


def test_subset_generic_predicate():
    """Verify the generic subset_predicate mode for arbitrary global conditions."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # keep only users whose yob is above the mean yob
    def above_mean(af: RF) -> RF:
        all_yobs: list[int] = [item.value.yob for item in af]
        mean_yob: float = sum(all_yobs) / len(all_yobs)
        return af.where(lambda item: item.value.yob > mean_yob)

    result: RF = subset(users, subset_predicate=above_mean).result

    # mean of {1972, 1983, 2003} = 1986 → only John (2003) qualifies
    assert len(result) == 1
    assert next(iter(result)).value.name == "John"


def test_subset_mutually_exclusive_params():
    """Verify that providing both ranking_key and subset_predicate raises an error."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    with pytest.raises(AssertionError):
        subset(
            users,
            ranking_key=lambda item: item.value.yob,
            k=2,
            subset_predicate=lambda af: af,
        )


def test_subset_neither_param():
    """Verify that providing neither ranking_key nor subset_predicate raises an error."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    with pytest.raises(AssertionError):
        subset(users)


def test_subset_explain():
    """Verify that explain() returns a descriptive string derived from to_plan()."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    op_topk: subset = subset(users, ranking_key=lambda item: item.value.yob, k=3)
    explanation: str = op_topk.explain()
    assert "subset" in explanation
    assert "k=3" in explanation
    assert "reverse=False" in explanation

    op_topk_rev: subset = subset(
        users, ranking_key=lambda item: item.value.yob, k=2, reverse=True
    )
    explanation_rev: str = op_topk_rev.explain()
    assert "subset" in explanation_rev
    assert "k=2" in explanation_rev
    assert "reverse=True" in explanation_rev

    op_pred: subset = subset(users, subset_predicate=lambda af: af)
    assert "subset" in op_pred.explain()


def test_convenience_top():
    """Verify the top() convenience method on AFs."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    result: RF = users.top(k=2, key=lambda item: item.value.yob)

    assert len(result) == 2
    yobs: set[int] = {item.value.yob for item in result}
    assert yobs == {1972, 1983}


def test_convenience_bottom():
    """Verify the bottom() convenience method on AFs."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    result: RF = users.bottom(k=2, key=lambda item: item.value.yob)

    assert len(result) == 2
    yobs: set[int] = {item.value.yob for item in result}
    assert yobs == {1983, 2003}


def test_subset_on_dbf():
    """Verify subset works at the DBF level (select relations by global condition)."""
    db: DBF = _create_testdata(frozen=True)

    # keep only the 1 relation with the fewest items
    result: DBF = subset(
        db,
        ranking_key=lambda item: len(item.value),
        k=1,
    ).result

    assert len(result) == 1
    # departments has 2 items, users has 3, customers has 5 → departments wins
    assert next(iter(result)).value == db.departments


def test_subset_returns_correct_items():
    """k keeps the k smallest-key items; output type and keys are preserved."""
    db: DBF = _create_testdata(
        frozen=True
    )  # 5-item customers RF with integer keys 1..5

    result = subset(
        db.customers, ranking_key=lambda item: item.key, k=3
    ).result  # keep lowest 3

    assert type(result) is RF  # output must be an RF, not a plain AF subtype
    assert len(result) == 3  # exactly k items returned
    assert result[1] == db.customers[1]  # key 1 (smallest) survives
    assert result[2] == db.customers[2]  # key 2 survives
    assert result[3] == db.customers[3]  # key 3 survives; keys 4 and 5 are dropped


def test_subset_offset_shifts_window():
    """offset skips the first n sorted items; the next k form the result."""
    db: DBF = _create_testdata(
        frozen=True
    )  # 5-item customers RF with integer keys 1..5

    result = subset(
        db.customers, ranking_key=lambda item: item.key, k=3, offset=2
    ).result  # sorted[2:5] → keys 3, 4, 5

    assert type(result) is RF  # output type unchanged
    assert len(result) == 3  # offset=2 skips keys 1,2; k=3 keeps keys 3,4,5
    assert (
        result[3] == db.customers[3]
    )  # first item after offset (0-based pos 2 → key 3)
    assert result[4] == db.customers[4]  # second item in window
    assert result[5] == db.customers[5]  # third item in window (last customer)


def test_subset_offset_beyond_end_returns_empty():
    """offset past the end of the sorted list yields an empty AF without error."""
    db: DBF = _create_testdata(frozen=True)  # 5-item customers RF

    result = subset(
        db.customers, ranking_key=lambda item: item.key, k=3, offset=100
    ).result  # sorted[100:103] → empty

    assert type(result) is RF  # still returns an RF, not None
    assert len(result) == 0  # no items survive when offset exceeds list length


def test_subset_negative_offset_raises():
    """Negative offset must be rejected at construction time."""
    db: DBF = _create_testdata(frozen=True)

    # -- begin AI-modified --
    with pytest.raises(ValueError):  # __init__ raises ValueError for offset < 0
        subset(db.customers, ranking_key=lambda item: item.key, k=2, offset=-1)
    # -- end AI-modified --


def test_subset_offset_with_predicate_raises():
    """Passing offset when using subset_predicate must raise; offset only applies to top-k mode."""
    db: DBF = _create_testdata(frozen=True)

    with pytest.raises(ValueError):  # offset is meaningless in predicate mode
        subset(db.customers, subset_predicate=lambda af: af, offset=1)


def test_convenience_top_bottom_offset():
    """top()/bottom() forward offset to subset, shifting the returned window."""
    db: DBF = _create_testdata(frozen=True)  # users have yob 1972/1983/2003
    users: RF = db.users  # 3-item users RF, keys 1..3

    top_page: RF = users.top(
        k=2, key=lambda item: item.value.yob, offset=1
    )  # ascending [1972,1983,2003], slice[1:3]

    assert len(top_page) == 2  # offset=1 skips the youngest; k=2 keeps the next two
    assert {item.value.yob for item in top_page} == {
        1983,
        2003,
    }  # 1972 (Horst) skipped by offset

    bottom_page: RF = users.bottom(
        k=2, key=lambda item: item.value.yob, offset=1
    )  # descending [2003,1983,1972], slice[1:3]

    assert len(bottom_page) == 2  # offset=1 skips the oldest; k=2 keeps the next two
    assert {item.value.yob for item in bottom_page} == {
        1983,
        1972,
    }  # 2003 (John) skipped by offset


def test_subset_offset_with_reverse():
    """offset skips from the front of the reversed (descending) order, not the ascending one."""
    db: DBF = _create_testdata(frozen=True)  # 5-item customers RF with keys 1..5

    result = subset(
        db.customers, ranking_key=lambda item: item.key, k=2, offset=1, reverse=True
    ).result  # descending keys [5,4,3,2,1], slice[1:3] → keys 4, 3

    assert type(result) is RF  # output type preserved
    assert len(result) == 2  # offset=1 skips key 5; k=2 keeps keys 4 and 3
    assert result[4] == db.customers[4]  # first item after the skipped largest key
    assert result[3] == db.customers[3]  # second item in the window
    assert 5 not in result  # largest key was skipped by offset
    assert 2 not in result  # beyond the k-window

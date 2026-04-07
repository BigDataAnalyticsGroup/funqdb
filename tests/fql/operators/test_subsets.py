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

from fdm.attribute_functions import TF, RF, DBF
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
    """Verify that explain() returns a descriptive string."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    op_topk: subset = subset(
        users, ranking_key=lambda item: item.value.yob, k=3
    )
    assert "top-3" in op_topk.explain()
    assert "smallest" in op_topk.explain()

    op_topk_rev: subset = subset(
        users, ranking_key=lambda item: item.value.yob, k=2, reverse=True
    )
    assert "top-2" in op_topk_rev.explain()
    assert "largest" in op_topk_rev.explain()

    op_pred: subset = subset(users, subset_predicate=lambda af: af)
    assert "subset operator" in op_pred.explain()


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

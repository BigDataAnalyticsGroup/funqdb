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

import collections.abc

import pytest

from fdm.attribute_functions import TF, RF, DBF, RSF, CompositeForeignObject
from fql.operators.filters import filter_keys
from fql.operators.rank import rank_by, items_sorted_by
from fql.operators.subsets import subset
from fql.util import Item
from tests.lib import _create_testdata

# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_rank_by_ascending_by_yob() -> None:
    """Ascending rank_by over users yields keys 0,1,2 and values ordered
    Horst (1972) < Tom (1983) < John (2003)."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    ranked: RF = rank_by(users, ranking_key=lambda item: item.value.yob).result

    assert len(ranked) == 3
    # ranks 0..2 in ascending yob order:
    assert ranked[0].name == "Horst"
    assert ranked[0].yob == 1972
    assert ranked[1].name == "Tom"
    assert ranked[1].yob == 1983
    assert ranked[2].name == "John"
    assert ranked[2].yob == 2003


def test_rank_by_descending_by_yob() -> None:
    """Descending rank_by reverses the ascending order."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    ranked: RF = rank_by(
        users, ranking_key=lambda item: item.value.yob, reverse=True
    ).result

    assert len(ranked) == 3
    assert ranked[0].name == "John"
    assert ranked[1].name == "Tom"
    assert ranked[2].name == "Horst"


def test_rank_by_result_is_rf_instance() -> None:
    """Regardless of input type (here: RF), rank_by returns an RF."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    ranked = rank_by(users, ranking_key=lambda item: item.value.yob).result
    assert type(ranked) is RF


def test_rank_by_output_keys_are_consecutive_naturals() -> None:
    """Output keys are exactly {0, 1, 2} — the rank domain ℕ."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    ranked: RF = rank_by(users, ranking_key=lambda item: item.value.yob).result
    observed_keys: set[int] = {item.key for item in ranked}
    assert observed_keys == {0, 1, 2}


def test_rank_by_is_lazy_and_cached() -> None:
    """Construction alone must not compute. Accessing .result twice returns
    the identically-same object (cached)."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # Constructing the operator performs no computation — only config storage.
    op: rank_by = rank_by(users, ranking_key=lambda item: item.value.yob)

    # First access triggers the compute, second access hits the cache:
    r1: RF = op.result
    r2: RF = op.result
    assert r1 is r2


def test_rank_by_call_is_equivalent_to_result() -> None:
    """Calling the operator instance returns the same cached result."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    op: rank_by = rank_by(users, ranking_key=lambda item: item.value.yob)
    assert op() is op.result


# ---------------------------------------------------------------------------
# Composition with other operators
# ---------------------------------------------------------------------------


def test_rank_by_top_k_via_filter_keys() -> None:
    """Top-k idiom: rank_by followed by filter_keys k < 2 returns the two
    oldest users (Horst and Tom)."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    ranked: RF = rank_by(users, ranking_key=lambda item: item.value.yob).result
    top_2: RF = filter_keys(ranked, filter_predicate=lambda k: k < 2).result

    assert len(top_2) == 2
    names: set[str] = {item.value.name for item in top_2}
    assert names == {"Horst", "Tom"}


def test_rank_by_pagination_via_filter_keys() -> None:
    """Pagination idiom: selecting rank == 1 yields the single middle user."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    ranked: RF = rank_by(users, ranking_key=lambda item: item.value.yob).result
    page: RF = filter_keys(ranked, filter_predicate=lambda k: k == 1).result

    assert len(page) == 1
    assert next(iter(page)).value.name == "Tom"


def test_rank_by_chained_into_filter_keys_resolves_input() -> None:
    """Passing a rank_by *operator instance* (not its .result) directly as
    the input of another operator must work — _resolve_input should call
    .result under the hood."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    op: rank_by = rank_by(users, ranking_key=lambda item: item.value.yob)

    # Note: `op` here is an Operator, not an AF. _resolve_input must unwrap it:
    top_1: RF = filter_keys(op, filter_predicate=lambda k: k == 0).result

    assert len(top_1) == 1
    assert next(iter(top_1)).value.name == "Horst"


def test_rank_by_chained_into_subset_resolves_input() -> None:
    """rank_by instance as the input of subset — verifies chainability
    across operator families."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    op: rank_by = rank_by(users, ranking_key=lambda item: item.value.yob)
    # take the single smallest-rank item (which is the youngest-by-oldest):
    result: RF = subset(op, ranking_key=lambda item: item.key, k=1).result

    assert len(result) == 1
    assert next(iter(result)).value.name == "Horst"


def test_rank_by_median_lookup() -> None:
    """Median idiom: look up rank n//2 in the ranked result."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    ranked: RF = rank_by(users, ranking_key=lambda item: item.value.yob).result
    median_value = ranked[len(ranked) // 2]  # len==3 → index 1 → Tom

    assert median_value.name == "Tom"
    assert median_value.yob == 1983


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_rank_by_empty_input() -> None:
    """Ranking an empty RF yields an empty RF."""
    empty: RF = RF({})
    ranked: RF = rank_by(empty, ranking_key=lambda item: 0).result

    assert type(ranked) is RF
    assert len(ranked) == 0


def test_rank_by_single_item_input() -> None:
    """A single-item input ranks to exactly one item at key 0."""
    single: RF = RF({"only": TF({"name": "Alone", "n": 7})})
    ranked: RF = rank_by(single, ranking_key=lambda item: item.value.n).result

    assert len(ranked) == 1
    assert ranked[0].name == "Alone"
    assert {item.key for item in ranked} == {0}


def test_rank_by_stable_tie_breaking() -> None:
    """Python's sorted() is stable, so items with equal ranking_key keep
    their input iteration order — rank_by must inherit that stability."""
    # Two ties on k=1 ('a' and 'c'), plus one unique k=2 ('b'). Insertion
    # order in the RF is a, b, c. After sorting ascending by k:
    #   a (k=1, input pos 0)  → rank 0
    #   c (k=1, input pos 2)  → rank 1   (stable: a before c)
    #   b (k=2, input pos 1)  → rank 2
    data: RF = RF(
        {
            "a": TF({"label": "a", "k": 1}),
            "b": TF({"label": "b", "k": 2}),
            "c": TF({"label": "c", "k": 1}),
        }
    )

    ranked: RF = rank_by(data, ranking_key=lambda item: item.value.k).result

    assert ranked[0].label == "a"
    assert ranked[1].label == "c"
    assert ranked[2].label == "b"


def test_rank_by_replaces_input_key_domain_with_naturals() -> None:
    """Input keys are intentionally not preserved — output keys are ℕ even
    if the input keys were strings."""
    data: RF = RF(
        {
            "a": TF({"score": 30}),
            "b": TF({"score": 10}),
            "c": TF({"score": 20}),
        }
    )

    ranked: RF = rank_by(data, ranking_key=lambda item: item.value.score).result

    # Output keys are integers 0,1,2 — none of the input string keys leaked in:
    assert {item.key for item in ranked} == {0, 1, 2}
    for item in ranked:
        assert isinstance(item.key, int)

    # And the values came through in ascending-score order:
    assert ranked[0].score == 10  # was "b"
    assert ranked[1].score == 20  # was "c"
    assert ranked[2].score == 30  # was "a"


def test_rank_by_does_not_mutate_input() -> None:
    """The input AF must be left untouched — snapshot before vs. after."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    keys_before: set = {item.key for item in users}
    values_before: list[str] = [item.value.name for item in users]

    _ = rank_by(users, ranking_key=lambda item: item.value.yob).result

    keys_after: set = {item.key for item in users}
    values_after: list[str] = [item.value.name for item in users]

    assert keys_before == keys_after
    assert values_before == values_after


def test_rank_by_determinism_two_operators_same_input() -> None:
    """Two independent rank_by operators on the same input produce
    equal-content outputs (same keys and same values in the same order)."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    a: RF = rank_by(users, ranking_key=lambda item: item.value.yob).result
    b: RF = rank_by(users, ranking_key=lambda item: item.value.yob).result

    # Different RF objects but identical rank → value mapping:
    assert a is not b
    assert len(a) == len(b)
    for k in range(len(a)):
        assert a[k].name == b[k].name
        assert a[k].yob == b[k].yob


# ---------------------------------------------------------------------------
# Type-checking / errors
# ---------------------------------------------------------------------------


def test_rank_by_rejects_none_ranking_key() -> None:
    """ranking_key=None must raise TypeError at construction time."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    with pytest.raises(TypeError):
        rank_by(users, ranking_key=None)


def test_rank_by_rejects_non_callable_ranking_key() -> None:
    """ranking_key=42 must raise TypeError mentioning 'callable'."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    with pytest.raises(TypeError, match="callable"):
        rank_by(users, ranking_key=42)


# ---------------------------------------------------------------------------
# Output type independence from input type
# ---------------------------------------------------------------------------


def test_rank_by_dbf_input_yields_rf_output() -> None:
    """Input is a DBF (values are RFs). Output must still be an RF whose
    values are those inner RFs, ranked by some key over the RFs."""
    db: DBF = _create_testdata(frozen=True)

    # Rank the three relations (departments=2, users=3, customers=5) by size:
    ranked = rank_by(db, ranking_key=lambda item: len(item.value)).result

    # Critical invariant: the output type is RF, NOT DBF — regardless of
    # what the input type was.
    assert type(ranked) is RF
    assert len(ranked) == 3
    # Ascending by size: departments (2) → users (3) → customers (5).
    assert ranked[0] is db.departments
    assert ranked[1] is db.users
    assert ranked[2] is db.customers


def test_rank_by_rsf_input_yields_rf_output() -> None:
    """Regression test: an RSF input (keys are CompositeForeignObjects)
    must not blow up trying to store integer ranks under composite keys.
    The output must be an RF with integer keys — not an RSF."""
    # Build two dummy foreign objects and wrap each in a CompositeForeignObject:
    fo_1: TF = TF({"x": 1})
    fo_2: TF = TF({"x": 2})
    key1: CompositeForeignObject = CompositeForeignObject([fo_1])
    key2: CompositeForeignObject = CompositeForeignObject([fo_2])

    rsf: RSF = RSF({key1: TF({"w": 10}), key2: TF({"w": 20})})

    # Rank descending by the scalar 'w' inside each relationship's value:
    ranked = rank_by(rsf, ranking_key=lambda item: item.value.w, reverse=True).result

    # The whole point of the fix: output is an RF, not an RSF.
    assert type(ranked) is RF
    assert len(ranked) == 2
    # And the integer keys 0, 1 are present (not CompositeForeignObjects):
    assert {item.key for item in ranked} == {0, 1}
    for item in ranked:
        assert isinstance(item.key, int)
    # Descending order: w=20 first, then w=10.
    assert ranked[0].w == 20
    assert ranked[1].w == 10


# ---------------------------------------------------------------------------
# Custom output_factory
# ---------------------------------------------------------------------------


def test_rank_by_with_explicit_rf_output_factory() -> None:
    """Passing an explicit output_factory=lambda _: RF() yields an RF
    (sanity check that the factory plumbing is wired up)."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    ranked = rank_by(
        users,
        ranking_key=lambda item: item.value.yob,
        output_factory=lambda _: RF(),
    ).result

    assert type(ranked) is RF
    assert len(ranked) == 3
    assert ranked[0].name == "Horst"


# ---------------------------------------------------------------------------
# items_sorted_by tests
# ---------------------------------------------------------------------------


def test_items_sorted_by_ascending() -> None:
    """items_sorted_by yields the original Items of `users` in ascending
    yob order (Horst, Tom, John), with the ORIGINAL input keys (1, 2, 3)
    preserved — this is the defining contrast to rank_by."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    result = items_sorted_by(users, key=lambda item: item.value.yob)
    items: list[Item] = list(result)

    assert len(items) == 3
    # Order follows yob ascending (1972, 1983, 2003):
    assert items[0].value.name == "Horst"
    assert items[1].value.name == "Tom"
    assert items[2].value.name == "John"

    # Original keys (1, 2, 3) are preserved — unlike rank_by which would
    # replace them with 0, 1, 2:
    assert items[0].key == 1
    assert items[1].key == 2
    assert items[2].key == 3

    # And each yielded object is a proper Item instance:
    for item in items:
        assert isinstance(item, Item)


def test_items_sorted_by_descending() -> None:
    """With reverse=True the items come out in descending yob order."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    items: list[Item] = list(
        items_sorted_by(users, key=lambda item: item.value.yob, reverse=True)
    )

    assert [item.value.name for item in items] == ["John", "Tom", "Horst"]
    # Original keys still preserved:
    assert [item.key for item in items] == [3, 2, 1]


def test_items_sorted_by_returns_iterator() -> None:
    """The return value is a genuine Iterator (not a list, not an AF).
    Consuming it with next() yields items one by one and eventually
    raises StopIteration."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    result = items_sorted_by(users, key=lambda item: item.value.yob)

    # Crucial type contract: it's an Iterator, not a list or an AF:
    assert isinstance(result, collections.abc.Iterator)
    assert not isinstance(result, list)
    assert not isinstance(result, RF)

    # Step through manually to document the one-item-at-a-time protocol:
    first: Item = next(result)
    assert first.value.name == "Horst"
    second: Item = next(result)
    assert second.value.name == "Tom"
    third: Item = next(result)
    assert third.value.name == "John"

    with pytest.raises(StopIteration):
        next(result)


def test_items_sorted_by_is_single_pass() -> None:
    """The returned iterator is single-pass: once exhausted, iterating it
    again yields nothing. Documents the 'presentation sink' intent — a
    caller who needs to re-iterate should wrap the call in list(...)."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    result = items_sorted_by(users, key=lambda item: item.value.yob)

    first_pass: list[Item] = list(result)
    assert len(first_pass) == 3

    # Second pass on the same (now-exhausted) iterator yields nothing:
    second_pass: list[Item] = list(result)
    assert second_pass == []


def test_items_sorted_by_empty_input() -> None:
    """Sorting the items of an empty RF yields an empty iterator."""
    empty: RF = RF({})
    result = items_sorted_by(empty, key=lambda item: 0)

    assert list(result) == []


def test_items_sorted_by_single_item() -> None:
    """A single-item input yields exactly one item, with its original key
    and value preserved."""
    single: RF = RF({"only": TF({"name": "Alone", "n": 7})})

    items: list[Item] = list(items_sorted_by(single, key=lambda item: item.value.n))

    assert len(items) == 1
    assert items[0].key == "only"
    assert items[0].value.name == "Alone"
    assert items[0].value.n == 7


def test_items_sorted_by_stable_ties() -> None:
    """Tie-breaking is stable: for items with equal sort key, the input's
    iteration order is preserved. We insert 'a', 'b', 'c' where 'a' and
    'c' share a sort key, and expect 'a' before 'c' on output."""
    # Insertion order: a (k=1), b (k=2), c (k=1).
    data: RF = RF(
        {
            "a": TF({"label": "a", "k": 1}),
            "b": TF({"label": "b", "k": 2}),
            "c": TF({"label": "c", "k": 1}),
        }
    )

    items: list[Item] = list(items_sorted_by(data, key=lambda item: item.value.k))

    # Ascending by k; 'a' and 'c' are tied at k=1, and 'a' came first in
    # the input, so it must come first in the output too:
    assert [item.value.label for item in items] == ["a", "c", "b"]
    assert [item.key for item in items] == ["a", "c", "b"]


def test_items_sorted_by_does_not_mutate_input() -> None:
    """items_sorted_by must leave its input AF untouched — same keys,
    same values, same iteration order before and after."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    keys_before: list = [item.key for item in users]
    names_before: list[str] = [item.value.name for item in users]

    # Fully consume the iterator to be sure any side effects would fire:
    _ = list(items_sorted_by(users, key=lambda item: item.value.yob))

    keys_after: list = [item.key for item in users]
    names_after: list[str] = [item.value.name for item in users]

    assert keys_before == keys_after
    assert names_before == names_after


def test_items_sorted_by_preserves_original_keys() -> None:
    """Explicit contrast to rank_by: with string input keys 'a', 'b', 'c',
    the yielded items carry those exact string keys — no integer ranks
    are substituted in."""
    data: RF = RF(
        {
            "a": TF({"score": 30}),
            "b": TF({"score": 10}),
            "c": TF({"score": 20}),
        }
    )

    items: list[Item] = list(items_sorted_by(data, key=lambda item: item.value.score))

    # Sorted ascending by score: b (10), c (20), a (30).
    assert [item.key for item in items] == ["b", "c", "a"]
    # And the keys are the original strings, not integers:
    for item in items:
        assert isinstance(item.key, str)


def test_items_sorted_by_rejects_none_key() -> None:
    """key=None must raise TypeError eagerly (on the call itself, before
    any iteration happens) and the message must mention 'callable'."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    with pytest.raises(TypeError, match="callable"):
        items_sorted_by(users, key=None)


def test_items_sorted_by_rejects_non_callable_key() -> None:
    """key=42 must raise TypeError on the call itself, NOT deferred until
    the caller starts iterating. This distinguishes a direct
    iter(sorted(...)) implementation from a generator-based one."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    # Deliberately do NOT iterate the result — the error must fire here:
    with pytest.raises(TypeError, match="callable"):
        items_sorted_by(users, key=42)


def test_items_sorted_by_propagates_sorted_typeerror() -> None:
    """If `key` returns mutually non-comparable values, the TypeError
    raised by Python's sorted() must propagate. Because items_sorted_by
    calls sorted() eagerly inside the function body before returning the
    iterator, this error fires on the call, not on the first next()."""
    # Two items whose values are TFs. TF does not define __lt__, so
    # sorted() will raise TypeError when it tries to compare them.
    data: RF = RF(
        {
            1: TF({"x": 1}),
            2: TF({"x": 2}),
        }
    )

    # The error fires on the call itself (eager sorted inside the body):
    with pytest.raises(TypeError):
        items_sorted_by(data, key=lambda item: item.value)


def test_items_sorted_by_after_rank_by() -> None:
    """Sanity check composition: items_sorted_by can consume an RF that
    itself came out of rank_by. Sorting a ranked RF by its (natural-
    number) key should yield the items in rank order."""
    db: DBF = _create_testdata(frozen=True)
    users: RF = db.users

    ranked: RF = rank_by(users, ranking_key=lambda item: item.value.yob).result

    items: list[Item] = list(items_sorted_by(ranked, key=lambda item: item.key))

    # rank_by puts Horst at rank 0, Tom at 1, John at 2:
    assert [item.key for item in items] == [0, 1, 2]
    assert [item.value.name for item in items] == ["Horst", "Tom", "John"]

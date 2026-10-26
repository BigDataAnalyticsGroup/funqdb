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

"""How a result is read, compared and displayed: rows under named columns."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from benchmarks.datasets import (
    FQLExecutionFailed,
    NotATable,
    NotExecuted,
    QueryResult,
    ReferenceValue,
    ResultMismatch,
)
from fdm.attribute_functions import DBF, RF, TF
from fql.operators.APIs import Operator


class _UnlistableTF(TF):
    """A tuple whose attributes cannot be listed."""

    def keys(self):
        raise RuntimeError()


def test_QueryResult_init_row_of_other_width_raises_ValueError():
    """A row of another width cannot be a row of the result."""
    with pytest.raises(ValueError, match="does not have the columns"):  # refused
        QueryResult(("a", "b"), [(1,)])  # one value for two columns


def test_QueryResult_init_keeps_decimal_and_float_as_given():
    """A DECIMAL stays a Decimal; a float keeps all its digits."""
    values = (Decimal("1.50"), 2007.8666666666666)  # as DuckDB and FDM give them

    result = QueryResult(("p", "f"), [values])  # stored as they are

    assert result.rows == (values,)  # both values unchanged, unrounded
    assert isinstance(result.rows[0][0], Decimal)  # still a Decimal


def test_QueryResult_of_fql_database_raises_NotATable():
    """Of the attribute functions, only a tuple and a relation read as rows."""
    with pytest.raises(NotATable, match="DBF"):  # names the type
        QueryResult.of_fql(DBF({}))  # the whole database


def test_QueryResult_of_fql_tuples_with_other_attributes_read_missing_as_None():
    """A row reads an attribute it does not map, but another row does, as None."""
    relation = RF({1: TF({"a": 1}), 2: TF({"a": 2, "b": 3})})  # b unmapped in row 1

    result = QueryResult.of_fql(relation)  # read

    assert result.columns == ("a", "b")  # every attribute, in first-seen order
    assert sorted(result.rows) == [(1, None), (2, 3)]  # the unmapped b as None


def test_QueryResult_of_fql_reference_value_raises_ReferenceValue():
    """A tuple-valued attribute raises `ReferenceValue`, naming the attribute."""
    genre = TF({"id": 1})  # a referenced tuple
    title = RF({1: TF({"id": 1, "genre_id": genre})})  # a swizzled reference

    with pytest.raises(ReferenceValue, match="'genre_id'") as raised:  # named
        QueryResult.of_fql(title)  # read

    assert raised.value.attribute == "genre_id"  # the caller can read the name


def test_QueryResult_of_fql_unlistable_attributes_raise_FQLExecutionFailed():
    """A failure to list attributes is a read error, named by the error type."""
    unlistable = "attributes .* cannot be read: RuntimeError$"  # the type, no ":"
    with pytest.raises(FQLExecutionFailed, match=unlistable) as raised:  # wrapped
        QueryResult.of_fql(_UnlistableTF({"a": 1}))  # its keys() fails

    assert isinstance(raised.value.__cause__, RuntimeError)  # kept as cause


def test_QueryResult_of_fql_unevaluable_attribute_raises_before_reference():
    """A read error wins over a reference, and keeps the engine error as cause."""
    row = TF({"r": TF({"id": 1})})  # a reference value
    row.add_computed("x", lambda tf: 1 // 0)  # and an attribute that fails

    unevaluable = "'x' cannot be evaluated: ZeroDivisionError"  # names x and cause
    with pytest.raises(FQLExecutionFailed, match=unevaluable) as raised:  # first
        QueryResult.of_fql(row)  # reads r and x, then checks r

    assert isinstance(raised.value.__cause__, ZeroDivisionError)  # kept as cause


def test_QueryResult_of_fql_failing_relation_entry_raises_FQLExecutionFailed():
    """An error while iterating the relation is a read error too."""
    relation = RF({1: TF({"a": 1})})  # one stored tuple
    relation.add_computed(2, lambda rf: 1 // 0)  # a computed tuple that fails

    with pytest.raises(FQLExecutionFailed, match="relation") as raised:  # wrapped
        QueryResult.of_fql(relation)  # iterates the relation

    assert isinstance(raised.value.__cause__, ZeroDivisionError)  # kept as cause


@pytest.mark.parametrize("value", [15, None, [1, 2]], ids=["scalar", "NULL", "list"])
def test_QueryResult_of_fql_other_value_is_one_row_in_unnamed_column(value):
    """Anything but an attribute function is one value, as in DuckDB."""
    result = QueryResult.of_fql(value)  # read

    assert result.columns == ("",)  # one unnamed column
    assert result.rows == ((value,),)  # holding the value as it is


def test_QueryResult_of_fql_empty_relation_has_no_columns():
    """An empty relation has no attributes to read."""
    result = QueryResult.of_fql(RF({}))  # nothing selected

    assert result.columns == ()  # no attributes
    assert result.rows == ()  # no rows


def test_QueryResult_of_fql_tuple_is_one_row_and_relation_one_row_per_tuple():
    """A relation's rows take the first tuple's attribute order."""
    heat = TF({"id": 1, "title": "Heat"})  # one FDM tuple
    ran = TF({"title": "Ran", "id": 2})  # another, its attributes in another order
    relation = RF({1: TF({"id": 1, "title": "Heat"}), 2: ran})  # two tuples

    one = QueryResult.of_fql(heat)  # read as one row
    many = QueryResult.of_fql(relation)  # read as two rows

    assert one.columns == ("id", "title")  # the tuple's attributes
    assert one.rows == ((1, "Heat"),)  # one row
    assert many.columns == ("id", "title")  # the first row's attribute order
    assert sorted(many.rows) == [(1, "Heat"), (2, "Ran")]  # aligned by name


@pytest.mark.parametrize(
    "fql_columns",
    [("id", "name"), ("id", "title", "extra")],  # a column SQL lacks, each time
    ids=["other name", "more columns"],  # fewer columns now align: missing is NULL
)
def test_QueryResult_check_matches_unaligned_columns_raise_ResultMismatch(
    fql_columns,
):
    """An FQL column SQL does not have never matches, even with equal values."""
    sql = QueryResult(("id", "title"), [(1, 1)])  # two columns
    values = (1,) * len(fql_columns)  # the same value in every column
    fql = QueryResult(fql_columns, [values])  # columns that do not align

    with pytest.raises(ResultMismatch):  # no alignment, no match
        sql.check_matches(fql)  # raises


@pytest.mark.parametrize(
    "sql_columns, fql_columns",
    [
        (("a", "a", "b"), ("a", "b")),
        (("a", "b"), ("a", "a", "b")),
        (("a", "a", "b"), ("a", "b", "b")),
    ],
    ids=["calling side", "other side", "both sides"],
)
def test_QueryResult_check_matches_duplicate_names_raise_ResultMismatch(
    sql_columns, fql_columns
):
    """A duplicate name never aligns, even when the column sets are equal."""
    sql = QueryResult(sql_columns, [(1,) * len(sql_columns)])  # set {a, b}
    fql = QueryResult(fql_columns, [(1,) * len(fql_columns)])  # set {a, b} too

    with pytest.raises(ResultMismatch):  # no alignment, no match
        sql.check_matches(fql)  # raises


def test_QueryResult_check_matches_rows_on_one_side_only_raise_ResultMismatch():
    """Rows against no rows are a mismatch, from either side."""
    rows = QueryResult(("x",), [(1,)])  # one row
    empty = QueryResult((), [])  # an empty relation

    with pytest.raises(ResultMismatch):  # FQL found nothing
        rows.check_matches(empty)  # raises
    with pytest.raises(ResultMismatch):  # SQL found nothing
        empty.check_matches(rows)  # raises


def test_QueryResult_check_matches_date_matches_only_date():
    """A date matches only a date: neither its ISO text nor a timestamp at midnight."""
    fql = QueryResult(("d",), [(date(1995, 12, 15),)])  # the FDM date
    text = QueryResult(("d",), [("1995-12-15",)])  # its ISO text
    midnight = QueryResult(("d",), [(datetime(1995, 12, 15),)])  # a timestamp

    with pytest.raises(ResultMismatch):  # text is no date
        text.check_matches(fql)  # raises
    with pytest.raises(ResultMismatch):  # nor is a timestamp at midnight
        midnight.check_matches(fql)  # raises


def test_QueryResult_check_matches_mismatch_names_its_reason():
    """The error says whether the columns, the rows or their order differ."""
    sql = QueryResult(("title", "id"), [("Heat", 1)])  # the reference
    renamed = QueryResult(("name", "id"), [("Heat", 1)])  # another column name
    other_row = QueryResult(("id", "title"), [(2, "Ran")])  # another row

    columns = r"\['name', 'id'\] do not align"  # the FQL columns, named
    with pytest.raises(ResultMismatch, match=columns):  # the columns differ
        sql.check_matches(renamed)  # names both column lists
    with pytest.raises(ResultMismatch, match="^the rows differ$"):  # values
        sql.check_matches(other_row)  # aligned, but not equal
    with pytest.raises(ResultMismatch, match="order"):  # ordered: the order counts
        sql.check_matches(other_row, ordered=True)  # aligned, but not equal


def test_QueryResult_check_matches_two_empty_results_match_whatever_columns():
    """Two empty results match, also when only one side knows its columns."""
    sql = QueryResult(("title",), [])  # a SELECT that found nothing
    fql = QueryResult((), [])  # an empty relation

    sql.check_matches(fql)  # passes: nothing on either side


def test_QueryResult_check_matches_aligns_columns_by_name():
    """The same columns in another order hold the same rows."""
    sql = QueryResult(("title", "id"), [("Heat", 1), ("Ran", 2)])  # SQL order
    fql = QueryResult(("id", "title"), [(2, "Ran"), (1, "Heat")])  # FQL order

    sql.check_matches(fql)  # passes: aligned by name, compared as a multiset


def test_QueryResult_check_matches_missing_column_matches_only_NULL():
    """A column FQL does not have reads as None: SQL's NULL, and nothing else."""
    fql = QueryResult(("id",), [(1,)])  # the note is unmapped
    null_note = QueryResult(("id", "note"), [(1, None)])  # SQL: the note is NULL
    some_note = QueryResult(("id", "note"), [(1, "x")])  # SQL: the note is 'x'

    null_note.check_matches(fql)  # passes: the missing note is NULL
    with pytest.raises(ResultMismatch, match="^the rows differ$"):  # aligned, unequal
        some_note.check_matches(fql)  # a missing note is not 'x'


def test_QueryResult_check_matches_attribute_unmapped_in_every_row_matches_NULL_column():
    """An attribute no FQL row maps still lines up with an all-NULL SQL column."""
    relation = RF({1: TF({"id": 1}), 2: TF({"id": 2})})  # no row maps the note
    fql = QueryResult.of_fql(relation)  # its columns are ('id',) only
    sql = QueryResult(("id", "note"), [(1, None), (2, None)])  # every note NULL

    sql.check_matches(fql)  # passes: the SQL columns define the row shape


def test_QueryResult_check_matches_tuple_without_attributes_matches_NULL_row():
    """A tuple that maps nothing is one row of NULLs, as an aggregate over no rows."""
    fql = QueryResult.of_fql(TF({}))  # one row, no attributes
    sql = QueryResult(("s",), [(None,)])  # SELECT SUM(...) over no rows

    sql.check_matches(fql)  # passes: the missing s is NULL


def test_QueryResult_check_matches_stored_None_matches_NULL():
    """A stored None, as a computed value gives it, still matches SQL's NULL."""
    fql = QueryResult.of_fql(TF({"id": 1, "c": None}))  # c is mapped to None
    sql = QueryResult(("id", "c"), [(1, None)])  # SQL: c is NULL

    sql.check_matches(fql)  # passes: None equals NULL


def test_QueryResult_check_matches_aligns_one_column_by_position():
    """A scalar's unnamed column aligns with any single column by position; two
    named single columns align only by name."""
    sql = QueryResult(("c",), [(15,)])  # SELECT COUNT(*) AS c
    fql = QueryResult(("",), [(15,)])  # the scalar FQL value
    renamed = QueryResult(("n",), [(15,)])  # a named column, another name
    other = QueryResult(("",), [(16,)])  # another scalar value

    sql.check_matches(fql)  # passes: positions agree
    fql.check_matches(sql)  # passes: from the other side, too
    with pytest.raises(ResultMismatch):  # two names that differ
        sql.check_matches(renamed)  # raises, although the values agree
    with pytest.raises(ResultMismatch):  # values still count
        sql.check_matches(other)  # raises


def test_QueryResult_check_matches_statement_result_raises_ResultMismatch():
    """A statement without rows has no result: it matches nothing, not even an
    empty result or another statement."""
    statement = QueryResult((), (), from_statement=True)  # e.g. FQL `del dbf.t[1]`
    empty = QueryResult(("title",), [])  # a SELECT that found nothing

    with pytest.raises(ResultMismatch, match="statement"):  # on the other side
        empty.check_matches(statement)  # raises
    with pytest.raises(ResultMismatch, match="statement"):  # on the calling side
        statement.check_matches(empty)  # raises
    with pytest.raises(ResultMismatch, match="statement"):  # on both sides
        statement.check_matches(statement)  # raises


def test_QueryResult_check_matches_different_numbers_raise_ResultMismatch():
    """Numbers compare exactly: however small the difference, it is a mismatch."""
    sql = QueryResult(("avg",), [(1.0,)])  # the exact average
    off = QueryResult(("avg",), [(1.001,)])  # off by 1e-3
    big_id = QueryResult(("id",), [(10**12,)])  # a large integer key
    next_id = QueryResult(("id",), [(10**12 + 1,)])  # one more

    with pytest.raises(ResultMismatch):  # a real difference
        sql.check_matches(off)  # raises
    with pytest.raises(ResultMismatch):  # ints compare exactly
        big_id.check_matches(next_id)  # raises


def test_QueryResult_check_matches_compares_multiset_unless_ordered():
    """Without ORDER BY the order is free; duplicates still count."""
    sql = QueryResult(("x",), [(1,), (2,), (2,)])  # the reference rows
    swapped = QueryResult(("x",), [(2,), (1,), (2,)])  # same multiset
    fewer = QueryResult(("x",), [(1,), (2,)])  # one duplicate less

    sql.check_matches(swapped)  # passes: a multiset by default
    with pytest.raises(ResultMismatch):  # but not row by row
        sql.check_matches(swapped, ordered=True)  # raises
    sql.check_matches(sql, ordered=True)  # passes: equal rows in equal order
    with pytest.raises(ResultMismatch):  # multiplicities differ
        sql.check_matches(fewer)  # raises


class _PendingOperator(Operator):
    """An operator that was never applied."""

    def _compute(self):
        raise AssertionError("must not run")


def test_QueryResult_of_fql_unexecuted_operator_raises_NotExecuted():
    """An operator is not a value: it must be called to give its result."""
    with pytest.raises(NotExecuted, match="_PendingOperator"):  # named in the message
        QueryResult.of_fql(_PendingOperator())  # not called, so no result


def test_QueryResult_check_matches_unorderable_values_do_not_raise_TypeError():
    """Values that Python cannot order still compare, as a multiset."""
    sql = QueryResult(("v",), [([1],), ([2],)])  # lists cannot be ordered with <
    fql = QueryResult(("v",), [([2],), ([1],)])  # the same rows, other order

    sql.check_matches(fql)  # passes: equal as multisets


def test_QueryResult_check_matches_different_exact_values_raise_ResultMismatch():
    """Two different texts never match, whatever the other values are."""
    sql = QueryResult(("t", "f"), [("a", 1.0)])  # text a
    fql = QueryResult(("t", "f"), [("b", 1.0)])  # text b, same float

    with pytest.raises(ResultMismatch):  # the texts differ
        sql.check_matches(fql)  # refuses


def test_QueryResult_check_matches_decimal_does_not_match_float_error():
    """A Decimal equals the same Decimal, not the float nearest to it."""
    sql = QueryResult(("p",), [(Decimal("0.10"),)])  # DuckDB's DECIMAL
    same = QueryResult(("p",), [(Decimal("0.1"),)])  # the FDM's Decimal, another scale
    as_float = QueryResult(("p",), [(0.1,)])  # a float, which is not exactly 0.1

    sql.check_matches(same)  # passes: equal as numbers
    with pytest.raises(ResultMismatch):  # a float is not the Decimal
        sql.check_matches(as_float)  # raises


def test_QueryResult_check_matches_duplicate_rows_count_on_both_sides():
    """A multiset: a row twice on one side is not the same as once."""
    twice = QueryResult(("x",), [(1,), (1,)])  # one row twice
    once = QueryResult(("x",), [(1,)])  # the same row once
    other = QueryResult(("x",), [(1,), (2,)])  # same length as `twice`

    with pytest.raises(ResultMismatch):  # different lengths
        twice.check_matches(once)  # raises
    with pytest.raises(ResultMismatch):  # a row on one side only
        twice.check_matches(other)  # raises


def test_QueryResult_check_matches_extra_row_on_other_side_raises_ResultMismatch():
    """Every row of the first result is found, but the other side has one more."""
    fewer = QueryResult(("x",), [(1,)])  # one row
    more = QueryResult(("x",), [(1,), (2,)])  # that row, and another

    with pytest.raises(ResultMismatch):  # the surplus row is a difference
        fewer.check_matches(more)  # raises

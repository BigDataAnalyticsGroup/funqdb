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

"""The expression tree: what it reads, how it is walked and how it is rewritten."""

import pytest

from benchmarks.datasets.dataset_types import Reference
from benchmarks.datasets.schema import Schema
from benchmarks.tools.ir.expressions import (
    AggregateExpression,
    BetweenExpression,
    BinaryExpression,
    CaseBranch,
    CaseWhenExpression,
    CastExpression,
    Column,
    ExistsExpression,
    ExtractExpression,
    ExtractPart,
    InExpression,
    InSubqueryExpression,
    IntervalUnit,
    IsBooleanExpression,
    IsNullExpression,
    LikeExpression,
    Literal,
    ParenExpression,
    ReferencedTupleExpression,
    SubstringExpression,
    TupleIdentityExpression,
    UnaryExpression,
    conjunction,
)
from benchmarks.tools.ir.join_tree import AliasGraph
from benchmarks.tools.ir.paths import Path
from benchmarks.tools.ir.query import Query

# the tuple of movie_companies AS mc, the only root
MC = Path(0, "mc", "mc", ())
# a query over title AS t alone, for a subquery no test looks into
TITLE_ONLY = AliasGraph(0, Schema({}, {}, {}), {"t": "title"}, (), ()).tree()
# t read from mc, and the same t read from mi after a rebase
FROM_MC = Path(0, "t", "mc", ("movie_id",))
FROM_MI = Path(0, "t", "mi", ("movie_id",))
# stands for a parsed subquery inside an IN, which map_paths never enters
SUBQUERY = object()
# the key of a tuple that a written foreign key refers to
KEY = Literal(7)


def rebase_to_mi(path: Path) -> Path:
    """`path` with t read from mi instead of mc; any other path unchanged."""
    return FROM_MI if path == FROM_MC else path  # only FROM_MC moves


def test_expression_conjuncts_flattens_an_and_chain_only():
    """An `and`-chain splits into its operands, any other expression is one."""
    a = Column(MC, "a")  # first plain operand
    b = Column(MC, "b")  # second operand
    c = Column(MC, "c")  # third operand
    a_and_b = BinaryExpression("and", a, b)  # a and b
    chain = BinaryExpression("and", a_and_b, c)  # (a and b) and c
    either = BinaryExpression("or", a, b)  # not a conjunction

    assert chain.conjuncts() == [a, b, c]  # flattened left to right
    assert either.conjuncts() == [either]  # an `or` stays whole
    assert a.conjuncts() == [a]  # a leaf is its own conjunct


def test_expression_conjuncts_split_an_and_chain_in_parentheses():
    """`(a and b) and c` and `(a and b)` split like the chain without parentheses."""
    a = Column(MC, "a")  # first operand
    b = Column(MC, "b")  # second operand
    c = Column(MC, "c")  # third operand
    inner = ParenExpression(BinaryExpression("and", a, b))  # (a and b)
    chain = BinaryExpression("and", inner, c)  # (a and b) and c
    either = ParenExpression(BinaryExpression("or", a, b))  # (a or b)

    assert inner.conjuncts() == [a, b]  # the parentheses do not hide the and
    assert chain.conjuncts() == [a, b, c]  # also nested in a chain
    assert either.conjuncts() == [either]  # an `or` in parentheses stays whole


@pytest.mark.parametrize(
    "wrap",
    [
        lambda column: column,
        lambda column: UnaryExpression("not", column),
        lambda column: BinaryExpression("<", column, column),
        lambda column: BetweenExpression(column, Literal(1), column),
        lambda column: InExpression(column, (Literal(1), column)),
        lambda column: InSubqueryExpression(column, SUBQUERY),
        lambda column: LikeExpression(column, "a%"),
        lambda column: IsNullExpression(column),
        lambda column: IsBooleanExpression(column, True),
        lambda column: ParenExpression(column),
        lambda column: CaseWhenExpression((CaseBranch(column, column),), column),
        lambda column: SubstringExpression(column, 1, 2),
        lambda column: ExtractExpression(ExtractPart.YEAR, column),
        lambda column: CastExpression(column),
        lambda column: TupleIdentityExpression(column, Column.tuple_at(column.path)),
        lambda column: TupleIdentityExpression(column, column),
        lambda column: AggregateExpression("sum", column),
    ],
)
def test_expression_map_paths_rebases_a_column_inside_each_expression_type(wrap):
    """Every path inside any expression is rebased, wherever the type keeps it."""
    before = wrap(Column(FROM_MC, "production_year"))  # t's year, read from mc
    after = wrap(Column(FROM_MI, "production_year"))  # the same year from mi

    assert before.map_paths(rebase_to_mi) == after  # every occurrence rebased


def test_expression_map_paths_keeps_the_paths_of_a_subquery():
    """A subquery resolves its own columns, so its paths are not rebased."""
    year = Column(FROM_MC, "production_year")  # t's year, read from mc
    subquery = Query(TITLE_ONLY, stated_condition=year)  # WHERE t.year
    exists = ExistsExpression(subquery)  # EXISTS (subquery)

    rebased = exists.map_paths(rebase_to_mi)  # rebase around the subquery

    assert rebased.subquery.condition == year  # still read from mc


def test_expression_paths_of_an_output_name_are_empty():
    """An output name has no path to rebase."""
    output = Column(None, "_aggregate_0")  # an aggregate result, read by name

    assert output.paths() == []  # no path, so nothing to rebase


def test_expression_paths_list_every_path_map_paths_rebases():
    """The read and the tuple of an identity are both paths; a subquery's are not."""
    read = Column(FROM_MC, "kind_id")  # t.kind_id, read from mc
    itself = Column.tuple_at(FROM_MI)  # the tuple t itself, reached from mi
    identity = TupleIdentityExpression(read, itself)  # t.kind_id is t (from mi)
    condition = InSubqueryExpression(read, SUBQUERY)  # t.kind_id IN (subquery)

    assert identity.paths() == [FROM_MC, FROM_MI]  # in field order
    assert condition.paths() == [FROM_MC]  # the subquery is not entered


def test_expression_paths_list_the_paths_inside_case_branches():
    """A CASE keeps its reads inside `CaseBranch`es, which the walk enters too."""
    read_from_mc = Column(FROM_MC, "kind_id")  # t.kind_id, read from mc
    read_from_mi = Column(FROM_MI, "title")  # t.title, read from mi
    note = Column(MC, "note")  # mc.note, the default
    branch = CaseBranch(read_from_mc, read_from_mi)  # WHEN t.kind_id THEN t.title
    case = CaseWhenExpression((branch,), note)  # CASE ... ELSE mc.note

    assert case.paths() == [FROM_MC, FROM_MI, MC]  # condition, result, default


def test_expression_nested_expressions_do_not_descend_into_a_subquery():
    """A subquery resolves its own columns, so it is yielded whole."""
    subquery = Query(TITLE_ONLY)  # a query over title
    exists = ExistsExpression(subquery)  # EXISTS (subquery)

    nested = list(exists.nested_expressions())  # the whole tree, depth first

    assert nested == [exists]  # nothing below it


def test_expression_unparenthesized_strips_every_level_of_parentheses():
    """Nested parentheses go away; an expression without them stays as it is."""
    note = Column(MC, "note")  # the bare expression
    single = ParenExpression(note)  # (note)
    double = ParenExpression(single)  # ((note))

    assert double.unparenthesized() == note  # both levels stripped
    assert note.unparenthesized() is note  # nothing to strip


def test_expression_is_null_literal_is_true_for_the_NULL_literal_only():
    """Only SQL's NULL literal; a falsy value or a column is no NULL."""
    null = Literal(None)  # NULL
    parenthesized_null = ParenExpression(null)  # (NULL)
    zero = Literal(0)  # falsy, but a value
    empty = Literal("")  # falsy, but a value
    note = Column(MC, "note")  # NULL only at run time, if ever

    assert null.is_null_literal()  # NULL
    assert parenthesized_null.is_null_literal()  # (NULL) is NULL too
    assert not zero.is_null_literal()  # a value
    assert not empty.is_null_literal()  # a value
    assert not note.is_null_literal()  # not a literal


def test_expression_is_comparison_is_true_for_comparing_expressions_only():
    """Comparisons, BETWEEN, IN, LIKE and IS TRUE compare; arithmetic and logic do not."""
    note = Column(MC, "note")  # any operand
    one = Literal(1)  # any value
    less = BinaryExpression("<", note, one)  # note < 1
    between = BetweenExpression(note, one, one)  # note BETWEEN 1 AND 1
    membership = InExpression(note, (one,))  # note IN (1)
    like = LikeExpression(note, "a%")  # note LIKE 'a%'
    is_true = IsBooleanExpression(note, True)  # note IS TRUE
    plus = BinaryExpression("+", note, one)  # arithmetic
    both = BinaryExpression("and", note, one)  # logic
    is_null = IsNullExpression(note)  # a definedness test

    assert less.is_comparison()  # note < 1
    assert between.is_comparison()  # note BETWEEN 1 AND 1
    assert membership.is_comparison()  # note IN (1)
    assert like.is_comparison()  # note LIKE 'a%'
    assert is_true.is_comparison()  # note IS TRUE
    assert not plus.is_comparison()  # arithmetic
    assert not both.is_comparison()  # logic
    assert not is_null.is_comparison()  # a definedness test
    assert not note.is_comparison()  # a plain column
    assert InSubqueryExpression(note, SUBQUERY).is_comparison()  # note IN (subquery)


def test_expression_null_tested_column_is_the_column_of_IS_NULL_only():
    """`column IS NULL` yields its column; anything else, None."""
    note = Column(MC, "note")  # a nullable column
    is_null = IsNullExpression(note)  # note IS NULL
    is_not_null = UnaryExpression("not", is_null)  # note IS NOT NULL
    plus = BinaryExpression("+", note, Literal(1))  # note + 1
    computed = IsNullExpression(plus)  # (note + 1) IS NULL

    assert is_null.null_tested_column() is note  # IS NULL over a column
    assert is_not_null.null_tested_column() is None  # IS NOT NULL
    assert computed.null_tested_column() is None  # over a computed value
    assert note.null_tested_column() is None  # no IS test at all


def test_expression_null_tested_column_sees_through_parentheses():
    """`(note IS NULL)` tests note, and so `NOT (note IS NULL)` is IS NOT NULL."""
    note = Column(MC, "note")  # a nullable column
    is_null = ParenExpression(IsNullExpression(note))  # (note IS NULL)
    is_not_null = UnaryExpression("not", is_null)  # NOT (note IS NULL)
    plain = ParenExpression(note)  # (note), no test

    assert is_null.null_tested_column() is note  # the parentheses do not hide it
    assert is_not_null.not_null_tested_column() is note  # nor below a NOT
    assert plain.null_tested_column() is None  # no IS NULL inside


def test_expression_not_null_tested_column_is_the_column_of_IS_NOT_NULL_only():
    """`column IS NOT NULL` (NOT over IS NULL) yields its column; else None."""
    note = Column(MC, "note")  # a nullable column
    is_null = IsNullExpression(note)  # note IS NULL
    is_not_null = UnaryExpression("not", is_null)  # note IS NOT NULL
    plus = BinaryExpression("+", note, Literal(1))  # note + 1
    computed = IsNullExpression(plus)  # (note + 1) IS NULL
    not_computed = UnaryExpression("not", computed)  # (note + 1) IS NOT NULL
    not_note = UnaryExpression("not", note)  # NOT note

    assert is_not_null.not_null_tested_column() is note  # IS NOT NULL
    assert is_null.not_null_tested_column() is None  # IS NULL is not its negation
    assert not_computed.not_null_tested_column() is None  # computed
    assert not_note.not_null_tested_column() is None  # NOT note
    negated = UnaryExpression("-", is_null)  # a minus sign is no negated test
    assert negated.not_null_tested_column() is None  # only NOT


def test_expression_compared_columns_of_a_comparison_are_its_operand_columns():
    """`note < year + 1` compares both columns; a literal adds none."""
    note = Column(MC, "note")  # left operand
    year = Column(MC, "year")  # read inside the arithmetic right operand
    plus = BinaryExpression("+", year, Literal(1))  # year + 1
    comparison = BinaryExpression("<", note, plus)  # note < year + 1

    assert list(comparison.compared_columns({})) == [note, year]  # left first


def test_expression_compared_columns_of_IN_skip_the_options():
    """In `note IN (year)` only note is compared; the options are not guarded."""
    note = Column(MC, "note")  # the compared value
    year = Column(MC, "year")  # an option
    membership = InExpression(note, (year,))  # note IN (year)

    assert list(membership.compared_columns({})) == [note]  # the value only


def test_expression_compared_columns_read_a_replaced_operand_as_its_column():
    """An operand in `replaced`, even an aggregate, is read as its column."""
    note = Column(MC, "note")  # the column the aggregate reads
    count = AggregateExpression("count", note)  # count(note)
    key = Column(None, "_aggregate_0")  # the output that holds count(note)
    comparison = BinaryExpression(">", count, Literal(1))  # count(note) > 1

    replaced = list(comparison.compared_columns({count: key}))  # count read as key
    unreplaced = list(comparison.compared_columns({}))  # count left as it is

    assert replaced == [key]  # replaced
    assert unreplaced == []  # an aggregate tests itself


def test_expression_compared_columns_skip_operands_that_test_themselves():
    """A nested comparison, a CASE and an IS NULL guard their own columns."""
    note = Column(MC, "note")  # read only inside self-testing operands
    one = Literal(1)  # any value
    nested = BinaryExpression("<", note, one)  # note < 1
    branch = CaseBranch(nested, note)  # WHEN note < 1 THEN note
    case = CaseWhenExpression((branch,), one)  # CASE WHEN note < 1 THEN note ELSE 1
    null_test = IsNullExpression(note)  # note IS NULL
    over_nested = BinaryExpression("==", nested, one)  # (note < 1) == 1
    over_case = BinaryExpression("==", case, one)  # CASE ... == 1
    over_null_test = BinaryExpression("==", null_test, one)  # (note IS NULL) == 1

    assert list(over_nested.compared_columns({})) == []  # nested comparison
    assert list(over_case.compared_columns({})) == []  # CASE
    assert list(over_null_test.compared_columns({})) == []  # IS NULL


@pytest.mark.parametrize(
    ("make", "children"),
    [
        (lambda a, b, c: UnaryExpression("-", a), lambda a, b, c: (a,)),
        (lambda a, b, c: BinaryExpression("+", a, b), lambda a, b, c: (a, b)),
        (lambda a, b, c: BetweenExpression(a, b, c), lambda a, b, c: (a, b, c)),
        (lambda a, b, c: InExpression(a, (b, c)), lambda a, b, c: (a, b, c)),
        (lambda a, b, c: InSubqueryExpression(a, SUBQUERY), lambda a, b, c: (a,)),
        (lambda a, b, c: LikeExpression(a, "x"), lambda a, b, c: (a,)),
        (lambda a, b, c: IsNullExpression(a), lambda a, b, c: (a,)),
        (lambda a, b, c: IsBooleanExpression(a, True), lambda a, b, c: (a,)),
        (lambda a, b, c: ParenExpression(a), lambda a, b, c: (a,)),
        (lambda a, b, c: SubstringExpression(a, 1), lambda a, b, c: (a,)),
        (lambda a, b, c: ExtractExpression(ExtractPart.DAY, a), lambda a, b, c: (a,)),
        (lambda a, b, c: CastExpression(a), lambda a, b, c: (a,)),
        (lambda a, b, c: Literal(1), lambda a, b, c: ()),
        (lambda a, b, c: ExistsExpression(SUBQUERY), lambda a, b, c: ()),
        (
            lambda a, b, c: ReferencedTupleExpression(Reference("kind", "kind"), KEY),
            lambda a, b, c: (KEY,),
        ),
    ],
)
def test_expression_children_are_the_operands_of_each_expression_type(make, children):
    """Each type names its own operands in order, and never a subquery."""
    a, b, c = Column(MC, "a"), Column(MC, "b"), Column(MC, "c")  # three distinct reads

    assert make(a, b, c).children() == children(a, b, c)  # all of them, in field order


def test_case_when_expression_children_without_a_branch_or_default_are_empty():
    """A CASE with no branch and no default has nothing below it."""
    case = CaseWhenExpression(branches=())  # no branch, no default

    assert case.children() == ()  # nothing


def test_case_when_expression_nested_expressions_walk_every_branch():
    """A CASE yields its conditions, its results and its default, in that order."""
    condition, result, default = Literal(True), Literal(1), Literal(0)  # the parts
    case = CaseWhenExpression(
        branches=(CaseBranch(condition, result),), default=default
    )  # one branch plus a default

    nested = list(case.nested_expressions())  # the whole tree, depth first

    assert nested == [case, condition, result, default]  # itself, then its parts


def test_tuple_identity_expression_children_of_a_read_against_a_tuple_are_both_reads():
    """The tuple itself is read as a column without a name, so it is a child."""
    t = Path(0, "t", "t", ())  # the root tuple of t
    read = Column(t, "episode_of_id", True)  # t.episode_of_id
    itself = Column.tuple_at(t)  # the tuple t itself
    identity = TupleIdentityExpression(read, itself)  # t.episode_of_id is t

    assert identity.children() == (read, itself)  # both operands


def test_column_tuple_at_reads_the_tuple_at_the_end_of_its_path():
    """The tuple itself is a column without a name, and not a reference read."""
    t = Path(0, "t", "mc", ("movie_id",))  # t, reached from mc

    itself = Column.tuple_at(t)  # the tuple t

    assert itself == Column(t, "", False)  # no attribute name, no reference flag


@pytest.mark.parametrize(
    ("unit", "is_sub_day"),
    [
        (IntervalUnit.SECOND, True),  # a timedelta smaller than a day
        (IntervalUnit.MINUTE, True),  # a timedelta smaller than a day
        (IntervalUnit.HOUR, True),  # a timedelta smaller than a day
        (IntervalUnit.DAY, False),  # a timedelta, but a whole day
        (IntervalUnit.MONTH, False),  # a relativedelta
        (IntervalUnit.YEAR, False),  # a relativedelta
    ],
)
def test_interval_unit_is_sub_day_holds_for_units_smaller_than_a_day(unit, is_sub_day):
    """DAY shares its delta type with the sub-day units, but is none of them."""
    assert unit.is_sub_day is is_sub_day  # one unit per row


def test_aggregate_expression_children_are_its_argument_if_any():
    """`COUNT(mc.id)` has its argument below it, `COUNT(*)` nothing."""
    argument = Column(MC, "id")  # mc.id
    counted = AggregateExpression("count", argument)  # COUNT(mc.id)
    count_star = AggregateExpression("count", None)  # COUNT(*)

    assert counted.children() == (argument,)  # the argument
    assert count_star.children() == ()  # COUNT(*): no argument


def test_conjunction_combines_its_operands_from_the_left():
    """Three operands become `((a and b) and c)`; one operand is itself."""
    a, b, c = Literal(1), Literal(2), Literal(3)  # three operands
    a_and_b = BinaryExpression("and", a, b)  # a and b
    expected = BinaryExpression("and", a_and_b, c)  # (a and b) and c

    assert conjunction(a, b, c) == expected  # left-nested
    assert conjunction(a) is a  # the operand itself

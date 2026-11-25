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

"""When a filter renders as structured predicates, and when it falls back to a lambda."""

from collections.abc import Iterator

import pytest

from benchmarks.datasets.dataset import LoadedDataset
from benchmarks.datasets.movies.movies_dataset import MOVIES
from benchmarks.datasets.schema import Schema
from benchmarks.datasets.special_names.special_names_dataset import SPECIAL_NAMES
from benchmarks.tools.render.config import Config, PredicateForm
from benchmarks.tools.render.structured_predicates import StructuredPredicates
from tests.benchmarks.tools.framework.parsing import query_from_sql
from tests.benchmarks.tools.framework.rendering import (
    check_fql_reads_as_sql,
    fql_of,
    run_on_both_sides,
)

STRUCTURED = Config(predicate_form=PredicateForm.STRUCTURED)  # the style under test
NO_COLUMNS = Schema(references={}, primary_keys={}, columns={})  # types unknown
# A SELECT's WHERE on `title`, and the predicate the renderer gives it. The optimizer
# rewrites a SELECT first, so BETWEEN and NOT arrive as plain comparisons.
SELECT_FILTERS = [
    pytest.param(
        "t.production_year BETWEEN 1990 AND 2005",
        "And(Lte('production_year', 2005), Gte('production_year', 1990))",
        id="between",
    ),  # a range, as two comparisons
    pytest.param(
        "NOT (t.production_year > 2000 OR t.title = 'Heat')",
        "And(Lte('production_year', 2000), Not(Eq('title', 'Heat')))",
        id="not-or",
    ),  # De Morgan by the optimizer, `<>` as Not(Eq)
    pytest.param(
        "t.title IN ('The Matrix', 'Heat')",
        "In('title', ['The Matrix', 'Heat'])",
        id="in-literals",
    ),  # a list of literals
    pytest.param(
        "t.production_year > t.id", "Lt('id', Ref('production_year'))", id="ref"
    ),  # two attributes: one is read by Ref
    pytest.param(
        "t.imdb_index IS NULL", "Missing('imdb_index')", id="is-null"
    ),  # an unmapped attribute
    pytest.param(
        "t.imdb_index IS NOT NULL", "Exists('imdb_index')", id="is-not-null"
    ),  # a mapped attribute
    pytest.param(
        "t.title LIKE 'The%'", "Like('title', 'The%')", id="like"
    ),  # a pattern Like supports
    pytest.param(
        "t.title LIKE '_he%'", "lambda", id="like-regex-falls-back"
    ),  # `_` is no Like pattern
    pytest.param(
        "t.production_year IN (1999, t.id)", "lambda", id="in-column-falls-back"
    ),  # In takes literals only
]
# A WHERE on the far end of the reference `company_id`, which leads from a
# movie company to its company. Each predicate holds for a movie company without a
# company; the test `Exists('company_id')` drops it, as the join does.
REFERENCED_CONDITIONS = [
    pytest.param(
        "cn.country_code <> '[us]'",
        "Not(Eq('company_id__country_code', '[us]'))",
        id="not-equal",
    ),  # a negated comparison
    pytest.param(
        "cn.country_code IS NULL", "Missing('company_id__country_code')", id="is-null"
    ),  # an unmapped attribute
    pytest.param(
        "NOT (cn.name LIKE '%a%')",
        "Not(Like('company_id__name', '%a%'))",
        id="not-like",
    ),  # a negated pattern
]
# A DELETE's WHERE on `order details`. A write statement skips the optimizer, so its
# condition arrives as written.
DELETE_FILTERS = [
    pytest.param(
        '"class" BETWEEN 2 AND 3',
        "And(Gte('class', 2), Lte('class', 3))",
        id="between",
    ),  # BETWEEN itself
    pytest.param(
        'NOT ("class" = 2)', "Not(Eq('class', 2))", id="not"
    ),  # NOT itself, through the parentheses
    pytest.param(
        '("class" > 2 AND id < 3) OR id = 1',
        "Or(And(Gt('class', 2), Lt('id', 3)), Eq('id', 1))",
        id="and-in-or",
    ),  # a conjunction nested in a disjunction
    pytest.param(
        '2 < "class"', "Gt('class', 2)", id="flipped"
    ),  # the literal moves right, the comparison flips
    pytest.param(
        '("class" > 2) IS TRUE', "lambda", id="is-true-falls-back"
    ),  # IS TRUE of a comparison has no attribute to compare
    pytest.param(
        "project LIKE 'A%'", "lambda", id="method-name-falls-back"
    ),  # `project` would name the FDM method in a structured path
]


@pytest.fixture(scope="module")
def movies() -> Iterator[LoadedDataset]:
    """The `movies` dataset, shared by the tests below that only read it."""
    dataset = MOVIES.open()  # the same rows on both sides
    yield dataset  # read, never written
    dataset.close()  # releases the DuckDB connection


@pytest.fixture
def orders() -> Iterator[LoadedDataset]:
    """A private `special_names` dataset, so a DELETE does not leak into other tests."""
    dataset = SPECIAL_NAMES.open()  # fresh rows on both sides
    yield dataset  # the test deletes from it
    dataset.close()  # releases the DuckDB connection


@pytest.mark.parametrize(("condition", "predicate"), REFERENCED_CONDITIONS)
def test_StructuredPredicates_render_renders_a_negation_through_a_reference_after_its_existence_test(
    condition, predicate, movies
):
    """A negation or a NULL test on a referenced tuple stays structured."""
    sql = (
        "SELECT mc.note FROM movie_companies AS mc, company_name AS cn "
        f"WHERE mc.company_id = cn.id AND {condition}"
    )  # the filter is on cn, read from mc

    fql = fql_of(sql, movies.schema(), STRUCTURED)  # the structured rendering

    assert f"And(Exists('company_id'), {predicate})" in fql  # test first, no lambda
    check_fql_reads_as_sql(sql, movies, STRUCTURED)  # the same rows as DuckDB


def test_StructuredPredicates_render_renders_unequal_when_the_right_side_reads_through_a_reference(
    movies,
):
    """`<>` with the reference read on the right side compares with a `Ref`."""
    sql = (
        "SELECT mc.note FROM movie_companies AS mc, company_name AS cn "
        "WHERE mc.company_id = cn.id AND mc.note <> cn.country_code"
    )  # unoptimized, so cn.country_code stays on the right
    query = query_from_sql(sql, movies.schema())  # the IR as written
    predicates = StructuredPredicates(query, False, False)  # a WHERE on mc's tuples
    test, unequal = query.condition.conjuncts()  # the reference test, then the `<>`

    rendered_test = predicates.render(test)  # the reference test
    rendered_unequal = predicates.render(unequal)  # the `<>` conjunct

    assert rendered_test == "Exists('company_id')"  # drops a row without a company
    assert (
        rendered_unequal == "Not(Eq('note', Ref('company_id__country_code')))"
    )  # true without a company too, so it needs the test above


@pytest.mark.parametrize(("condition", "predicate"), SELECT_FILTERS)
def test_StructuredPredicates_render_renders_a_SELECT_filter(
    condition, predicate, movies
):
    """The filter renders as `predicate` and reads the rows DuckDB reads."""
    sql = f"SELECT t.title FROM title AS t WHERE {condition}"  # one filter on title

    fql = fql_of(sql, movies.schema(), STRUCTURED)  # the structured rendering

    assert predicate in fql  # the expected predicate, or the lambda fallback
    check_fql_reads_as_sql(sql, movies, STRUCTURED)  # the same rows as DuckDB


def test_StructuredPredicates_render_renders_a_condition_on_a_referenced_tuple_by_its_path(
    movies,
):
    """A condition on the far end of a reference reads it by its path."""
    sql = (
        "SELECT mc.note FROM movie_companies AS mc, title AS t "
        "WHERE mc.movie_id = t.id AND t.production_year > 2000"
    )  # the filter is on t, read from mc

    fql = fql_of(sql, movies.schema(), STRUCTURED)  # the structured rendering

    assert "Gt('movie_id__production_year', 2000)" in fql  # the path from mc
    check_fql_reads_as_sql(sql, movies, STRUCTURED)  # the same rows as DuckDB


def test_StructuredPredicates_render_renders_a_HAVING_on_the_aggregate(movies):
    """A HAVING compares the aggregate's attribute of each group."""
    sql = (
        "SELECT t.production_year, COUNT(*) AS c FROM title AS t "
        "GROUP BY t.production_year HAVING COUNT(*) > 1"
    )  # groups with more than one title

    fql = fql_of(sql, movies.schema(), STRUCTURED)  # the structured rendering

    assert "Gt('c', 1)" in fql  # on the aggregate's attribute, after grouping
    check_fql_reads_as_sql(sql, movies, STRUCTURED)  # the same rows as DuckDB


@pytest.mark.parametrize(("condition", "predicate"), DELETE_FILTERS)
def test_StructuredPredicates_render_renders_a_DELETE_filter(
    condition, predicate, orders
):
    """The filter renders as `predicate` and deletes the rows DuckDB deletes."""
    delete = f'DELETE FROM "order details" WHERE {condition}'  # one filter

    fql = fql_of(delete, orders.schema(), STRUCTURED)  # the structured rendering
    run_on_both_sides(delete, orders, STRUCTURED)  # deletes on both sides

    assert predicate in fql  # the expected predicate, or the lambda fallback
    check_fql_reads_as_sql('SELECT * FROM "order details"', orders)  # same rows left


@pytest.mark.parametrize(
    ("condition", "predicate"),
    [
        pytest.param("flag IS TRUE", "Eq('flag', True)", id="is-true"),  # equal to True
        pytest.param(
            "flag IS NOT FALSE", "Not(Eq('flag', False))", id="is-not-false"
        ),  # not equal to False
    ],
)
def test_StructuredPredicates_render_renders_IS_TRUE_as_a_comparison(
    condition, predicate
):
    """IS TRUE and IS FALSE on a column compare it with the boolean literal."""
    delete = f"DELETE FROM flags WHERE {condition}"  # a table without declared columns

    fql = fql_of(delete, NO_COLUMNS, STRUCTURED)  # the structured rendering

    assert (
        f"filter_values(dbf.flags, {predicate})" in fql
    )  # the whole filter, so no Not() around it

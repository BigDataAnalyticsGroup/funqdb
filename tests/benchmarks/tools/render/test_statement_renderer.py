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

"""How the statement renderer writes INSERT, UPDATE, DELETE and DDL as FQL."""

from collections.abc import Iterator

import duckdb
import pytest

from benchmarks.datasets.dataset import (
    DatasetDefinition,
    LoadedDataset,
    declare_primary_keys,
)
from benchmarks.datasets.dataset_types import Reference
from benchmarks.datasets.schema import PrimaryKey
from benchmarks.datasets.special_names.special_names_dataset import SPECIAL_NAMES
from benchmarks.tools.render.config import Config
from tests.benchmarks.tools.framework.rendering import (
    check_fql_reads_as_sql,
    fql_of,
    run_on_both_sides,
)

ORDERS = '"order details"'  # the table of `special_names`, quoted for SQL
ALL_ORDERS = f"SELECT * FROM {ORDERS}"  # every row and column, to compare after a write
# The tables of the `films` dataset below. A star marks a key column.
#
#   poster                 film                         genre
#   +--------------+       +--------------+             +--------+
#   | *film_id ----+------>| *id          |             | *id    |
#   |  url         |       |  genre_id ---+------------>|  name  |
#   +--------------+       +--------------+             +--------+
#
#   genre_id is swizzled into the reference `genre`, not into `genre_id`;
#   the key film_id of poster is swizzled into the reference `film`
FILM_KEYS = {
    "film": PrimaryKey.of("id"),
    "genre": PrimaryKey.of("id"),
    "poster": PrimaryKey.of("film_id"),
}  # a poster is keyed by its film


def build_films_connection() -> duckdb.DuckDBPyConnection:
    """A fresh DuckDB connection with two genres, two noir films and no poster."""
    connection = duckdb.connect()  # in memory, private to one dataset
    connection.execute("CREATE TABLE genre (id INTEGER, name TEXT)")  # the target
    connection.execute("CREATE TABLE film (id INTEGER, genre_id INTEGER)")  # the source
    connection.execute("CREATE TABLE poster (film_id INTEGER, url TEXT)")  # empty
    declare_primary_keys(connection, FILM_KEYS)  # DuckDB rejects a duplicate key too
    connection.execute("INSERT INTO genre VALUES (1, 'noir'), (2, 'western')")  # 2
    connection.execute("INSERT INTO film VALUES (1, 1), (2, 1)")  # both films noir
    return connection  # read by `FILMS.open`


FILMS = DatasetDefinition(
    name="films",
    references={
        "film": {"genre_id": Reference("genre", "genre")},
        "poster": {"film_id": Reference("film", "film")},
    },
    primary_keys=FILM_KEYS,
    build_connection=build_films_connection,
)
# each film with the name of its genre, read along the reference
FILM_GENRES = "SELECT f.id, g.name FROM film AS f, genre AS g WHERE f.genre_id = g.id"


@pytest.fixture
def orders() -> Iterator[LoadedDataset]:
    """A private `special_names` dataset, so a write does not leak into other tests."""
    dataset = SPECIAL_NAMES.open()  # fresh rows on both sides
    yield dataset  # the test writes into it
    dataset.close()  # releases the DuckDB connection


@pytest.fixture
def films() -> Iterator[LoadedDataset]:
    """A private `films` dataset, whose foreign key has a renamed reference."""
    dataset = FILMS.open()  # fresh rows on both sides
    yield dataset  # the test writes into it
    dataset.close()  # releases the DuckDB connection


def test_StatementRenderer_render_statement_SELECT_renders_the_query(orders):
    """A query renders as the query renderer renders it."""
    sql = f"SELECT o.id FROM {ORDERS} AS o WHERE o.id = 1"  # a plain read

    check_fql_reads_as_sql(sql, orders)  # the rendered FQL reads row 1 too


def test_StatementRenderer_render_statement_INSERT_adds_the_rows(orders):
    """Each VALUES row becomes one tuple under its key."""
    insert = (
        f'INSERT INTO {ORDERS} (id, "first name", project, "class", "it\'s") '
        "VALUES (4, 'Linus', 'Kernel', 5, 40), (5, 'Ken', 'Unix', 6, 50)"
    )  # two full rows, every column named

    run_on_both_sides(insert, orders)  # two new rows on both sides

    check_fql_reads_as_sql(ALL_ORDERS, orders)  # rows 4 and 5 in both


def test_StatementRenderer_render_statement_INSERT_leaves_a_NULL_column_unmapped(
    orders,
):
    """A column that the INSERT does not name holds NULL, so the tuple lacks it."""
    insert = f"INSERT INTO {ORDERS} (id, \"first name\") VALUES (4, 'Linus')"  # 2 of 5

    run_on_both_sides(insert, orders)  # project, class and it's stay NULL
    fql = fql_of(insert, orders.schema())  # the rendered write

    assert "'project'" not in fql  # no attribute is written for a NULL column
    check_fql_reads_as_sql(  # row 4 reads as NULL in DuckDB and as missing in FQL
        f'SELECT o.id, o."first name" FROM {ORDERS} AS o', orders
    )


def test_StatementRenderer_render_statement_INSERT_writes_a_foreign_key_as_its_reference(
    films,
):
    """A literal foreign key becomes the tuple it names, under the reference's name."""
    insert = "INSERT INTO film (id, genre_id) VALUES (3, 2)"  # film 3 of genre 2

    fql = fql_of(insert, films.schema())  # the rendered write
    run_on_both_sides(insert, films)  # the new film on both sides

    assert "'genre': " in fql  # written under the reference `genre`, not `genre_id`
    check_fql_reads_as_sql(FILM_GENRES, films)  # film 3 reads genre 2's name


def test_StatementRenderer_render_statement_INSERT_keys_a_tuple_by_the_raw_value_of_its_foreign_key(
    films,
):
    """A key that is a foreign key stores the tuple under the key value, not the tuple."""
    insert = (
        "INSERT INTO poster (film_id, url) VALUES (2, 'noir.png')"  # film 2's poster
    )

    fql = fql_of(insert, films.schema())  # the rendered write
    run_on_both_sides(insert, films)  # the new poster on both sides

    assert ".poster[2] = " in fql  # keyed by 2, not by the tuple dbf.film[2]
    check_fql_reads_as_sql(  # the poster reads its film along the reference
        "SELECT p.url, f.id FROM poster AS p, film AS f WHERE p.film_id = f.id", films
    )


def test_StatementRenderer_render_statement_UPDATE_assigns_from_the_old_values(orders):
    """Every SET reads the row as it was before the UPDATE, as in SQL."""
    update = (
        f'UPDATE {ORDERS} SET "class" = "it\'s", "it\'s" = "class" WHERE id < 3'  # swap
    )

    run_on_both_sides(update, orders)  # swaps both columns of rows 1 and 2

    check_fql_reads_as_sql(ALL_ORDERS, orders)  # swapped, not both set to one value


def test_StatementRenderer_render_statement_UPDATE_computes_every_row_before_assigning(
    orders,
):
    """A subquery in SET reads the table as it was before the UPDATE, for every row."""
    update = (
        f'UPDATE {ORDERS} SET "class" = '
        f'(SELECT MAX(o."class") FROM {ORDERS} AS o) + 1'
    )  # every row gets the old maximum plus 1: class 5

    run_on_both_sides(update, orders)  # a row-by-row loop would raise it to 5, 6, 7

    check_fql_reads_as_sql(ALL_ORDERS, orders)  # class 5 in every row of both


def test_StatementRenderer_render_statement_UPDATE_to_NULL_unmaps_the_attribute(
    orders,
):
    """`SET column = NULL` removes the attribute; the other SET still assigns."""
    update = f'UPDATE {ORDERS} SET project = NULL, "class" = 7 WHERE id = 1'  # 2 kinds

    run_on_both_sides(update, orders)  # row 1 loses its project, class becomes 7

    check_fql_reads_as_sql(ALL_ORDERS, orders)  # NULL in DuckDB, missing in FQL


def test_StatementRenderer_render_statement_UPDATE_of_only_NULLs_unmaps_and_assigns_nothing(
    orders,
):
    """An UPDATE that only sets NULL renders as a removal loop without assignment."""
    update = f"UPDATE {ORDERS} SET project = NULL"  # every row loses its project

    fql = fql_of(update, orders.schema())  # the rendered write
    run_on_both_sides(update, orders)  # runs it on both sides

    assert "= " not in fql  # no assignment, only `del`
    check_fql_reads_as_sql(ALL_ORDERS, orders)  # no row has a project any more


def test_StatementRenderer_render_statement_UPDATE_to_NULL_again_skips_the_unmapped_attribute(
    orders,
):
    """A second `SET column = NULL` finds the attribute gone and leaves the row as it is."""
    update = f"UPDATE {ORDERS} SET project = NULL WHERE id = 1"  # row 1 only

    run_on_both_sides(update, orders)  # removes the project of row 1
    run_on_both_sides(update, orders)  # the same write again: nothing to remove

    check_fql_reads_as_sql(ALL_ORDERS, orders)  # no error, and the same rows in both


def test_StatementRenderer_render_statement_DELETE_removes_the_selected_rows(orders):
    """Only the rows that the WHERE selects are deleted."""
    delete = f'DELETE FROM {ORDERS} WHERE "class" > 2'  # rows 2 and 3

    run_on_both_sides(delete, orders)  # row 1 is left on both sides

    check_fql_reads_as_sql(ALL_ORDERS, orders)  # only row 1 in both


def test_StatementRenderer_render_statement_CREATE_TABLE_adds_an_empty_relation(orders):
    """A new table holds no rows until something is inserted."""
    create = "CREATE TABLE genre (id INTEGER PRIMARY KEY, name VARCHAR)"  # a new table

    run_on_both_sides(create, orders)  # created on both sides

    check_fql_reads_as_sql("SELECT g.id FROM genre AS g", orders)  # empty in both


def test_StatementRenderer_render_statement_DROP_TABLE_deletes_the_relation(orders):
    """DROP TABLE removes the relation from the database function."""
    fql = fql_of(f"DROP TABLE {ORDERS}", orders.schema())  # the rendered DDL

    assert fql == "del dbf['order details']"  # the quoted name, read by subscript


def test_StatementRenderer_render_statement_DROP_TABLE_uses_the_configured_database_name(
    orders,
):
    """The statement deletes from the database name of the `Config`."""
    config = Config(dbf_name="db")  # another database name
    fql = fql_of(f"DROP TABLE {ORDERS}", orders.schema(), config)  # the rendered DDL

    assert fql == "del db['order details']"  # `db`, not the default `dbf`

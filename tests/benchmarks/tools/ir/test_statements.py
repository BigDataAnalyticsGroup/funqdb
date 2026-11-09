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

"""What an IR statement can hold."""

import pytest

from benchmarks.datasets.schema import PrimaryKey, Schema
from benchmarks.tools.ir.expressions import Literal
from benchmarks.tools.ir.join_tree import AliasGraph
from benchmarks.tools.ir.query import Query
from benchmarks.tools.ir.statements import Delete, Insert, Update

SCHEMA = Schema(references={}, primary_keys={}, columns={})  # no references


def query_over(tables: dict[str, str]) -> Query:
    """The query that reads `tables`, each alias a root of its own."""
    graph = AliasGraph(0, SCHEMA, tables, (), ())  # no edges between the aliases
    return Query(graph.tree())  # the rows a write statement reads


def test_delete_post_init_rejects_a_query_of_several_roots():
    """A DELETE writes one table, so `written_table` must have one to name."""
    query = query_over({"a": "a", "b": "b"})  # two roots

    with pytest.raises(ValueError):  # the statement cannot exist
        Delete(query)  # no table to write


def test_update_post_init_rejects_a_query_of_several_roots():
    """An UPDATE writes one table, so `written_table` must have one to name."""
    query = query_over({"a": "a", "b": "b"})  # two roots

    with pytest.raises(ValueError):  # the statement cannot exist
        Update(query, ())  # no table to write


def test_insert_post_init_rejects_a_row_misaligned_to_its_columns():
    """`Insert` cannot exist with a row misaligned to its columns."""
    key = PrimaryKey(("id",))  # any key; the check is about row width

    with pytest.raises(ValueError):  # one value for two columns
        Insert("keyword", key, ("id", "keyword"), ((Literal(1),),))  # lacks `keyword`


def test_insert_post_init_admits_rows_aligned_to_their_columns():
    """Both rows hold one value per column, so the statement exists."""
    key = PrimaryKey(("id",))  # any key; the check is about row width
    rows = ((Literal(1), Literal("a")), (Literal(2), Literal("b")))  # two full rows

    Insert("keyword", key, ("id", "keyword"), rows)  # raises if a full row is refused


def test_delete_written_table_is_the_table_of_its_root_not_its_alias():
    """A statement writes into the table, whatever its query calls it."""
    query = query_over({"t": "title"})  # the alias differs

    assert Delete(query).written_table == "title"  # the table


def test_update_written_table_is_the_table_of_its_root_not_its_alias():
    """A statement writes into the table, whatever its query calls it."""
    query = query_over({"t": "title"})  # the alias differs

    assert Update(query, ()).written_table == "title"  # the table

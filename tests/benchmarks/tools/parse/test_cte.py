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

"""A CTE's conditions as a query reading it under an alias reads them."""

from benchmarks.tools.parse.cte import CteDefinition
from tests.benchmarks.tools.framework.parsing import parse_cte

NOTHING_RESERVED = frozenset()


def renamed(definition: CteDefinition, alias: str, reserved=NOTHING_RESERVED):
    """The conditions of `definition` under `alias`, as SQL text."""
    conditions = definition.conditions_under(alias, reserved)  # under the given alias
    return [condition.sql() for condition in conditions]  # as SQL, to compare as text


def test_cte_definition_conditions_under_renames_a_condition_that_is_a_bare_column():
    """A conjunct may be the column itself, with no node above it to look below."""
    bare = parse_cte(
        "SELECT * FROM title AS t WHERE t.ok AND t.id = 1"
    )  # two conjuncts

    assert renamed(bare, "c") == ["c.ok", "c.id = 1"]  # both read the alias c


def test_cte_definition_conditions_under_renames_each_capturing_subquery_on_its_own():
    """Each query declaring the alias gets a fresh one for its own scope, and its columns follow."""
    nested = parse_cte(
        "SELECT * FROM title AS t WHERE EXISTS (SELECT * FROM aka_title AS x WHERE "
        "x.id = t.id AND EXISTS (SELECT * FROM kind_type AS x WHERE x.id = 1))"
    )  # x declared at two depths

    assert renamed(nested, "x") == [
        "EXISTS(SELECT * FROM aka_title AS x_0 WHERE x_0.id = x.id AND "
        "EXISTS(SELECT * FROM kind_type AS x_0 WHERE x_0.id = 1))"
    ]  # the inner x_0 shadows the middle one, as the inner x shadowed the middle x


def test_cte_definition_conditions_under_renames_a_subquery_alias_that_would_capture():
    """A subquery declaring the reading alias gets a fresh one, so the renamed column still reads the CTE's tuple."""
    capturing = parse_cte(
        "SELECT * FROM title AS t WHERE EXISTS "
        "(SELECT * FROM aka_title AS x WHERE x.title = t.title)"
    )  # x inside, x the reading alias
    reserved = frozenset({"x_0"})  # x_0 is taken elsewhere in the statement

    assert renamed(capturing, "x") == [
        "EXISTS(SELECT * FROM aka_title AS x_0 WHERE x_0.title = x.title)"
    ]  # the inner x is x_0
    assert renamed(capturing, "x", reserved) == [
        "EXISTS(SELECT * FROM aka_title AS x_1 WHERE x_1.title = x.title)"
    ]  # the next free name


def test_cte_definition_conditions_under_renames_a_joined_alias_that_would_capture():
    """An alias a subquery declares in a JOIN captures the reading alias just as one in its FROM."""
    capturing = parse_cte(
        "SELECT * FROM title AS t WHERE EXISTS (SELECT * FROM aka_title AS a "
        "JOIN kind_type AS x ON x.id = a.id WHERE x.id = t.kind_id)"
    )  # x joined inside, x the reading alias

    assert renamed(capturing, "x") == [
        "EXISTS(SELECT * FROM aka_title AS a JOIN kind_type AS x_0 ON x_0.id = a.id "
        "WHERE x_0.id = x.kind_id)"
    ]  # the joined x is x_0, the outer column reads the reading alias


def test_cte_definition_conditions_under_leaves_a_subquery_declaring_the_cte_alias():
    """The subquery's `t` is its own, so only the condition's outer `t` is renamed."""
    shadowing = parse_cte(
        "SELECT * FROM title AS t WHERE t.id = 1 AND EXISTS "
        "(SELECT * FROM aka_title AS t WHERE t.title = 'x')"
    )  # t declared twice

    assert renamed(shadowing, "c") == [
        "c.id = 1",
        "EXISTS(SELECT * FROM aka_title AS t WHERE t.title = 'x')",
    ]  # the subquery reads its own t


def test_cte_definition_conditions_under_renames_inside_a_nested_subquery():
    """A correlated column is renamed at any depth, and the original stays as it was."""
    nested = parse_cte(
        "SELECT * FROM title AS t WHERE EXISTS (SELECT * FROM aka_title AS a WHERE "
        "EXISTS (SELECT * FROM kind_type AS k WHERE k.id = t.kind_id))"
    )  # two levels down

    first = renamed(nested, "c")  # under one alias
    second = renamed(nested, "d")  # the definition is read again under another

    assert first == [
        "EXISTS(SELECT * FROM aka_title AS a WHERE "
        "EXISTS(SELECT * FROM kind_type AS k WHERE k.id = c.kind_id))"
    ]  # renamed at the bottom only, the middle subquery unchanged
    assert second == [
        "EXISTS(SELECT * FROM aka_title AS a WHERE "
        "EXISTS(SELECT * FROM kind_type AS k WHERE k.id = d.kind_id))"
    ]  # from the original, not from the first copy

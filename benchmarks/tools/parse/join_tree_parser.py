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

"""The join-tree step of the query parser, which reads FROM, ON and WHERE."""

from dataclasses import dataclass

from sqlglot import Expr, exp

from benchmarks.datasets.dataset_types import Reference
from benchmarks.datasets.schema import PrimaryKey, Schema
from benchmarks.tools.ir.join_equalities import (
    Edge,
    ForeignKeyEquality,
    JoinEquality,
    ReferenceRead,
)
from benchmarks.tools.ir.join_tree import AliasGraph, JoinTree
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.parse.cte import CteDefinition
from benchmarks.tools.parse.node_args import sql_conjuncts

# "" is a plain `JOIN` or a comma join
_INNER_JOIN_KINDS = frozenset({"", "INNER", "CROSS"})


@dataclass(frozen=True)
class FromClause:
    """The aliases a query declares with their tables, and the conditions: its CTEs', then its ON conjuncts."""

    tables: dict[str, str]
    conditions: list[Expr]


@dataclass(frozen=True)
class SplitConditions:
    """The conditions of a query: the join equalities, and the filters."""

    equalities: list[JoinEquality]
    filters: list[Expr]


def read_from(
    ast: exp.Select, ctes: dict[str, CteDefinition], reserved: frozenset[str]
) -> FromClause:
    """The FROM clause of `ast`; a CTE alias stands for the CTE's table."""
    from_node = ast.args.get("from_")
    if from_node is None:
        raise UnsupportedSql(Limitation.NO_FROM, "From not found.")

    join_nodes = ast.args.get("joins") or []
    for join_node in join_nodes:
        _reject_non_inner_join(join_node)

    tables: dict[str, str] = {}
    conditions: list[Expr] = []
    for table_node in [from_node.this, *(join.this for join in join_nodes)]:
        _reject_derived_table(table_node)
        alias = table_node.alias_or_name
        cte = ctes.get(table_node.name)
        if cte is None:
            tables[alias] = table_node.name
            continue

        _reject_non_inlinable_cte(table_node.name, cte)
        (cte_alias,) = cte.query.tree.aliases
        tables[alias] = cte.query.tree.table_of(cte_alias)
        cte_conditions = cte.conditions_under(alias, reserved)
        conditions.extend(cte_conditions)

    for join_node in join_nodes:
        # a cross join has no ON, and a comma join has no ON either
        on_node = join_node.args.get("on")
        if on_node is not None:
            on_conjuncts = sql_conjuncts(on_node)
            conditions.extend(on_conjuncts)

    return FromClause(tables, conditions)


def split_conditions(
    conditions: list[Expr], tables: dict[str, str], schema: Schema
) -> SplitConditions:
    """The join equalities among `conditions`, and the other conditions as filters."""
    equalities: list[JoinEquality] = []
    filters: list[Expr] = []
    for condition in conditions:
        equality = _join_equality(condition, tables, schema)
        if equality is None:
            filters.append(condition)
        else:
            equalities.append(equality)
    return SplitConditions(equalities, filters)


def build_join_tree(
    tables: dict[str, str],
    equalities: list[JoinEquality],
    schema: Schema,
    depth: int,
    reserved: frozenset[str],
) -> JoinTree:
    """The join tree of `tables` and `equalities`; an edge stated twice counts once."""
    edges: list[Edge] = []
    foreign_key_equalities: list[ForeignKeyEquality] = []
    for equality in equalities:
        if isinstance(equality, Edge) and equality not in edges:
            edges.append(equality)
        if isinstance(equality, ForeignKeyEquality):
            foreign_key_equalities.append(equality)

    graph = AliasGraph(
        depth, schema, tables, tuple(edges), tuple(foreign_key_equalities), reserved
    )
    return graph.tree()


def _join_equality(
    condition: Expr, tables: dict[str, str], schema: Schema
) -> JoinEquality | None:
    """The edge or foreign-key equality `condition` states between two aliases of `tables`;
    `None` for any other condition, also for a foreign key equated with itself."""
    if not isinstance(condition, exp.EQ):
        return None

    left = condition.this
    right = condition.expression
    if not isinstance(left, exp.Column) or not isinstance(right, exp.Column):
        return None
    if left.table not in tables or right.table not in tables:
        return None

    left_reference = schema.reference_of(tables[left.table], left.name)
    right_reference = schema.reference_of(tables[right.table], right.name)
    if left_reference is None and right_reference is None:
        return None
    if right_reference is None or _is_key_of_target(
        left_reference, right, tables, schema
    ):
        return _edge(left, left_reference, right, tables, schema)
    if left_reference is None or _is_key_of_target(
        right_reference, left, tables, schema
    ):
        return _edge(right, right_reference, left, tables, schema)

    return _foreign_key_equality(left, left_reference, right, right_reference)


def _is_key_of_target(
    reference: Reference | None,
    column: exp.Column,
    tables: dict[str, str],
    schema: Schema,
) -> bool:
    """True when `column` is the key `reference` leads to, even if a foreign key itself."""
    if reference is None or reference.target_table != tables[column.table]:
        return False
    return schema.primary_keys.get(reference.target_table) == PrimaryKey((column.name,))


def _reject_non_inner_join(join_node: exp.Join) -> None:
    """Rejects an outer or natural join, which the join tree would read as inner."""
    if join_node.side or join_node.method or join_node.kind not in _INNER_JOIN_KINDS:
        raise UnsupportedSql(
            Limitation.OUTER_JOIN,
            f"Unsupported join: only inner joins translate, not '{join_node.sql()}'.",
        )


def _reject_derived_table(table_node: Expr) -> None:
    """Rejects a FROM item that is no table, which only a write still holds after the optimizer."""
    if not isinstance(table_node, exp.Table):
        raise UnsupportedSql(
            Limitation.DERIVED_TABLE_IN_WRITE,
            f"Unsupported derived table {table_node.sql()}: a write statement does "
            "not turn it into a CTE as a query does.",
        )


def _reject_non_inlinable_cte(name: str, cte: CteDefinition) -> None:
    """Rejects a CTE that does more than filter a single table."""
    if not cte.query.inlinable:
        raise UnsupportedSql(
            Limitation.CTE_NOT_INLINABLE,
            f"Unsupported reference to CTE '{name}': only a CTE that filters a "
            "single table can be inlined into the query reading it.",
        )


def _edge(
    foreign_key: exp.Column,
    reference: Reference,
    key: exp.Column,
    tables: dict[str, str],
    schema: Schema,
) -> Edge:
    """The edge from the alias of `foreign_key` to the alias of `key`; rejects a `key` that is not the target table's key."""
    target_table = reference.target_table
    key_table = tables[key.table]
    if target_table != key_table:
        raise UnsupportedSql(
            Limitation.FOREIGN_KEY_TO_NON_KEY,
            f"Unsupported join: a foreign key into '{target_table}' is equated with "
            f"'{key.table}.{key.name}' of '{key_table}'.",
        )
    key_columns = PrimaryKey((key.name,))
    if schema.primary_keys.get(target_table) != key_columns:
        raise UnsupportedSql(
            Limitation.FOREIGN_KEY_TO_NON_KEY,
            f"Unsupported join: a foreign key is equated with "
            f"'{key.table}.{key.name}', which is not the key of '{target_table}'.",
        )

    return Edge(foreign_key.table, reference.ref_key, key.table)


def _foreign_key_equality(
    left: exp.Column,
    left_reference: Reference,
    right: exp.Column,
    right_reference: Reference,
) -> ForeignKeyEquality | None:
    """The equality of two foreign keys into one table; `None` for other tables or one key twice."""
    # keys into different tables stay a filter, which the expression parser rejects
    target_table = left_reference.target_table
    left_read = ReferenceRead(left.table, left_reference.ref_key)
    right_read = ReferenceRead(right.table, right_reference.ref_key)
    if target_table != right_reference.target_table or left_read == right_read:
        return None

    return ForeignKeyEquality(target_table, left_read, right_read)

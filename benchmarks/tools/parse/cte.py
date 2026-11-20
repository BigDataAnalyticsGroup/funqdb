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


"""A CTE as the queries reading it inline it."""

from dataclasses import dataclass

from sqlglot import Expr, exp

from benchmarks.tools.ir.names import fresh_name
from benchmarks.tools.ir.query import Query


@dataclass(frozen=True)
class CteDefinition:
    """A CTE as a reading query inlines it: its query and its SQL conjuncts, parsed again under the reader's alias."""

    query: Query
    conditions: tuple[Expr, ...]

    def conditions_under(self, alias: str, reserved: frozenset[str]) -> list[Expr]:
        """Copies of the conditions reading `alias`; a subquery declaring `alias` gets a fresh alias outside `reserved`."""
        (own,) = self.query.tree.aliases
        renamed = []
        for condition in self.conditions:
            # _rename_in_scope renames only below its node, and a condition may be a column
            holder = exp.Paren(this=condition.copy())
            _rename_in_scope(holder, own, alias, reserved)
            renamed.append(holder.this)
        return renamed


def _rename_in_scope(node: Expr, old: str, new: str, reserved: frozenset[str]) -> None:
    """Renames `old` to `new` in every column of `node` reading the alias `old` of its own scope."""
    for child in list(node.iter_expressions()):
        if isinstance(child, exp.Select):
            _rename_in_select(child, old, new, reserved)
        elif isinstance(child, exp.Column) and child.table == old:
            identifier = exp.to_identifier(new)
            child.set("table", identifier)
        else:
            _rename_in_scope(child, old, new, reserved)


def _rename_in_select(
    select: exp.Select, old: str, new: str, reserved: frozenset[str]
) -> None:
    """Renames `old` to `new` in the subquery `select`, after renaming its own `new` away."""
    declared = _declared_aliases(select)
    # the subquery's own `new` gets a fresh name, so that `new` reads the outer alias only
    if new in declared:
        taken = reserved | set(declared)
        fresh = fresh_name(new, taken)
        _rename_declaration(select, new, fresh)
        _rename_in_scope(select, new, fresh, reserved)
    # the subquery declares `old` again, so none of its columns read ours
    if old in declared:
        return

    _rename_in_scope(select, old, new, reserved)


def _rename_declaration(select: exp.Select, old: str, new: str) -> None:
    """Declares the alias `old` of `select`'s own FROM as `new`."""
    for table in select.find_all(exp.Table):
        if table.parent_select is select and table.alias_or_name == old:
            identifier = exp.to_identifier(new)
            alias = exp.TableAlias(this=identifier)
            table.set("alias", alias)


def _declared_aliases(select: exp.Select) -> list[str]:
    """The aliases that the FROM and JOIN items of `select` declare."""
    from_node = select.args.get("from_")
    joins = select.args.get("joins") or []
    tables = [] if from_node is None else [from_node.this]
    tables.extend(join.this for join in joins)
    return [table.alias_or_name for table in tables]

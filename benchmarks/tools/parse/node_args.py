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

"""Helpers that read a sqlglot node: its argument names, its conjuncts and its sign."""

from sqlglot import Expr, exp


def arg_names(keys: list[str]) -> str:
    """`keys` as SQL reads them, without sqlglot's trailing `_`."""
    return ", ".join(key.rstrip("_") for key in keys)


def sql_conjuncts(expression: Expr) -> list[Expr]:
    """`expression` as its top-level conjuncts."""
    if isinstance(expression, exp.And):
        return list(expression.flatten())
    return [expression]


def is_negative_literal(node: Expr | None) -> bool:
    """True when `node` is a minus sign in front of a literal, as sqlglot reads `-1` or `-(1)`."""
    return isinstance(node, exp.Neg) and isinstance(node.this.unnest(), exp.Literal)

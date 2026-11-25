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

"""Renders a query and its expressions as one FQL expression."""

import re
from contextlib import contextmanager
from dataclasses import dataclass
from inspect import Parameter, signature
from statistics import fmean
from typing import Callable, Iterator

from benchmarks.datasets import is_python_name
from benchmarks.tools.ir.expressions import (
    UNEQUAL_SYMBOLS,
    AggregateExpression,
    BetweenExpression,
    BinaryExpression,
    CaseWhenExpression,
    CastExpression,
    Column,
    ExistsExpression,
    Expression,
    ExtractExpression,
    InExpression,
    InSubqueryExpression,
    IntervalExpression,
    IsBooleanExpression,
    IsNullExpression,
    LikeExpression,
    Literal,
    ParenExpression,
    ReferencedTupleExpression,
    SubqueryExpression,
    SubstringExpression,
    TupleIdentityExpression,
    UnaryExpression,
)
from benchmarks.tools.ir.join_tree import Join
from benchmarks.tools.ir.query import (
    GeneratedColumns,
    Limit,
    OrderKey,
    Query,
)
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.render.config import (
    CallChaining,
    Config,
    OperatorResult,
    PredicateForm,
)
from benchmarks.tools.render.fql_names import (
    ELEMENT_NAME,
    FDM_LOOKUP_SEPARATOR,
    ITEM_NAME,
    PROJECTION_SEPARATOR,
    TUPLE_NAME,
    VALUES_NAME,
    reject_unaddressable_name,
)
from benchmarks.tools.render.fragments import (
    group_aware_segments,
    join_path,
    render_group_key_attribute,
    render_literal,
    render_path_argument,
    render_tuple_literal,
)
from benchmarks.tools.render.like import (
    LikePattern,
    LikeShape,
)
from benchmarks.tools.render.structured_predicates import StructuredPredicates
from fdm.attribute_functions import DBF, DictionaryAttributeFunction
from fql.operators import (
    aggregates,
    filters,
    partition_and_aggregate,
    projections,
    rank,
    subsets,
)
from fql.operators.constraints import drop_reference
from fql.operators.joins import join
from fql.operators.transforms import key_to_value
from fql.predicates import And

_AGGREGATE_NAMES = {
    "count": aggregates.Count.__name__,
    "sum": aggregates.Sum.__name__,
    "avg": aggregates.Avg.__name__,
    "min": aggregates.Min.__name__,
    "max": aggregates.Max.__name__,
}
# per SQL aggregate, the function computing it over a set of distinct values
_DISTINCT_AGGREGATE_FUNCTIONS = {
    "count": len.__name__,
    "sum": sum.__name__,
    "avg": fmean.__name__,
    "min": min.__name__,
    "max": max.__name__,
}
# the named parameters of the operators that take aggregates as keyword arguments;
# the signature of a class itself leaves out `self`
_OPERATOR_PARAMETER_NAMES = frozenset(
    name
    for operator in (aggregates.aggregate, partition_and_aggregate.group_by_aggregate)
    for name, parameter in signature(operator.__init__).parameters.items()
    if parameter.kind is Parameter.POSITIONAL_OR_KEYWORD
)


@dataclass(frozen=True)
class ValueBase:
    """Where a rendered path starts: the Python `expression` naming a root
    tuple, or a join tuple when `joined` (every path then leads with its root)."""

    expression: str
    joined: bool


class QueryRenderer:
    """Renders a `Query` and its expressions as FQL in the style of a `Config`."""

    def __init__(self, config: Config) -> None:
        self._config = config
        # per enclosing query, the binding of its tuple while a subquery of it is
        # rendered: a filter, ranking or computed lambda, or an UPDATE loop
        self._enclosing_tuples: dict[Query, _EnclosingTuple] = {}

    def render_query(self, query: Query) -> str:
        """`query` as FQL; an ungrouped aggregation stays its result TF unless a
        computed column reads helper aggregates."""
        fql = self._render_unprojected(query)
        if query.aggregate_computed_columns.by_expression:
            fql = self._render_single_value(query, fql)

        # an ungrouped aggregation is a single TF of scalars (or one scalar), which
        # `project` cannot take
        if not query.is_ungrouped_aggregation:
            fql = self._render_project(query, fql)
            fql = self._render_rename(query, fql)

        # last, as a copy reads its source lazily, under the source's final name
        fql = self._render_copies(query, fql)
        return fql.value

    def render_table(self, table: str) -> str:
        """The relation of `table`, read off the database."""
        return self.render_access(self._config.dbf_name, [table])

    def render_filter(
        self,
        query: Query,
        fql: str,
        condition: Expression | None,
        post_group: bool,
        joined: bool,
    ) -> str:
        """`fql` filtered by `condition`; `fql` itself without a condition."""
        relation = _Value(fql)
        filtered = self._render_filtered(query, relation, condition, post_group, joined)
        return filtered.value

    def render_expression(
        self,
        expression: Expression,
        query: Query | None,
        value_base: ValueBase,
        post_group: bool,
    ) -> str:
        """`expression` as Python, read off the tuple that `value_base` names."""

        def render(sub: Expression) -> str:
            """`sub` rendered in the context of the enclosing call."""
            return self.render_expression(sub, query, value_base, post_group)

        replaced = _reintroduced_keys(query, post_group)
        if expression in replaced:
            # a computed group key, which grouping reintroduced as an attribute
            column = replaced[expression]
            return self._render_column(column, query, value_base, post_group)

        if isinstance(expression, Column):
            return self._render_column(expression, query, value_base, post_group)

        if isinstance(expression, Literal):
            return render_literal(expression.value)

        mapped_column = expression.not_null_tested_column()
        if mapped_column is not None:
            return self._render_membership(
                mapped_column, "in", query, value_base, post_group
            )

        unmapped_column = expression.null_tested_column()
        if unmapped_column is not None:
            return self._render_membership(
                unmapped_column, "not in", query, value_base, post_group
            )

        if isinstance(expression, UnaryExpression) and expression.op == "not":
            negated = expression.value.unparenthesized()
            if negated.is_comparison():
                return self._render_comparison(
                    negated, query, value_base, post_group, negated=True
                )

        if expression.is_comparison():
            return self._render_comparison(
                expression, query, value_base, post_group, negated=False
            )

        if isinstance(expression, TupleIdentityExpression):
            return self._render_tuple_identity(
                expression, query, value_base, post_group
            )

        if isinstance(expression, UnaryExpression):
            value = render(expression.value)
            return f"({expression.op} ({value}))"

        if isinstance(expression, ParenExpression):
            return f"({render(expression.value)})"

        if isinstance(expression, CaseWhenExpression):
            rendered = (
                "None" if expression.default is None else render(expression.default)
            )
            for branch in reversed(expression.branches):
                result = render(branch.result)
                condition = render(branch.condition)
                rendered = f"({result} if {condition} else {rendered})"
            return rendered

        if isinstance(expression, BinaryExpression) and expression.op == "and":
            rendered = [render(conjunct) for conjunct in expression.conjuncts()]
            return f"({' and '.join(rendered)})"

        if isinstance(expression, BinaryExpression):
            left = render(expression.left)
            right = render(expression.right)
            return f"({left} {expression.op} {right})"

        if isinstance(expression, ExistsExpression):
            subquery = expression.subquery
            if subquery.is_ungrouped_aggregation:
                raise UnsupportedSql(
                    Limitation.EXISTS_OVER_AGGREGATE,
                    "EXISTS over an ungrouped aggregate is always true; it is not translated.",
                )
            # an AttributeFunction is falsy exactly when it holds no item
            with self._entering_subquery(subquery, value_base, post_group):
                return self._render_subquery(subquery)

        if isinstance(expression, IsNullExpression):
            raise UnsupportedSql(
                Limitation.NULL_IN_PREDICATE,
                "IS NULL over a computed value is not translated: FQL tests only "
                "whether an attribute is mapped.",
            )

        if isinstance(expression, SubstringExpression):
            return self._render_substring(expression, query, value_base, post_group)

        if isinstance(expression, ReferencedTupleExpression):
            relation = self.render_table(expression.reference.target_table)
            key = render_literal(expression.key.value)
            return f"{relation}[{key}]"

        if isinstance(expression, ExtractExpression):
            return f"{render(expression.value)}.{expression.part.value}"

        if isinstance(expression, CastExpression):
            return f"{float.__name__}({render(expression.value)})"

        if isinstance(expression, IntervalExpression):
            unit = expression.unit
            return f"{unit.delta.__name__}({unit.keyword}={expression.count})"

        if isinstance(expression, SubqueryExpression):
            with self._entering_subquery(expression.subquery, value_base, post_group):
                return self._render_subquery(expression.subquery)

        raise UnsupportedSql(
            Limitation.UNSUPPORTED_EXPRESSION,
            f"Unsupported expression type: {type(expression)}",
        )

    def render_access(self, base: str, names: list[str]) -> str:
        """The attribute path `names`, read off `base`."""
        return self._config.attribute_access.render(base, names)

    def _render_subquery(self, query: Query) -> str:
        """The result a subquery contributes: a single-value aggregation as its one
        value."""
        fql = self._render_unprojected(query)
        fql = self._render_single_value(query, fql)
        return fql.value

    def _render_unprojected(self, query: Query) -> "_Relation":
        """`query` up to but excluding its SELECT list: what `_render_project`
        projects."""
        fql = self._render_from(query)
        fql = self._render_computed_columns(query, fql)

        if query.is_grouped:
            fql = self._render_group_by(query, fql)
            fql = self._render_having(query, fql)
        else:
            fql = self._render_aggregates(query, fql)

        # ORDER BY may name a computed aggregation column, so it must exist first
        fql = self._render_computed_aggregation_columns(query, fql)
        fql = self._render_regroup(query, fql)
        return self._render_order_keys(query, fql)

    def _render_project(self, query: Query, fql: "_Relation") -> "_Relation":
        """`fql` projected to the selected columns; `fql` itself for a star."""
        if not query.outputs:
            return fql

        names = [
            self._render_output_attribute(query, output.column)
            for output in query.projected_outputs
        ]
        if len(set(names)) != len(names):
            raise UnsupportedSql(
                Limitation.PROJECTED_NAME_COLLISION,
                f"Unsupported SELECT: the projected columns {names} would share an FQL "
                "attribute name.",
            )

        paths = []
        for output in query.projected_outputs:
            segments = group_aware_segments(
                output.column, query, query.is_grouped, query.tree.is_joined
            )
            path = join_path(segments, PROJECTION_SEPARATOR)
            paths.append(repr(path))
        columns = ",".join(paths)

        if self._config.call_chaining is CallChaining.METHOD:
            project_name = self._config.operator_naming.project_name
            return _Value(f"{fql.value}.{project_name}({columns})")

        return self._render_operator(
            f"{projections.project.__name__}({fql.operand},{columns})"
        )

    def _render_rename(self, query: Query, fql: "_Relation") -> "_Relation":
        """`fql` with each projected attribute renamed to its output name where the two differ."""
        renames = {}
        for output in query.projected_outputs:
            attribute = self._render_output_attribute(query, output.column)
            if attribute == output.name:
                continue
            self._reject_unreadable_name(output.name)
            renames[attribute] = repr(output.name)
        if not renames:
            return fql

        arguments = self._render_keyword_arguments(renames)
        rename_name = self._config.operator_naming.rename_name
        return _Value(f"{fql.value}.{rename_name}({','.join(arguments)})")

    @staticmethod
    def _reject_unreadable_name(name: str) -> None:
        """Raises for an output name FDM would read as a path."""
        # only `__`: a key containing `.` still reads back, so
        # reject_unaddressable_name would reject too much
        if FDM_LOOKUP_SEPARATOR in name:
            raise UnsupportedSql(
                Limitation.PATH_SEPARATOR_NAME,
                f"Unsupported output name {name!r}: FDM reads "
                f"{FDM_LOOKUP_SEPARATOR!r} inside a key as a path separator, so "
                "the attribute could not be read.",
            )

    @staticmethod
    def _render_output_attribute(query: Query, column: Column) -> str:
        """The attribute `column` holds once projected: FDM `project` keeps the
        last segment of each path."""
        segments = group_aware_segments(
            column, query, query.is_grouped, query.tree.is_joined
        )
        return segments[-1]

    def _render_from(self, query: Query) -> "_Relation":
        """The tuples `query` reads, already filtered by its condition: its one
        root, or the join of its roots."""
        query_join = query.join
        if query_join is not None:
            return self._render_join(query, query_join)

        (root,) = query.tree.roots
        relation = self._render_relation(query, root)
        return self._render_filtered(query, relation, query.condition, False, False)

    def _render_relation(self, query: Query, alias: str) -> "_Value":
        """The relation of the table that `alias` names."""
        table = query.tree.table_of(alias)
        relation = self.render_table(table)
        return _Value(relation)

    def _render_join(self, query: Query, query_join: Join) -> "_Relation":
        """`query_join`: pushed conjuncts filter their root, the remaining ones
        the join tuples."""
        fql = self._render_dbf(query, query_join.pushed_conditions)
        for edge in query_join.dropped_edges:
            fql = self._render_operator(
                f"{drop_reference.__name__}({fql.operand},source={edge.source!r},"
                f"ref_key={edge.ref_key!r},target={edge.target!r})"
            )
        fql = self._render_operator(f"{join.__name__}({fql.operand})")
        return self._render_filtered(
            query, fql, query_join.remaining_condition, False, True
        )

    def _render_dbf(
        self, query: Query, pushed_conditions: dict[str, Expression]
    ) -> "_Value":
        """The DBF of the join members, each filtered by the conjuncts pushed into it."""
        entries = []
        for member in query.tree.join_members:
            # the alias names a join tuple attribute, so every path leads with it
            reject_unaddressable_name(member)
            relation = self._render_relation(query, member)
            if member in pushed_conditions:
                relation = self._render_filtered(
                    query, relation, pushed_conditions[member], False, False
                )
            entries.append(f"{member!r}: {relation.value}")

        return _Value(f"{DBF.__name__}({{{','.join(entries)}}})")

    def _render_computed_columns(self, query: Query, fql: "_Relation") -> "_Relation":
        """`fql` with each computed column added to every tuple."""
        return self._render_computed(
            query,
            fql,
            query.computed_columns,
            DictionaryAttributeFunction.add_computed_per_value,
            False,
        )

    def _render_group_by(self, query: Query, fql: "_Relation") -> "_Relation":
        """`fql` grouped by the GROUP BY keys into tuples of its aggregates."""
        keys = []
        for column in query.group_keys:
            path = render_path_argument(
                column, FDM_LOOKUP_SEPARATOR, query.tree.is_joined
            )
            keys.append(repr(path))

        reintroduced = []
        for column in query.group_keys:
            attribute = render_group_key_attribute(column, query.tree.is_joined)
            reintroduced.append(repr(attribute))

        aggregate_arguments = self._render_aggregate_arguments(query)
        return self._render_grouping(fql, keys, aggregate_arguments, reintroduced)

    def _render_regroup(self, query: Query, fql: "_Relation") -> "_Relation":
        """The groups of `query` grouped once more by their selected attributes,
        which keep their names."""
        if not query.regroups:
            return fql

        attributes = [
            repr(self._render_output_attribute(query, output.column))
            for output in query.projected_outputs
        ]
        return self._render_grouping(fql, attributes, [], attributes)

    def _render_grouping(
        self,
        fql: "_Relation",
        keys: list[str],
        aggregate_arguments: list[str],
        reintroduced: list[str],
    ) -> "_Relation":
        """`fql` grouped by `keys` into tuples of `aggregate_arguments`, with each
        group's key reintroduced under the `reintroduced` attributes."""
        arguments = ",".join(keys + aggregate_arguments)
        group_by_aggregate = partition_and_aggregate.group_by_aggregate.__name__
        grouped = self._render_operator(
            f"{group_by_aggregate}({fql.operand},{arguments})"
        )

        names = render_tuple_literal(reintroduced)
        return self._render_operator(
            f"{key_to_value.__name__}({grouped.operand},{names})"
        )

    def _render_having(self, query: Query, fql: "_Relation") -> "_Relation":
        """The groups in `fql` filtered by HAVING."""
        return self._render_filtered(
            query, fql, query.having_condition, True, query.tree.is_joined
        )

    def _render_aggregates(self, query: Query, fql: "_Relation") -> "_Relation":
        """`fql` aggregated into one tuple of its aggregates; `fql` itself without any."""
        if not query.aggregates.by_expression:
            return fql

        arguments = ",".join(self._render_aggregate_arguments(query))
        return self._render_operator(
            f"{self._config.operator_naming.aggregate_name}({fql.operand},{arguments})"
        )

    def _render_aggregate_arguments(self, query: Query) -> list[str]:
        """The aggregates of `query` as keyword arguments of the operator that computes them."""
        calls = {}
        for aggregate, column in query.aggregates.by_expression.items():
            _reject_operator_parameter_name(column.name)
            calls[column.name] = self._render_aggregate_call(
                aggregate, query.tree.is_joined
            )
        return self._render_keyword_arguments(calls)

    @staticmethod
    def _render_aggregate_call(aggregate: AggregateExpression, joined: bool) -> str:
        """`aggregate` as the FQL aggregation function that computes it."""
        function = _AGGREGATE_NAMES[aggregate.function]
        argument = aggregate.argument
        if argument is None:
            return f"{function}()"

        path = render_path_argument(argument, FDM_LOOKUP_SEPARATOR, joined)
        if not aggregate.distinct:
            return f"{function}({path!r})"

        distinct_function = _DISTINCT_AGGREGATE_FUNCTIONS[aggregate.function]
        distinct_values = f"{set.__name__}({VALUES_NAME})"
        compute = f"lambda {VALUES_NAME}: {distinct_function}({distinct_values})"
        return f"{aggregates.AggregationFunction.__name__}({compute}, {path!r})"

    @staticmethod
    def _render_keyword_arguments(arguments: dict[str, str]) -> list[str]:
        """`arguments` as Python call arguments, collecting the names that cannot be
        written as keywords into one trailing ``**{...}``."""
        for name in arguments:
            reject_unaddressable_name(name)

        rendered = [
            f"{name}={value}"
            for name, value in arguments.items()
            if is_python_name(name)
        ]

        mapped = {
            name: value for name, value in arguments.items() if not is_python_name(name)
        }
        if mapped:
            entries = ", ".join(f"{name!r}: {value}" for name, value in mapped.items())
            rendered.append(f"**{{{entries}}}")

        return rendered

    def _render_computed_aggregation_columns(
        self, query: Query, fql: "_Relation"
    ) -> "_Relation":
        """`fql` with each value over aggregates added, per group or to the one result."""
        function = (
            DictionaryAttributeFunction.add_computed_per_value
            if query.is_grouped
            else DictionaryAttributeFunction.add_computed
        )
        return self._render_computed(
            query,
            fql,
            query.aggregate_computed_columns,
            function,
            query.is_grouped,
        )

    def _render_copies(self, query: Query, fql: "_Relation") -> "_Relation":
        """The result `fql` with each copy added, reading its source's output."""
        function = DictionaryAttributeFunction.add_computed_per_value
        if query.is_ungrouped_aggregation:
            function = DictionaryAttributeFunction.add_computed

        parameter = _nested_name(TUPLE_NAME, query.depth)
        for copy, source in query.copy_sources.items():
            self._reject_unreadable_name(copy.name)
            body = self.render_access(parameter, [source.name])
            fql = _Value(
                f"{fql.value}.{function.__name__}({copy.name!r}, "
                f"lambda {parameter}: {body})"
            )

        return fql

    def _render_computed(
        self,
        query: Query,
        fql: "_Relation",
        mapping: GeneratedColumns,
        function: Callable[..., DictionaryAttributeFunction],
        post_group: bool,
    ) -> "_Relation":
        """`fql` with each column of `mapping` added by `function` as a lambda over its expression."""
        # nested like every binding, so a subquery's computed column never shadows
        # the enclosing one a correlated column reads off
        parameter = _nested_name(TUPLE_NAME, query.depth)
        base = ValueBase(parameter, query.tree.is_joined)
        for expression, column in mapping.by_expression.items():
            body = self.render_expression(expression, query, base, post_group)
            fql = _Value(
                f"{fql.value}.{function.__name__}({column.name!r}, "
                f"lambda {parameter}: {body})"
            )

        return fql

    def _render_order_keys(self, query: Query, fql: "_Relation") -> "_Relation":
        """`fql` ranked by ORDER BY and cut to LIMIT; `fql` itself without ORDER BY."""
        if not query.order_keys:
            if query.limit:
                raise UnsupportedSql(
                    Limitation.LIMIT_WITHOUT_ORDER,
                    "LIMIT without ORDER BY is not supported: FQL works on unordered "
                    "sets, so the limit would select arbitrary rows.",
                )

            return fql

        # Mixed ASC/DESC: rank_by carries a single reverse flag, so decompose into a
        # chain of stable sorts with the least significant key innermost.
        groups: list[list[OrderKey]] = []
        for order_key in query.order_keys:
            if groups and groups[-1][0].descending == order_key.descending:
                groups[-1].append(order_key)
            else:
                groups.append([order_key])

        for order_keys in reversed(groups[1:]):
            fql = self._render_subset(query, fql, order_keys)
        return self._render_subset(query, fql, groups[0], query.limit)

    def _render_single_value(self, query: Query, fql: "_Relation") -> "_Relation":
        """The one value of a single-value aggregation in `fql`; `fql` itself for other queries."""
        single_value_column = query.single_value_column
        if not single_value_column:
            return fql

        segments = single_value_column.segments(query.tree.is_joined)
        value = self.render_access(fql.value, segments)
        return _Value(value)

    def _render_filtered(
        self,
        query: Query,
        fql: "_Relation",
        condition: Expression | None,
        post_group: bool,
        joined: bool,
    ) -> "_Relation":
        """`fql` filtered by `condition`; `fql` itself without a condition."""
        if condition is None:
            return fql

        if self._config.predicate_form is PredicateForm.LAMBDA:
            predicate = self._render_lambda(query, condition, post_group, joined)
            return self._render_filter_call(fql, predicate)

        return self._render_structured_filter(query, fql, condition, post_group, joined)

    def _render_structured_filter(
        self,
        query: Query,
        fql: "_Relation",
        expression: Expression,
        post_group: bool,
        joined: bool,
    ) -> "_Relation":
        """`fql` filtered by the structured predicates of the conjuncts, and by a lambda per conjunct without one."""
        conditions = expression.conjuncts()
        predicates = StructuredPredicates(query, post_group, joined)
        structured = [predicates.render(conjunct) for conjunct in conditions]

        # fallback to lambda syntax if any conjunct is not expressible as a structured predicate
        for conjunct, rendered in zip(conditions, structured):
            if rendered is None:
                predicate = self._render_lambda(query, conjunct, post_group, joined)
                fql = self._render_filter_call(fql, predicate)

        expressible = [rendered for rendered in structured if rendered is not None]
        if not expressible:
            return fql

        predicate = self._render_conjunction(expressible)
        return self._render_filter_call(fql, predicate)

    def _render_lambda(
        self,
        query: Query,
        expression: Expression,
        post_group: bool,
        joined: bool,
    ) -> str:
        """`expression` as a filter lambda over the tuples of `query`."""
        call_chaining = self._config.call_chaining
        parameter = _nested_name(call_chaining.binding, query.depth)
        base = ValueBase(call_chaining.render_value_access(parameter), joined)
        body = self.render_expression(expression, query, base, post_group)
        return f"lambda {parameter}: {body}"

    @staticmethod
    def _render_conjunction(rendered_predicates: list[str]) -> str:
        """The predicates combined with `And`; a single one as it is."""
        if len(rendered_predicates) == 1:
            return rendered_predicates[0]

        return f"{And.__name__}({', '.join(rendered_predicates)})"

    def _render_filter_call(
        self, fql: "_Relation", rendered_predicate: str
    ) -> "_Relation":
        """`fql` filtered by `rendered_predicate` in the configured call style."""
        if self._config.call_chaining is CallChaining.METHOD:
            filter_name = self._config.operator_naming.filter_name
            return _Value(f"{fql.value}.{filter_name}({rendered_predicate})")

        return self._render_operator(
            f"{filters.filter_values.__name__}({fql.operand}, {rendered_predicate})"
        )

    def _render_subset(
        self,
        query: Query,
        fql: "_Relation",
        order_keys: list[OrderKey],
        limit: Limit | None = None,
    ) -> "_Relation":
        """`fql` ranked by `order_keys`, cut to `limit` when given."""
        item_name = _nested_name(ITEM_NAME, query.depth)
        item_value = CallChaining.METHOD.render_value_access(item_name)
        base = ValueBase(item_value, query.tree.is_joined)
        keys = [
            self.render_expression(order_key.expression, query, base, query.is_grouped)
            for order_key in order_keys
        ]
        ranking_key = render_tuple_literal(keys)
        reverse_argument = ", reverse=True" if order_keys[0].descending else ""

        args = [
            fql.operand,
            f"ranking_key=lambda {item_name}: {ranking_key}{reverse_argument}",
        ]

        if not limit:
            return self._render_operator(f"{rank.rank_by.__name__}({', '.join(args)})")

        args.append(f"k={limit.count}")

        if limit.offset:
            args.append(f"offset={limit.offset}")

        return self._render_operator(f"{subsets.subset.__name__}({', '.join(args)})")

    def _render_comparison(
        self,
        comparison: Expression,
        query: Query | None,
        value_base: ValueBase,
        post_group: bool,
        negated: bool,
    ) -> str:
        """`comparison`, or its negation, per decision 004; `!=` counts as a negation.
        `Expression.columns_needed_to_hold` relies on the `and`-joined guards: `Query` leaves
        out a reference test only for a column guarded here."""
        body = self._render_comparison_body(comparison, query, value_base, post_group)
        replaced = _reintroduced_keys(query, post_group)
        compared = comparison.compared_columns(replaced)
        columns = dict.fromkeys(compared)
        if not columns:
            return f"(not {body})" if negated else body

        unequal = (
            isinstance(comparison, BinaryExpression)
            and comparison.op in UNEQUAL_SYMBOLS
        )
        test = f"not {body}" if negated else body
        operator = "not in" if negated != unequal else "in"
        joiner = " or " if negated != unequal else " and "
        memberships = [
            self._render_membership(column, operator, query, value_base, post_group)
            for column in columns
        ]
        guarded = joiner.join([*memberships, test])
        return f"({guarded})"

    def _render_comparison_body(
        self,
        comparison: Expression,
        query: Query | None,
        value_base: ValueBase,
        post_group: bool,
    ) -> str:
        """`comparison` as a Python test, without the guards for unmapped attributes."""

        def render(sub: Expression) -> str:
            """`sub` rendered in the context of the enclosing call."""
            return self.render_expression(sub, query, value_base, post_group)

        if isinstance(comparison, BinaryExpression):
            left = render(comparison.left)
            right = render(comparison.right)
            return f"({left} {comparison.op} {right})"

        if isinstance(comparison, BetweenExpression):
            value = render(comparison.value)
            lower = render(comparison.lower)
            upper = render(comparison.upper)
            return f"({lower} <= {value} <= {upper})"

        if isinstance(comparison, InExpression):
            value = render(comparison.value)
            options = [render(option) for option in comparison.options]
            return f"({value} in [{','.join(options)}])"

        if isinstance(comparison, InSubqueryExpression):
            return self._render_in_subquery(comparison, query, value_base, post_group)

        if isinstance(comparison, LikeExpression):
            return self._render_like(comparison, query, value_base, post_group)

        if isinstance(comparison, IsBooleanExpression):
            value = render(comparison.value)
            return f"({value} == {render_literal(comparison.expected)})"

        raise UnsupportedSql(
            Limitation.UNSUPPORTED_EXPRESSION,
            f"Unsupported comparison type: {type(comparison)}",
        )

    def _render_in_subquery(
        self,
        expression: InSubqueryExpression,
        query: Query | None,
        value_base: ValueBase,
        post_group: bool,
    ) -> str:
        """A test whether the value of `expression` is among the values the subquery selects."""
        subquery = expression.subquery
        if len(subquery.outputs) != 1:
            raise ValueError("An IN subquery must select exactly one column.")

        element_value = CallChaining.METHOD.render_value_access(ELEMENT_NAME)
        value = self.render_expression(expression.value, query, value_base, post_group)
        with self._entering_subquery(subquery, value_base, post_group):
            rendered_subquery = self._render_subquery(subquery)
            selected = self.render_expression(
                subquery.outputs[0].column,
                subquery,
                ValueBase(element_value, subquery.tree.is_joined),
                subquery.is_grouped,
            )
        return f"({value} in [{selected} for {ELEMENT_NAME} in {rendered_subquery}])"

    @contextmanager
    def _entering_subquery(
        self, subquery: Query, value_base: ValueBase, post_group: bool
    ) -> Iterator[None]:
        """While the block renders `subquery`, its correlated columns read the
        enclosing query's tuple off `value_base`, the binding that tuple has where the
        subquery sits, grouped when `post_group`. The binding is forgotten when the
        block ends."""
        if subquery.enclosing is None:
            yield
            return

        enclosing = _EnclosingTuple(value_base, post_group)
        self._enclosing_tuples[subquery.enclosing] = enclosing
        try:
            yield
        finally:
            del self._enclosing_tuples[subquery.enclosing]

    def _render_tuple_identity(
        self,
        identity: TupleIdentityExpression,
        query: Query | None,
        value_base: ValueBase,
        post_group: bool,
    ) -> str:
        """`identity` unguarded: a read that fails drops the tuple, as the join
        drops a tuple without the reference."""
        left = self._render_column(identity.left, query, value_base, post_group)
        right = self._render_column(identity.right, query, value_base, post_group)
        return f"({left} is {right})"

    def _render_like(
        self,
        expression: LikeExpression,
        query: Query | None,
        value_base: ValueBase,
        post_group: bool,
    ) -> str:
        """A Python test whether the value matches the LIKE pattern."""
        value = self.render_expression(expression.value, query, value_base, post_group)
        pattern = expression.pattern
        like = LikePattern(pattern)
        shape = like.shape

        if shape is LikeShape.EXACT:
            return f"({value} == {pattern!r})"

        if shape is LikeShape.CONTAINS:
            return f"({pattern[1:-1]!r} in {value})"

        if shape is LikeShape.SUFFIX:
            return f"({value}.endswith({pattern[1:]!r}))"

        if shape is LikeShape.PREFIX:
            return f"({value}.startswith({pattern[:-1]!r}))"

        regex_pattern = like.regex
        return f"({re.__name__}.{re.fullmatch.__name__}({regex_pattern!r}, {value}) is not None)"

    def _render_membership(
        self,
        column: Column,
        operator: str,
        query: Query | None,
        value_base: ValueBase,
        post_group: bool,
    ) -> str:
        """`column`'s attribute tested with `operator` (`in` or `not in`) against the
        tuple holding it. A reference on the way is read, not tested: in a filter
        lambda the failed read drops a tuple without it, as the join drops it."""
        read = self._column_read(column, query, value_base, post_group)
        *holder_segments, attribute = read.segments
        reject_unaddressable_name(attribute)
        holder = self.render_access(read.base.expression, holder_segments)
        return f"({attribute!r} {operator} {holder})"

    def _render_column(
        self,
        column: Column,
        query: Query | None,
        value_base: ValueBase,
        post_group: bool,
    ) -> str:
        """`column` read off `value_base`, unless it is correlated into an enclosing
        query - it then reads off that query's item."""
        read = self._column_read(column, query, value_base, post_group)
        return self.render_access(read.base.expression, read.segments)

    def _column_read(
        self,
        column: Column,
        query: Query | None,
        value_base: ValueBase,
        post_group: bool,
    ) -> "_ColumnRead":
        """The binding `column` is read off, and its path from there: the enclosing
        query's tuple, as grouped there, when `column` is correlated."""
        if query is None:
            raise ValueError(
                "No query provided, cannot determine whether column is correlated."
            )

        owner = query.owner_of(column)
        if owner is not query:
            enclosing = self._enclosing_tuples.get(owner)
            if enclosing is None:
                raise ValueError(
                    "A correlated column is rendered outside the query that encloses it."
                )
            value_base = enclosing.value_base
            post_group = enclosing.post_group

        segments = group_aware_segments(column, owner, post_group, value_base.joined)
        return _ColumnRead(value_base, segments)

    def _render_substring(
        self,
        expression: SubstringExpression,
        query: Query | None,
        value_base: ValueBase,
        post_group: bool,
    ) -> str:
        """The SQL SUBSTRING as a Python slice."""
        value = self.render_expression(expression.value, query, value_base, post_group)
        # SQL SUBSTRING is 1-based, Python slicing is 0-based; SQL counts a start
        # of 0 as before the first character, which the length then covers
        start = expression.start - 1
        first = max(start, 0)
        if expression.length is None:
            return f"{value}[{first}:]"

        end = max(start + expression.length, 0)
        return f"{value}[{first}:{end}]"

    def _render_operator(self, operator: str) -> "_OperatorCall":
        """`operator` as a call whose result is taken as `Config.operator_result` says."""
        return _OperatorCall(operator, self._config.operator_result)


@dataclass(frozen=True)
class _Value:
    """FQL text that already is an attribute function or a value."""

    text: str

    @property
    def value(self) -> str:
        """The text as it is."""
        return self.text

    @property
    def operand(self) -> str:
        """The text as it is: an operator takes a value as its input too."""
        return self.text


@dataclass(frozen=True)
class _OperatorCall:
    """An FQL operator call whose result is not taken yet."""

    call: str
    operator_result: OperatorResult

    @property
    def value(self) -> str:
        """The call with its result taken."""
        return self.operator_result.render(self.call)

    @property
    def operand(self) -> str:
        """The bare call: an operator resolves an operator input itself."""
        return self.call


type _Relation = _Value | _OperatorCall


@dataclass(frozen=True)
class _ColumnRead:
    """The binding a column is read off, and its attribute path from there."""

    base: ValueBase
    segments: list[str]


@dataclass(frozen=True)
class _EnclosingTuple:
    """The tuple of an enclosing query, as a subquery's correlated columns read it."""

    value_base: ValueBase
    post_group: bool


def _reject_operator_parameter_name(name: str) -> None:
    """Raises `UnsupportedSql` for an aggregate name that the operator call would bind
    twice."""
    if name in _OPERATOR_PARAMETER_NAMES:
        raise UnsupportedSql(
            Limitation.OPERATOR_PARAMETER_NAME,
            f"Unsupported aggregate name {name!r}: the FQL operator has a parameter "
            "of that name.",
        )


def _reintroduced_keys(
    query: Query | None, post_group: bool
) -> dict[Expression, Column]:
    """After grouping, each computed group key as the attribute it is reintroduced
    under; nothing before grouping."""
    if not post_group or query is None:
        return {}

    return query.computed_columns.by_expression


def _nested_name(name: str, index: int) -> str:
    """`name` at nesting level `index`, suffixed from the first level inwards so a
    subquery's binding never shadows the one it is rendered inside of."""
    return name if index == 0 else f"{name}{index}"

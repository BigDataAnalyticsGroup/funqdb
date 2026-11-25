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

"""Reads a SELECT, as the optimizer normalized it, into a `Query` of the IR."""

from sqlglot import Expr, exp

from benchmarks.datasets.schema import Schema
from benchmarks.tools.ir.expressions import Column, Expression, conjunction
from benchmarks.tools.ir.query import Limit, OrderKey, Output, Query
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.parse.cte import CteDefinition
from benchmarks.tools.parse.expression_parser import (
    parse_aggregate_expression,
    parse_operand,
    parse_value_expression,
    parse_where_expression,
    reject_reference_value,
    resolve_column,
)
from benchmarks.tools.parse.join_tree_parser import (
    build_join_tree,
    read_from,
    split_conditions,
)
from benchmarks.tools.parse.node_args import sql_conjuncts
from benchmarks.tools.parse.parse_state import ParseState


def parse_query(
    ast: Expr,
    schema: Schema,
    visible_ctes: dict[str, CteDefinition] = {},
    enclosing: Query | None = None,
    reserved: frozenset[str] = frozenset(),
) -> Query:
    """`ast` as its IR; `reserved` adds alias names of an enclosing statement."""
    definition = _parse_definition(ast, schema, visible_ctes, enclosing, reserved)
    return definition.query


def aliases(ast: Expr) -> frozenset[str]:
    """Every alias the statement holding `ast` declares, in any of its queries."""
    tables = ast.root().find_all(exp.Table)
    return frozenset(table.alias_or_name for table in tables)


def where_conjuncts(ast: Expr) -> list[Expr]:
    """The top-level conjuncts of the WHERE clause of `ast`; none without one."""
    where_node = ast.args.get("where")
    if where_node is None:
        return []

    return sql_conjuncts(where_node.this)


def _parse_ctes(
    ast: Expr,
    schema: Schema,
    visible_ctes: dict[str, CteDefinition],
    enclosing: Query | None,
    reserved: frozenset[str],
) -> dict[str, CteDefinition]:
    """The CTEs `ast` may reference: the visible ones plus its own WITH clause,
    each parsed against those declared before it."""
    ctes = dict(visible_ctes)

    with_node = ast.args.get("with_")
    if with_node is None:
        return ctes

    for cte_node in with_node.expressions:
        ctes[cte_node.alias] = _parse_definition(
            cte_node.this, schema, ctes, enclosing, reserved
        )

    return ctes


def _parse_where_condition(conditions: list[Expr], state: ParseState) -> None:
    """Sets the stated condition from `conditions`."""
    parsed_conditions: list[Expression] = []
    for condition in conditions:
        expression = parse_where_expression(condition, state)
        parsed_conditions.append(expression)

    if parsed_conditions:
        state.query.stated_condition = conjunction(*parsed_conditions)


def _parse_select(
    ast: Expr,
    state: ParseState,
) -> None:
    """Adds the outputs of the SELECT list; a star adds none, which reads its root's attributes."""
    wildcards = [column for column in ast.expressions if _is_wildcard_column(column)]
    if wildcards:
        if len(ast.expressions) > 1:
            raise UnsupportedSql(
                Limitation.WILDCARD_WITH_EXPRESSIONS,
                "Wildcard select cannot be combined with other expressions.",
            )
        (wildcard,) = wildcards
        _reject_partial_star(wildcard, state.query)
        return

    for item in ast.expressions:
        if isinstance(item, exp.Alias):
            expression, alias = item.this, item.alias
        else:
            expression, alias = item, item.alias_or_name

        column = _parse_select_item(expression, alias, state)
        # an unnamed output (in an INSERT's VALUES subquery) keeps its generated name
        name = alias or column.name
        output = Output(column, name)
        state.query.outputs.append(output)

    _reject_duplicate_output_names(state.query)


def _parse_select_item(expression: Expr, alias: str, state: ParseState) -> Column:
    """The column holding `expression`; a computed or aggregated one is named `alias`, or generated if `alias` is empty."""
    if isinstance(expression, exp.Column):
        return resolve_column(expression, state)

    name = alias or None

    if isinstance(expression, exp.AggFunc):
        aggregate_expression = parse_aggregate_expression(expression, state)
        return state.query.aggregates.column_for(aggregate_expression, name)

    if not _contains_aggregate(expression):
        computed_column = parse_value_expression(expression, state)
        return state.query.computed_columns.column_for(computed_column, name)

    computed_column = parse_value_expression(expression, state, aggregation_level=True)
    return state.query.aggregate_computed_columns.column_for(computed_column, name)


def _reject_partial_star(wildcard: Expr, query: Query) -> None:
    """Rejects a star of a query's result that reads more than the attributes of one root."""
    # EXISTS reads no outputs of its subquery, while IN reads the one output
    if isinstance(wildcard.parent_select.parent, exp.Exists):
        return

    alias = wildcard.table if isinstance(wildcard, exp.Column) else ""
    is_root_star = query.tree.roots == (alias,)
    is_single_alias_star = not alias and len(query.tree.aliases) == 1
    if is_root_star or is_single_alias_star:
        return

    raise UnsupportedSql(
        Limitation.WILDCARD_WITH_EXPRESSIONS,
        f"Unsupported {wildcard.sql()}: without a schema to expand it, a star "
        "reads the attributes of a single root only.",
    )


def _reject_duplicate_output_names(query: Query) -> None:
    """Rejects repeated output names: a result row is keyed by output name."""
    output_names = [output.name for output in query.outputs]
    if len(set(output_names)) != len(output_names):
        raise UnsupportedSql(
            Limitation.DUPLICATE_OUTPUT_NAME,
            "Unsupported SELECT: every output column needs its own name.",
        )


def _reject_computed_names_of_stored_columns(query: Query, schema: Schema) -> None:
    """Rejects a computed column named as a key of the tuples it is added to."""
    stored = _stored_names(query, schema)
    for column in query.computed_columns.by_expression.values():
        if column.name in stored:
            raise UnsupportedSql(
                Limitation.COMPUTED_ALIAS_SHADOWS_COLUMN,
                f"Unsupported computed output {column.name!r}: the tuples it is "
                "added to store a key of that name.",
            )


def _stored_names(query: Query, schema: Schema) -> set[str]:
    """The keys of the tuples a computed column is added to: the aliases of a join
    tuple, else the root's columns with each foreign key stored as its reference."""
    tree = query.tree
    if tree.is_joined:
        return set(tree.join_members)

    stored: set[str] = set()
    # a query without FROM has no root and stores nothing
    for root in tree.roots:
        table = tree.table_of(root)
        foreign_keys = schema.references.get(table, {})
        for column in schema.columns.get(table, {}):
            if column not in foreign_keys:
                stored.add(column)
        for reference in foreign_keys.values():
            stored.add(reference.ref_key)
    return stored


def _parse_distinct(ast: Expr, query: Query) -> None:
    """SELECT DISTINCT as grouping by every selected column; a grouped query is grouped once more."""
    if not ast.args.get("distinct"):
        return
    if query.is_grouped:
        query.regroups = True
        return
    if query.is_ungrouped_aggregation:
        return
    if not query.outputs:
        raise UnsupportedSql(
            Limitation.WILDCARD_WITH_EXPRESSIONS,
            "SELECT DISTINCT of a star the schema does not expand has no columns to "
            "group by.",
        )

    for output in query.projected_outputs:
        column = output.column
        # a correlated column is one value per evaluation of this query
        if query.owner_of(column) is not query:
            continue
        reject_reference_value(column)
        query.group_keys.append(column)


def _parse_group_by(
    ast: Expr,
    state: ParseState,
) -> None:
    """Adds the GROUP BY keys; a computed key gets a computed column."""
    group_node = ast.args.get("group")
    if group_node is None:
        return

    for expression in group_node.expressions:
        if isinstance(expression, exp.Column):
            column = resolve_column(expression, state)
            reject_reference_value(column)
            state.query.group_keys.append(column)
            continue

        computed_column = parse_operand(expression, state, aggregation_level=False)
        key = state.query.computed_columns.column_for(computed_column)
        state.query.group_keys.append(key)


def _parse_limit(
    ast: Expr,
    query: Query,
) -> None:
    """Sets LIMIT and OFFSET; an OFFSET needs a LIMIT."""
    limit_node = ast.args.get("limit")
    offset_node = ast.args.get("offset")
    if limit_node is None and offset_node is not None:
        raise UnsupportedSql(
            Limitation.OFFSET_WITHOUT_LIMIT, "Unsupported OFFSET without LIMIT."
        )
    if limit_node is None:
        return

    count = _literal_int(limit_node.expression, "LIMIT")
    if count == 0:
        raise UnsupportedSql(
            Limitation.LIMIT_ZERO, "Unsupported LIMIT 0: it selects no rows."
        )

    offset = (
        None if offset_node is None else _literal_int(offset_node.expression, "OFFSET")
    )
    query.limit = Limit(count=count, offset=offset)


def _parse_order_by(
    ast: Expr,
    state: ParseState,
) -> None:
    """Adds the ORDER BY keys in stated order."""
    order_node = ast.args.get("order")
    if order_node is None:
        return

    for ordered in order_node.expressions:
        descending = bool(ordered.args.get("desc"))
        key = _parse_order_key(ordered.this, state)
        state.query.order_keys.append(OrderKey(key, descending))


def _parse_order_key(
    key: Expr,
    state: ParseState,
) -> Expression:
    """The value `key` orders by; a column may name an output."""
    if isinstance(key, exp.Column):
        resolved = resolve_column(key, state)
        column = state.query.lookup_output_column(resolved)
        reject_reference_value(column)
        return column

    aggregated = _contains_aggregate(key)
    return parse_operand(key, state, aggregation_level=aggregated)


def _parse_having(
    ast: Expr,
    state: ParseState,
) -> None:
    """Sets the HAVING condition; it needs a GROUP BY."""
    having_node = ast.args.get("having")
    if having_node is None:
        return
    if not state.query.group_keys:
        raise UnsupportedSql(
            Limitation.HAVING_WITHOUT_GROUP_BY,
            "HAVING without GROUP BY is not supported.",
        )

    aggregated = _contains_aggregate(having_node.this)
    expression = parse_where_expression(
        having_node.this, state, aggregation_level=aggregated
    )
    state.query.having_condition = expression


def _contains_aggregate(node: Expr) -> bool:
    """True when `node` aggregates in its own query; a subquery aggregates in its own."""
    nodes = node.walk(prune=lambda child: isinstance(child, exp.Select))
    return any(isinstance(child, exp.AggFunc) for child in nodes)


def _reject_ungrouped_columns(query: Query) -> None:
    """Rejects an output column a grouped or aggregating query has no single value of."""
    if not query.is_grouped and not query.aggregates.by_expression:
        return

    for output in query.outputs:
        column = output.column
        is_own_column = column.path is not None and query.owner_of(column) is query
        if is_own_column and column not in query.group_keys:
            raise ValueError(
                f"Output '{output.name}' is neither grouped by nor aggregated."
            )


def _is_wildcard_column(column: Expr) -> bool:
    """True for `*` and for `alias.*`."""
    return isinstance(column, exp.Star) or (
        isinstance(column, exp.Column) and isinstance(column.this, exp.Star)
    )


def _literal_int(node: Expr, clause: str) -> int:
    """The integer literal `node` of `clause`; rejects any other count."""
    if not (isinstance(node, exp.Literal) and node.is_int):
        raise UnsupportedSql(
            Limitation.NON_LITERAL_LIMIT,
            f"Unsupported {clause} {node.sql()}: only an integer literal translates.",
        )
    return int(node.this)


def _parse_definition(
    ast: Expr,
    schema: Schema,
    visible_ctes: dict[str, CteDefinition],
    enclosing: Query | None,
    reserved: frozenset[str],
) -> CteDefinition:
    """`ast` as its IR, with all conjuncts of FROM, ON and WHERE, also those that became edges."""
    if isinstance(ast, exp.SetOperation):
        raise UnsupportedSql(
            Limitation.SET_OPERATION, f"Unsupported {ast.key.upper()}."
        )
    if not isinstance(ast, exp.Select):
        raise UnsupportedSql(Limitation.NO_FROM, "Select not found.")

    # a copied CTE condition has no parent, so `reserved` adds the statement's aliases
    statement_aliases = aliases(ast)
    depth = 0 if enclosing is None else enclosing.depth + 1
    reserved_aliases = reserved | statement_aliases
    ctes = _parse_ctes(ast, schema, visible_ctes, enclosing, reserved_aliases)

    from_clause = read_from(ast, ctes, reserved_aliases)
    where_conditions = where_conjuncts(ast)
    conditions = [*from_clause.conditions, *where_conditions]

    split = split_conditions(conditions, from_clause.tables, schema)

    tree = build_join_tree(
        from_clause.tables, split.equalities, schema, depth, reserved_aliases
    )
    query = Query(tree, enclosing)
    state = ParseState(query, ctes, reserved_aliases)
    _parse_where_condition(split.filters, state)

    # SELECT first as it defines the alias of expressions used throughout the query (e.g. in GROUP BY, HAVING, ORDER BY)
    _parse_select(ast, state)
    _reject_computed_names_of_stored_columns(query, schema)
    _parse_group_by(ast, state)
    _reject_ungrouped_columns(query)
    _parse_having(ast, state)
    # after HAVING, so grouping for DISTINCT does not admit a HAVING without GROUP BY
    _parse_distinct(ast, query)
    _parse_order_by(ast, state)
    _parse_limit(ast, query)

    return CteDefinition(query, tuple(conditions))

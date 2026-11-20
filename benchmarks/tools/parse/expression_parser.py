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

"""Reads the conditions and values of a query into expressions of the IR."""

from datetime import date, datetime
from decimal import Decimal

from sqlglot import Expr, exp

from benchmarks.datasets.schema import PrimaryKey
from benchmarks.tools.ir.expressions import (
    AggregateExpression,
    BetweenExpression,
    BinaryExpression,
    CaseBranch,
    CaseWhenExpression,
    CastExpression,
    Column,
    ExistsExpression,
    Expression,
    ExtractExpression,
    ExtractPart,
    InExpression,
    InSubqueryExpression,
    IntervalExpression,
    IntervalUnit,
    IsBooleanExpression,
    IsNullExpression,
    LikeExpression,
    Literal,
    ParenExpression,
    SubqueryExpression,
    SubstringExpression,
    TupleIdentityExpression,
    UnaryExpression,
)
from benchmarks.tools.ir.query import Query
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.parse.parse_state import ParseState

# keyed by exact type: sqlglot defines no subclass of any of these node classes
_LOGICAL_OPERATORS: dict[type[Expr], str] = {exp.And: "and", exp.Or: "or"}
_COMPARISON_OPERATORS: dict[type[Expr], str] = {
    exp.EQ: "==",
    exp.NEQ: "!=",
    exp.LT: "<",
    exp.LTE: "<=",
    exp.GT: ">",
    exp.GTE: ">=",
}
_EQUALITY_OPERATORS = frozenset({"==", "!="})
# foreign keys compare by the tuple their references lead to
_REFERENCE_IDENTITY_OPERATORS = {"==": "is", "!=": "is not"}
_ARITHMETIC_OPERATORS: dict[type[Expr], str] = {
    exp.Add: "+",
    exp.Sub: "-",
    exp.Mul: "*",
    exp.Div: "/",
}
# the Python type a temporal literal of each SQL type is read as
_TEMPORAL_LITERAL_TYPES: dict[exp.DataType.Type, type[date]] = {
    exp.DataType.Type.DATE: date,
    exp.DataType.Type.TIMESTAMP: datetime,
    exp.DataType.Type.TIMESTAMPNTZ: datetime,
    exp.DataType.Type.DATETIME: datetime,
}
# FDM has no time of day without a date, and no time zone
_UNTRANSLATED_TEMPORAL_TYPES = frozenset(
    {
        exp.DataType.Type.TIME,
        exp.DataType.Type.TIMETZ,
        exp.DataType.Type.TIMESTAMPTZ,
        exp.DataType.Type.TIMESTAMPLTZ,
    }
)


def reject_reference_value(expression: Expression) -> None:
    """Rejects a foreign key read as a value (computed with, aggregated, ordered, grouped or assigned)."""
    unwrapped = expression.unparenthesized()
    if _is_reference(unwrapped):
        raise UnsupportedSql(
            Limitation.FOREIGN_KEY_AS_VALUE,
            "Unsupported use of a foreign key as a value: a foreign key is a "
            "reference in FDM, not the key value SQL reads.",
        )


def parse_operand(
    expression: Expr,
    state: ParseState,
    aggregation_level: bool,
) -> Expression:
    """`expression` as the operand of a computation, which reads its value."""
    operand = parse_value_expression(expression, state, aggregation_level)
    reject_reference_value(operand)
    return operand


def parse_where_expression(
    expression: Expr,
    state: ParseState,
    aggregation_level: bool = False,
) -> Expression:
    """`expression` as a condition or, for anything else, as a value."""
    if isinstance(expression, exp.Not):
        value = parse_where_expression(expression.this, state, aggregation_level)
        return UnaryExpression("not", value)

    if isinstance(expression, exp.Paren):
        value = parse_where_expression(expression.this, state, aggregation_level)
        return ParenExpression(value)

    if type(expression) in _LOGICAL_OPERATORS:
        operator = _LOGICAL_OPERATORS[type(expression)]
        return _parse_binary_where_expression(
            expression, operator, state, aggregation_level
        )

    if type(expression) in _COMPARISON_OPERATORS:
        operator = _COMPARISON_OPERATORS[type(expression)]
        comparison = _parse_binary_where_expression(
            expression, operator, state, aggregation_level
        )
        key_identity = _key_identity(comparison, state.query)
        if key_identity is not None and operator == "==":
            return key_identity
        if key_identity is not None and operator == "!=":
            return UnaryExpression("not", key_identity)

        _reject_untranslatable_operands(
            (comparison.left, comparison.right),
            state.query,
            equality=operator in _EQUALITY_OPERATORS,
        )
        if _is_reference(comparison.left.unparenthesized()):
            identity = _REFERENCE_IDENTITY_OPERATORS[operator]
            return BinaryExpression(identity, comparison.left, comparison.right)
        return comparison

    if isinstance(expression, exp.Between):
        value = parse_where_expression(expression.this, state, aggregation_level)
        lower = parse_where_expression(expression.args["low"], state, aggregation_level)
        upper = parse_where_expression(
            expression.args["high"], state, aggregation_level
        )
        _reject_untranslatable_operands(
            (value, lower, upper), state.query, equality=False
        )
        return BetweenExpression(value=value, lower=lower, upper=upper)

    if isinstance(expression, exp.In):
        return _parse_in_expression(expression, state, aggregation_level)

    if isinstance(expression, exp.Exists):
        subquery = _parse_subquery(expression.this, state)
        return ExistsExpression(subquery=subquery)

    if isinstance(expression, exp.Like):
        value = parse_operand(expression.this, state, aggregation_level)
        pattern_node = expression.expression

        if not isinstance(pattern_node, exp.Literal) or not pattern_node.is_string:
            raise _unsupported_where(expression, "only a text literal is a pattern")

        like = LikeExpression(value=value, pattern=pattern_node.this)
        if expression.args.get("negate"):
            return UnaryExpression("not", like)
        return like

    if isinstance(expression, exp.Is):
        value = parse_where_expression(expression.this, state, aggregation_level)
        check = parse_where_expression(expression.expression, state, aggregation_level)
        if not isinstance(check, Literal):
            raise _unsupported_where(expression, "IS tests only NULL, TRUE or FALSE")

        if check.value is None:
            return IsNullExpression(value=value)

        if isinstance(check.value, bool):
            return IsBooleanExpression(value=value, expected=check.value)

        raise _unsupported_where(expression, "IS tests only NULL, TRUE or FALSE")

    return parse_value_expression(expression, state, aggregation_level)


def parse_value_expression(
    expression: Expr,
    state: ParseState,
    aggregation_level: bool = False,
) -> Expression:
    """`expression` as a value; `aggregation_level` rejects a column outside an aggregate."""
    if isinstance(expression, exp.Column):
        if aggregation_level:
            raise UnsupportedSql(
                Limitation.AGGREGATE_MIXED_WITH_COLUMN,
                f"Unsupported column {expression.sql()} beside an aggregate: a group "
                "has no single value of it.",
            )
        return resolve_column(expression, state)

    if isinstance(expression, exp.AggFunc):
        if not aggregation_level:
            raise UnsupportedSql(
                Limitation.AGGREGATE_MIXED_WITH_COLUMN,
                f"Unsupported aggregate {expression.sql()} in a value of one row, as "
                "in WHERE or beside a plain column.",
            )
        aggregate_expression = parse_aggregate_expression(expression, state)
        column = state.query.aggregates.column_for(aggregate_expression)
        return column

    if isinstance(expression, exp.Literal):
        if expression.is_string:
            return Literal(expression.this)

        number = _parse_numeric_literal(expression.this)
        return Literal(number)

    if isinstance(expression, exp.Null):
        return Literal(None)

    if isinstance(expression, exp.Boolean):
        truth = bool(expression.this)
        return Literal(truth)

    if isinstance(expression, exp.Paren):
        value = parse_value_expression(expression.this, state, aggregation_level)
        return ParenExpression(value)

    if isinstance(expression, exp.Case):
        return _parse_case_expression(expression, state, aggregation_level)

    if isinstance(expression, exp.Substring):
        return _parse_substring_expression(expression, state, aggregation_level)

    if isinstance(expression, exp.Extract):
        return _parse_extract_expression(expression, state, aggregation_level)

    if isinstance(expression, exp.Cast):
        return _parse_cast_expression(expression, state, aggregation_level)

    if isinstance(expression, exp.Interval):
        return _parse_interval_expression(expression)

    is_negated_test = isinstance(expression, exp.Not) and isinstance(
        expression.this, exp.Is
    )
    if isinstance(expression, exp.Is) or is_negated_test:
        return parse_where_expression(expression, state, aggregation_level)

    if isinstance(expression, exp.Neg):
        value = parse_operand(expression.this, state, aggregation_level)
        return UnaryExpression("-", value)

    if type(expression) in _ARITHMETIC_OPERATORS:
        operator = _ARITHMETIC_OPERATORS[type(expression)]
        return _parse_binary_value_expression(
            expression, operator, state, aggregation_level
        )

    # Python's `+` concatenates only strings, as `||` does
    if isinstance(expression, exp.DPipe):
        return _parse_binary_value_expression(expression, "+", state, aggregation_level)

    if isinstance(expression, exp.Subquery):
        # `unnest`: SQL may wrap the subquery in further parentheses
        subquery = _parse_subquery(expression.unnest(), state)
        if not subquery.is_single_value_aggregation:
            raise UnsupportedSql(
                Limitation.SCALAR_SUBQUERY_ROWS,
                "Unsupported scalar subquery: it must aggregate to a single value, "
                "as one selecting rows renders to a relation, not to a value.",
            )
        return SubqueryExpression(subquery)

    raise UnsupportedSql(
        Limitation.UNSUPPORTED_EXPRESSION,
        f"Unsupported expression {expression.sql()}: no IR node reads "
        f"{expression.key.upper()}.",
    )


def resolve_column(column: exp.Column, state: ParseState) -> Column:
    """`column` read at the end of its alias' path, in this query or the nearest enclosing one declaring it;
    a foreign key column becomes the reference stored under its name."""
    if not column.table:
        return Column(None, column.name)

    query = state.query
    table = query.tree.table_of(column.table)
    while table is None:
        if query.enclosing is None:
            raise ValueError(
                f"Unknown table '{column.table}' in column reference '{column}'."
            )
        query = query.enclosing
        table = query.tree.table_of(column.table)

    tree = query.tree
    path = tree.path_of(column.table)
    reference = tree.schema.reference_of(table, column.name)
    if reference is None:
        return Column(path, column.name)

    return Column(path, reference.ref_key, is_reference_column=True)


def parse_aggregate_expression(
    expression: Expr,
    state: ParseState,
) -> AggregateExpression:
    """`expression` as an aggregate; an argument that is no column becomes a computed one."""
    function = expression.key.lower()
    arg = expression.this
    distinct = isinstance(arg, exp.Distinct)
    if distinct:
        # the SQL baseline admits one expression only
        (arg,) = arg.expressions
    if isinstance(arg, exp.Column) and isinstance(arg.this, exp.Star):
        raise UnsupportedSql(
            Limitation.DIALECT_SPECIFIC,
            f"Unsupported aggregate of {arg.sql()}: ISO SQL counts rows with COUNT(*).",
        )
    _reject_aggregate_of_enclosing_query(expression, state)
    if arg is None or isinstance(arg, exp.Star):
        if function != exp.Count.key or distinct:
            raise ValueError(
                f"{expression.sql()} is no aggregate: only COUNT(*) reads a star."
            )
        return AggregateExpression(function=function, argument=None)
    if isinstance(arg, exp.Column):
        column = resolve_column(arg, state)
        # COUNT reads whether a value is there, which a reference tells as well;
        # but references cannot be told apart by value
        if function != exp.Count.key or distinct:
            reject_reference_value(column)
        return AggregateExpression(
            function=function, argument=column, distinct=distinct
        )
    computed_expression = parse_value_expression(arg, state, aggregation_level=False)
    if function != exp.Count.key or distinct:
        reject_reference_value(computed_expression)
    computed_column = state.query.computed_columns.column_for(computed_expression)
    return AggregateExpression(
        function=function, argument=computed_column, distinct=distinct
    )


def _reject_aggregate_of_enclosing_query(aggregate: Expr, state: ParseState) -> None:
    """Rejects an aggregate reading only enclosing columns: SQL evaluates it in that
    query, while the IR holds it in the query it is written in."""
    tree = state.query.tree
    tables = [column.table for column in aggregate.find_all(exp.Column)]
    if tables and all(tree.table_of(table) is None for table in tables):
        raise UnsupportedSql(
            Limitation.UNSUPPORTED_EXPRESSION,
            f"Unsupported {aggregate.sql()}: it aggregates in the enclosing query.",
        )


def _parse_subquery(ast: Expr, state: ParseState) -> Query:
    """`ast` as a query nested in the one of `state`, which its columns may read."""
    # a subquery is a whole query again, import locally to avoid circular imports
    from benchmarks.tools.parse.query_parser import parse_query

    query = state.query
    schema = query.tree.schema
    return parse_query(ast, schema, state.ctes, query, state.reserved)


def _key_identity(
    comparison: BinaryExpression, query: Query
) -> TupleIdentityExpression | None:
    """The identity of a foreign key with the tuple whose key it equals; `None` for other comparisons."""
    left = comparison.left.unparenthesized()
    right = comparison.right.unparenthesized()
    if _is_reference(right):
        left, right = right, left
    if not _is_reference(left) or not isinstance(right, Column):
        return None
    if right.path is None:
        return None

    target_table = _target_table(left, query)
    key_tree = query.owner_of(right).tree
    key_table = key_tree.table_of(right.path.alias)
    key_columns = PrimaryKey((right.name,))
    if (
        key_table != target_table
        or key_tree.schema.primary_keys.get(key_table) != key_columns
    ):
        return None

    keyed_tuple = Column.tuple_at(right.path)
    return TupleIdentityExpression(left, keyed_tuple)


def _reject_untranslatable_operands(
    operands: tuple[Expression, ...], query: Query, equality: bool
) -> None:
    """Rejects operands of one comparison, BETWEEN or IN in `query` that Python would not compare as SQL does.
    `equality` is True for `=`, `<>` and IN; foreign keys then compare as references, so only keys into one table.
    """
    unwrapped = [operand.unparenthesized() for operand in operands]
    if Literal(None) in unwrapped:
        raise UnsupportedSql(
            Limitation.NULL_LITERAL_COMPARISON,
            "Unsupported comparison with NULL: in SQL its outcome is unknown rather "
            "than true or false, so use IS [NOT] NULL.",
        )

    references = [operand for operand in unwrapped if _is_reference(operand)]
    if not references:
        return

    if not equality:
        raise UnsupportedSql(
            Limitation.FOREIGN_KEY_ORDERED,
            "Unsupported ordering of a foreign key: a reference in FDM has no order, "
            "where SQL orders the key values.",
        )

    if len(references) != len(operands):
        raise UnsupportedSql(
            Limitation.FOREIGN_KEY_AGAINST_SCALAR,
            "Unsupported comparison of a foreign key with a scalar: a foreign key is "
            "a reference in FDM, not the key value SQL compares.",
        )

    target_tables = {_target_table(reference, query) for reference in references}
    if len(target_tables) > 1:
        raise UnsupportedSql(
            Limitation.FOREIGN_KEYS_COMPARED_AS_REFERENCES,
            "Unsupported comparison of foreign keys into different tables: FQL "
            "compares them as references, which never match, where SQL compares "
            "their key values.",
        )


def _target_table(reference: Column, query: Query) -> str:
    """The table that `reference`, read in `query`, refers to."""
    tree = query.owner_of(reference).tree
    table = tree.table_of(reference.path.alias)
    table_references = tree.schema.references_of(table)
    return next(
        candidate.target_table
        for candidate in table_references
        if candidate.ref_key == reference.name
    )


def _parse_in_expression(
    expression: exp.In,
    state: ParseState,
    aggregation_level: bool,
) -> Expression:
    """IN as a membership test; IN a subquery aggregating to one value is the equality with it."""
    value = parse_where_expression(expression.this, state, aggregation_level)

    query_node = expression.args.get("query")
    if isinstance(query_node, exp.Subquery):
        subquery = _parse_subquery(query_node.this, state)
        if len(subquery.outputs) > 1:
            raise ValueError(f"IN compares with one column: {query_node.sql()}.")
        selected = [output.column for output in subquery.outputs]
        _reject_untranslatable_operands((value, *selected), subquery, equality=True)
        if subquery.is_single_value_aggregation:
            aggregate = SubqueryExpression(subquery)
            return BinaryExpression("==", left=value, right=aggregate)

        return InSubqueryExpression(value=value, subquery=subquery)

    if not expression.expressions:
        raise _unsupported_where(expression, "IN reads a list or a subquery")

    options = tuple(
        parse_where_expression(option, state, aggregation_level)
        for option in expression.expressions
    )
    _reject_untranslatable_operands((value, *options), state.query, equality=True)
    return InExpression(value=value, options=options)


def _parse_case_expression(
    expression: exp.Case,
    state: ParseState,
    aggregation_level: bool,
) -> CaseWhenExpression:
    """CASE as its branches in order and its default; a simple CASE as a searched one."""
    searched = _without_case_base(expression)
    branches = tuple(
        CaseBranch(
            parse_where_expression(if_node.this, state, aggregation_level),
            parse_operand(if_node.args["true"], state, aggregation_level),
        )
        for if_node in searched.args.get("ifs", [])
    )

    default_node = searched.args.get("default")
    default = (
        None
        if default_node is None
        else parse_operand(default_node, state, aggregation_level)
    )

    return CaseWhenExpression(branches=branches, default=default)


def _without_case_base(case: exp.Case) -> exp.Case:
    """`case` with its base value desugared
    (`CASE x WHEN v` into `CASE WHEN x = v`) to check each branch as a comparison."""
    base = case.this
    if base is None:
        return case

    searched = case.copy()
    searched.set("this", None)
    for if_node in searched.args.get("ifs", []):
        compared = base.copy()
        comparison = exp.EQ(this=compared, expression=if_node.this)
        if_node.set("this", comparison)

    return searched


def _parse_substring_expression(
    expression: exp.Substring,
    state: ParseState,
    aggregation_level: bool,
) -> SubstringExpression:
    """SUBSTRING with literal bounds, which fix the slice when transpiling."""
    value = parse_operand(expression.this, state, aggregation_level)
    start = _parse_substring_bound(expression.args.get("start"))
    length_node = expression.args.get("length")
    length = None if length_node is None else _parse_substring_bound(length_node)

    return SubstringExpression(value=value, start=start, length=length)


def _parse_substring_bound(node: Expr | None) -> int:
    """The integer literal `node`; rejects any other bound."""
    if not isinstance(node, exp.Literal) or not node.is_int:
        raise UnsupportedSql(
            Limitation.SUBSTRING_BOUNDS,
            f"Unsupported SUBSTRING bound type: {type(node)}",
        )
    return int(node.this)


def _parse_extract_expression(
    expression: exp.Extract,
    state: ParseState,
    aggregation_level: bool,
) -> ExtractExpression:
    """EXTRACT of a date or time part; rejects a timezone part."""
    name = expression.this.name.upper()
    part = ExtractPart.__members__.get(name)
    # the SQL-92 fields beyond ExtractPart are the timezone ones
    if part is None:
        raise UnsupportedSql(
            Limitation.EXTRACT_TIMEZONE,
            f"Unsupported EXTRACT part: {expression.this.name.lower()}",
        )

    value = parse_operand(expression.expression, state, aggregation_level)
    return ExtractExpression(part=part, value=value)


def _parse_cast_expression(
    expression: exp.Cast,
    state: ParseState,
    aggregation_level: bool,
) -> Expression:
    """A date or datetime literal, a date passed through, or a CAST to DOUBLE."""
    target = expression.to.this
    operand = expression.this
    # sqlglot reads a typed literal such as `DATE '1995-03-01'` as a CAST of text
    is_text_literal = isinstance(operand, exp.Literal) and operand.is_string
    if target in _UNTRANSLATED_TEMPORAL_TYPES and is_text_literal:
        raise UnsupportedSql(
            Limitation.TEMPORAL_LITERAL,
            f"Unsupported {expression.to.sql()} literal {operand.this!r}: FDM has no "
            "time of day without a date and no time zone.",
        )
    if target in _UNTRANSLATED_TEMPORAL_TYPES:
        raise UnsupportedSql(
            Limitation.TEMPORAL_CAST,
            f"Unsupported CAST to {expression.to.sql()}: FDM has no time of day "
            "without a date and no time zone.",
        )

    if target in _TEMPORAL_LITERAL_TYPES:
        if is_text_literal:
            return _parse_temporal_literal(operand, target)

        # a stored value of another type is rejected by `reject_ill_typed_sql`, so
        # this cast is the optimizer's widening of `DATE ± INTERVAL`: FQL keeps the date
        return parse_operand(operand, state, aggregation_level)

    # int() truncates where SQL rounds, float() ignores a DECIMAL's scale and is
    # double where SQL's FLOAT/REAL is single precision
    if target != exp.DataType.Type.DOUBLE:
        raise UnsupportedSql(
            Limitation.INEXACT_CAST,
            f"Unsupported CAST target type: {expression.to.sql()}; only DOUBLE converts as "
            f"SQL does.",
        )

    value = parse_operand(operand, state, aggregation_level)
    return CastExpression(value)


def _parse_temporal_literal(node: exp.Literal, target: exp.DataType.Type) -> Literal:
    """The string literal `node` as the date or datetime of SQL type `target`; rejects what SQL reads differently."""
    try:
        value = _TEMPORAL_LITERAL_TYPES[target].fromisoformat(node.this)
    except ValueError:
        raise UnsupportedSql(
            Limitation.TEMPORAL_LITERAL,
            f"Unsupported {target.value} literal {node.this!r}: it is no ISO "
            f"{target.value.lower()} Python reads as SQL does.",
        ) from None

    return Literal(value)


def _parse_interval_expression(expression: exp.Interval) -> IntervalExpression:
    """A single-field INTERVAL; a span (`DAY TO HOUR`) is not translated."""
    if isinstance(expression.unit, exp.IntervalSpan):
        raise UnsupportedSql(
            Limitation.UNSUPPORTED_EXPRESSION,
            f"Unsupported INTERVAL span: {expression.sql()}.",
        )

    unit = IntervalUnit[expression.unit.name.upper()]
    count_text = expression.this.name.strip()
    count = _parse_numeric_literal(count_text)
    # timedelta multiplies a whole count exactly; it refuses a Decimal
    if type(count) is not int:
        raise UnsupportedSql(
            Limitation.UNSUPPORTED_EXPRESSION,
            f"Unsupported fractional INTERVAL: {expression.sql()}.",
        )
    return IntervalExpression(unit=unit, count=count)


def _parse_binary_where_expression(
    expression: Expr,
    operator: str,
    state: ParseState,
    aggregation_level: bool,
) -> BinaryExpression:
    """A logical operator or a comparison of two conditions or values."""
    left = parse_where_expression(expression.this, state, aggregation_level)
    right = parse_where_expression(expression.expression, state, aggregation_level)
    return BinaryExpression(operator, left=left, right=right)


def _parse_binary_value_expression(
    expression: Expr,
    operator: str,
    state: ParseState,
    aggregation_level: bool,
) -> BinaryExpression:
    """An arithmetic or concatenation operator over two values that are no foreign keys."""
    left = parse_operand(expression.this, state, aggregation_level)
    right = parse_operand(expression.expression, state, aggregation_level)
    return BinaryExpression(operator, left=left, right=right)


def _parse_numeric_literal(value: str) -> int | Decimal | float:
    """`value` typed as ISO SQL does: `int` or `Decimal` if exact, `float` if it has an exponent."""
    if "e" in value.lower():
        return float(value)
    if "." in value:
        return Decimal(value)
    return int(value)


def _unsupported_where(expression: Expr, cause: str) -> UnsupportedSql:
    """The rejection of the condition `expression` for `cause`."""
    return UnsupportedSql(
        Limitation.UNSUPPORTED_EXPRESSION,
        f"Unsupported condition {expression.sql()}: {cause}.",
    )


def _is_reference(expression: Expression) -> bool:
    """True for a foreign key, which the IR reads as its reference."""
    return isinstance(expression, Column) and expression.is_reference_column

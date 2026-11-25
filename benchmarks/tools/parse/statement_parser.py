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

"""Reads a normalized sqlglot statement into a `Statement` of the IR."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from sqlglot import Expr, exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify import qualify

from benchmarks.datasets.schema import PrimaryKey, Schema
from benchmarks.tools.ir.expressions import (
    Expression,
    Literal,
    ReferencedTupleExpression,
    UnaryExpression,
)
from benchmarks.tools.ir.query import Query
from benchmarks.tools.ir.statements import (
    Assignment,
    CreateTable,
    Delete,
    DropTable,
    Insert,
    Statement,
    Update,
)
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.parse.expression_parser import (
    parse_operand,
    parse_value_expression,
)
from benchmarks.tools.parse.node_args import arg_names
from benchmarks.tools.parse.parse_state import ParseState
from benchmarks.tools.parse.query_parser import (
    aliases,
    parse_query,
)
from benchmarks.tools.parse.type_checks import (
    DATETIME_TYPES,
    has_other_type,
    reject_ill_typed_sql,
)
from benchmarks.tools.sql.optimization import OPTIMIZER_DIALECT, quoted_schema

_TRANSLATED_DDL_ARGS = frozenset({"this", "tables", "kind"})
# declared column types are spelled as the datasets' DuckDB references declare them
_DECLARED_TYPE_DIALECT = "duckdb"


@dataclass(frozen=True)
class _StoredType:
    """SQL column types with the Python types of their literals and the SQL types of other values."""

    column_types: frozenset[exp.DataType.Type]
    literal_types: tuple[type, ...]
    value_types: frozenset[exp.DataType.Type]


_TEXT_TYPES = frozenset(exp.DataType.TEXT_TYPES)
_INTEGER_TYPES = frozenset(exp.DataType.INTEGER_TYPES)
_DATE_TYPES = frozenset({exp.DataType.Type.DATE})
_BOOLEAN_TYPES = frozenset({exp.DataType.Type.BOOLEAN})
_DOUBLE_TYPES = frozenset({exp.DataType.Type.DOUBLE})
# the Python types are compared exactly, as bool is an int and datetime a date
_STORED_TYPES = (
    _StoredType(_INTEGER_TYPES, (int,), _INTEGER_TYPES),
    # a Python float is a DOUBLE
    _StoredType(_DOUBLE_TYPES, (float,), _DOUBLE_TYPES),
    _StoredType(_TEXT_TYPES, (str,), _TEXT_TYPES),
    _StoredType(_DATE_TYPES, (date,), _DATE_TYPES),
    _StoredType(DATETIME_TYPES, (datetime,), DATETIME_TYPES),
    _StoredType(_BOOLEAN_TYPES, (bool,), _BOOLEAN_TYPES),
)
# compared exactly, as bool is an int
_PYTHON_NUMBER_TYPES = (int, Decimal, float)


def parse_statement(ast: Expr, schema: Schema) -> Statement:
    """`ast` as its IR; a SELECT must come from the optimizer, a write is qualified here."""
    # expects a statement `reject_non_baseline_sql` admitted; in a SELECT the
    # optimizer did not qualify, an unqualified column reads an output name
    try:
        return _parse_statement(ast, schema)
    except RecursionError:
        raise UnsupportedSql(
            Limitation.EXPRESSION_TOO_DEEP,
            "Unsupported statement: its expressions are nested too deeply.",
        ) from None


def _parse_statement(ast: Expr, schema: Schema) -> Statement:
    """`ast` as its IR, by its kind of statement."""
    if isinstance(ast, (exp.Select, exp.SetOperation)):
        query = parse_query(ast, schema)
        _reject_foreign_key_output(query)
        return query

    # unquoted names are case-insensitive, and the checks below compare them with the
    # schema's; the optimizer normalizes a query, but writes do not go through it
    copied = ast.copy()
    normalized = normalize_identifiers(copied, dialect=OPTIMIZER_DIALECT)
    if isinstance(normalized, exp.Insert):
        return _parse_insert(normalized, schema)

    if isinstance(normalized, exp.Update):
        return _parse_update(normalized, schema)

    if isinstance(normalized, exp.Delete):
        return _parse_delete(normalized, schema)

    if isinstance(normalized, exp.Create):
        return _parse_create(normalized)

    if isinstance(normalized, exp.Drop):
        return _parse_drop_table(normalized)

    raise UnsupportedSql(
        Limitation.UNSUPPORTED_STATEMENT,
        f"Unsupported statement: {type(ast).__name__}.",
    )


def _parse_drop_table(ast: exp.Drop) -> DropTable:
    """`DROP TABLE` of one table, without modifiers."""
    _reject_non_table(ast)
    _reject_untranslated_modifiers(ast)
    (table,) = ast.args["tables"]
    return DropTable(table.name)


def _parse_delete(ast: exp.Delete, schema: Schema) -> Delete:
    """`DELETE`, as the query of the rows it deletes."""
    where_node = ast.args.get("where")
    select = _qualified_select(ast.this, where_node, [], schema)
    query = _all_rows_query(select, schema)
    return Delete(query)


def _parse_insert(ast: exp.Insert, schema: Schema) -> Insert:
    """`INSERT … VALUES` with a column list, as rows keyed by the table's primary key."""
    if not isinstance(ast.this, exp.Schema):
        raise UnsupportedSql(
            Limitation.INSERT_WITHOUT_COLUMNS,
            "INSERT must name the columns it inserts: without a column list the values have no attribute names to bind to.",
        )

    values_node = ast.args.get("expression")
    if not isinstance(values_node, exp.Values):
        raise UnsupportedSql(
            Limitation.INSERT_FROM_SELECT,
            "Only INSERT ... VALUES is supported, not INSERT ... SELECT.",
        )

    table = ast.this.this
    listed = tuple(identifier.name for identifier in ast.this.expressions)
    if len(set(listed)) != len(listed):
        raise ValueError(f"INSERT into '{table.name}' names a column twice.")

    primary_key = _primary_key(table.name, listed, schema)
    columns = _inserted_columns(table.name, listed, schema)
    select = _qualified_select(table, None, [], schema)
    query = _all_rows_query(select, schema)
    typed_values = _typed_values(values_node, schema)
    parsed_rows = tuple(
        _parse_row(table.name, row_node, listed, columns, query)
        for row_node in typed_values.expressions
    )
    rows = tuple(_written_row(table.name, columns, row, schema) for row in parsed_rows)
    for row in rows:
        for column, value in zip(columns, row):
            _reject_null_key(table.name, column, value, primary_key)
    _reject_repeated_key(table.name, columns, rows, primary_key)
    return Insert(table.name, primary_key, columns, rows)


def _reject_null_key(
    table: str, column: str, value: Expression, primary_key: PrimaryKey
) -> None:
    """Rejects a NULL written into a key column: the FDM keys every tuple by it."""
    if column in primary_key.columns and value.is_null_literal():
        raise ValueError(f"INSERT writes NULL into the key column '{table}.{column}'.")


def _reject_repeated_key(
    table: str,
    columns: tuple[str, ...],
    rows: tuple[tuple[Expression, ...], ...],
    primary_key: PrimaryKey,
) -> None:
    """Rejects two rows under one key, however it is spelled: a later one would replace the earlier one."""
    positions = [columns.index(column) for column in primary_key.columns]
    keys = [[_key_value(row[position]) for position in positions] for row in rows]
    if any(key in keys[:index] for index, key in enumerate(keys)):
        raise ValueError(f"INSERT into '{table}' writes one key twice.")


def _parse_update(ast: exp.Update, schema: Schema) -> Update:
    """`UPDATE`, as the query of the rows it changes and an assignment per column."""
    table = ast.this.name
    key = schema.primary_keys.get(table)
    key_columns = key.columns if key is not None else ()
    declared = schema.columns.get(table)
    columns = []

    for assignment in ast.expressions:
        if not isinstance(assignment, exp.EQ) or not isinstance(
            assignment.this, exp.Column
        ):
            raise ValueError("Every UPDATE assignment must target a column.")

        column = assignment.this.name
        if assignment.this.table not in ("", table):
            raise ValueError(f"UPDATE of '{table}' assigns '{assignment.this}'.")
        if declared is not None and column not in declared:
            raise ValueError(f"UPDATE of '{table}' names a column it lacks: {column}.")
        if column in columns:
            raise ValueError(f"UPDATE of '{table}' assigns '{column}' twice.")

        # the key also names the tuple in its RF
        if column in key_columns:
            raise UnsupportedSql(
                Limitation.UPDATE_PRIMARY_KEY,
                f"UPDATE of '{table}.{column}' is not supported: it is a key.",
            )
        if _is_default_keyword(assignment.expression):
            raise UnsupportedSql(
                Limitation.UNSUPPORTED_EXPRESSION,
                f"UPDATE of '{table}.{column}' to DEFAULT is not supported.",
            )
        columns.append(column)

    sql_values = [assignment.expression for assignment in ast.expressions]
    where_node = ast.args.get("where")
    select = _qualified_select(ast.this, where_node, sql_values, schema)
    query = _all_rows_query(select, schema)
    reserved = aliases(select)
    state = ParseState(query, {}, reserved)
    value_nodes = [output.unalias() for output in select.expressions]
    values = [
        parse_operand(node, state, aggregation_level=False) for node in value_nodes
    ]
    for column, node, value in zip(columns, value_nodes, values):
        if _written_literal(value) is None:
            _reject_converted_value(table, column, node, schema)
    assignments = tuple(
        _assignment(table, column, value, schema)
        for column, value in zip(columns, values)
    )
    return Update(query, assignments)


def _parse_create(ast: exp.Create) -> CreateTable:
    """`CREATE TABLE` with its primary key; rejects constraints the relation would not enforce."""
    _reject_non_table(ast)
    _reject_untranslated_modifiers(ast)
    if not isinstance(ast.this, exp.Schema):
        raise UnsupportedSql(
            Limitation.TABLE_WITHOUT_PRIMARY_KEY,
            "CREATE TABLE must declare its columns.",
        )

    primary_key = _declared_key(ast.this)
    _reject_untranslated_constraints(ast.this, primary_key)
    return CreateTable(ast.this.this.name, primary_key)


def _primary_key(table: str, columns: tuple[str, ...], schema: Schema) -> PrimaryKey:
    """The primary key of `table`; raises unless `columns` supply all of it."""
    if table not in schema.primary_keys:
        raise UnsupportedSql(
            Limitation.TABLE_WITHOUT_PRIMARY_KEY,
            f"Unknown primary key of '{table}': an inserted tuple is keyed by it, so the relation must declare one.",
        )

    primary_key = schema.primary_keys[table]
    missing = primary_key.missing_from(columns)
    if missing:
        raise ValueError(
            f"INSERT into '{table}' does not supply its key column(s): "
            f"{', '.join(missing)}."
        )

    return primary_key


def _qualified_select(
    table_node: exp.Table,
    where_node: Expr | None,
    values: list[Expr],
    schema: Schema,
) -> exp.Select:
    """`SELECT values FROM table_node WHERE where_node` (`*` without `values`), qualified and typed as a query is."""
    copied_table = table_node.copy()
    select = exp.Select(
        expressions=[value.copy() for value in values] or [exp.Star()],
        from_=exp.From(this=copied_table),
    )
    if where_node is not None:
        copied_where = where_node.copy()
        select.set("where", copied_where)

    _qualify(select, schema)
    columns = quoted_schema(schema.columns)
    typed = annotate_types(select, schema=columns, dialect=OPTIMIZER_DIALECT)
    reject_ill_typed_sql(typed, OPTIMIZER_DIALECT)
    return typed


def _typed_values(values_node: exp.Values, schema: Schema) -> exp.Values:
    """`values_node` qualified and typed as a query is, each subquery on its own."""
    values = values_node.copy()
    columns = quoted_schema(schema.columns)
    for select in list(values.find_all(exp.Select)):
        if select.find_ancestor(exp.Select) is None:
            _qualify(select, schema)
            typed_select = annotate_types(
                select, schema=columns, dialect=OPTIMIZER_DIALECT
            )
            reject_ill_typed_sql(typed_select, OPTIMIZER_DIALECT)

    typed = annotate_types(values, schema=columns, dialect=OPTIMIZER_DIALECT)
    reject_ill_typed_sql(typed, OPTIMIZER_DIALECT)
    return typed


def _qualify(select: exp.Select, schema: Schema) -> None:
    """Qualifies `select` in place; a name it cannot resolve raises as in a query."""
    columns = quoted_schema(schema.columns)
    # infer_schema: a table without declared SQL columns reads its unqualified
    # columns as its own; expand_alias_refs off: an output alias is no column of a WHERE
    qualify(
        select,
        schema=columns,
        dialect=OPTIMIZER_DIALECT,
        infer_schema=True,
        expand_stars=False,
        expand_alias_refs=False,
    )


def _reject_foreign_key_output(query: Query) -> None:
    """Rejects a foreign key in the result: SQL returns its key value, FDM the tuple it refers to."""
    for output in query.outputs:
        if output.column.is_reference_column:
            raise UnsupportedSql(
                Limitation.FOREIGN_KEY_AS_VALUE,
                f"Unsupported output '{output.name}': SQL returns the value of the "
                "foreign key, while FDM holds the tuple it refers to.",
            )


def _is_default_keyword(value: Expr) -> bool:
    """True when `value` is SQL-92's `DEFAULT`, which sqlglot reads as a column."""
    return (
        isinstance(value, exp.Column)
        and not value.table
        and not value.this.quoted
        and value.name.upper() == "DEFAULT"
    )


def _all_rows_query(select: exp.Select, schema: Schema) -> Query:
    """The query reading every row of `select`, whatever it selects."""
    every_column = select.select(exp.Star(), append=False)
    return parse_query(every_column, schema)


def _inserted_columns(
    table: str, listed: tuple[str, ...], schema: Schema
) -> tuple[str, ...]:
    """The columns an inserted tuple of `table` carries. See `Insert.columns`."""
    declared = schema.columns.get(table)
    if declared is None:
        return listed

    unknown = [column for column in listed if column not in declared]
    if unknown:
        raise ValueError(
            f"INSERT into '{table}' names column(s) it does not have: {', '.join(unknown)}."
        )

    return tuple(declared)


def _parse_row(
    table: str,
    row_node: exp.Tuple,
    listed: tuple[str, ...],
    columns: tuple[str, ...],
    query: Query,
) -> tuple[Expression, ...]:
    """The row's value for each of `columns`: `NULL` for a column `listed` lacks."""
    if len(row_node.expressions) != len(listed):
        raise ValueError(
            f"INSERT lists {len(listed)} column(s) but a row supplies {len(row_node.expressions)} value(s)."
        )

    # a subquery reads its own tables, which qualifying has checked
    columns_read = row_node.find_all(exp.Column)
    if any(column.find_ancestor(exp.Select) is None for column in columns_read):
        raise ValueError(
            "An inserted value cannot read a column: the tuple it would read from is the one being created."
        )

    state = ParseState(query, {}, frozenset())
    values = {
        column: parse_value_expression(value_node, state)
        for column, value_node in zip(listed, row_node.expressions)
    }
    for column, value_node in zip(listed, row_node.expressions):
        if _written_literal(values[column]) is None:
            _reject_converted_value(table, column, value_node, query.tree.schema)
    return tuple(values.get(column, Literal(None)) for column in columns)


def _written_row(
    table: str,
    columns: tuple[str, ...],
    row: tuple[Expression, ...],
    schema: Schema,
) -> tuple[Expression, ...]:
    """`row` as stored, each value under its column of `columns`."""
    return tuple(
        _written_value(table, column, value, schema)
        for column, value in zip(columns, row)
    )


def _assignment(
    table: str, column: str, value: Expression, schema: Schema
) -> Assignment:
    """The attribute `SET column = value` writes, a foreign key under its reference's name."""
    written = _written_value(table, column, value, schema)
    reference = schema.reference_of(table, column)
    attribute = column if reference is None else reference.ref_key
    return Assignment(attribute, written)


def _written_value(
    table: str, column: str, value: Expression, schema: Schema
) -> Expression:
    """`value` as stored; a foreign key as the tuple its literal key refers to."""
    _reject_converted_literal(table, column, value, schema)
    reference = schema.reference_of(table, column)
    if reference is None or value.is_null_literal():
        return value

    key = _written_literal(value)
    if key is None:
        raise UnsupportedSql(
            Limitation.FOREIGN_KEY_AS_VALUE,
            f"Unsupported value for the foreign key '{table}.{column}': only a "
            "literal key names the tuple it refers to.",
        )
    return ReferencedTupleExpression(reference, key)


def _key_value(value: Expression) -> Expression:
    """`value` as a key compares: a literal however it is spelled, any other value without parentheses."""
    literal = _written_literal(value)
    if literal is None:
        return value.unparenthesized()

    return literal


def _written_literal(value: Expression) -> Literal | None:
    """`value` as a literal, also in parentheses or negated; `None` for any other value."""
    plain = value.unparenthesized()
    if isinstance(plain, Literal):
        return plain

    if not isinstance(plain, UnaryExpression) or plain.op != "-":
        return None

    operand = plain.value.unparenthesized()
    is_number = (
        isinstance(operand, Literal) and type(operand.value) in _PYTHON_NUMBER_TYPES
    )
    return Literal(-operand.value) if is_number else None


def _reject_converted_literal(
    table: str, column: str, value: Expression, schema: Schema
) -> None:
    """Rejects a literal not exactly of the column's declared type (`2` into a DOUBLE);
    NULL and undeclared columns pass."""
    declared = schema.columns.get(table, {}).get(column)
    literal = _written_literal(value)
    if declared is None or literal is None or literal.value is None:
        return

    data_type = exp.DataType.build(declared, dialect=_DECLARED_TYPE_DIALECT)
    written = literal.value
    params = [int(param.name) for param in data_type.expressions]
    is_decimal = data_type.is_type(exp.DataType.Type.DECIMAL)
    if is_decimal and type(written) is Decimal:
        scale = params[1] if len(params) == 2 else 0
        fraction_digits = -written.as_tuple().exponent
        if fraction_digits <= scale:
            return
    if any(
        data_type.is_type(*stored.column_types)
        and type(written) in stored.literal_types
        for stored in _STORED_TYPES
    ):
        return

    is_number = type(written) in _PYTHON_NUMBER_TYPES
    shown = repr(written)
    raise _conversion_error(table, column, shown, declared, is_number=is_number)


def _reject_converted_value(
    table: str, column: str, node: Expr, schema: Schema
) -> None:
    """Rejects a computed value not exactly of the column's declared type; unknown types and undeclared columns pass."""
    declared = schema.columns.get(table, {}).get(column)
    if declared is None:
        return

    data_type = exp.DataType.build(declared, dialect=_DECLARED_TYPE_DIALECT)
    # a product or quotient has more digits than SQL's column keeps: it rounds them
    if data_type.is_type(exp.DataType.Type.DECIMAL) and node.find(exp.Mul, exp.Div):
        shown = node.sql()
        raise _conversion_error(table, column, shown, declared, is_number=True)

    if not has_other_type(node, data_type.this):
        return

    if any(
        data_type.is_type(*stored.column_types)
        and not has_other_type(node, *stored.value_types)
        for stored in _STORED_TYPES
    ):
        return

    is_number = node.is_type(*exp.DataType.NUMERIC_TYPES)
    shown = node.sql()
    raise _conversion_error(table, column, shown, declared, is_number=is_number)


def _conversion_error(
    table: str, column: str, value: str, declared: str, is_number: bool
) -> UnsupportedSql:
    """The rejection of `value` for a `declared` column: SQL converts within numbers,
    but ISO SQL refuses across type families."""
    data_type = exp.DataType.build(declared, dialect=_DECLARED_TYPE_DIALECT)
    if is_number and data_type.is_type(*exp.DataType.NUMERIC_TYPES):
        return UnsupportedSql(
            Limitation.LITERAL_TYPE_COERCION,
            f"Unsupported value {value} for '{table}.{column}': SQL converts it to the "
            f"column's type {declared}, while FQL would store it unchanged.",
        )

    return UnsupportedSql(
        Limitation.DIALECT_SPECIFIC,
        f"Unsupported value {value} for '{table}.{column}': ISO SQL stores into a "
        f"{declared} column only a value of its type family.",
    )


def _declared_key(declaration: exp.Schema) -> PrimaryKey:
    """The one key `declaration` states, on a column or on the table."""
    keys = []
    column_names = set()
    for definition in declaration.expressions:
        table_key = _table_primary_key(definition)
        if table_key is not None:
            key_columns = tuple(column.name for column in table_key.expressions)
            table_level_key = PrimaryKey(key_columns)
            keys.append(table_level_key)

        if isinstance(definition, exp.ColumnDef):
            column_names.add(definition.name)
            if any(
                isinstance(constraint.kind, exp.PrimaryKeyColumnConstraint)
                for constraint in definition.args.get("constraints", [])
            ):
                column_key = PrimaryKey((definition.name,))
                keys.append(column_key)

    table = declaration.this.name
    if not keys:
        raise UnsupportedSql(
            Limitation.TABLE_WITHOUT_PRIMARY_KEY,
            f"CREATE TABLE '{table}' declares no primary key: a relation function stores its tuples under one.",
        )
    if len(keys) > 1:
        raise ValueError(f"CREATE TABLE '{table}' declares two primary keys.")

    (key,) = keys
    unknown = [column for column in key.columns if column not in column_names]
    if unknown:
        raise ValueError(f"The key of '{table}' names unknown column(s): {unknown}.")
    return key


def _table_primary_key(definition: Expr) -> exp.PrimaryKey | None:
    """The table-level `PRIMARY KEY (...)` `definition` declares, named or not; `None` for anything else."""
    if isinstance(definition, exp.Constraint) and len(definition.expressions) == 1:
        definition = definition.expressions[0]
    return definition if isinstance(definition, exp.PrimaryKey) else None


def _reject_untranslated_constraints(
    declaration: exp.Schema, primary_key: PrimaryKey
) -> None:
    """Rejects every constraint that changes what SQL stores or accepts, as the rendered relation enforces none;
    see `_is_translated`."""
    table = declaration.this.name
    for definition in declaration.expressions:
        if _table_primary_key(definition) is not None:
            continue
        if not isinstance(definition, exp.ColumnDef):
            raise UnsupportedSql(
                Limitation.UNENFORCED_CONSTRAINT,
                f"Unsupported constraint of '{table}': {definition.sql()}; the "
                "rendered relation would not enforce it.",
            )
        is_key = definition.name in primary_key.columns
        for constraint in definition.args.get("constraints", []):
            if not _is_translated(constraint, is_key):
                raise UnsupportedSql(
                    Limitation.UNENFORCED_CONSTRAINT,
                    f"Unsupported constraint of '{table}.{definition.name}': "
                    f"{constraint.sql()}; the rendered relation would neither "
                    "enforce it nor generate the value it fills in.",
                )


def _is_translated(constraint: exp.ColumnConstraint, is_key: bool) -> bool:
    """True when the rendered relation keeps the constraint as SQL does:
    the primary key, NOT NULL on a key column, `DEFAULT NULL`."""
    kind = constraint.kind
    if isinstance(kind, exp.DefaultColumnConstraint):
        return isinstance(kind.this.unnest(), exp.Null)

    if isinstance(kind, exp.NotNullColumnConstraint):
        return is_key

    return isinstance(kind, exp.PrimaryKeyColumnConstraint)


def _reject_untranslated_modifiers(ast: exp.Create | exp.Drop) -> None:
    """Rejects a CREATE / DROP with more than the named object: DROP's CASCADE / RESTRICT, CREATE's GLOBAL TEMPORARY."""
    untranslated = [
        key
        for key, value in ast.args.items()
        if value and key not in _TRANSLATED_DDL_ARGS
    ]
    if untranslated:
        raise UnsupportedSql(
            Limitation.DML_MODIFIER,
            f"Unsupported {ast.key.upper()} modifier(s): {arg_names(untranslated)}.",
        )


def _reject_non_table(ast: Expr) -> None:
    """Rejects a CREATE or DROP of anything but a table, such as a view or an index."""
    kind = ast.args.get("kind")
    if str(kind).upper() != "TABLE":
        raise UnsupportedSql(
            Limitation.NON_TABLE_OBJECT, f"Only tables are supported, not {kind}."
        )

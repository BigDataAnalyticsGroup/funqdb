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

"""The allow-list of the SQL the transpiler looks at: ISO SQL-92, what the JOB and
TPC-H queries add, and OFFSET and BOOLEAN by decision. Three tables split the check:
`_ALLOWED_ARGS` names the node types and the arguments each may set,
`_ALLOWED_ARG_VALUES` the values one argument may take, and `_VALUE_CHECKS` the
combinations of values one node may hold."""

import re
from collections.abc import Callable

from sqlglot import Expr, exp
from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.tokens import Tokenizer

from benchmarks.tools.ir.expressions import ExtractPart, IntervalUnit
from benchmarks.tools.limitations import Limitation, UnsupportedSql
from benchmarks.tools.parse.node_args import arg_names, is_negative_literal

# the dialect the transpiler reads SQL in: sqlglot's default
SQL_READ_DIALECT: DialectType = None

_REFERENTIAL_ACTIONS = ("CASCADE", "SET NULL", "SET DEFAULT", "NO ACTION")
# per node type, the arguments it may set; a node type missing here is dialect-specific
_ALLOWED_ARGS: dict[type[Expr], tuple[str, ...]] = {
    exp.Select: (
        "expressions",
        "from_",
        "joins",
        "where",
        "group",
        "having",
        "distinct",
        "order",
        "with_",  # JOB / TPC-H
        "limit",  # JOB / TPC-H
        "offset",  # extension
    ),
    exp.Union: ("this", "expression", "distinct", "by_name", "kind", "on", "order"),
    exp.Intersect: (
        "this",
        "expression",
        "distinct",
        "by_name",
        "kind",
        "on",
        "order",
    ),
    exp.Except: (
        "this",
        "expression",
        "distinct",
        "by_name",
        "kind",
        "on",
        "order",
    ),
    exp.From: ("this",),
    exp.Join: ("this", "on", "using", "side", "kind", "method"),
    exp.Table: ("this", "alias", "db", "catalog"),
    exp.Subquery: ("this", "alias"),
    exp.TableAlias: ("this", "columns"),
    exp.Where: ("this",),
    exp.Group: ("expressions",),
    exp.Having: ("this",),
    exp.Order: ("expressions",),
    exp.Ordered: ("this", "desc", "nulls_first"),
    exp.Distinct: ("expressions",),
    exp.Alias: ("this", "alias"),
    exp.Star: (),
    exp.Column: ("this", "table", "db", "catalog"),
    exp.Identifier: ("this", "quoted"),
    exp.Literal: ("this", "is_string"),
    exp.National: ("this",),
    exp.Null: (),
    exp.Paren: ("this",),
    exp.Neg: ("this",),
    exp.Not: ("this",),
    exp.Exists: ("this",),
    exp.Add: ("this", "expression"),
    exp.Sub: ("this", "expression"),
    exp.Mul: ("this", "expression"),
    exp.Div: ("this", "expression"),
    exp.DPipe: ("this", "expression", "safe"),
    exp.And: ("this", "expression"),
    exp.Or: ("this", "expression"),
    exp.EQ: ("this", "expression"),
    exp.NEQ: ("this", "expression"),
    exp.LT: ("this", "expression"),
    exp.LTE: ("this", "expression"),
    exp.GT: ("this", "expression"),
    exp.GTE: ("this", "expression"),
    exp.Between: ("this", "low", "high"),
    exp.In: ("this", "expressions", "query"),
    exp.Like: ("this", "expression", "negate"),
    exp.Escape: ("this", "expression"),
    exp.Is: ("this", "expression"),
    exp.Any: ("this",),
    exp.All: ("this",),
    exp.Tuple: ("expressions",),
    exp.Case: ("this", "ifs", "default"),
    exp.If: ("this", "true"),
    exp.Cast: ("this", "to"),
    exp.DataType: ("this", "expressions"),
    exp.DataTypeParam: ("this",),
    exp.Interval: ("this", "unit"),
    exp.IntervalSpan: ("this", "expression"),
    exp.Extract: ("this", "expression"),
    exp.Substring: ("this", "start", "length"),
    exp.Trim: ("this", "expression", "position"),
    exp.Upper: ("this",),
    exp.Lower: ("this",),
    exp.Length: ("this",),
    exp.StrPosition: ("this", "substr"),
    exp.Anonymous: ("this", "expressions"),
    exp.Coalesce: ("this", "expressions"),
    exp.Nullif: ("this", "expression"),
    exp.Collate: ("this", "expression"),
    exp.CurrentDate: (),
    exp.CurrentTime: ("this",),
    exp.CurrentTimestamp: ("this",),
    exp.CurrentUser: (),
    exp.SessionUser: (),
    exp.Count: ("this", "big_int"),
    exp.Sum: ("this",),
    exp.Avg: ("this",),
    exp.Min: ("this",),
    exp.Max: ("this",),
    exp.Insert: ("this", "expression", "default"),
    exp.Values: ("expressions",),
    exp.Schema: ("this", "expressions"),
    exp.Update: ("this", "expressions", "where"),
    exp.Delete: ("this", "where"),
    exp.Create: ("this", "kind", "expression", "properties"),
    exp.Properties: ("expressions",),
    exp.GlobalProperty: (),
    exp.TemporaryProperty: (),
    exp.Drop: ("tables", "kind", "cascade", "restrict"),
    exp.ColumnDef: ("this", "kind", "constraints"),
    exp.ColumnConstraint: ("this", "kind"),
    exp.PrimaryKeyColumnConstraint: (),
    exp.NotNullColumnConstraint: (),
    exp.DefaultColumnConstraint: ("this",),
    exp.CheckColumnConstraint: ("this",),
    exp.UniqueColumnConstraint: ("this",),
    exp.Reference: ("this", "options"),
    exp.ForeignKey: ("expressions", "reference"),
    exp.PrimaryKey: ("expressions", "include"),
    exp.IndexParameters: (),
    exp.Constraint: ("this", "expressions"),
    exp.Var: ("this",),
    exp.With: ("expressions",),  # JOB / TPC-H
    exp.CTE: ("this", "alias"),  # JOB / TPC-H
    exp.Limit: ("expression",),  # JOB / TPC-H
    exp.Offset: ("expression",),  # extension
    exp.Boolean: ("this",),  # extension
}

# SQL-92 orders, limits and binds CTEs only for the whole statement, never a subquery
ROOT_ONLY_ARGS = frozenset({"with_", "order", "limit", "offset"})

_OBJECT_KINDS = frozenset({"TABLE", "VIEW", "SCHEMA"})
_INTERVAL_UNITS = frozenset(IntervalUnit.__members__)
# per node type and argument, the values the argument may take
_ALLOWED_ARG_VALUES: dict[type[Expr], dict[str, frozenset[str]]] = {
    exp.Extract: {
        "this": frozenset(ExtractPart.__members__)
        | {"TIMEZONE_HOUR", "TIMEZONE_MINUTE"}
    },
    exp.Interval: {"unit": _INTERVAL_UNITS},
    exp.IntervalSpan: {"this": _INTERVAL_UNITS, "expression": _INTERVAL_UNITS},
    exp.Trim: {"position": frozenset({"LEADING", "TRAILING", "BOTH"})},
    exp.Anonymous: {"this": frozenset({"OCTET_LENGTH", "BIT_LENGTH"})},
    exp.Create: {"kind": _OBJECT_KINDS},
    exp.Drop: {"kind": _OBJECT_KINDS},
    exp.Reference: {
        "options": frozenset(
            {f"ON DELETE {action}" for action in _REFERENTIAL_ACTIONS}
            | {f"ON UPDATE {action}" for action in _REFERENTIAL_ACTIONS}
            | {"MATCH FULL", "MATCH PARTIAL"}
        )
    },
}

# the words sqlglot's tokenizer reads as keywords
SQLGLOT_KEYWORDS = frozenset(word for word in Tokenizer.KEYWORDS if word.isidentifier())
# keywords the JOB and TPC-H queries use as names
CORPUS_NAMES = frozenset({"character"})
# sqlglot fills in this NULL ordering where an ORDER BY key states none
_NULL_ORDERING = Dialect.get_or_raise(SQL_READ_DIALECT).NULL_ORDERING


def reject_non_baseline_sql(statement: Expr) -> None:
    """Rejects `statement` if it is outside the baseline, has no FDM counterpart, or names what later stages ignore."""
    _reject_non_baseline_statement(statement)
    for node in statement.walk():
        is_root = node is statement
        _reject_non_baseline_args(node, is_root)
        _reject_non_baseline_values(node)
        _reject_untranslated_names(node)


def _reject_untranslated_names(node: Expr) -> None:
    """Rejects a schema-qualified table or column and an alias's column list, which the pipeline would ignore."""
    # sqlglot reads the name of CREATE SCHEMA s as a table of the schema s
    names_a_schema = node.parent is not None and node.parent.text("kind") == "SCHEMA"
    if (
        isinstance(node, (exp.Table, exp.Column))
        and node.text("db")
        and not names_a_schema
    ):
        raise UnsupportedSql(
            Limitation.SCHEMA_QUALIFIED_TABLE,
            f"Unsupported name {node.sql()}: a dataset has no schemas.",
        )
    if isinstance(node, exp.TableAlias) and node.columns:
        raise UnsupportedSql(
            Limitation.ALIAS_COLUMN_LIST,
            f"Unsupported alias {node.sql()}: no rename of columns by position yet.",
        )


def _reject_non_baseline_statement(statement: Expr) -> None:
    """Rejects a SQL-92 statement without FDM counterpart, and any statement kind outside the baseline."""
    untranslated_statements = (
        exp.Alter,
        exp.Grant,
        exp.Revoke,
        exp.Commit,
        exp.Rollback,
    )
    checked_statements = (
        exp.Select,
        exp.Union,
        exp.Intersect,
        exp.Except,
        exp.Insert,
        exp.Update,
        exp.Delete,
        exp.Create,
        exp.Drop,
    )
    kind = type(statement).__name__

    if isinstance(statement, untranslated_statements):
        raise UnsupportedSql(
            Limitation.UNSUPPORTED_STATEMENT, f"Unsupported statement: {kind}."
        )

    if type(statement) not in checked_statements:
        raise _dialect_specific(f"statement {kind}")


def _reject_non_baseline_args(node: Expr, is_root: bool) -> None:
    """Rejects arguments `node` sets that _ALLOWED_ARGS lacks, and ROOT_ONLY_ARGS unless `is_root`."""
    node_type = type(node)
    allowed_args = _ALLOWED_ARGS.get(node_type)
    if allowed_args is None:
        description = _describe(node)
        raise _dialect_specific(description)

    selected_args = {arg for arg, value in node.args.items() if value}
    allowed = set(allowed_args)
    if not is_root:
        allowed -= ROOT_ONLY_ARGS
    disallowed = selected_args - allowed
    if disallowed:
        ordered = sorted(disallowed)
        names = arg_names(ordered)
        description = _describe(node)
        raise _dialect_specific(f"{names} on {description}")


def _reject_non_baseline_values(node: Expr) -> None:
    """Rejects argument values of `node` outside _ALLOWED_ARG_VALUES or failing _VALUE_CHECKS."""
    node_type = type(node)
    for arg, allowed_values in _ALLOWED_ARG_VALUES.get(node_type, {}).items():
        selected_values = _selected_arg_values(node, arg)
        disallowed = selected_values - allowed_values
        if disallowed:
            ordered = sorted(disallowed)
            names = ", ".join(ordered)
            description = _describe(node)
            raise _dialect_specific(f"{names} on {description}")

    value_check = _VALUE_CHECKS.get(node_type)
    if value_check is not None and not value_check(node):
        description = _describe(node)
        raise _dialect_specific(description)


def _selected_arg_values(node: Expr, key: str) -> set[str]:
    """The upper-case text of the values `node` sets for `key`; empty if none."""
    value = node.args.get(key)
    if value is None:
        return set()

    elements = value if isinstance(value, list) else [value]
    return {_value_text(element) for element in elements}


def _value_text(value: object) -> str:
    """The upper-case name of `value`, or its text if it is no node."""
    text = value.name if isinstance(value, Expr) else str(value)
    return text.upper()


def _is_baseline_join(join: exp.Join) -> bool:
    """True when `join` is CROSS / NATURAL without ON / USING, or another with."""
    side, kind, method = join.side, join.kind, join.method
    sided = (side, kind) in {
        ("", "INNER"),
        ("LEFT", ""),
        ("LEFT", "OUTER"),
        ("RIGHT", ""),
        ("RIGHT", "OUTER"),
        ("FULL", ""),
        ("FULL", "OUTER"),
    }
    plain = (side, kind) == ("", "")
    conditioned = bool(join.args.get("on") or join.args.get("using"))
    if plain and not method:
        return True
    if conditioned:
        return sided and not method
    is_cross = (side, kind, method) == ("", "CROSS", "")
    is_natural = method == "NATURAL" and (sided or plain)
    return is_cross or is_natural


def _is_baseline_table(table: exp.Table) -> bool:
    """True when `table` has no alias if a statement writes into it."""
    holder = table.parent if isinstance(table.parent, exp.Schema) else table
    is_write_target = holder.arg_key == "this" and isinstance(
        holder.parent, (exp.Insert, exp.Update, exp.Delete)
    )
    return not (is_write_target and table.args.get("alias"))


def _is_baseline_set_operation(operation: exp.SetOperation) -> bool:
    """True when `by_name` is CORRESPONDING (kind INNER), not BY NAME."""
    corresponding = bool(operation.args.get("by_name"))
    inner = operation.text("kind").upper() == "INNER"
    return corresponding == inner


def _is_baseline_ordered(ordered: exp.Ordered) -> bool:
    """True when `ordered` keeps SQL_READ_DIALECT's NULL ordering, which SQL-92 leaves open."""
    nulls_first = bool(ordered.args.get("nulls_first"))
    descending = bool(ordered.args.get("desc"))
    nulls_are_small = _NULL_ORDERING == "nulls_are_small"
    default_nulls_first = (
        _NULL_ORDERING != "nulls_are_last" and nulls_are_small != descending
    )
    return nulls_first == default_nulls_first


def _is_baseline_data_type(data_type: exp.DataType) -> bool:
    """True when `data_type` is a SQL-92 type, an INTERVAL or BOOLEAN."""
    if isinstance(data_type.this, exp.Interval):
        return True

    types = exp.DataType.Type
    return data_type.this in {
        types.CHAR,
        types.NCHAR,
        types.VARCHAR,
        types.NVARCHAR,
        types.BIT,
        types.DECIMAL,
        types.INT,
        types.SMALLINT,
        types.FLOAT,
        types.DOUBLE,
        types.DATE,
        types.TIME,
        types.TIMETZ,
        types.TIMESTAMP,
        types.TIMESTAMPTZ,
        types.BOOLEAN,
    }


def _is_baseline_cast(cast: exp.Cast) -> bool:
    """True when text cast to DATE / TIMESTAMP has SQL-92's datetime format."""
    types = exp.DataType.Type
    datetime_text = {
        types.DATE: r"\s*\d+-\d+-\d+\s*",
        types.TIMESTAMP: r"\s*\d+-\d+-\d+ \d+:\d+:\d+(\.\d*)?\s*",
    }
    pattern = datetime_text.get(cast.to.this)
    operand = cast.this
    if pattern is None or not isinstance(operand, exp.Literal) or not operand.is_string:
        return True
    return re.fullmatch(pattern, operand.this) is not None


def _is_baseline_interval(interval: exp.Interval) -> bool:
    """True when `interval` has a unit and a whole count (fraction for SECOND)."""
    count = interval.this
    unit = interval.unit
    is_text = isinstance(count, exp.Literal) and count.is_string
    if isinstance(unit, exp.IntervalSpan):
        return count is None or is_text
    if not isinstance(unit, exp.Var):
        return False
    if count is None:
        return True

    fraction = r"(\.\d*)?" if unit.name.upper() == "SECOND" else ""
    pattern = rf"\s*[+-]?\d+{fraction}\s*"
    return is_text and re.fullmatch(pattern, count.this) is not None


def _counts_whole_rows(clause: exp.Limit | exp.Offset) -> bool:
    """True when `clause` counts by a non-negative integer literal, as SQL:2008's FETCH FIRST, or by no literal."""
    count = clause.expression.unnest()
    if is_negative_literal(count) or isinstance(count, exp.Null):
        return False
    if not isinstance(count, exp.Literal):
        return True
    return count.is_int


def _is_baseline_substring(substring: exp.Substring) -> bool:
    """True when `substring` has no negative literal bound, which DuckDB reads unlike SQL-92."""
    start = substring.args.get("start")
    length = substring.args.get("length")
    return not is_negative_literal(start) and not is_negative_literal(length)


def _takes_one_argument(function: exp.Anonymous) -> bool:
    """True when `function` (OCTET_LENGTH, BIT_LENGTH) has one argument."""
    return len(function.expressions) == 1


def _is_baseline_distinct(distinct: exp.Distinct) -> bool:
    """True when `distinct` applies to one expression at most, as in `COUNT(DISTINCT x)`."""
    return len(distinct.expressions) <= 1


def _is_baseline_select(select: exp.Select) -> bool:
    """True when a SELECT DISTINCT orders only by selected items or their names, as SQL-92 requires."""
    order = select.args.get("order")
    if not select.args.get("distinct") or order is None:
        return True

    items = [item.unalias() for item in select.expressions]
    names = {item.alias_or_name for item in select.expressions}
    for ordered in order.expressions:
        key = ordered.this
        is_name = isinstance(key, exp.Column) and not key.table and key.name in names
        if key not in items and not is_name:
            return False
    return True


def _is_baseline_tuple(row: exp.Tuple) -> bool:
    """True when `row` is not empty and holds no keyword but DEFAULT."""
    keywords = [
        value.name.upper() for value in row.expressions if isinstance(value, exp.Var)
    ]
    defaults_only = all(keyword == "DEFAULT" for keyword in keywords)
    return bool(row.expressions) and defaults_only


def _drops_one_object(drop: exp.Drop) -> bool:
    """True when `drop` names one object, as SQL-92 requires."""
    return len(drop.args["tables"]) == 1


def _is_baseline_create(create: exp.Create) -> bool:
    """True when only a view has a query and the only property is GLOBAL TEMPORARY."""
    is_view = create.text("kind").upper() == "VIEW"
    defined_by_query = create.args.get("expression") is not None
    properties = create.args.get("properties")
    property_types = (
        [] if properties is None else [type(p) for p in properties.expressions]
    )
    is_plain_or_global_temporary = property_types in (
        [],
        [exp.GlobalProperty, exp.TemporaryProperty],
    )
    return (is_view or not defined_by_query) and is_plain_or_global_temporary


def _is_plain_name(identifier: exp.Identifier) -> bool:
    """True when `identifier` is quoted, no sqlglot keyword, or an exempt name."""
    name = identifier.name
    if identifier.quoted or name.upper() not in SQLGLOT_KEYWORDS:
        return True
    return name.lower() in CORPUS_NAMES or _is_assigned_default(identifier)


def _is_assigned_default(identifier: exp.Identifier) -> bool:
    """True when `identifier` is the DEFAULT of `UPDATE … SET c = DEFAULT`, read as a column."""
    column = identifier.parent
    assignment = column.parent if column is not None else None
    return (
        identifier.name.upper() == "DEFAULT"
        and isinstance(assignment, exp.EQ)
        and assignment.expression is column
        and isinstance(assignment.parent, exp.Update)
    )


def _describe(node: Expr) -> str:
    """`node`'s kind and SQL text, for an error message."""
    return f"{node.key.upper()} ({node.sql()})"


def _dialect_specific(what: str) -> UnsupportedSql:
    """The DIALECT_SPECIFIC error for `what`."""
    return UnsupportedSql(
        Limitation.DIALECT_SPECIFIC,
        f"Unsupported {what}: outside ISO SQL-92 and the JOB / TPC-H queries.",
    )


# per node type, the check of the values its arguments may combine
_VALUE_CHECKS: dict[type[Expr], Callable[[Expr], bool]] = {
    exp.Join: _is_baseline_join,
    exp.Table: _is_baseline_table,
    exp.Union: _is_baseline_set_operation,
    exp.Intersect: _is_baseline_set_operation,
    exp.Except: _is_baseline_set_operation,
    exp.Ordered: _is_baseline_ordered,
    exp.DataType: _is_baseline_data_type,
    exp.Cast: _is_baseline_cast,
    exp.Interval: _is_baseline_interval,
    exp.Limit: _counts_whole_rows,
    exp.Offset: _counts_whole_rows,
    exp.Substring: _is_baseline_substring,
    exp.Anonymous: _takes_one_argument,
    exp.Distinct: _is_baseline_distinct,
    exp.Select: _is_baseline_select,
    exp.Tuple: _is_baseline_tuple,
    exp.Create: _is_baseline_create,
    exp.Drop: _drops_one_object,
    exp.Identifier: _is_plain_name,
}

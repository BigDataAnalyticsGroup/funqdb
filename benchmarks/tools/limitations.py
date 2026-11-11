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

"""The catalog of every known limitation of the SQL-to-FQL transpiler: each one the
transpiler raises as `UnsupportedSql`."""

from enum import Enum, StrEnum, unique


class LimitationCategory(StrEnum):
    """Why a limitation exists: refused on purpose, or open work on the transpiler or FQL side."""

    REJECTED = "rejected"
    """SQL refused on purpose: FQL deliberately does not follow it. Invalid SQL is
    catalogued, or raises `ValueError` or, for a name that does not resolve,
    sqlglot's `OptimizeError`."""

    TRANSPILER = "limitation (transpiler)"
    """Open work on the transpiler side."""

    FQL = "limitation (fql)"
    """Open work on the FDM / FQL side."""


_REJECTED = LimitationCategory.REJECTED
_TRANSPILER = LimitationCategory.TRANSPILER
_FQL = LimitationCategory.FQL


@unique
class Limitation(Enum):
    """A known limitation of the transpiler, with its category and description."""

    def __init__(self, category: LimitationCategory, description: str):
        self.category = category
        self.description = description

    NO_FROM = (
        _REJECTED,
        "A query without a FROM table has no relation to translate.",
    )
    LIMIT_WITHOUT_ORDER = (
        _REJECTED,
        "LIMIT without ORDER BY picks arbitrary rows, which is meaningless on FQL's "
        "unordered relations.",
    )
    AGGREGATE_MIXED_WITH_COLUMN = (
        _REJECTED,
        "An expression mixes aggregates with plain columns at one level (e.g. a "
        "group key added to `COUNT(*)`, or an aggregate in WHERE).",
    )
    EXISTS_OVER_AGGREGATE = (
        _REJECTED,
        "An ungrouped aggregate always yields one row, so EXISTS over it is always "
        "true.",
    )
    DUPLICATE_OUTPUT_NAME = (
        _REJECTED,
        "Two output columns share a name, and a result tuple is a mapping holding "
        "one value per name.",
    )
    INSERT_WITHOUT_COLUMNS = (
        _REJECTED,
        "An INSERT without a column list has no attribute names to bind its values "
        "to; a tuple function is a mapping, not a position list.",
    )
    TABLE_WITHOUT_PRIMARY_KEY = (
        _REJECTED,
        "A relation function stores its tuples under a key, so the table has to "
        "declare a primary key.",
    )
    UNSUPPORTED_STATEMENT = (
        _REJECTED,
        "The SQL-92 statement has no FDM counterpart (e.g. ALTER: a tuple function "
        "carries whatever attributes it was given, there is no schema to alter; "
        "GRANT, REVOKE, COMMIT, ROLLBACK).",
    )
    FOREIGN_KEY_AGAINST_SCALAR = (
        _REJECTED,
        "A foreign key is swizzled into a reference in FDM, not the key value SQL "
        "compares, so it cannot be tested for equality with a scalar.",
    )
    FOREIGN_KEY_AS_VALUE = (
        _REJECTED,
        "A foreign key is swizzled into a reference in FDM, not the key value SQL "
        "computes with, aggregates, groups or assigns.",
    )
    FOREIGN_KEY_ORDERED = (
        _REJECTED,
        "A reference has no order, where SQL orders the key values foreign keys "
        "hold.",
    )
    FOREIGN_KEY_TO_NON_KEY = (
        _REJECTED,
        "A foreign key leads to the tuple stored under the target table's key, "
        "so equating it with any other column follows no reference.",
    )
    FOREIGN_KEYS_COMPARED_AS_REFERENCES = (
        _REJECTED,
        "FQL compares two foreign keys as references, which never match for keys "
        "into different tables, where SQL finds equal key values.",
    )
    REFERENCE_NOT_ROW_COMPARABLE = (
        _REJECTED,
        "A result attribute holds a swizzled reference (a tuple or relation), not "
        "the key value SQL returns, so the result is not row-comparable.",
    )
    REFERENTIAL_INTEGRITY = (
        _REJECTED,
        "The FDM refuses to delete a tuple another relation references, or to "
        "refer to a missing one; the reference database carries no foreign-key "
        "constraints and does both.",
    )
    DATE_VS_TIMESTAMP = (
        _REJECTED,
        "DuckDB widens DATE + INTERVAL to a TIMESTAMP where ISO SQL keeps a DATE, "
        "as FQL does, so the reference result differs.",
    )
    LITERAL_TYPE_COERCION = (
        _REJECTED,
        "SQL converts a number to the column's numeric type, by rounding or truncation "
        "as the implementation defines, to store or compare it; FQL keeps it as is, and "
        "Python compares a Decimal and a float by their exact values.",
    )
    OFFSET_WITHOUT_LIMIT = (
        _REJECTED,
        "OFFSET without LIMIT is not translated: FQL's subset keeps a counted "
        "number of ranked tuples.",
    )
    HAVING_WITHOUT_GROUP_BY = (
        _REJECTED,
        "HAVING without GROUP BY filters the single aggregate row, which FQL "
        "yields as one value, not as a relation to filter.",
    )
    WILDCARD_WITH_EXPRESSIONS = (
        _REJECTED,
        "A star the schema does not expand cannot be combined with other SELECT "
        "expressions or with DISTINCT.",
    )
    SCHEMA_QUALIFIED_TABLE = (
        _REJECTED,
        "A table is named with its schema (FROM s.t), but a dataset is one namespace "
        "of relations, so the name would read another schema's table as its own.",
    )
    UPDATE_PRIMARY_KEY = (
        _REJECTED,
        "A tuple is stored under its key, so assigning the key attribute would "
        "leave it under the old key.",
    )
    COMPUTED_ALIAS_SHADOWS_COLUMN = (
        _REJECTED,
        "A computed output reuses the name of a stored column, and FDM refuses a "
        "computed key that is already stored; renaming around it would bloat the FQL.",
    )
    SCALAR_SUBQUERY_ROWS = (
        _REJECTED,
        "A scalar subquery must aggregate to a single value; one selecting rows "
        "relies on the data holding at most one, which FQL does not assume.",
    )
    EXTRACT_TIMEZONE = (
        _REJECTED,
        "FDM dates and datetimes carry no timezone, so EXTRACT has no timezone part "
        "to read.",
    )
    INEXACT_INTERVAL = (
        _REJECTED,
        "Python's relativedelta drops the fraction of a divided or fractionally scaled "
        "interval, and a sub-day shift of a date, without an error; so a sub-day "
        "interval passes only as the direct shift of a datetime.",
    )
    DIALECT_SPECIFIC = (
        _REJECTED,
        "The SQL is neither ISO SQL-92 nor used by the JOB / TPC-H queries, but a "
        "dialect's extension (QUARTER, a WEEK INTERVAL, ILIKE, CONCAT(), MEDIAN, "
        "TEXT, QUALIFY, ASOF JOIN, TRUNCATE, IF EXISTS, RETURNING, …).",
    )

    SET_OPERATION = (
        _TRANSPILER,
        "UNION / INTERSECT / EXCEPT are not translated.",
    )
    NON_LITERAL_LIMIT = (
        _TRANSPILER,
        "LIMIT and OFFSET translate only as integer literals, not as a subquery.",
    )
    ALIAS_COLUMN_LIST = (
        _TRANSPILER,
        "An alias renames columns by position (FROM t AS a(x, y)), which the "
        "transpiler does not yet rewrite into names.",
    )
    SUBSTRING_BOUNDS = (
        _TRANSPILER,
        "SUBSTRING translates only integer literal bounds, which fix the slice when "
        "transpiling.",
    )
    INEXACT_CAST = (
        _TRANSPILER,
        "Only a CAST to DOUBLE converts as SQL does: int() truncates where SQL "
        "rounds, float() ignores a DECIMAL's scale and FLOAT's single precision, "
        "and the other SQL-92 targets have no conversion.",
    )
    TEMPORAL_CAST = (
        _TRANSPILER,
        "A temporal CAST passes only a value already of that type through; SQL "
        "converts other values, which the FQL does not.",
    )
    TEMPORAL_LITERAL = (
        _TRANSPILER,
        "A SQL-92 datetime literal translates only as a DATE / TIMESTAMP Python "
        "reads as SQL does, not with unpadded fields, a TIME or a time zone.",
    )
    UNSUPPORTED_EXPRESSION = (
        _TRANSPILER,
        "The SQL-92 expression kind is not translated (COALESCE, NULLIF, UPPER, "
        "TRIM, CHAR_LENGTH, POSITION, CURRENT_DATE, ANY / ALL, row values, …).",
    )
    CTE_NOT_INLINABLE = (
        _TRANSPILER,
        "Only a CTE that filters a single table is inlined into the query "
        "reading it; a grouping, aggregating or renaming one is not.",
    )
    OUTER_JOIN = (
        _TRANSPILER,
        "Only inner joins translate: following a reference drops the tuples an "
        "outer join keeps. A NATURAL join translates only over known shared columns.",
    )
    SELECT_SUBQUERY_RECURSION = (
        _TRANSPILER,
        "A SELECT-list scalar subquery is attached to the relation it aggregates, "
        "so reading it recurses without bound.",
    )
    SUBQUERY_PER_ROW = (
        _TRANSPILER,
        "A subquery is rendered into the enclosing predicate and evaluated for "
        "every row (no decorrelation), which takes too long.",
    )
    DML_MODIFIER = (
        _TRANSPILER,
        "The statement carries a SQL-92 modifier (DROP … CASCADE / RESTRICT, "
        "CREATE GLOBAL TEMPORARY) the rendered write would silently drop.",
    )
    INSERT_FROM_SELECT = (
        _TRANSPILER,
        "INSERT translates VALUES rows, not the result of a query.",
    )
    UNENFORCED_CONSTRAINT = (
        _TRANSPILER,
        "The rendered relation enforces no constraint but the primary key, and "
        "fills in no DEFAULT but NULL.",
    )

    PATH_SEPARATOR_NAME = (
        _FQL,
        "FDM reads `__` inside a key as a path separator, so a name containing it "
        "cannot be addressed.",
    )
    NON_TABLE_OBJECT = (
        _FQL,
        "Only tables have an FDM counterpart, not views or schemas.",
    )
    PROJECT_DROPS_COMPUTED_SOURCE = (
        _FQL,
        "project() drops the source attributes a computed column reads, so the "
        "column cannot be evaluated, or a definedness test reads them as unmapped.",
    )
    UNGROUPED_AGGREGATE_NOT_PROJECTABLE = (
        _FQL,
        "An ungrouped aggregation is one TF of scalars, which project() cannot "
        "take, so helper aggregates stay on the result.",
    )
    FILTERED_JOIN_READ_ONLY = (
        _FQL,
        "A filter on the join tuples returns a read-only relation, so no computed "
        "column can be added after it.",
    )
    CARTESIAN_PRODUCT = (
        _FQL,
        "join refuses relations connected by no reference, so a Cartesian product "
        "(or a theta-only join) is not executable.",
    )
    ALIASED_RELATION = (
        _FQL,
        "Dropping unused references clones every relation, which splits the "
        "aliases of one relation apart and isolates all but the last.",
    )
    NULL_IN_PREDICATE = (
        _FQL,
        "SQL reads a predicate over NULL as unknown and drops the row, while FQL's "
        "predicates are two-valued: a comparison over an unmapped attribute is "
        "false and its negation true; a value read unguarded (an IN subquery's "
        "selected value, an IN list's column option, a reference) drops the row, or "
        "raises in an OPERATOR-style filter; IS NULL over a computed value is not "
        "translated.",
    )
    NULL_IN_AGGREGATE = (
        _FQL,
        "Over no rows SQL counts no distinct values as 0 and computes a value from "
        "aggregates as NULL, while FQL leaves a distinct count unmapped and raises "
        "on reading a missing aggregate.",
    )
    NULL_IN_VALUE = (
        _FQL,
        "SQL reads a value over NULL as NULL, while FQL raises on reading an "
        "unmapped attribute in a computed value or an UPDATE, and stores None for "
        "a CASE without ELSE.",
    )
    NULL_IN_GROUPING = (
        _FQL,
        "SQL groups NULLs into one group, while FQL's partition raises on an "
        "unmapped grouping key, and a filter lambda around it drops the row.",
    )
    NULL_ORDERING = (
        _FQL,
        "DuckDB sorts NULLs last, while rank_by raises on an unmapped ordering key.",
    )
    NULL_LITERAL_COMPARISON = (
        _FQL,
        "SQL's comparison with a NULL literal is never true (its outcome is "
        "unknown), where Python compares with None; IS [NOT] NULL is the test.",
    )
    NULL_FOREIGN_KEY_INSERT = (
        _FQL,
        "SQL inserts a row whose foreign key is NULL; FDM's ForeignValueConstraint "
        "reads the reference of every inserted tuple and raises on an unmapped one.",
    )
    PROJECTED_NAME_COLLISION = (
        _FQL,
        "project() keeps the last segment of each path, so two columns of that name "
        "(e.g. `t.id AS tid, mi.id AS mid`) collapse before they can be renamed.",
    )


class UnsupportedSql(ValueError):
    """SQL the transpiler does not translate, for the catalogued `limitation`."""

    def __init__(self, limitation: Limitation, detail: str):
        super().__init__(f"[{limitation.category}] {limitation.name}: {detail}")
        self.limitation = limitation

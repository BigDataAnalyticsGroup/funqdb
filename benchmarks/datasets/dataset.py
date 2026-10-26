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

"""What a dataset is: its registry declaration and the dataset opened from it."""

from __future__ import annotations

import ast
import textwrap
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, override

import duckdb

from benchmarks.datasets.fql_runtime import FQL_RUNTIME
from benchmarks.datasets.query_result import (
    FQLExecutionFailed,
    QueryResult,
    describe_error,
)
from benchmarks.datasets.schema import PrimaryKey, Schema
from benchmarks.datasets.dataset_types import DatasetRowData, References, SqlSchema
from fdm.attribute_functions import DBF, RF, TF


@dataclass(frozen=True, kw_only=True)
class DatasetDefinition:
    """A dataset as the registry declares it, under `name`. Without
    `build_connection` it is schema-only: its FQL renders but never executes."""

    name: str
    references: References = field(default_factory=dict)
    primary_keys: dict[str, PrimaryKey] = field(default_factory=dict)
    build_connection: Callable[[], duckdb.DuckDBPyConnection] | None = None

    @property
    def has_data(self) -> bool:
        """True when an open dataset executes FQL and compares it against DuckDB."""
        return self.build_connection is not None

    def open(self) -> Dataset:
        """A new dataset with its own copy of `primary_keys`; a `LoadedDataset` when
        `has_data`, whose FDM rows are read out of the connection, so both sides
        hold the same data. Raises `ValueError` for a foreign key without a target
        row; the connection is closed then."""
        references = _copy_references(self.references)
        primary_keys = dict(self.primary_keys)
        if self.build_connection is None:
            return Dataset(references=references, primary_keys=primary_keys)

        connection = self.build_connection()
        try:
            rows = read_rows(connection, primary_keys)
            dbf = build_dbf(self.name, rows, references, primary_keys)
        except Exception:
            connection.close()
            raise

        return LoadedDataset(
            references=references,
            primary_keys=primary_keys,
            _connection=connection,
            _dbf=dbf,
        )


class DatasetClosed(Exception):
    """A call on a dataset after its `close`."""


@dataclass(kw_only=True)
class Dataset:
    """An open dataset: its tables' keys and references, without data."""

    references: References = field(default_factory=dict)
    primary_keys: dict[str, PrimaryKey] = field(default_factory=dict)
    _closed: bool = field(default=False, init=False)

    def schema(self) -> Schema:
        """The declared keys and references as they were at `open`, with the
        columns as they stand now. Raises `DatasetClosed` after `close`."""
        self._check_open()
        columns = self._sql_schema()
        references = _copy_references(self.references)
        return Schema(
            references=references,
            primary_keys=dict(self.primary_keys),
            columns=columns,
        )

    def close(self) -> bool:
        """Release the dataset; ``False`` (and nothing happens) when it is already
        closed."""
        if self._closed:
            return False

        self._closed = True
        return True

    def _check_open(self) -> None:
        """Raise `DatasetClosed` when the dataset is closed."""
        if self._closed:
            raise DatasetClosed("The dataset is closed.")

    def _sql_schema(self) -> SqlSchema:
        """Every table's columns and SQL types; none are known without data."""
        return {}


@dataclass(kw_only=True)
class LoadedDataset(Dataset):
    """An open dataset with data: an FDM database for FQL and a DuckDB database for
    SQL, each its own copy of the same rows."""

    _dbf: DBF = field(repr=False, compare=False)
    _connection: duckdb.DuckDBPyConnection = field(repr=False, compare=False)

    def execute_sql(self, sql: str) -> QueryResult:
        """The rows of the last statement of `sql` when DuckDB types it a SELECT,
        else an empty result `from_statement`. Raises `DatasetClosed` after
        `close`."""
        self._check_open()
        result = QueryResult((), (), from_statement=True)
        for statement in self._connection.extract_statements(sql):
            result = self._execute_statement(statement)
        return result

    def execute_fql(self, fql: str, dbf_name: str = "dbf") -> QueryResult:
        """The rows of `fql`, trusted code that reads the database as `dbf_name`;
        an empty result `from_statement` when `fql` is not one expression. Raises
        `DatasetClosed`, `FQLExecutionFailed` if the FQL fails to execute, and
        `NotTabular` if the result is not tabular."""
        self._check_open()
        namespace = self._build_namespace(dbf_name)
        source = textwrap.dedent(fql).strip()
        expression = _is_expression(source)
        try:
            value = eval(source, namespace) if expression else exec(source, namespace)
        except Exception as exc:
            raise FQLExecutionFailed(describe_error(exc)) from exc

        if not expression:
            return QueryResult((), (), from_statement=True)
        return QueryResult.of_fql(value)

    def _execute_statement(self, statement: duckdb.Statement) -> QueryResult:
        """The rows of `statement` when DuckDB types it a SELECT, else an empty
        result `from_statement`."""
        cursor = self._connection.execute(statement)

        if statement.type != duckdb.StatementType.SELECT:
            return QueryResult((), (), from_statement=True)

        columns = tuple(description[0] for description in cursor.description)
        rows = cursor.fetchall()
        return QueryResult(columns, rows)

    def _build_namespace(self, dbf_name: str) -> dict[str, object]:
        """The globals an FQL string runs in: the runtime and the database as `dbf_name`."""
        return {"__builtins__": {}, **FQL_RUNTIME, dbf_name: self._dbf}

    @override
    def close(self) -> bool:
        closed = super().close()
        if closed:
            self._connection.close()
        return closed

    @override
    def _sql_schema(self) -> SqlSchema:
        tables = self._connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        return {
            table: {
                name: type_
                for name, type_, *_ in self._connection.execute(
                    f'DESCRIBE "{table}"'
                ).fetchall()
            }
            for (table,) in tables
        }


def read_rows(
    connection: duckdb.DuckDBPyConnection, tables: Iterable[str]
) -> DatasetRowData:
    """Every table's rows as plain dicts in scan order, a `DECIMAL` read as
    `Decimal`, like dates, ints and strings: every type stays native."""
    rows = {}
    for table in tables:
        cursor = connection.execute(f'SELECT * FROM "{table}"')
        columns = [description[0] for description in cursor.description]
        rows[table] = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return rows


def declare_primary_keys(
    connection: duckdb.DuckDBPyConnection, primary_keys: dict[str, PrimaryKey]
) -> None:
    """Make the reference database enforce the keys the FDM stores its rows
    under, so a duplicate-key write fails there too instead of diverging."""
    for table, key in primary_keys.items():
        columns = ", ".join(f'"{column}"' for column in key.columns)
        connection.execute(f'ALTER TABLE "{table}" ADD PRIMARY KEY ({columns})')


def build_dbf(
    name: str,
    rows: DatasetRowData,
    references: References,
    primary_keys: dict[str, PrimaryKey],
) -> DBF:
    """`rows` as an FDM `DBF`, keying each relation by its primary key and
    swizzling every foreign key in `references` into a direct reference to the
    target relation's TF (mirroring the DuckDB rows). The FDM has no NULL: a NULL
    column, foreign key or not, leaves its attribute unmapped.
    Raises `ValueError` naming table, column and count when a non-NULL foreign key
    refers to no row, and naming the table when a reference starts or ends at a
    table without a primary key."""
    relations: dict[str, RF] = {
        table: RF(
            {
                primary_keys[table].key_of(row): TF(_mapped(row), frozen=False)
                for row in table_rows
            },
            lineage=[f"RF({table})"],
            frozen=False,
        )
        for table, table_rows in rows.items()
    }

    for table, mapping in references.items():
        if table not in relations:
            raise ValueError(f"{table!r} declares references but has no primary key")
        relation = relations[table]
        for fk_column, reference in mapping.items():
            if reference.target_table not in relations:
                raise ValueError(
                    f"{table}.{fk_column} refers to {reference.target_table!r}, "
                    "which has no primary key"
                )
            target = relations[reference.target_table]
            _check_targets_exist(table, fk_column, relation, target)
            for item in relation:
                if fk_column not in item.value:
                    continue
                foreign_key = item.value[fk_column]
                item.value[reference.reference_name] = target[foreign_key]
            relation.references(reference.reference_name, target)

    return DBF(relations, lineage=[f"DBF({name})"], frozen=False)


def _check_targets_exist(table: str, column: str, relation: RF, target: RF) -> None:
    """Raise `ValueError` when a non-NULL `column` value of `relation` has no row
    in `target`."""
    dangling = sum(
        1
        for item in relation
        if column in item.value and item.value[column] not in target
    )
    if dangling:
        raise ValueError(
            f"{table}.{column} refers to no row of the target in {dangling} rows"
        )


def _mapped(row: dict[str, Any]) -> dict[str, Any]:
    """`row` without its NULL columns: the FDM has no NULL."""
    return {column: value for column, value in row.items() if value is not None}


def _copy_references(references: References) -> References:
    """A copy of `references` that shares no dict with it."""
    return {table: dict(mapping) for table, mapping in references.items()}


def _is_expression(fql: str) -> bool:
    """True when `fql` parses as one Python expression."""
    try:
        ast.parse(fql, mode="eval")
    except SyntaxError:
        return False
    return True

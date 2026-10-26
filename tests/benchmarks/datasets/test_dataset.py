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

"""A dataset's registry declaration and the dataset opened from it."""

from datetime import date
from decimal import Decimal

import duckdb
import pytest

from benchmarks.datasets import (
    Dataset,
    DatasetClosed,
    DatasetDefinition,
    FQLExecutionFailed,
    Reference,
    LoadedDataset,
    NotATable,
    PrimaryKey,
)
from benchmarks.datasets.dataset import (
    build_dbf,
    declare_primary_keys,
    read_rows,
    read_sql_schema,
)

ID = PrimaryKey.of("id")  # the key of most tables below


def test_DatasetDefinition_has_data_follows_build_connection(genre, flat):
    """`has_data` is True exactly when the definition can open a connection."""
    assert flat.has_data is False  # no connection builder: renders only
    assert genre.has_data is True  # opens a DuckDB connection and builds an FDM


def test_DatasetDefinition_open_without_data_returns_plain_dataset():
    """A definition without data opens a `Dataset` with the declared keys only."""
    definition = DatasetDefinition(name="flat", primary_keys={"a": ID})  # no data

    opened = definition.open()  # no connection, no FDM

    assert not isinstance(opened, LoadedDataset)  # nothing to execute on
    assert opened.schema().primary_keys == {"a": ID}  # the declared keys


def test_DatasetDefinition_open_copies_declared_keys():
    """A change to one dataset's keys reaches no definition, constant or open."""
    declared_keys = {"a": ID}  # stands in for a module constant such as TPCH's
    definition = DatasetDefinition(name="a", primary_keys=declared_keys)  # registry
    first = definition.open()  # one open dataset

    first.primary_keys["b"] = ID  # a caller changes that dataset's keys
    second = definition.open()  # the next open

    assert definition.primary_keys == {"a": ID}  # the definition kept its keys
    assert declared_keys == {"a": ID}  # and so did the constant behind it
    assert second.schema().primary_keys == {"a": ID}  # the next open starts clean


def test_DatasetDefinition_open_copies_declared_references():
    """A change to one dataset's references reaches no definition, constant or open."""
    declared = {"a": {"b_id": Reference("b_id", "b")}}  # stands in for the IMDB FKs
    definition = DatasetDefinition(name="a", references=declared)  # registry entry
    first = definition.open()  # one open dataset

    first.references["a"]["c_id"] = Reference("c_id", "c")  # a caller changes a dict
    second = definition.open()  # the next open

    assert declared == {"a": {"b_id": Reference("b_id", "b")}}  # kept its mapping
    assert second.schema().references == declared  # the next open starts clean


def test_DatasetDefinition_open_with_data_gives_every_dataset_its_own_data(genre):
    """A write to one open dataset is invisible in another open of it."""
    first = genre.open()  # the dataset the writes go to

    first.execute_sql("DELETE FROM genre")  # a write on the first dataset
    first.execute_fql("del dbf.genre[1]")  # and the same write on its FDM side
    second = genre.open()  # opened after the writes
    second_rows = second.execute_sql("SELECT * FROM genre").rows  # its DuckDB rows
    second_has_noir = second.execute_fql("1 in dbf.genre").rows  # its FDM row
    first.close()  # the test owns both datasets
    second.close()  # likewise

    assert second_rows == ((1, "noir"),)  # the second DuckDB kept the row
    assert second_has_noir == ((True,),)  # and so did its FDM database


def test_Dataset_schema_after_close_raises_DatasetClosed(any_definition):
    """Using a closed dataset is a caller bug, so it is not hidden."""
    dataset = any_definition.open()  # a fresh dataset of either kind

    dataset.close()  # releases it

    with pytest.raises(DatasetClosed, match="closed"):  # the error says why
        dataset.schema()  # no schema after close


def test_Dataset_schema_without_data_has_keys_and_references_only():
    """Without a reference database, no column is known."""
    references = {"a": {"b_id": Reference("b_id", "b")}}  # a.b_id references b
    keys = {"a": ID, "b": PrimaryKey(("x", "y"))}  # a single and a composite key
    dataset = Dataset(references=references, primary_keys=keys)  # no database

    schema = dataset.schema()  # what the parser reads for the next statement

    assert schema.references == references  # the dataset's own FK mapping
    assert schema.primary_keys == keys  # the dataset's own keys
    assert schema.columns == {}  # no database, so no columns


def test_Dataset_schema_returns_copy_of_keys():
    """A caller that changes the keys of its schema does not change the dataset."""
    dataset = Dataset(primary_keys={"a": ID})  # one declared table

    dataset.schema().primary_keys["b"] = ID  # a caller changes its copy

    assert dataset.schema().primary_keys == {"a": ID}  # the dataset is unchanged


def test_Dataset_schema_returns_copy_of_references():
    """A caller that changes the references of its schema does not change the dataset."""
    references = {"a": {"b_id": Reference("b_id", "b")}}  # a.b_id references b
    dataset = Dataset(references=references)  # no database

    dataset.schema().references["a"]["c_id"] = Reference("c_id", "c")  # its copy

    assert dataset.schema().references == {
        "a": {"b_id": Reference("b_id", "b")}
    }  # unchanged


def test_Dataset_close_twice_returns_False_and_stays_closed(any_definition):
    """A second `close` is a no-op that says so; the dataset stays closed."""
    dataset = any_definition.open()  # a fresh dataset of either kind

    first = dataset.close()  # releases it
    second = dataset.close()  # nothing left to release

    assert first is True  # the first close took effect
    assert second is False  # the second did nothing, and says so
    with pytest.raises(DatasetClosed):  # still closed after the second close
        dataset.schema()  # any other call is still refused


def test_LoadedDataset_execute_sql_after_close_raises_DatasetClosed(genre):
    """No SQL runs on a closed dataset."""
    dataset = genre.open()  # a dataset with data

    dataset.close()  # released

    with pytest.raises(DatasetClosed):  # refused before DuckDB is asked
        dataset.execute_sql("SELECT 1")  # would need the connection


def test_LoadedDataset_execute_sql_invalid_sql_raises_duckdb_error(dataset):
    """Invalid SQL raises DuckDB's own error, not a wrapper."""

    with pytest.raises(duckdb.ParserException):  # DuckDB's own error
        dataset.execute_sql("SELEC 1")  # a typo


@pytest.mark.parametrize(
    "sql",
    ["CREATE TABLE extra (id INTEGER)", "INSERT INTO genre VALUES (2, 'heist')"],
    ids=["DDL", "DML"],
)
def test_LoadedDataset_execute_sql_without_select_returns_empty_result(sql, genre):
    """Only a SELECT gives rows, not DDL (no columns) or DML (a count)."""
    dataset = genre.open()  # a private copy: the statement writes

    result = dataset.execute_sql(sql)  # no SELECT
    dataset.close()  # the test owns this dataset

    assert result.columns == ()  # no columns
    assert result.rows == ()  # and no rows
    assert result.from_statement  # marked, so check_matches refuses it


def test_LoadedDataset_execute_sql_returns_last_statement_rows(dataset):
    """A script gives its last statement's rows, under DuckDB's column names."""
    insert = "INSERT INTO genre VALUES (2, 'heist')"  # the first statement
    script = f"{insert}; SELECT g.label AS l FROM genre g"  # then a SELECT

    result = dataset.execute_sql(script)  # both statements run

    assert result.columns == ("l",)  # the alias
    assert sorted(result.rows) == [("heist",), ("noir",)]  # after the INSERT
    assert not result.from_statement  # a SELECT's rows can be compared


def test_LoadedDataset_execute_fql_after_close_raises_DatasetClosed(genre):
    """No FQL runs on a closed dataset."""
    dataset = genre.open()  # a dataset with data

    dataset.close()  # released

    with pytest.raises(DatasetClosed):  # refused before the FQL is read
        dataset.execute_fql("1")  # would need the FDM database


def test_LoadedDataset_execute_fql_failing_statement_raises_FQLExecutionFailed(dataset):
    """FQL that is not one expression is executed; Python's error is the cause."""

    with pytest.raises(FQLExecutionFailed) as raised:  # the package's error
        dataset.execute_fql("dbf.genre[")  # unbalanced: no expression, exec'd

    assert isinstance(raised.value.__cause__, SyntaxError)  # Python's, kept


def test_LoadedDataset_execute_fql_failing_expression_raises_FQLExecutionFailed(
    dataset,
):
    """A failing expression names Python's error and keeps it as the cause."""

    with pytest.raises(FQLExecutionFailed, match="NameError") as raised:  # named
        dataset.execute_fql("nothing_bound")  # an expression, eval'd

    assert isinstance(raised.value.__cause__, NameError)  # Python's, kept


def test_LoadedDataset_execute_fql_non_table_result_raises_NotATable(dataset):
    """A result that is no table raises its `NotATable` error unwrapped."""

    with pytest.raises(NotATable):  # not wrapped in FQLExecutionFailed
        dataset.execute_fql("dbf")  # the whole database is no table


def test_LoadedDataset_execute_fql_statement_returns_empty_result(dataset):
    """A statement gives no rows, but takes effect."""

    result = dataset.execute_fql("del dbf.genre[1]")  # a statement, no value
    left = dataset.execute_fql("len(dbf.genre)")  # what the statement did

    assert result.columns == ()  # no columns
    assert result.rows == ()  # and no rows
    assert result.from_statement  # marked, so check_matches refuses it
    assert left.rows == ((0,),)  # the tuple is gone


def test_LoadedDataset_execute_fql_expression_returns_its_rows(dataset):
    """An expression's value is read as rows under its attributes."""

    result = dataset.execute_fql("dbf.genre")  # a relation

    assert result.columns == ("id", "label")  # the tuples' attributes
    assert result.rows == ((1, "noir"),)  # one row per tuple


def test_LoadedDataset_execute_fql_reads_database_under_dbf_name(dataset):
    """The FQL string reads the database under the name the caller gives."""

    result = dataset.execute_fql("db.genre.project('label')", dbf_name="db")  # db

    assert result.rows == (("noir",),)  # the same rows as under dbf


def test_LoadedDataset_execute_fql_tests_NULL_with_Exists_and_Missing():
    """A NULL is an unmapped attribute, which Exists/Missing test."""

    def labelled_and_unlabelled() -> duckdb.DuckDBPyConnection:  # the reference data
        connection = duckdb.connect()  # a tiny reference database
        connection.execute("CREATE TABLE genre (id INTEGER, label VARCHAR)")  # table
        connection.execute("INSERT INTO genre VALUES (1, 'noir'), (2, NULL)")  # 2: NULL
        return connection  # the dataset's own connection

    definition = DatasetDefinition(
        name="genre",
        primary_keys={"genre": ID},
        build_connection=labelled_and_unlabelled,
    )  # one genre with a label, one without
    dataset = definition.open()  # loads the NULL label as unmapped

    unlabelled = "dbf.genre.where(Missing('label')).project('id')"  # NULL label
    labelled = "dbf.genre.where(Exists('label')).project('id')"  # non-NULL label
    missing = dataset.execute_fql(unlabelled)  # runs Missing in the FQL
    present = dataset.execute_fql(labelled)  # runs Exists in the FQL
    dataset.close()  # the test owns this dataset

    assert missing.rows == ((2,),)  # only genre 2 lacks a label
    assert present.rows == ((1,),)  # only genre 1 has one


def test_LoadedDataset_execute_sql_and_execute_fql_change_only_their_own_copy(dataset):
    """A write through one engine is not seen by the other."""
    heist = "TF({'id': 2, 'label': 'heist'}, frozen=False)"  # a new FDM tuple

    dataset.execute_sql("DELETE FROM genre")  # SQL loses noir
    dataset.execute_fql(f"dbf.genre[2] = {heist}")  # FQL gains heist
    sql = dataset.execute_sql("SELECT g.label FROM genre AS g")  # SQL now
    fql = dataset.execute_fql("dbf.genre.project('label')")  # FQL now

    assert sql.rows == ()  # deleted in SQL, and the FQL write did not arrive
    assert sorted(fql.rows) == [("heist",), ("noir",)]  # the SQL delete did not


def test_LoadedDataset_schema_reads_columns_from_connection(dataset):
    """A loaded dataset takes each table's columns from its reference database."""

    columns = dataset.schema().columns  # DESCRIBEd from the connection

    assert columns == {"genre": {"id": "INTEGER", "label": "VARCHAR"}}  # SQL types


def test_LoadedDataset_close_closes_connection(genre):
    """A loaded dataset's `close` also releases its DuckDB connection."""
    opened = []  # every connection the definition below hands out

    def recorded_genre_connection() -> duckdb.DuckDBPyConnection:
        connection = genre.build_connection()  # the genre data
        opened.append(connection)  # kept, so the test can look at it
        return connection  # the dataset's own connection

    definition = DatasetDefinition(
        name="genre",
        primary_keys={"genre": ID},
        build_connection=recorded_genre_connection,
    )  # GENRE, with its connection recorded
    dataset = definition.open()  # opens one connection

    dataset.close()  # the caller is done with it

    with pytest.raises(duckdb.ConnectionException):  # closed with the dataset
        opened[0].execute("SELECT 1")  # the connection refuses further queries


def test_read_rows_reads_each_table_as_the_fdm_keeps_it():
    """One dict per row in scan order; every type native, DECIMAL as Decimal."""
    connection = duckdb.connect()  # a tiny reference database
    table = '"order details"'  # a table name with a space, as in special_names
    columns = "id INTEGER, price DECIMAL(5,2), day DATE"  # three column types
    connection.execute(f"CREATE TABLE {table} ({columns})")  # the table
    rows = "(2, 1.50, '1995-12-15'), (1, 0.25, '1996-01-01')"  # two rows
    connection.execute(f"INSERT INTO {table} VALUES {rows}")  # filled

    read = read_rows(connection, ["order details"])  # a name that needs quoting
    connection.close()  # the rows are plain Python values now

    assert read == {
        "order details": [
            {"id": 2, "price": Decimal("1.50"), "day": date(1995, 12, 15)},  # first
            {"id": 1, "price": Decimal("0.25"), "day": date(1996, 1, 1)},  # second
        ]
    }  # every column by name, as the FDM tuples will carry it
    assert isinstance(read["order details"][0]["price"], Decimal)  # not a float


def test_declare_primary_keys_makes_reference_database_reject_duplicates():
    """DuckDB rejects duplicates of single, composite and quoted-name keys."""
    connection = duckdb.connect()  # a fresh reference database
    connection.execute('CREATE TABLE "a b" (x INTEGER, y INTEGER)')  # quoted name
    connection.execute("CREATE TABLE c (id INTEGER)")  # a single-column key
    keys = {"a b": PrimaryKey(("x", "y")), "c": ID}  # a composite and a single key

    declare_primary_keys(connection, keys)  # enforced by the reference database
    connection.execute('INSERT INTO "a b" VALUES (1, 1), (1, 2)')  # distinct pairs
    connection.execute("INSERT INTO c VALUES (1)")  # the first id 1

    with pytest.raises(duckdb.ConstraintException):  # the pair (1, 2) exists
        connection.execute('INSERT INTO "a b" VALUES (1, 2)')  # a duplicate pair
    with pytest.raises(duckdb.ConstraintException):  # id 1 exists
        connection.execute("INSERT INTO c VALUES (1)")  # a duplicate id
    connection.close()  # the test owns this connection


def test_build_dbf_swizzles_foreign_keys_into_references():
    """A foreign key becomes the referenced tuple; a NULL one, no reference."""
    rows = {
        "genre": [{"id": 1}],
        "movie": [
            {"id": 1, "genre_id": 1, "sequel_id": None},  # no sequel
            {"id": 2, "genre_id": None, "sequel_id": 1},  # no genre
        ],
    }  # one table with a same-named and a renamed reference
    references = {
        "movie": {
            "genre_id": Reference("genre_id", "genre"),  # replaces the key column
            "sequel_id": Reference("sequel", "movie"),  # added beside the key
        }
    }  # movie -> genre, and movie -> movie
    keys = {"genre": ID, "movie": ID}  # both keyed by id

    dbf = build_dbf("cinema", rows, references, keys)  # the FDM database
    first = dbf.movie[1]  # the movie without a sequel
    second = dbf.movie[2]  # the movie without a genre

    assert first["genre_id"] is dbf.genre[1]  # the key became the genre tuple
    assert "sequel" not in first  # a NULL renamed key adds no reference
    assert "sequel_id" not in first  # and its NULL key column is unmapped too
    assert "genre_id" not in second  # a NULL same-named key is removed
    assert second["sequel"] is first  # the renamed key references movie 1
    assert second["sequel_id"] == 1  # beside its key column


def test_build_dbf_leaves_NULL_column_unmapped():
    """A NULL value maps no attribute; every other value is kept as it is."""
    rows = {"genre": [{"id": 1, "label": None}, {"id": 2, "label": "noir"}]}  # one NULL
    keys = {"genre": ID}  # keyed by id

    dbf = build_dbf("cinema", rows, {}, keys)  # no references
    unnamed = dbf.genre[1]  # the genre without a label
    noir = dbf.genre[2]  # the genre with one

    assert "label" not in unnamed  # the FDM has no NULL: the label is unmapped
    assert unnamed["id"] == 1  # its other attributes stay
    assert noir["label"] == "noir"  # a non-NULL value is kept


def test_build_dbf_dangling_foreign_key_raises_ValueError_naming_it():
    """A foreign key without a target row names its table, column and count."""
    rows = {
        "genre": [{"id": 1}],
        "movie": [{"id": 1, "genre_id": 1}, {"id": 2, "genre_id": 7}],
    }  # movie 2 refers to genre 7, which does not exist
    mapping = {"movie": {"genre_id": Reference("genre", "genre")}}  # movie -> genre
    keys = {"genre": ID, "movie": ID}  # both keyed by id

    with pytest.raises(ValueError, match=r"movie\.genre_id.* 1 rows"):  # named
        build_dbf("cinema", rows, mapping, keys)  # refuses the dangling key


def test_build_dbf_reference_to_table_without_primary_key_raises_ValueError():
    """A reference must end at a table with a primary key: the FDM stores its
    tuples under that key."""
    rows = {"movie": [{"id": 1, "genre_id": 1}]}  # genre has no rows: no key declared
    mapping = {"movie": {"genre_id": Reference("genre", "genre")}}  # movie -> genre
    keys = {"movie": ID}  # only movie is keyed

    with pytest.raises(ValueError, match=r"movie\.genre_id refers to 'genre'"):
        build_dbf("cinema", rows, mapping, keys)  # names the reference and target


def test_build_dbf_reference_on_missing_column_raises_ValueError():
    """A reference must name a column of its table: a misspelled one is no NULL."""
    rows = {
        "genre": [{"id": 1}],
        "movie": [{"id": 1, "genre_id": 1}],
    }  # movie has a genre_id column
    mapping = {"movie": {"genre_idd": Reference("genre", "genre")}}  # misspelled
    keys = {"genre": ID, "movie": ID}  # both keyed by id

    with pytest.raises(ValueError, match=r"movie\.genre_idd is no column"):  # named
        build_dbf("cinema", rows, mapping, keys)  # refuses the unknown column


def test_build_dbf_reference_on_all_NULL_column_swizzles_nothing():
    """A column that is NULL in every row exists: its reference maps nothing."""
    rows = {
        "genre": [{"id": 1}],
        "movie": [{"id": 1, "genre_id": None}],
    }  # the only movie has no genre
    mapping = {"movie": {"genre_id": Reference("genre", "genre")}}  # movie -> genre
    keys = {"genre": ID, "movie": ID}  # both keyed by id

    dbf = build_dbf("cinema", rows, mapping, keys)  # no error

    assert "genre_id" not in dbf.movie[1]  # the NULL key is unmapped
    assert "genre" not in dbf.movie[1]  # and no reference is added


def test_build_dbf_reference_on_missing_column_of_empty_table_raises_ValueError():
    """With the declared columns, a table without rows is checked too."""
    rows = {"genre": [], "movie": []}  # no rows: the rows name no column
    columns = {
        "genre": {"id": "INTEGER"},
        "movie": {"id": "INTEGER", "genre_id": "INTEGER"},
    }  # as the reference database declares them
    mapping = {"movie": {"genre_idd": Reference("genre", "genre")}}  # misspelled
    keys = {"genre": ID, "movie": ID}  # both keyed by id

    with pytest.raises(ValueError, match=r"movie\.genre_idd is no column"):  # named
        build_dbf("cinema", rows, mapping, keys, columns)  # refuses it all the same


def test_DatasetDefinition_open_checks_foreign_key_columns_of_empty_tables():
    """`open` checks a reference against the columns DuckDB declares, so a
    misspelled column fails also while its table has no rows."""

    def build() -> duckdb.DuckDBPyConnection:
        """Two empty tables, a -> b."""
        connection = duckdb.connect()  # a fresh in-memory database
        connection.execute("CREATE TABLE a (id INTEGER, b_id INTEGER)")  # a -> b
        connection.execute("CREATE TABLE b (id INTEGER)")  # the target
        return connection  # for open() to read

    definition = DatasetDefinition(
        name="a",
        references={"a": {"b_idd": Reference("b", "b")}},  # misspelled
        primary_keys={"a": ID, "b": ID},  # both keyed by id
        build_connection=build,
    )  # a dataset whose tables have no rows yet

    with pytest.raises(ValueError, match=r"a\.b_idd is no column"):  # named
        definition.open()  # refuses the unknown column


def test_read_sql_schema_names_columns_of_every_table_also_without_rows():
    """The columns come from DuckDB's declaration, not from the rows."""
    connection = duckdb.connect()  # a fresh in-memory database
    connection.execute("CREATE TABLE a (id INTEGER, label VARCHAR)")  # no rows
    connection.execute("CREATE TABLE b (id INTEGER)")  # a second table
    connection.execute("INSERT INTO b VALUES (1)")  # with a row

    schema = read_sql_schema(connection)  # DuckDB's columns of every table
    connection.close()  # the test owns this connection

    assert schema == {
        "a": {"id": "INTEGER", "label": "VARCHAR"},
        "b": {"id": "INTEGER"},
    }  # both tables, with and without rows


def test_read_sql_schema_finds_given_tables_as_a_query_does():
    """Given tables are found regardless of case and keyed by the name given."""
    connection = duckdb.connect()  # a fresh in-memory database
    connection.execute("CREATE TABLE movie (id INTEGER)")  # stored as movie
    connection.execute("CREATE TABLE extra (id INTEGER)")  # not asked for

    schema = read_sql_schema(connection, ["Movie"])  # asked for as Movie
    connection.close()  # the test owns this connection

    assert schema == {"Movie": {"id": "INTEGER"}}  # under the given name only


def test_DatasetDefinition_open_finds_tables_as_queries_do():
    """A table declared in another case than DuckDB stores it opens as before:
    the column check finds it the same way the rows are read."""

    def build() -> duckdb.DuckDBPyConnection:
        """movie -> genre, with one row each, created in lower case."""
        connection = duckdb.connect()  # a fresh in-memory database
        connection.execute("CREATE TABLE genre (id INTEGER)")  # the target
        connection.execute(
            "CREATE TABLE movie (id INTEGER, genre_id INTEGER)"  # movie -> genre
        )
        connection.execute("INSERT INTO genre VALUES (1)")  # genre 1
        connection.execute("INSERT INTO movie VALUES (1, 1)")  # movie 1 -> genre 1
        return connection  # for open() to read

    definition = DatasetDefinition(
        name="cinema",
        references={"Movie": {"genre_id": Reference("genre_id", "genre")}},
        primary_keys={"Movie": ID, "genre": ID},  # Movie in another case
        build_connection=build,
    )  # declared as Movie, stored as movie

    dataset = definition.open()  # no KeyError, no ValueError
    fql = "dbf['Movie'][1]['genre_id'] is dbf.genre[1]"  # movie 1's genre, a bool
    same = dataset.execute_fql(fql)  # read under the declared name
    dataset.close()  # the test owns this dataset

    assert same.rows == ((True,),)  # the reference is swizzled


def test_build_dbf_reference_from_table_without_primary_key_raises_ValueError():
    """A reference must start at a table with a primary key, too."""
    rows = {"genre": [{"id": 1}]}  # movie has no rows: no key declared
    mapping = {"movie": {"genre_id": Reference("genre", "genre")}}  # movie -> genre
    keys = {"genre": ID}  # only genre is keyed

    with pytest.raises(ValueError, match=r"'movie' declares references"):
        build_dbf("cinema", rows, mapping, keys)  # names the table


def test_DatasetDefinition_open_dangling_foreign_key_closes_connection():
    """A failed open does not leak the connection it built."""
    built = []  # the connections the definition asked for

    def build() -> duckdb.DuckDBPyConnection:
        connection = duckdb.connect()  # a fresh in-memory database
        connection.execute("CREATE TABLE a (id INTEGER, b_id INTEGER)")  # a -> b
        connection.execute("CREATE TABLE b (id INTEGER)")  # b stays empty
        connection.execute("INSERT INTO a VALUES (1, 5)")  # b 5 does not exist
        built.append(connection)  # remembered for the check below
        return connection

    definition = DatasetDefinition(
        name="a",
        references={"a": {"b_id": Reference("b", "b")}},
        primary_keys={"a": ID, "b": ID},
        build_connection=build,
    )  # a definition whose data is inconsistent

    with pytest.raises(ValueError):  # the open fails
        definition.open()  # reads the rows, then swizzles

    with pytest.raises(duckdb.ConnectionException):  # the connection is closed
        built[0].execute("SELECT 1")  # unusable after the failed open


def test_LoadedDataset_execute_fql_indented_source_runs(dataset):
    """FQL pasted with leading indentation runs as it would without."""

    expression = dataset.execute_fql("    len(dbf.genre)\n")  # an indented expression
    statement = dataset.execute_fql("    x = 1\n    y = x\n")  # indented statements

    assert expression.rows == ((1,),)  # evaluated
    assert statement.rows == ()  # executed, and no rows


def test_LoadedDataset_repr_leaves_out_database(dataset):
    """The repr stays short: it does not print the FDM database or the connection."""

    text = repr(dataset)  # what a log line or a failing assert would show

    assert "_dbf" not in text  # no DBF dump
    assert "_connection" not in text  # no connection dump


def test_LoadedDataset_execute_fql_has_no_builtins(dataset):
    """FQL runs without builtins: `__import__` is undefined."""
    with pytest.raises(FQLExecutionFailed, match="NameError"):  # not available
        dataset.execute_fql("__import__('os')")  # what a builtin-less runtime refuses

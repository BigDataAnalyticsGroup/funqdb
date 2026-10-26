# benchmarks.datasets

The datasets that the benchmarks and the SQL-to-FQL transpiler run on.
A dataset with data holds each row twice:

- in an FDM database (`DBF`), which FQL reads;
- in an in-memory DuckDB database, the SQL reference.

The two copies are independent. A write through one engine does not reach the other.
A dataset without data has only its keys and references: its FQL renders but never executes.

## Usage

Code outside the package imports every name from the package root.
The submodules are internal; only tests may import them.

```python
from benchmarks.datasets import DATASETS, LoadedDataset

dataset = DATASETS["movies"].open()
assert isinstance(dataset, LoadedDataset)

sql = dataset.execute_sql("SELECT t.title FROM title AS t")
fql = dataset.execute_fql("dbf.title.project('title')")
sql.check_matches(fql)  # raises ResultMismatch when the rows differ

dataset.close()
```

## Interfaces

| Name                                   | Purpose                                                                                                                                                |
|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `DATASETS`                             | Every registered `DatasetDefinition`, by name.                                                                                                         |
| `DatasetDefinition`                    | The declaration of a dataset: name, foreign keys, primary keys, connection builder. `has_data` tells if it executes. `open()` returns a new `Dataset`. |
| `Dataset`                              | An open dataset without data: `schema()` and `close()`. `schema()` returns copies, so a change to it does not change the dataset.                      |
| `LoadedDataset`                        | A `Dataset` with data: also `execute_sql(sql)` and `execute_fql(fql, dbf_name="dbf")`.                                                                 |
| `DatasetClosed`                        | Raised by every call on a closed dataset, except `close`: it returns `False` and does nothing.                                                         |
| `Schema`, `PrimaryKey`                 | The keys, references and columns that a statement is parsed against.                                                                                   |
| `References`, `Reference`, `SqlSchema` | The types of `Schema.references` (a `Reference` names the reference and its target table) and `Schema.columns`.                                        |
| `QueryResult`                          | Rows under named columns. `check_matches` compares two results. A column the FQL side lacks is NULL.                                                 |
| `FQLExecutionFailed`                   | The FQL string or the read of its result failed. The engine error is its cause.                                                                        |
| `NotTabular`                           | The FQL result cannot be read as rows. Subtypes: `ReferenceValue`, `NotATable`, `NotExecuted`.                                                         |
| `ResultMismatch`                       | Raised by `check_matches`.                                                                                                                             |
| `FQL_RUNTIME`                          | Every name that an FQL string can use. FQL runs without other builtins.                                                                                |

`execute_sql` raises DuckDB's own errors.
`execute_fql` raises `FQLExecutionFailed` or `NotTabular`.
Both raise `DatasetClosed` after `close`.
`open()` raises `ValueError` for a foreign key without a target row, for a reference from or to a table
without a primary key, or for a reference on a column its table does not have, and closes its connection.
`execute_fql` raises `NotExecuted` for an operator that was not called.
`check_matches` compares values with Python's `==`:
`1`, `1.0` and `True` are equal, but `Decimal("0.1")` and the float `0.1` are not.
A statement without rows (DDL, DML, or FQL that is not one expression) returns an empty `QueryResult`
with `from_statement` set. `check_matches` refuses it: it has no result to compare.
A single column aligns by position only with the unnamed column of an FQL scalar; otherwise columns align by name.

## Limits

- FQL is trusted code. `FQL_RUNTIME` withholds the builtins, but this is no security boundary:
  an FQL object reaches `__import__` through `__globals__`. Do not run FQL from an untrusted source.
  A caller that puts an SQL literal into FQL writes it as `repr(value)`, never by string formatting.

- A `Reference` covers one foreign key column. A multi-column foreign key, such as TPC-H `lineitem` to `partsupp`,
  cannot be declared.

- `tpch` needs DuckDB's `tpch` extension, which DuckDB downloads in `open()` unless it is already installed.
  It needs network access and a writable extension directory (`~/.duckdb/extensions/` by default);
  otherwise that `open()` raises `duckdb.IOException`.

## Datasets

| Name                    | Data                        | Content                                                                                                                                  |
|-------------------------|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| `movies`                | always                      | A small hand-written database in the IMDB schema of the JOB queries. Use it for cases that must execute and match.                       |
| `tpch`                  | after download, see Limits  | Derived from TPC-H: generated by DuckDB's `dbgen` at scale factor 0.01.                                                                  |

All IMDB-based datasets share `imdb/schema.py`.

### TPC-H

This workload is derived from the TPC-H Benchmark and is not comparable to published TPC-H Benchmark results,
as this implementation does not comply with all requirements of the TPC-H Benchmark.

The data is generated by `dbgen`, which DuckDB's `tpch` extension ships under the
[TPC End User License Agreement](https://www.tpc.org/TPC_Documents_Current_Versions/txt/EULA_v2.2.0.txt).
`dbgen` is not part of this repository; DuckDB downloads it with its `tpch` extension.

## Adding a dataset

A dataset subpackage is inside the package, so it can import the internal module `benchmarks.datasets.dataset`.

1. Declare a `DatasetDefinition` in a new subpackage. Give it a `build_connection` that returns a populated
   DuckDB connection.
2. In `build_connection`, call `declare_primary_keys` from `benchmarks.datasets.dataset`,
   so that DuckDB also rejects a duplicate key.
3. Add the definition to `DATASETS` in `registry.py`.

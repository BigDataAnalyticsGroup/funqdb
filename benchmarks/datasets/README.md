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
| `job`                   | never                       | The full IMDB dump (about 4 GB of CSV), schema-only: 93 `aka_title.movie_id` values refer to no `title` row.                             |
| `job_seeds`             | if `job/.data_seeds` exists | Up to 3 rows of the join of every JOB query, plus every row that they reference.                                                         |
| `tpch`                  | after download, see Limits  | Derived from TPC-H: generated by DuckDB's `dbgen` at scale factor 0.01.                                                                  |

All IMDB-based datasets share `imdb/schema.py`.

### JOB data setup

The JOB data folders (`job/.data` and `job/.data_seeds`) are not in git. Run these commands once,
from the repository root; they need about 8 GB of free disk space:

```
python -m benchmarks.datasets.job.setup.download_job_data
python -m benchmarks.datasets.job.setup.downscale_job_data
```

The first command downloads the dump (1.26 GB) into `job/.data` over HTTPS.
It checks the SHA-256 of the archive (`DATA_SHA256`) before it keeps or extracts the archive,
and stops when the server sends more or fewer bytes than the archive's size, or nothing for a minute.
It does not extract the archive's `schematext.sql`: every load runs the committed `job/schematext.sql`,
so no SQL from a data folder ever runs.
The second command writes `job/.data_seeds`. The CSV files have no header row.

Whether `job_seeds` has data is decided when `benchmarks.datasets` is first imported:
after writing `job/.data_seeds`, start a new Python process (e.g. restart the notebook kernel).
Importing again in the same process changes nothing.

### How `downscale_job_data` works

The downscaler writes the seeds: a copy of the full dump that is small enough to open on both sides,
with two properties:

- every JOB query whose join finds rows on the full dump still finds rows;
- no foreign key refers to a row that the copy does not keep.

It does these steps:

1. **Load.** It refuses to load while `job/.data/imdb.tgz` still exists: then the extraction did not finish.
   It loads the CSV files of `job/.data` into `job/.data/job.duckdb.part`, then renames that file to
   `job.duckdb`. A later run uses `job.duckdb` again. An interrupted load leaves only the `.part` file,
   and the next run loads again. Once `job.duckdb` exists, the CSV files in `job/.data` are no longer read.
2. **Harvest the seeds.** For each JOB query, it runs a changed query that keeps the original query from `FROM` on.
   That query returns the `id` of each table alias, for the first 3 rows of the join in id order.
   These rows are the seeds. A query whose join finds no rows is noted.
3. **Close the seeds.** It adds every row that a seed row refers to, and repeats until no rows are added.
4. **Write and check.** It writes one CSV file per table into `job/.data_seeds.part`.
   Then it loads that folder and stops with `GuaranteeViolated` in two cases:
   a JOB query whose join finds rows on the full dump finds none, or a foreign key refers to a row that is not kept.
   Only when both checks pass does it replace `job/.data_seeds` with that folder;
   until then, the seeds of an earlier run stay as they were.

The seed closure (step 3) adds every row that a kept foreign key refers to.
The full dump itself has foreign keys without a target row (see the `job` dataset above):
if a seed row had one, the check in step 4 would stop the run.

### Source and terms of the JOB data

Information courtesy of IMDb (https://www.imdb.com). Used with permission.

The JOB data is the IMDb snapshot of May 2013 that the Join Order Benchmark
([Leis et al., PVLDB 9(3), 2015](https://www.vldb.org/pvldb/vol9/p204-leis.pdf)) runs on.
`download_job_data` fetches it from CWI (<https://event.cwi.nl/da/job/>); it is not part of this repository,
and the AGPL of funqDB does not apply to it.

- IMDb allows only personal and non-commercial use of its data
  ([Can I use IMDb data in my software?](https://help.imdb.com/article/imdb/general-information/can-i-use-imdb-data-in-my-software/G5JTRESSHJBBHTGX)).
- CWI provides this copy only for query optimizer experiments in database systems engineering or scientific research
  ([readme.txt](https://event.cwi.nl/da/job/readme.txt)). Use it for nothing else.
- Do not commit, upload or publish the folders `job/.data*` (including `job.duckdb`) or files made from them:
  they are copies of IMDb data. git ignores these folders.
- `job/schematext.sql` is `schema.sql` of [viktorleis/job](https://github.com/viktorleis/job) (commit `fa6f033`),
  under the MIT License, Copyright (c) 2023 Viktor Leis (see [`LICENSE`](../job/queries/LICENSE)),
  without the `NOT NULL` of four columns that the CWI copy's own schema leaves nullable too.

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
   so that DuckDB also rejects a duplicate key. The JOB datasets need not: `job/schematext.sql` declares the keys.
3. Add the definition to `DATASETS` in `registry.py`.

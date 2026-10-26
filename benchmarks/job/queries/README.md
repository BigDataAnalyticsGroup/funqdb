# JOB queries

The 113 `.sql` files here are the queries of the Join Order Benchmark (JOB) from the paper
[How Good Are Query Optimizers, Really?](https://www.vldb.org/pvldb/vol9/p204-leis.pdf)
by Viktor Leis, Andrey Gubichev, Atanas Mirchev, Peter Boncz, Alfons Kemper and Thomas Neumann (PVLDB 9(3), 2015).

They are copied unchanged, byte for byte, from [viktorleis/job](https://github.com/viktorleis/job)
(commit [`fa6f033`](https://github.com/viktorleis/job/tree/fa6f033a4289c5d6ae1606fc76f5f7f24457cd35)).
`tests/benchmarks/job/test_job_queries.py` checks them against a SHA-256 digest of that commit's 113 query files
(without its `schema.sql` and `fkindexes.sql`).

Queries 15a to 15d name `aka_title` with the alias `AT`. In DuckDB `at` is a keyword, so these four queries
only run there with every use of the alias quoted (`AS "AT"`, `"at".movie_id`).

The queries are licensed under the MIT License, Copyright (c) 2023 Viktor Leis; see [`LICENSE`](LICENSE).
That license covers only the `.sql` files: `SQL vs FQL.md` is part of funqDB, although the query 10c it quotes
is Leis's and under the MIT license too.
The AGPL of funqDB does not apply to the queries.

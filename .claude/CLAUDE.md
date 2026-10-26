# funqDB — project-specific notes for Claude

These notes complement the global config in `~/.claude/CLAUDE.md` with the
parts that are specific to funqDB.

## What this project is

funqDB is a **research prototype / early alpha** exploring a **Functional Data Model (FDM)**
and **Functional Query Language (FQL)** as a replacement for the relational model, SQL,
and ORMs. It is the reference implementation for the paper:

> Jens Dittrich. *A Functional Data Model and Query Language is All You Need.* EDBT 2026.

Read the paper (linked from `README.md`) before making non-trivial design changes —
terminology like **attribute function (AF)**, **relationship function (RF)**,
**subdatabase**, **swizzling**, etc. all come from it.

This is a proof of concept, not production code. Performance is explicitly a non-goal
at this stage; clarity and faithfulness to the FDM/FQL ideas are.

## Repository layout

- `fdm/`  — the Functional Data Model: `AttributeFunction`, schemas, core API
- `fql/`  — the Functional Query Language façade
    - `fql/operators/` — unary operators (filters, joins, projections, aggregates,
      partitions, subdatabases, set operations, transforms, …)
    - `fql/predicates/` — predicates and constraints
    - `fql/plan/` — logical IR and extractor for FQL operator pipelines
- `store/` — persistence layer (currently `SqliteDict` as key/blob store)
- `tests/` — pytest suite, mirrors the `fdm` / `fql` / `store` layout
- `benchmarks/job/` — the 113 Join Order Benchmark queries (`queries/`, MIT, from
  `viktorleis/job`; read them with `job_query_files()`) and the `SQL vs FQL.md` comparison
- `benchmarks/datasets/` — the datasets the benchmarks run on (e.g. `movies`, `tpch`,
  `job`, `job_seeds`), each as an FDM database and a DuckDB reference; see its README
- `docs/tutorial/` — work-in-progress tutorial (often mirrors test examples)
- `examples/` — standalone example scripts (e.g. schema visualization)
- `scripts/` — CLI tools (e.g. `funqdb-viz`)
- `ci/` — CI helper scripts (coverage checks)
- `.claude/` — project-local Claude Code configuration

## Project-specific conventions

In addition to the conventions in the global `CLAUDE.md`:

1. **AGPL-3.0 license header** on every new source file (see existing files
   in `fdm/`, `fql/`, `store/` for the exact header text).
2. If you need to pass a complex object to a function and no proper type
   exists, also consider FDM's `DictionaryAttributeFunction` and its
   subtypes before creating a new ad-hoc class.
3. Existing tests double as tutorial examples — write them readably, prefer
   small self-contained examples over elaborate setup.
4. Coverage target is high (≥ 90 %, see `CONTRIBUTING.md`). Don't let it
   regress.

## AI-written tests

Every statement in an AI-written or AI-modified test body must carry an end-of-line
comment explaining what it does or asserts and why — so the reviewer can verify each
line without running the code.

## Things to be careful about

- **Don't "fix" things that look SQL-ish by making them more SQL-ish.** The whole
  point of funqDB is to *not* reproduce SQL/relational-algebra assumptions
  (NULL handling, single-table results, n-ary joins, etc.). When in doubt, re-read
  the paper or ask.
- The store currently treats values as opaque blobs — query pushdown is intentionally
  *not* implemented yet. Don't add it without discussion.
- Swizzling/unswizzling currently works for **reads only**; writes are a known TODO.
- `TODO.md` tracks known work items — check it before proposing new ones.

## Project-specific skills

- `/finish-mr` — finalize a feature branch (two reviewer rounds, `SPEC.md`
  status update, `black`, full `pytest tests`, commit & push).

`SPEC.md` (project root) is the living feature specification — gold standard
is a passing test.

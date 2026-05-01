# funqDB — Notes for Claude

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

## Git workflow

- **Never commit or push directly to `main`.** Always work on a feature branch
  and create a merge request. This applies to all changes — code, CI, docs.
- Create descriptive branch names (e.g. `feat/plan-extraction`, `ci/coverage-report`).
- If you need to develop something and are currently on `main`, create a new
  feature branch first. If in doubt whether a new branch is needed, ask.
- During POC / iterative development it is fine to change files directly on
  the current feature branch (no sub-branches needed).
- **Commit and push after every phase** (see `/develop`) so progress is always saved remotely.
- **Run `black` before every push** on all changed `.py` files to ensure
  consistent formatting. The CI pipeline will reject unformatted code.

## Repository layout

- `fdm/`  — the Functional Data Model: `AttributeFunction`, schemas, core API
- `fql/`  — the Functional Query Language façade
    - `fql/operators/` — unary operators (filters, joins, projections, aggregates,
      partitions, subdatabases, set operations, transforms, …)
    - `fql/predicates/` — predicates and constraints
    - `fql/plan/` — logical IR and extractor for FQL operator pipelines
- `store/` — persistence layer (currently `SqliteDict` as key/blob store)
- `tests/` — pytest suite, mirrors the `fdm` / `fql` / `store` layout
- `benchmarks/job/` — Join Order Benchmark experiments, incl. the
  `SQL vs FQL.md` comparison
- `docs/tutorial/` — work-in-progress tutorial (often mirrors test examples)
- `examples/` — standalone example scripts (e.g. schema visualization)
- `scripts/` — CLI tools (e.g. `funqdb-viz`)
- `ci/` — CI helper scripts (coverage checks)
- `.claude/` — Claude Code configuration and subagent definitions

## Tooling

- Python **>= 3.12**, managed via **Poetry** (`poetry install`)
- Tests: **pytest** — run `pytest tests` or target individual files
- Formatting: **black** (already a dev dependency — use it, don't hand-format)
- Coverage: **coverage** (target is high, per `CONTRIBUTING.md`)

## Conventions to follow

These come from `CONTRIBUTING.md` and the existing code — please respect them:

1. **Type hints everywhere** — function signatures, variables, return types.
2. **Docstrings on all public functions and classes and __init__**, plus comments for non-trivial
   logic. Existing code in `fdm/attribute_functions.py` is a good style reference.
3. **Tests for every new feature or bug fix.** Tests double as tutorial examples, so
   write them readably.
4. **One concern per PR** — don't mix unrelated changes; split large tasks into smaller independent MRs (see Surgical changes).
5. **AGPL-3.0 license header** on new source files (see existing files for the exact
   header text).
6. Prefer editing existing files over creating new ones; prefer small, focused changes.
7. add/update the documentation/tutorial when changing/adding code
8. if you need to pass complex a complex object to a function, use a proper type, i.e. a separate class, for it, if no
   proper type exists, create one; also consider using FDM's DictionaryAttributeFunction and its subtypes

## AI-generated test marking

Every statement in an AI-written or AI-modified test body must carry an end-of-line
comment explaining what it does or asserts and why — so the reviewer can verify each
line without running the code.

Every test method that Claude adds or modifies **must** be flagged for human review.
The CI job `no-unreviewed-tests` blocks the merge while any flagged test remains.
Two separate markers distinguish new tests from modified tests.

### New test methods

Add `@pytest.mark.needs_review_new` immediately before the `def`:

```python
@pytest.mark.needs_review_new
def test_something_new():
    """Docstring explaining what this test verifies."""
    ...
```

The reviewer must read and debug the **entire** test before removing the marker.

### Modified test methods

If you change an **existing, already-reviewed** test (i.e. one that does *not*
already carry a `needs_review_*` marker):

1. Add `@pytest.mark.needs_review_modified` to the method.
2. Wrap only the lines you changed or added with section comments so the
   reviewer can see exactly what is new:

```python
@pytest.mark.needs_review_modified
def test_existing():
    """Already-reviewed test."""
    assert something_old()

    # -- begin AI-modified --
    assert something_new()
    assert another_new_thing()
    # -- end AI-modified --

    assert something_else_old()
```

The reviewer only needs to check the `# -- begin/end AI-modified --` sections,
then removes **both** the decorator and the section comments.

If the change is so pervasive that section comments would be more noise than
signal (e.g. a complete rewrite), use `@pytest.mark.needs_review_new` instead
(treat it as a new test).

## Surgical changes

Touch only what the task requires. Every changed line should trace directly to the request.

- Don't improve adjacent code, comments, or formatting that isn't broken.
- Match existing style even if you'd do it differently.
- If you notice unrelated dead code or issues, mention them — don't fix them silently.
- Remove only imports/variables/functions that *your* changes made unused.

**Keep changes small.** Every line of code added is code the user must review and maintain.
When a larger task can be split into semantically independent pieces, propose splitting it
into separate MRs rather than delivering one large changeset. If in doubt, do less and ask.

If multiple interpretations of a request exist, present them — don't pick silently.

## Things to be careful about

- **Don't "fix" things that look SQL-ish by making them more SQL-ish.** The whole
  point of funqDB is to *not* reproduce SQL/relational-algebra assumptions
  (NULL handling, single-table results, n-ary joins, etc.). When in doubt, re-read
  the paper or ask.
- The store currently treats values as opaque blobs — query pushdown is intentionally
  *not* implemented yet. Don't add it without discussion.
- Swizzling/unswizzling currently works for **reads only**; writes are a known TODO.
- `TODO.md` tracks known work items — check it before proposing new ones.

## Development workflows

The full workflow is documented in `.claude/WORKFLOW.md` — update it when skills or agents change.
`SPEC.md` (project root) is the living feature specification — gold standard is a passing test.

Always use the available skills and subagents — don't do their work inline.

Use `/plan` to design a feature before any code is written (explore → design → approval).
Use `/develop` to implement an approved plan (POC → checkpoint → iterate).
Use `/finish-mr` to finalize and push a completed feature branch.

The subagents in `.claude/agents/` (`researcher`, `test-writer`, `code-reviewer`) are
invoked from within `/develop` and `/finish-mr` — don't bypass them.

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
2. **Docstrings on all public functions and classes**, plus comments for non-trivial
   logic. Existing code in `fdm/attribute_functions.py` is a good style reference.
3. **Tests for every new feature or bug fix.** Tests double as tutorial examples, so
   write them readably.
4. **One concern per PR** — don't mix unrelated changes.
5. **AGPL-3.0 license header** on new source files (see existing files for the exact
   header text).
6. Prefer editing existing files over creating new ones; prefer small, focused changes.
7. add/update the documentation/tutorial when changing/adding code

## AI-generated test marking

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

## Things to be careful about
- **Don't "fix" things that look SQL-ish by making them more SQL-ish.** The whole
  point of funqDB is to *not* reproduce SQL/relational-algebra assumptions
  (NULL handling, single-table results, n-ary joins, etc.). When in doubt, re-read
  the paper or ask.
- The store currently treats values as opaque blobs — query pushdown is intentionally
  *not* implemented yet. Don't add it without discussion.
- Swizzling/unswizzling currently works for **reads only**; writes are a known TODO.
- `TODO.md` tracks known work items — check it before proposing new ones.

## Subagent workflow — iterative development

Three project-local subagents live in `.claude/agents/`. The workflow is **iterative
and human-in-the-loop**: start minimal, get feedback early, then refine.

### Phase 1: POC — get the core idea working

1. **Research (optional)** — use the `researcher` agent if you need to look up
   library behaviour, best practices, or anything requiring web searches.
2. **Implement a bare-bones POC** — focus only on the core functionality. Skip
   edge cases, full docstrings, and polish. Apply basic conventions (type hints,
   AGPL header on new files) but keep it minimal.
3. **Write initial tests** — invoke the `test-writer` agent for happy-path tests
   only. These prove the POC works and serve as executable documentation of the
   intended behaviour.
4. **Review** — invoke the `code-reviewer` agent on the POC. At this stage,
   "PASS WITH NOTES" is expected and fine — the goal is to catch fundamental
   design issues early, not to polish.

### Phase 2: Human checkpoint (mandatory)

**Stop and present the POC to the user before proceeding.** Include:
- A short summary of the approach taken and any design decisions made.
- What works, what is deliberately left out, and what the known limitations are.
- Specific questions or options where the user's input would steer the design
  (e.g. "Should X be eager or lazy?", "Do you want Y to support Z?").

**Do not continue to Phase 3 until the user gives explicit go-ahead.** If the
user requests changes, loop back to Phase 1 with the revised direction.

### Phase 3: Iterate to completion

Once the user approves the direction:

1. **Flesh out the implementation** — add edge-case handling, full docstrings,
   inline comments, and any remaining conventions from `CONTRIBUTING.md`.
2. **Extend tests** — invoke the `test-writer` agent again, now covering edge
   cases, failure modes, and security-relevant paths.
3. **Final review** — invoke the `code-reviewer` agent. Expect:
   - **PASS** — proceed to presenting the result.
   - **PASS WITH NOTES** — fix the noted items, then proceed.
   - **NEEDS CHANGES** — fix all blocking issues and re-invoke the reviewer.
4. **Update documentation/tutorial** as required.
5. **Present the final result to the user** for approval before committing.

### General principles
- **Prefer small increments over big-bang delivery.** If a feature is large,
  break it into multiple Phase 1→2→3 cycles rather than one huge cycle.
- **When in doubt, ask.** A quick clarifying question is always cheaper than
  reworking a wrong assumption.
- **Never gold-plate silently.** If you see an improvement opportunity beyond
  what was asked, mention it to the user instead of just doing it.

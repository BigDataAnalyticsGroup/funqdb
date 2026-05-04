Reproduce, fix, and regression-test a bug. Test-before-fix is the default; deviation requires explicit, written justification. UI bugs require Playwright screenshot verification.

**Style throughout:** be as concise and to the point as possible while remaining clear and unambiguous.

---

## Step 1: Clarify the bug

Before any code is read or written, get an unambiguous repro from the user:
- Symptom: what does the user observe vs. what should happen?
- Repro steps: minimal sequence to trigger it (FQL pipeline, schema setup, store operation, UI interaction, …)?
- Affected area: FDM core (`fdm/`), FQL operators (`fql/operators/`), predicates (`fql/predicates/`), plan IR (`fql/plan/`), store (`store/`), schema visualization / future UI, or somewhere else?
- Inputs: schema, sample data, exact pipeline expression, click/drag sequence — whatever is needed to drive the repro.
- For UI bugs explicitly ask: does the bug only appear **during** an interaction (drag, rotation, hover, key-hold)? This decides whether a mid-interaction screenshot is needed.

Ask targeted questions until the repro is concrete. Do not proceed on a guess.

## Step 2: Branch off main

If the user is currently on `main`, create a feature branch first (e.g. `fix/<slug>`) before any code is read or written — see CLAUDE.md → "Git workflow". Never investigate on `main` if the next step might lead to commits.

## Step 3: Locate the cause

Search the codebase for the suspect components and trace the data flow:
- Which file/function is most likely responsible?
- Are there recent commits in that area (`git log -p <file>`) that correlate?
- If external behaviour is involved (Python stdlib, SqliteDict, browser API, third-party library), invoke `[researcher]`.
- Re-read the relevant section of the EDBT 2026 paper if FDM/FQL semantics are in question — bugs that "look SQL-ish" are often correct under FDM (see CLAUDE.md → "Things to be careful about"). Do not "fix" them by reintroducing SQL/relational assumptions.

State a **hypothesis** about the root cause, not a guess. If you cannot form one, return to Step 1 and ask for more info.

## Step 4: Draft the fix plan

Keep it minimal:
- **Root cause** (one sentence)
- **Fix location**: exact file + function/line. Surgical changes (CLAUDE.md) — only the lines that the bug requires.
- **Alternatives considered** + why this one
- **Risks / side effects** on other features
- **Test strategy**: which test file, which assertions, which fixtures/data; for UI bugs whether screenshots are needed (and pre/mid/post pattern)

## Step 5: Challenge the plan

Invoke the `plan-challenger` agent on the draft. Loop until **READY TO PRESENT**.

## Step 6: Get user approval

Present the plan. Note explicitly that no code has been written. **Wait for explicit approval before continuing.**

## Step 7: Write the failing regression test

Place the test where it fits the existing layout (`tests/` mirrors `fdm/` / `fql/` / `store/`):

- **Non-UI bug**: e.g. `tests/fql/operators/test_bug_<slug>.py` (slug = short kebab-case from the symptom).
- **UI bug**: `tests/pw/test_bug_<slug>.py`. Use the `app_page` fixture from `tests/pw/conftest.py` (create the fixture if this is the first UI test in the project).

Conventions for the new test:

- Header docstring: symptom, repro steps, expected vs. observed.
- Drive the implementation through the `[test-writer]` agent with a clear brief.
- The assertion must target the **observable** failure (return value, raised exception, DOM attribute, computed style, position). Screenshots are evidence, not the assertion.
- **Mark the test for human review.** Add `@pytest.mark.needs_review_new` immediately before the `def`. See CLAUDE.md → "AI-generated test marking".
- **Add an end-of-line comment to every statement in the test body** explaining what it does or asserts and why — same section in CLAUDE.md.
- For UI bugs, capture screenshots into `tests/pw/screenshots/<slug>/`:
  - `pre.png` after setup, before the failing assertion.
  - For drag/rotate/hover bugs — capture **mid-interaction** by manually sequencing the mouse:
    ```python
    page.mouse.move(x1, y1)
    page.mouse.down()
    page.mouse.move(x2, y2, steps=10)
    page.screenshot(path="tests/pw/screenshots/<slug>/mid.png")
    page.mouse.up()
    ```

## Step 8: Test must be RED

Run only the new test:
```
poetry run pytest tests/<path>/test_bug_<slug>.py
```
- **Green → bug not reproduced.** Stop and return to Step 1; do not fix anything.
- **Red → continue.** If you write the test in a form that can only be exercised after the fix, document the deviation explicitly in the skill output (justification per the soft-TDD default).
- For UI bugs: open `pre.png` (and `mid.png` if applicable) via the Read tool and **describe in writing what you see** — this description is part of the skill output and proves the bug is visually present.

## Step 9: Implement the fix

Touch only the files named in Step 4. Surgical-changes rule from CLAUDE.md applies. Add type hints, docstrings, and inline comments per CONTRIBUTING.md / CLAUDE.md → "Conventions to follow". If a new source file is created, include the AGPL-3.0 header.

## Step 10: Test must be GREEN

Run the bug test again:
```
poetry run pytest tests/<path>/test_bug_<slug>.py
```
- **Red → fix is wrong or incomplete.** Return to Step 9; do not claim a fix.
- **Green → continue.**
- For UI bugs: regenerate `post.png` (and `mid.png` if applicable). Read both `pre.png` and `post.png` via the Read tool and write a side-by-side comparison: what was visible before, what is visible now, why the bug is gone. If the comparison is not unambiguous, return to Step 9.

## Step 11: Regression suite

```
poetry run pytest tests
```
- All tests must be green.
- If the change touches code with measurable coverage impact, also run coverage and confirm it has not regressed below the project target (CONTRIBUTING.md: ideally 100%, at least 90%). If the new code dropped coverage, ask `[test-writer]` to extend tests.

## Step 12: Code review

Invoke the `[code-reviewer]` agent on the changeset. One round suffices for a focused bugfix; loop on **NEEDS CHANGES**.

## Step 13: Commit and push

- Run `black` on all changed `.py` files (CLAUDE.md → "Git workflow"). CI rejects unformatted code.
- Do **not** add a new `[✅]` feature entry in `SPEC.md` for the bugfix — bugs are tracked via `git log --grep "^fix:"`. If the bug exposes a missed requirement or broken feature, add or update a `[⚠️]` / `[❌]` entry in `SPEC.md` instead.
- Commit message format:
  ```
  fix: <symptom> (<affected component>)

  Cause: <one sentence>
  Fix: <one sentence>
  Regression test: tests/<path>/test_bug_<slug>.py
  ```
- Push the feature branch — never push to `main` (CLAUDE.md → "Git workflow"). Open the MR and report the URL/identifier to the user; do not merge without explicit user approval.

Start iterative feature development for funqDB using the three-phase workflow below.
If the user provided a feature description or task, use it as the starting point for Phase 1.

---

## Phase 1: POC — get the core idea working

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

## Phase 2: Human checkpoint (mandatory)

**Stop and present the POC to the user before proceeding.** Include:

- A short summary of the approach taken and any design decisions made.
- What works, what is deliberately left out, and what the known limitations are.
- Specific questions or options where the user's input would steer the design
  (e.g. "Should X be eager or lazy?", "Do you want Y to support Z?").

Add a `[🔄]` entry for each new feature being developed to `SPEC.md`.

**Do not continue to Phase 3 until the user gives explicit go-ahead.** If the
user requests changes, loop back to Phase 1 with the revised direction.

## Phase 3: Iterate to completion

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
5. **Run `/finish-mr`** to format, run tests, commit, and push.

## General principles

- **Prefer small increments over big-bang delivery.** If a feature is large,
  break it into multiple Phase 1→2→3 cycles rather than one huge cycle.
- **When in doubt, ask.** A quick clarifying question is always cheaper than
  reworking a wrong assumption.
- **Never gold-plate silently.** If you see an improvement opportunity beyond
  what was asked, mention it to the user instead of just doing it.

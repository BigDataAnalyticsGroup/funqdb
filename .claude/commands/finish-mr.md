Finalize the current feature branch and prepare it for merge review.
Run each step in order; do not skip steps or combine rounds.

---

## Steps

1. **Code review — round 1**: Invoke the `code-reviewer` agent on the complete
   changeset of the current branch (all modified files).

2. **Address findings**: Fix every issue flagged in round 1 before continuing.

3. **Code review — round 2**: Invoke the `code-reviewer` agent again on the
   updated changeset. Round 2 catches issues that only surface after round-1
   fixes are in place.

4. **Update `SPEC.md`**: Advance `[🔄]` entries for completed features to `[✅]`.
   Add `[⚠️]` or `[❌]` entries for any gaps or regressions discovered during review.

5. **Format**: Run `black` on all changed `.py` files.

6. **Test suite**: Run `pytest tests` — all tests must pass.

7. **Commit and push**: Commit all changes with a descriptive message, then push
   the branch to the remote.

8. **Report**: Tell the user the MR is ready for their review, and summarise
   what was done (features added, tests written, review findings addressed).

## Notes

- Two reviewer rounds are mandatory, not optional. Do not report the MR as ready
  after only one round.
- If round 2 surfaces blocking issues, fix them and run round 2 again before
  committing.
- `black` must run after the final round of fixes, not before.

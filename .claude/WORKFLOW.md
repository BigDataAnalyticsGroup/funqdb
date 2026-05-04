# funqDB — Feature Development Workflow

> **Keep this diagram in sync with the skill and agent definitions.**
> Update it whenever `.claude/commands/` or `.claude/agents/` change.

```
   idea / feature request                     bug report
             │                                     │
             ▼                                     ▼
  ┌───────────────────────┐         ┌───────────────────────┐
  │        /design        │         │        /fix-bug       │
  ├───────────────────────┤         ├───────────────────────┤
  │ 1. clarify            │         │ 1. clarify repro      │
  │ 2. explore codebase   │         │ 2. branch off main    │
  │ 3. draft plan         │         │ 3. locate cause       │
  │    ├ tier 1: minimal  │         │    [researcher]?      │
  │    └ tier 2: options  │         │ 4. draft fix plan     │
  │ 4. [plan-challenger]  │◄─ loop  │ 5. [plan-challenger]  │◄─ loop
  │ 5. present to user    │         │ 6. user approval      │
  └───────────┬───────────┘         │ 7. [test-writer]      │
              │ USER APPROVAL       │    write RED test     │
              ▼                     │ 8. RED check          │
  ┌───────────────────────┐         │ 9. implement fix      │
  │        /develop       │         │10. GREEN check        │
  ├───────────────────────┤         │11. full pytest suite  │
  │ Phase 1 — POC         │         │12. [code-reviewer] *  │◄─ loop
  │   [researcher]?       │         │13. black + commit     │
  │   implement           │         │    + push (fix/<slug>)│
  │   [test-writer]       │  ← hp   └───────────┬───────────┘
  │   [code-reviewer] *   │                     │
  ├───────────────────────┤                     ▼
  │ Phase 2 — CHECKPOINT  │◄─ loop  ┌───────────────────────┐
  ├───────────────────────┤         │       MR ready ✓      │
  │ Phase 3 — iterate     │         └───────────────────────┘
  │   flesh out           │
  │   [test-writer]       │  ← edge cases
  │   [code-reviewer] *   │◄─ loop until PASS
  │   update docs         │
  └───────────┬───────────┘
              │
              ▼
  ┌───────────────────────┐
  │       /finish-mr      │
  ├───────────────────────┤
  │ [code-reviewer] * ×1  │
  │   fix findings        │
  │ [code-reviewer] * ×2  │
  │ black + pytest        │
  │ commit + push         │
  ├───────────────────────┤
  │       MR ready ✓      │
  └───────────────────────┘
```

## * [code-reviewer] — parallel execution

Each invocation of `[code-reviewer]` spawns four specialists simultaneously:

```
      ┌─────────────────────────────────────────┐
      │  [reviewer-correctness]  logic, FDM/FQL │
      │  [reviewer-readability]  docs, style    │──► synthesized verdict
      │  [reviewer-performance]  efficiency     │
      │  [reviewer-security]     OWASP, CVEs    │
      └─────────────────────────────────────────┘
```

Total review time = slowest specialist (not the sum of all four).

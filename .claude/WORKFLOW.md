# funqDB — Feature Development Workflow

> **Keep this diagram in sync with the skill and agent definitions.**
> Update it whenever `.claude/commands/` or `.claude/agents/` change.

```
        idea / feature request
                  │
                  ▼
      ┌───────────────────────┐
      │         /plan         │
      ├───────────────────────┤
      │ 1. clarify            │
      │ 2. explore codebase   │
      │ 3. draft plan         │
      │    ├ tier 1: minimal  │
      │    └ tier 2: options  │
      │ 4. [plan-challenger]  │◄─ loop until READY
      │ 5. present to user    │
      └───────────┬───────────┘
                  │ USER APPROVAL
                  ▼
      ┌───────────────────────┐
      │        /develop       │
      ├───────────────────────┤
      │ Phase 1 — POC         │
      │   [researcher]?       │
      │   implement           │
      │   [test-writer]       │  ← happy path
      │   [code-reviewer] *   │
      ├───────────────────────┤
      │ Phase 2 — CHECKPOINT  │◄─ loop until approved
      ├───────────────────────┤
      │ Phase 3 — iterate     │
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

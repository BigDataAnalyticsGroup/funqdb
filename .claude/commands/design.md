Design an implementation plan for the feature or change described by the user.
No code is written during this skill — planning only. Implementation starts with `/develop` after approval.

**Style throughout:** be as concise and to the point as possible while remaining clear and unambiguous.

---

## Step 1: Clarify requirements

Before exploring the codebase, resolve any ambiguity in the request:
- What is the exact intended behaviour? What are the inputs and outputs?
- Are there edge cases or constraints the user has in mind?
- Does this touch FDM/FQL concepts from the paper — if so, which ones?

Ask the user targeted questions if the request leaves room for multiple interpretations.
Do not proceed until you have a clear, shared understanding of what needs to be built.

## Step 2: Explore the codebase

Search the relevant parts of the codebase to understand the existing landscape:
- Which existing operators, functions, or classes are most related?
- Is there an existing pattern or abstraction that the new feature should follow or reuse?
- Which files will need to be created or modified?
- Are there tests that illustrate how similar features are exercised?

Focus on understanding what already exists so the plan can reuse it rather than reinvent it.

## Step 3: Draft the implementation plan

Structure the plan in two tiers:

**Tier 1 — Minimal solution**: the smallest implementation that correctly satisfies
the core request. This is what gets built by default.
- **Approach**: the core design decision and why it fits the FDM/FQL model
- **Files to modify / create**: list with a one-line note on what changes in each
- **Reuse**: existing functions, classes, or patterns to build on (with file paths)

**Tier 2 — Optional extensions**: independent additions that could follow in separate
MRs if the user wants them. List each as a named option with a one-line description
and an honest cost/benefit note. Do not include these in Tier 1.

Also note any **key tradeoffs** and what is explicitly **out of scope** for this iteration.

Keep the plan short enough to scan quickly, detailed enough to execute without guessing.

## Step 4: Challenge the plan

Invoke the `plan-challenger` agent on the draft plan. If it returns **NEEDS REVISION**,
address the concerns and re-invoke until the verdict is **READY TO PRESENT**.
Incorporate any open questions surfaced by the challenger into the user presentation.

## Step 5: Present and get approval

Present the plan to the user. Include:
- A summary of what you found in the codebase that is relevant
- The proposed approach and its rationale
- Any open questions or design decisions where user input is needed
- An explicit note that no code has been written yet

**Do not start implementing until the user explicitly approves the plan.**
Once approved, use `/develop` to execute it.

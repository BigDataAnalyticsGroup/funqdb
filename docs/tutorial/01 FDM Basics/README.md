# 01 FDM Basics

The Functional Data Model replaces tuples, relations, databases, and sets of
databases with a single concept: the **attribute function (AF)**.

### Core Concepts

| Topic | Description |
|:------|:------------|
| [Attribute Functions](Attribute%20Functions.md) | TF, RF, DBF, SDBF — the building blocks of FDM |
| [Accessing Data](Accessing%20Data.md) | Bracket, dot, call syntax and `__`-path traversal |
| [Computed Attribute Values](Computed%20Attribute%20Values.md) | `computed=` — derived values indistinguishable from stored ones |
| [Computed Attribute Functions](Computed%20Attribute%20Functions.md) | `default=` / `domain=` — AFs that generate values on the fly |
| [Frozen Attribute Functions](Frozen%20Attribute%20Functions.md) | Read-only AFs via `freeze()` / `unfreeze()` |
| [Rename](Rename.md) | `rename()` — rename keys inside AF values (ρ operator) |

### Schemas, Constraints, and Relationships

| Topic | Description |
|:------|:------------|
| [Schemas](Schemas.md) | Type definitions for AF values |
| [Constraints](Constraints.md) | Validation rules on AFs |
| [Composite Keys](Composite%20Keys.md) | CompositeForeignObject, RSF (N:M relationships), Tensor |
| [Observers](Observers.md) | Reactive notifications on AF mutations |

### Tooling

| Topic | Description |
|:------|:------------|
| [Visualization](Visualization.md) | Interactive schema graphs via Cytoscape.js |

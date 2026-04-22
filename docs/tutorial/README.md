# FDM and FQL Tutorial

Welcome to this tutorial! In this guide, we will walk you through the basics of using our FDM and FQL. For the code
examples, we will use Python syntax. But note, that all these concepts apply to other programming languages as well.
The ideas behind FDM and FQL are not bound to one particular programming language.

## Table of Contents

### [01 FDM Basics](<01 FDM Basics>)

The Functional Data Model — attribute functions as a uniform replacement for
tuples, relations, databases, and sets of databases.

- [Attribute Functions](<01 FDM Basics/Attribute Functions.md>) — TF, RF, DBF, SDBF
- [Accessing Data](<01 FDM Basics/Accessing Data.md>) — bracket, dot, call syntax
- [Computed Attribute Values](<01 FDM Basics/Computed Attribute Values.md>) — `computed=`
- [Computed Attribute Functions](<01 FDM Basics/Computed Attribute Functions.md>) — `default=` / `domain=`
- [Frozen Attribute Functions](<01 FDM Basics/Frozen Attribute Functions.md>) — read-only AFs
- [Rename](<01 FDM Basics/Rename.md>) — rename keys (ρ operator)
- [Schemas](<01 FDM Basics/Schemas.md>) & [Constraints](<01 FDM Basics/Constraints.md>)
- [Composite Keys](<01 FDM Basics/Composite Keys.md>) — RSF, Tensor
- [Observers](<01 FDM Basics/Observers.md>) — reactive notifications
- [Visualization](<01 FDM Basics/Visualization.md>) — interactive schema graphs

### [02 FQL](<02 FQL>)

The Functional Query Language — unary operators that transform AFs.

- **Filtering:** [filter](<02 FQL/filter.md>), [subset](<02 FQL/subset.md>)
- **Projection:** [project](<02 FQL/project.md>)
- **Ranking:** [rank_by](<02 FQL/rank.md>)
- **Transform:** [transform](<02 FQL/transform.md>)
- **Grouping:** [partition](<02 FQL/partition.md>), [group_by](<02 FQL/group_by.md>)
- **Aggregation:** [aggregate](<02 FQL/aggregate.md>), [disaggregate](<02 FQL/disaggregate.md>)
- **Set operations:** [union](<02 FQL/union.md>), [intersect / minus](<02 FQL/set_operations.md>)
- **Join:** [join](<02 FQL/join.md>), [flatten](<02 FQL/flatten.md>), [subdatabase](<02 FQL/subdatabase.md>)
- **Predicates:** [structured predicates](<02 FQL/predicates.md>)
- **Plan inspection:** [explain() and plan IR](<02 FQL/plan.md>)

### [03 FDM Store](<03 FDM Store>)

Persistence for attribute functions — SQLite-backed key/blob store with
automatic swizzling/unswizzling.

- [Store](<03 FDM Store/Store.md>) — register, load, and lazy-load AFs

### Podcast

[Listen to the podcast](https://bigdata.uni-saarland.de/publications/Replacing_SQL_Tables_With_Pure_Functions_01.mp3) — an English-language dialogue summarizing the core ideas of the FDM/FQL paper ([script](../podcast/podcast_script.md)). A good starting point before diving into the details above.

*Note: Both the podcast audio and script were generated with AI assistance.*

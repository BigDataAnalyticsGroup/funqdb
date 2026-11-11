# Benchmark Tools

This directory holds the SQL transpiler (`ir/`, `parse/`, `sql/`, `limitations.py`)
and the code that writes reports (`report/`, `shared/`).
See [report/README.md](report/README.md) for the reports. This file describes the transpiler.

## SQL Transpiler

The transpiler changes SQL into FQL. It shows which SQL FQL can already express.
It is not an SQL interface for funqDB.

So far, the transpiler consists of the IR, the form in which the transpiler holds one SQL statement,
and of the checks and rewrites that run on the SQL before the parser reads it.
A parser and a renderer are planned. The parser will make the IR from SQL with the library sqlglot.
The renderer will write the IR as FQL.


### Files

| Path                         | Purpose                                                                        |
|------------------------------|--------------------------------------------------------------------------------|
| `limitations.py`             | `Limitation`, the catalogue, and `UnsupportedSql`, the error that names one.   |
| `Limitations.md`             | Each limitation and why it exists; a test compares it with `limitations.py`.   |
| `sql/optimization.py`        | `factor_common_conjuncts`, and `OPTIMIZER_RULES`: sqlglot's rules, adapted.    |
| `parse/sql_baseline.py`      | `reject_non_baseline_sql`: an allow-list of the SQL the transpiler looks at.   |
| `parse/type_checks.py`       | `reject_ill_typed_sql`: rejects typed SQL that FQL would read otherwise.       |
| `parse/node_args.py`         | Helpers for sqlglot nodes: `arg_names`, `sql_conjuncts`.                       |
| `ir/statements.py`           | The IR of a statement: `Statement` and its subtypes.                           |
| `ir/query.py`                | The IR of a query: `Query`, `Output`, `OrderKey`, `Limit`, `GeneratedColumns`. |
| `ir/expressions.py`          | The IR of a value or condition: `Expression` and its subtypes.                 |
| `ir/join_equalities.py`      | The equalities between aliases: `Edge`, `ForeignKeyEquality`, `ReferenceRead`. |
| `ir/join_tree.py`            | `AliasGraph`, `JoinTree`, `Join`: see [below](#from-alias-graph-to-join-tree). |
| `ir/paths.py`                | `Path`: how a query reaches the tuple of an alias.                             |
| `ir/names.py`                | Names that the transpiler makes up: `fresh_name`, `as_python_name`.            |


### Stages

| Stage            | Code                                          | Result                                         |
|------------------|-----------------------------------------------|------------------------------------------------|
| SQL Parsing      | sqlglot                                       | An sqlglot AST.                                |
| Baseline check   | `parse/sql_baseline.py`                       | `UnsupportedSql` for SQL outside the baseline. |
| SQL Optimization | `sql/optimization.py`, `parse/type_checks.py` | A normalized AST.                              |

1. **SQL Parsing.** sqlglot reads the text in its default dialect (`SQL_READ_DIALECT`).
2. **Baseline check.** The baseline is ISO SQL-92, plus what the JOB and TPC-H queries use,
   plus OFFSET and BOOLEAN. All JOB queries and all TPC-H queries but Q13 pass.
   The check also rejects a name with a schema and an alias with a column list (as in Q13),
   because the later stages would ignore them.
3. **SQL Optimization.** `factor_common_conjuncts` moves conjuncts out of an OR,
   then sqlglot's rules normalize the SELECT, in the SQLite dialect, which resolves names case-insensitively.
   After `qualify`, every column names its alias.
   The rule `ill_typed_sql_rule` then runs the type checks on the statement as written,
   because later rules convert values.
   Other ill-typed SQL is not looked for. `OPTIMIZER_RULES` leaves out two of sqlglot's rules:
   - `unnest_subqueries` turns EXISTS and IN into a LEFT JOIN on a CTE, which do not translate.
     The IR holds the subquery instead.
   - `optimize_joins` raises a `Cycle error` when a subquery reuses an outer alias.

The planned parser will read the normalized AST into a `Statement` from `ir/`.

### The IR

The IR holds one statement in terms of FDM: it reads tuples along references, not rows of joined tables.
It holds no sqlglot node. The parts nest as follows:

```text
Statement
├── Query                 a SELECT
│   ├── JoinTree          the aliases, as a tree with at most one join
│   │   └── Path          how to reach the tuple of an alias from a root
│   └── Expression        conditions and values, which read a Column along a Path
├── Insert                the table, its key, its columns and its rows of Expressions
├── Update                a Query for the rows to change, and its Assignments
├── Delete                a Query for the rows to delete
├── CreateTable           the table and its key
└── DropTable             the table
```

**`Query`** (`ir/query.py`) holds one clause per field:

| Field                        | SQL clause or meaning                                                   |
|------------------------------|-------------------------------------------------------------------------|
| `tree`                       | The join tree. (see [below](#from-alias-graph-to-join-tree))            |
| `enclosing`                  | The query this subquery is nested in; `None` for the outermost.         |
| `stated_condition`           | The conjuncts of ON and WHERE that are no join equality.                |
| `outputs`                    | The SELECT list: each `Output` is a `Column`, shown under a name.       |
| `computed_columns`           | Values computed per tuple, such as `t.year + 1`.                        |
| `aggregates`                 | Aggregates, such as `SUM(t.year)`.                                      |
| `aggregate_computed_columns` | Values computed from aggregates, such as `SUM(t.year) + 1`.             |
| `group_keys`                 | GROUP BY, and the selected columns of a DISTINCT query.                 |
| `having_condition`           | HAVING.                                                                 |
| `regroups`                   | True for DISTINCT on a grouped query: its groups are grouped once more. |
| `order_keys`, `limit`        | ORDER BY, and LIMIT with OFFSET.                                        |

The property `condition` gives the tree's identities, then `stated_condition`.
The three `GeneratedColumns` fields give each expression a `Column` that the other clauses read by name.
An expression that a query uses twice gets one column.

**`Expression`** (`ir/expressions.py`) is one node of a value or a condition:

| Kind       | Nodes                                                                                                               |
|------------|---------------------------------------------------------------------------------------------------------------------|
| Reads      | `Column`: the attribute at the end of a `Path`, or an output by name when it has no path.                           |
| Constants  | `Literal` (`None` is NULL), `IntervalExpression`.                                                                   |
| Operators  | `UnaryExpression`, `BinaryExpression`, `ParenExpression`.                                                           |
| Tests      | `BetweenExpression`, `InExpression`, `LikeExpression`, `IsNullExpression`, `IsBooleanExpression`.                   |
| Functions  | `CaseWhenExpression`, `SubstringExpression`, `ExtractExpression`, `CastExpression`, `AggregateExpression`.          |
| Subqueries | `ExistsExpression`, `InSubqueryExpression`, `SubqueryExpression`: each holds a `Query`.                             |
| References | `TupleIdentityExpression`: two reads lead to one tuple. `ReferencedTupleExpression`: the tuple a written key names. |

A foreign key is no value in the IR. A `Column` with `is_reference_column` reads the reference under its `ref_key`,
and two references compare by the tuple they lead to (`is`, not `==`).

A correlated column keeps the `Path` of the enclosing query: `Path.depth` tells which query it reads.

### From alias graph to join tree

SQL states a join as equalities between aliases. FDM follows references from a tuple.
So `AliasGraph.tree()` changes the equalities into a tree, with at most one join.

#### Step 1: read the equalities

The parser (planned) will sort each condition of FROM, ON and WHERE:

| Condition                                | Result               | Meaning                                         |
|------------------------------------------|----------------------|-------------------------------------------------|
| A foreign key = the key of its target    | `Edge`               | The reference of `source` leads to `target`.    |
| A foreign key = a foreign key, one table | `ForeignKeyEquality` | The two references lead to one tuple.           |
| Any other condition                      | Stays a condition    | A filter on the query.                          |

An `AliasGraph` holds the edges and the foreign-key equalities. `AliasGraph.tree()` then does steps 2 to 6.

#### Step 2: attach the foreign-key equalities

Each `ForeignKeyEquality` becomes edges, so that the later steps read only edges:

- Both references already lead to one alias: nothing to add.
- One reference leads to an alias: the other reference gets an edge into that alias too.
- Neither does: both get an edge into a new provisional alias of the target table, such as `title_0`.

A provisional alias is a Python name: each character that Python does not allow in a name becomes `_`.
So the table `order details` gives `order_details_0`.

#### Step 3: break the cycles

A walk starts at the roots, or at the first alias in declaration order if no roots exist.
Then it starts at each alias that it did not reach. It removes each edge that closes a cycle.
The removed edge becomes an identity: the tuple at its end must be the tuple of the alias it leads back to.

```text
a ── r ──▶ b
▲          │
└─── s ────┘     keeps r, removes s

identity: a.r.s is a
```

This graph has no root, as every alias has an incoming edge.
So the walk starts at the first alias in declaration order, here `a`.
The removed edge was the last incoming edge of `a`, so `a` becomes the root of the tree.

#### Step 4: resolve the hubs

A hub is an alias with more than one incoming edge. In a tree, each alias keeps only one incoming edge per component.
The components are the groups of roots that are already connected.
The reach of a hub is the number of roots with a chain of edges to it. The hub with the largest reach comes first.
If two hubs have the same reach, the hub that comes first in topological order comes first.
Before a hub, its upstream hubs come first, in topological order.
A hub groups its edges by the first root that reaches each source.
So each upstream hub must have joined the other roots of that source before.

This order is a heuristic that aims at fewer join members. It counts roots, not the aliases on the chains to a hub.
So with long chains into the hub of the largest reach, another order can give fewer join members.

**Edges from one component.** The hub keeps the first edge in stated order. Each other edge becomes an identity.

```text
a ── r ──▶ b ── u ──▶ d          a ── r ──▶ b ── u ──▶ d
│                     ▲    ==>   │
└─ s ──▶ c ── v ──────┘          └─ s ──▶ c

identity: a.s.v is a.r.u
```

**Edges from several components.** The hub keeps one edge per component, and it joins the components.
This is the join of the query.

```text
mc ── movie ──▶ t ◀── movie ── mi       roots: mc, mi       join members: mc, mi, t
```

**A hub whose roots another hub joined.** Here `h` has a reach of 3 and `t` a reach of 2.
So `h` comes first and joins `s1`, `s2` and `s3`.
At `t`, the edges come from one component. So `t` keeps its first edge, and the other edge becomes an identity.

```text
s1 ── t_id ──▶ t ◀── t_id (removed) ── s2
│                                      │
│                                      ▼ h_id
└──────────── h_id ──────────────────▶ h ◀── h_id ── s3

join members: s1, s2, s3, h       identity: s2.t_id is s1.t_id
```

A provisional alias that joins nothing is removed. Its first edge gives no identity.
Each other edge becomes an identity with the first edge.
So `a.x = a.y` on one root removes two edges and gives one identity, `a.y is a.x`, without a new alias.
A provisional alias with a larger reach comes before a declared hub.
Then the declared hub can find one component and join nothing.

**Roots that no hub connects.** In `FROM a, b` without an edge, both stay roots and both are join members.
No edge is added, and a condition that reads both stays on the join tuples.

#### Step 5: assign the paths

Each root walks down its edges. Each alias gets one `Path` per root that reaches it,
for example `Path(0, "t", "mc", ("movie",))`.
A column reads along such a path: `t.year` is `mc.movie.year`. `path_of` gives the path from the first root.

#### Step 6: state the identities

Each removed edge gives at most one `TupleIdentityExpression`, in stated order (see step 4 for the exception).
An identity that is trivial or that repeats another is left out.
`Query.condition` puts the identities in front of the stated conjuncts.

#### The result

| `JoinTree` member | Meaning                                                                 |
|-------------------|-------------------------------------------------------------------------|
| `roots`           | The aliases that no edge leads to.                                      |
| `is_joined`       | True with two or more roots.                                            |
| `join_members`    | The roots, the hubs that join them and the aliases between them.        |
| `identities`      | The conditions that the removed edges left.                             |
| `path_of(alias)`  | How to read `alias`; `None` for an alias of another query.              |
| `join(condition)` | A `Join`, which splits `condition` over the roots; `None` for one root. |

`join` pushes each conjunct into a root that reaches all the aliases it reads, and rebases its paths on that root.
A root that answers more conjuncts comes first, then the earlier root.
A conjunct with a subquery stays on the join tuples.

`Join.dropped_edges` names the schema references between members that are no edge of the join,
so that the join does not follow them. It leaves out two kinds of references:
- a self-reference, as FQL never follows one;
- a reference into a root with a pushed conjunct, as the filtered root is a new relation that no reference leads to.

It is empty when a table has several members, as FQL refuses a relation entered under several names.

### Tests

`tests/benchmarks/tools/` has the same layout as this directory, with at most one test file per source file.
Modules of plain dataclasses, such as `ir/paths.py`, have none.
The section `## SQL transpiler` of `SPEC.md` names the tests of each feature.

### Limitations

SQL that the transpiler cannot translate raises `UnsupportedSql` with one `Limitation`.
[`Limitations.md`](Limitations.md) lists each limitation and why it exists.
The limitations are categorized as:
- `rejected`: SQL that is specifically rejected by the transpiler and is not intended to be transpiled.
- `limitation (transpiler)`: SQL that the transpiler cannot translate
  due to limitations in its implementation or design.
- `limitation (fql)`: SQL that cannot be expressed in FQL due to limitations in FQL or the functional data model itself.

Invalid SQL is catalogued, or raises `ValueError` or, for a name that does not resolve,
sqlglot's `OptimizeError`.

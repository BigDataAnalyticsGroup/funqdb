# Transpiler limitations

Index of every known limitation of the SQL → FQL transpiler. The source of truth is
`Limitation` in [`limitations.py`](limitations.py): the transpiler
raises it as `UnsupportedSql` (message `[<category>] <NAME>: …`).
`tests/benchmarks/tools/test_limitations.py` fails when this file and the enum
disagree, so **update both together**.

The limitations are grouped by the change that would lift them, so one group is one
piece of work:

- **rejected**: SQL FQL deliberately does not follow. Not open work. Invalid SQL is
  catalogued, or raises `ValueError` or, for a name that does not resolve, sqlglot's
  `OptimizeError`.
- **limitation (transpiler)**: open work in `parse/` and `render/` only. FQL/FDM stay
  unchanged. Each entry has a note on how to implement it.
- **limitation (fql)**: needs a change or a decision in FDM / FQL first. Only then can
  the transpiler follow.

## rejected

### R1 Query shapes without a translation

| Limitation                    | Why                                                                             |
|-------------------------------|---------------------------------------------------------------------------------|
| `NO_FROM`                     | No FROM table: nothing to translate.                                            |
| `AGGREGATE_MIXED_WITH_COLUMN` | Aggregate and plain column at one level (e.g. a group key added to `COUNT(*)`). |

### R2 Foreign keys are references, not values

FDM swizzles a foreign key into a reference to the related tuple.

| Limitation                            | Why                                                                     |
|---------------------------------------|-------------------------------------------------------------------------|
| `FOREIGN_KEY_AGAINST_SCALAR`          | A reference is not compared with a key value.                           |
| `FOREIGN_KEY_AS_VALUE`                | A reference is not computed with, aggregated, grouped or assigned.      |
| `FOREIGN_KEY_ORDERED`                 | References have no order.                                               |
| `FOREIGN_KEY_TO_NON_KEY`              | A reference only leads to the target table's key.                       |
| `FOREIGN_KEYS_COMPARED_AS_REFERENCES` | Keys into different tables never compare equal (SQL compares raw ids).  |
| `REFERENCE_NOT_ROW_COMPARABLE`        | A result attribute holding a reference is not comparable with SQL's id. |
| `REFERENTIAL_INTEGRITY`               | FDM refuses to delete a referenced tuple or to refer to a missing one.  |

### R3 No arbitrary rows: FQL relations are unordered, and cardinality is not assumed

| Limitation                | Why                                                               |
|---------------------------|-------------------------------------------------------------------|
| `LIMIT_WITHOUT_ORDER`     | Picks arbitrary rows.                                             |
| `OFFSET_WITHOUT_LIMIT`    | A subset keeps a counted number of ranked tuples.                 |
| `SCALAR_SUBQUERY_ROWS`    | Relies on the data holding at most one row.                       |
| `EXISTS_OVER_AGGREGATE`   | Always true.                                                      |
| `HAVING_WITHOUT_GROUP_BY` | FQL's ungrouped aggregate is one value, not a relation to filter. |

### R4 Relations are keyed mappings

| Limitation                      | Why                                                                                 |
|---------------------------------|-------------------------------------------------------------------------------------|
| `DUPLICATE_OUTPUT_NAME`         | A tuple holds one value per name.                                                   |
| `INSERT_WITHOUT_COLUMNS`        | A tuple is a mapping, not a position list.                                          |
| `TABLE_WITHOUT_PRIMARY_KEY`     | A relation stores its tuples under a key.                                           |
| `UPDATE_PRIMARY_KEY`            | The key names the tuple; changing it is no update.                                  |
| `COMPUTED_ALIAS_SHADOWS_COLUMN` | A computed key may not shadow a stored one; renaming around it would bloat the FQL. |

### R5 No FDM counterpart

| Limitation                  | Why                                                                                                      |
|-----------------------------|----------------------------------------------------------------------------------------------------------|
| `WILDCARD_WITH_EXPRESSIONS` | Without a schema the star is not expanded, so it cannot be combined with other outputs or with DISTINCT. |
| `UNSUPPORTED_STATEMENT`     | ALTER (FDM has no schema to alter), GRANT, REVOKE, COMMIT, ROLLBACK.                                     |
| `SCHEMA_QUALIFIED_TABLE`    | A dataset is one namespace of relations; it has no schemas.                                              |
| `EXTRACT_TIMEZONE`          | FDM dates carry no timezone.                                                                             |

### R6 Dialect-specific SQL, and SQL that Python reads otherwise

The baseline is ISO SQL-92, plus what the JOB and TPC-H queries use (WITH, `LIMIT`),
plus OFFSET and the BOOLEAN type and literals, accepted by decision. Anything else is
rejected. A literal count of `LIMIT` and OFFSET is a non-negative whole number, as
SQL:2008's FETCH FIRST requires. The type checks in `parse/type_checks.py` then reject
typed SQL that FQL would read otherwise without an error.

| Limitation              | Why                                                                                   |
|-------------------------|---------------------------------------------------------------------------------------|
| `DIALECT_SPECIFIC`      | Other dialects lack it or read it differently.                                        |
| `DATE_VS_TIMESTAMP`     | DuckDB widens DATE + INTERVAL to a TIMESTAMP; ISO SQL and FQL keep a DATE.            |
| `LITERAL_TYPE_COERCION` | Python would compare exact values where ISO SQL either rounds or truncates.           |
| `INEXACT_INTERVAL`      | Python drops an interval's fraction and a date's sub-day shift without an error.      |


## limitation (transpiler)

The groups are ordered by increasing scope: T1 is a local change to one parser or
renderer function, and T6 introduces new rendering patterns.

### T1 Scalar expressions — `parse/expression_parser.py` + `render/`

Local, self-contained additions to the expression IR and its renderer.

| Limitation               | SQL                                                                              | How to implement                                                                            |
|--------------------------|----------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| `SUBSTRING_BOUNDS`       | bounds that are no integer literals                                              | Render SQL-92's positions as a slice expression, e.g. `s[max(a - 1, 0):max(a - 1 + n, 0)]`. |
| `UNSUPPORTED_EXPRESSION` | SQL-92 kinds (COALESCE, UPPER, TRIM, …), `DEFAULT` in SET, a fractional INTERVAL | Add one IR node + render per function. Each new name must be bound in `FQL_RUNTIME`.        |

### T2 Type conversions — a conversion layer in `fql_runtime.py`

Needs small runtime helpers bound in the FQL sandbox, which has an empty
`__builtins__`, so every emitted name must be bound explicitly.

| Limitation              | SQL                                           | How to implement                                                                                                                                                                  |
|-------------------------|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `INEXACT_CAST`          | CAST to INT / DECIMAL / FLOAT / …             | Helpers with SQL semantics: round-half-away-from-zero for INT, `Decimal.quantize` for DECIMAL(p,s), float32 rounding for FLOAT.                                                   |
| `TEMPORAL_CAST`         | CAST of a non-temporal / other temporal value | Render `datetime.combine(d, time())` (DATE → TIMESTAMP), `ts.date()`, and `date.fromisoformat(text)` (text → DATE).                                                               |
| `TEMPORAL_LITERAL`      | SQL-92 literal Python misreads                | Unpadded fields (`'1995-3-1'`): parse the parts at transpile time and emit the ISO literal. TIME / time zone: no FDM type yet.                                                    |

### T3 Output shape — `parse/query_parser.py` + `render/query_renderer.py`

| Limitation          | SQL                | How to implement                                                                        |
|---------------------|--------------------|-----------------------------------------------------------------------------------------|
| `NON_LITERAL_LIMIT` | `LIMIT (SELECT …)` | The optimizer already folds constant expressions. Render the scalar subquery as `k`.    |

### T4 Subqueries and derived relations — bind a relation once, reference it by name

One shared mechanism lifts this whole group. Render a subquery that is not
correlated or a grouping CTE (the optimizer turns a derived table into one) as
its own statement (`_cte0 = <fql>`), then reference that name like the relation
of a table.

| Limitation                  | SQL                                             | How to implement                                                                                                                                                                            |
|-----------------------------|-------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ALIAS_COLUMN_LIST`         | `AS a(x, y)` on a table, derived table or CTE   | Before the optimizer, rewrite the list into aliases of the derived table's or CTE's select items, in order; for a table, take the column order from the dataset's SQL schema.               |
| `CTE_NOT_INLINABLE`         | grouping / aggregating / renaming CTE           | Bind the CTE's FQL once and treat the name as a relation with the CTE's outputs as attributes.                                                                                              |
| `SET_OPERATION`             | UNION / INTERSECT / EXCEPT [ALL], CORRESPONDING | Bind both sides, re-key each by its projected values (FQL's `union` / `intersect` / `minus` are key-based), then apply the operator. UNION ALL keeps duplicates, so it needs distinct keys. |
| `SELECT_SUBQUERY_RECURSION` | scalar subquery in the SELECT list              | Evaluate a subquery that is not correlated once (bind it), and render a correlated one with `add_computed_per_value` per tuple instead of `add_computed` on the relation.                   |
| `SUBQUERY_PER_ROW`          | correlated subquery evaluated per row           | Decorrelate: compute the subquery grouped by its correlation key once, then look up per row. Bind an uncorrelated one once.                                                                 |

### T5 Joins — `parse/join_tree_parser.py` + `ir/join_tree.py`

| Limitation   | SQL                                              | How to implement                                                                                                                                                                                                                                                                                                                           |
|--------------|--------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `OUTER_JOIN` | LEFT/RIGHT/FULL; NATURAL, no known shared column | Keep the preserved side as the root and attach the optional side per tuple (`add_computed_per_value` over a semijoin / subdatabase). Leave the optional side unmapped for a tuple without a match (F1). NATURAL: the optimizer expands it to the equalities of the shared columns; without known shared columns, it stays a cross product. |

### T6 Writes — `parse/statement_parser.py` + `render/statement_renderer.py`

| Limitation              | SQL                                                             | How to implement                                                                                                                                                                                                                      |
|-------------------------|-----------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `INSERT_FROM_SELECT`    | `INSERT … SELECT`                                               | Render the query, loop over its tuples, and assign a `TF` under each row's key.                                                                                                                                                       |
| `DML_MODIFIER`          | DROP … CASCADE / RESTRICT, CREATE GLOBAL TEMPORARY              | CASCADE: walk the schema's references; RESTRICT: refuse when referenced. GLOBAL TEMPORARY has no FDM notion (candidate for rejected).                                                                                                 |
| `UNENFORCED_CONSTRAINT` | CHECK, NOT NULL, UNIQUE, FOREIGN KEY, DEFAULT                   | Render constraints on the new RF: `ForeignValueConstraint` for foreign keys, and a values constraint for CHECK / NOT NULL. DEFAULT is filled in when an INSERT is rendered.                                                           |

## limitation (fql)

### F1 NULL semantics — SQL NULL vs. FDM absence

FDM decided on NULL in [`004-undefined-attribute-semantics`](../../docs/changes/004-undefined-attribute-semantics/DECISION.md):
a NULL is an unmapped attribute, and logic stays two-valued. A comparison over an
unmapped attribute is false, `Exists` / `Missing` test for it, aggregates skip it, and
`join` drops a tuple without its reference. The datasets leave every NULL unmapped.

| Limitation                | Why                                                                                                                                                                                                                                                                                                                                               |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `NULL_IN_PREDICATE`       | SQL drops a row whose predicate is unknown; FQL is two-valued: a comparison over an unmapped attribute is false and its negation true; a value read unguarded (an IN subquery's selected value, an IN list's column option, a reference) drops the row, or raises in an OPERATOR-style filter; `IS NULL` over a computed value is not translated. |
| `NULL_IN_AGGREGATE`       | Over no rows SQL counts no distinct values as 0 and computes a value from aggregates as NULL; FQL leaves a distinct count unmapped and raises on reading a missing aggregate.                                                                                                                                                                     |
| `NULL_IN_VALUE`           | SQL reads a value over NULL as NULL; FQL raises on reading an unmapped attribute in a computed value or an `UPDATE`, and stores `None` for a `CASE` without `ELSE`.                                                                                                                                                                               |
| `NULL_IN_GROUPING`        | SQL groups NULLs into one group; FQL's `partition` raises on an unmapped grouping key, and a filter lambda around it drops the row.                                                                                                                                                                                                               |
| `NULL_ORDERING`           | DuckDB sorts NULLs last; `rank_by` raises on an unmapped ordering key.                                                                                                                                                                                                                                                                              |
| `NULL_LITERAL_COMPARISON` | `= NULL` is never true in SQL; Python compares with `None`.                                                                                                                                                                                                                                                                                       |
| `NULL_FOREIGN_KEY_INSERT` | SQL inserts a row whose foreign key is NULL; FDM's `ForeignValueConstraint` raises on a tuple without the reference.                                                                                                                                                                                                                              |

### F2 `project()` and computed attributes

| Limitation                            | Why                                                                                                                                |
|---------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `PROJECT_DROPS_COMPUTED_SOURCE`       | `project()` drops the source attributes a computed column reads; reading them fails, or a definedness test reads them as unmapped. |
| `UNGROUPED_AGGREGATE_NOT_PROJECTABLE` | An ungrouped aggregate is a TF of scalars, which `project()` cannot take.                                                          |
| `PROJECTED_NAME_COLLISION`            | `project()` keeps the last path segment, so two columns named the same collapse before a rename.                                   |

### F3 The `join` operator

| Limitation                          | Why                                                                                                       |
|-------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `FILTERED_JOIN_READ_ONLY`           | A filter on the join tuples returns a read-only relation, so no computed column can be added after it.    |
| `CARTESIAN_PRODUCT`                 | Relations connected by no reference, or by theta conditions only.                                         |
| `ALIASED_RELATION`                  | `drop_reference` clones relations, splitting the aliases of one relation.                                 |

### F4 Data model

| Limitation            | Why                                             |
|-----------------------|-------------------------------------------------|
| `PATH_SEPARATOR_NAME` | `__` inside a name is read as a path separator. |
| `NON_TABLE_OBJECT`    | No FDM counterpart for views or schemas.        |

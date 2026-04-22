# 03 FQL

## What is an FQL Operator?

An FQL operator is a **pure function** that transforms one attribute function (AF) into another:

```output: AF = operator(input: AF)```

Every operator inherits from ```Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]``` (defined in
```fql/operators/APIs.py```). Every operator provides an ```explain()``` method that pretty-prints the full operator
subtree by delegating to ```to_plan().explain()``` — a single source of truth for plan representation.

## Two-Phase Usage Pattern

Operators follow a **construct-then-access** pattern:

1. **Construct** the operator with its input AF and parameters (predicate, partitioning function, output factory, etc.)
2. **Access** the result via ``.result`` (lazily computed on first access)

```python
# Phase 1: construct with input and parameters
op: Operator[RF, RF] = filter_items[RF, RF](
    users,
    lambda i: i.value.department.name == "Dev",
)

# Phase 2: access the result (triggers computation)
result: RF = op.result
```

This separation is intentional: the operator can be **chained** with other operators and
**explained** before execution via ``op.explain()``.

## Convenience API: where()

Attribute functions also provide a ```where()``` method for inline filtering without explicitly constructing an
operator:

```python
# lambda predicate:
users.where(lambda i: i.value.department.name == "Dev")

# Django ORM-style keyword syntax (equivalent):
users.where(department__name="Dev")

# with comparison lookups:
users.where(yob__gte=1980, yob__lte=2000)

# structured predicates (serializable, not opaque):
from fql.predicates import Eq, Gt, And
users.where(Eq("department.name", "Dev"))
users.where(And(Eq("department.name", "Dev"), Gt("yob", 1980)))
```

The ```__```-syntax serves double duty: it resolves **nested attributes** (```department__name``` traverses
```department.name```) and, when the last segment is a known lookup, applies a **comparison operator**
(```yob__gte``` means ```yob >= value```). Available lookups: ```exact```, ```lt```, ```lte```, ```gt```,
```gte```, ```in```, ```contains```, ```icontains```, ```startswith```, ```endswith```, ```isnull```, ```range```.

## Convenience API: project()

Attribute functions also provide a ```project()``` method for inline projection without explicitly constructing an
operator:

```python
# project to specific attributes:
users.project("name", "department")
```

This returns a new AF where each value is reduced to only the specified keys. Attributes not present in a value
are simply omitted (no error). The relational algebra alias ```π()``` is equivalent to ```project()```.

## Generic Operators vs Specialized Operators

Most FQL operators are **generic**: they work on any level of the AF type hierarchy (TF, RF, DBF, SDBF) with the
same semantics. For example, ```filter``` works the same way whether applied to a TF, RF, DBF, or SDBF — it always
returns a new instance of the same type containing only the qualifying items.

Some operators change the **type level** of the AF. For instance, ```partition``` maps an RF to a DBF (one level up),
while ```aggregate``` maps an RF to a TF (one level down). Inverse operator pairs exist:

| up one level (↑) | down one level (↓) |
|:------------------|:-------------------|
| partition         | union              |
| disaggregate      | aggregate          |

A few operators are **specialized** to a specific type level, most notably the [subdatabase](subdatabase.md)
operators which operate exclusively on DBFs (DBF → DBF).

## Operator Catalog

| Operator | Form | Description |
|:---------|:-----|:------------|
| [filter](filter.md) | AF → AF | Select items matching a local predicate |
| [project](project.md) | AF → AF | Retain only specified attributes per value |
| [subset](subset.md) | AF → AF | Select items matching a global condition (top-k) |
| [rank_by](rank.md) | AF → AF | FDM-faithful ORDER BY: produces a new AF with `ℕ`-key domain |
| [items_sorted_by](rank.md#items_sorted_by-af--iteratoritem) | AF → Iterator[Item] | Terminal sink for ordered consumption (presentation only) |
| [transform](transform.md) | AF → AF | Apply arbitrary transformation to an AF |
| [partition](partition.md) | AF → AF↑ | Split into partitions (inverse of union) |
| [group_by](group_by.md) | RF → DBF | Partition by attribute equality (SQL GROUP BY) |
| [group_by_aggregate](group_by.md#group_by_aggregate--group-then-aggregate-in-one-step) | RF → RF | Group then aggregate in one step |
| [union](union.md) | AF → AF↓ | Merge partitions (inverse of partition) |
| [intersect](set_operations.md#intersect) | DBF → AF | Keep items whose key is in all inputs |
| [minus](set_operations.md#minus--difference) | DBF → AF | Keep items whose key is only in first input |
| [aggregate](aggregate.md) | AF → AF↓ | Compute aggregates (inverse of disaggregate) |
| [disaggregate](disaggregate.md) | AF → AF↑ | Expand aggregates (inverse of aggregate) |
| [subdatabase](subdatabase.md) | DBF → DBF | Reduce a database to participating tuples |
| [add_reference / drop_reference / add_join_predicate / drop_join_predicate](constraints.md) | DBF → DBF | Assemble join specifications as constraints on a DBF |
| [join](join.md) | DBF → RF | Materialize the constraint-decorated DBF as an RF of nested per-row TFs (reference-based, minimal POC — see [scope](join.md#scope-and-deferred-follow-ups)) |
| [flatten](flatten.md) | RF → RF | Convert nested join rows into SQL-style flat TFs with dot-separated keys (`"relation.attribute"`) |

### Additional Topics

| Topic | Description |
|:------|:------------|
| [Structured predicates](predicates.md) | Serializable filter predicates (Eq, Gt, Like, In, And, Or, Not) |
| [Plan extraction](plan.md) | Inspect operator pipelines via `explain()` and `to_plan()` |
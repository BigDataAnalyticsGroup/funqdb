# 06 FQL Operators

## What is an FQL Operator?

An FQL operator is a **pure function** that transforms one attribute function (AF) into another:

```output: AF = operator(input: AF)```

Every operator inherits from ```Operator[INPUT_AttributeFunction, OUTPUT_AttributeFunction]``` (defined in
```fql/operators/APIs.py```), which itself extends ```PureFunction``` and ```Explainable```. This means every operator
is callable and can explain its behavior.

## Two-Phase Usage Pattern

Operators follow a **configure-then-apply** pattern:

1. **Construct** the operator with its parameters (predicate, partitioning function, output factory, etc.)
2. **Call** the operator instance on an input AF to produce the output AF

```python
# Phase 1: construct with parameters
op: Operator[RF, RF] = filter_items[RF, RF](
    filter_predicate=lambda i: i.value.department.name == "Dev",
)

# Phase 2: apply to input
result: RF = op(users)
```

This separation is intentional: the same operator instance can be **reused** on different inputs, **chained**
with other operators, and **explained** before execution.

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
```

The ```__```-syntax serves double duty: it resolves **nested attributes** (```department__name``` traverses
```department.name```) and, when the last segment is a known lookup, applies a **comparison operator**
(```yob__gte``` means ```yob >= value```). Available lookups: ```exact```, ```lt```, ```lte```, ```gt```,
```gte```, ```in```, ```contains```, ```icontains```, ```startswith```, ```endswith```, ```isnull```, ```range```.

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
| [subset](subset.md) | AF → AF | Select items matching a global condition (top-k) |
| [partition](partition.md) | AF → AF↑ | Split into partitions (inverse of union) |
| [union](union.md) | AF → AF↓ | Merge partitions (inverse of partition) |
| [aggregate](aggregate.md) | AF → AF↓ | Compute aggregates (inverse of disaggregate) |
| [disaggregate](disaggregate.md) | AF → AF↑ | Expand aggregates (inverse of aggregate) |
| [subdatabase](subdatabase.md) | DBF → DBF | Reduce a database to participating tuples |
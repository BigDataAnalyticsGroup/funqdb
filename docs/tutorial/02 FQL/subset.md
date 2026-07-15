## Subset (aka top-k)

> **See also: [`rank_by`](rank.md).** The `subset` operator's declarative
> top-k mode (`subset(ranking_key=…, k=…)`) is one of the canonical
> motivating cases for ordering. The newer [`rank_by`](rank.md) operator
> generalizes it: any top-k expressed via `subset(ranking_key=…, k=k)`
> can equivalently be expressed as `rank_by(...) | filter_keys(k < k_max)`.
> Both stay in the codebase — `subset` for the declarative one-shot form,
> `rank_by` when you want a ranked AF that you can compose further (e.g.
> for median, percentile lookups). For simple pagination (page N of size k)
> `subset(ranking_key=…, k=k, offset=n*k)` is the most direct expression;
> for multi-step pipelines that reuse the ranked AF, prefer `rank_by`.
> See [rank.md](rank.md) for the FDM-faithful framing.

### Generic Form: AF → AF

```output: AF = subset(input: AF)```

Computes a subset of the items contained in the ```input``` AF and returns a new ```output``` AF.
In contrast to [filter](filter.md), the subset operator computes a condition based on a **global** condition.
In other words:

> **filter operator**: uses a predicate phrased against the **individual** items in the input AF (or just the keys or
> just the values). Such predicate may be evaluated for each item independently. For instance, a condition like
> ```foo==42``` may be computed for each ```foo``` individually without influencing the outcome of other predicates.
>
> **vs**
>
> **subset operator**: uses a predicate phrased against **all** items in the input AF. Such predicate **cannot** be
> evaluated for each item independently. For instance, a condition like 'k-smallest items with respect to their foo
> value' cannot be evaluated independently: the outcome depends on the other items present in the input AF.

### Two modes of operation (mutually exclusive)

#### Mode 1 - Declarative top-k

Provide `ranking_key` and `k` (and optionally `reverse` and `offset`).

```python
from fdm.attribute_functions import RF, TF
from fql.operators.subsets import subset

users: RF = RF({
    1: TF({"name": "Horst", "yob": 1972}),
    2: TF({"name": "Tom",   "yob": 1983}),
    3: TF({"name": "John",  "yob": 2003}),
})

# k-smallest: youngest 2 users
top2 = subset(users, ranking_key=lambda i: i.value.yob, k=2).result
# → { 1: Horst(1972), 2: Tom(1983) }

# reverse=True: 2 oldest users
bottom2 = subset(users, ranking_key=lambda i: i.value.yob, k=2, reverse=True).result
# → { 3: John(2003), 2: Tom(1983) }
```

Internally: all items are sorted by `ranking_key`, then the slice
`sorted_items[offset : offset + k]` is returned.  Original keys are
preserved in the output AF.

##### Parameters (top-k mode)

| Parameter        | Type                    | Default | Description                                                                                       |
|:-----------------|:------------------------|:--------|:--------------------------------------------------------------------------------------------------|
| `ranking_key`    | `Callable[[Item], Any]` | -       | Maps each `Item` to a comparable value used for sorting.                                          |
| `k`              | `int`                   | -       | Number of items to keep. Must be ≥ 1.                                                             |
| `offset`         | `int`                   | `0`     | Number of items to skip from the start of the sorted list. Must be ≥ 0. Only valid in top-k mode. |
| `reverse`        | `bool`                  | `False` | If `True`, sort descending (largest first).                                                       |
| `output_factory` | `Callable`              | `None`  | Factory for the output AF. Defaults to `type(input_function)()`.                                  |

##### Pagination with `offset`

`offset` lets you retrieve a specific page of the sorted result without
materialising a ranked AF first:

```python
PAGE_SIZE = 2

# page 1: items 0..1 (positions 0-based)
page1 = subset(users, ranking_key=lambda i: i.value.yob, k=PAGE_SIZE, offset=0).result

# page 2: items 2..3
page2 = subset(users, ranking_key=lambda i: i.value.yob, k=PAGE_SIZE, offset=PAGE_SIZE).result
```

If `offset` is at or beyond the length of the AF, the result is an empty
AF (no error is raised - Python slice semantics). If `offset + k` exceeds
the length, fewer than `k` items are returned.

The convenience methods `AttributeFunction.top()` and `.bottom()` also
expose `offset`:

```python
page2_top = users.top(k=2, key=lambda i: i.value.yob, offset=2)
```

#### Mode 2 - Generic subset predicate

Provide `subset_predicate`: a callable that receives the entire input AF
and returns a new AF containing only the qualifying items. This covers
arbitrary global conditions that cannot be decomposed per item.

```python
def above_mean(af: RF) -> RF:
    all_yobs = [item.value.yob for item in af]
    mean_yob = sum(all_yobs) / len(all_yobs)
    return af.where(lambda item: item.value.yob > mean_yob)

result = subset(users, subset_predicate=above_mean).result
# mean of {1972, 1983, 2003} = 1986 → only John (2003) survives
```

`offset` is **not** available in predicate mode (pass it and a
`ValueError` is raised immediately).

##### Parameters (predicate mode)

| Parameter          | Type                              | Default | Description                                                      |
|:-------------------|:----------------------------------|:--------|:-----------------------------------------------------------------|
| `subset_predicate` | `Callable[[INPUT_AF], OUTPUT_AF]` | -       | Receives the full input AF, returns a subset AF.                 |
| `output_factory`   | `Callable`                        | `None`  | Factory for the output AF. Defaults to `type(input_function)()`. |

### Special cases

#### TF → TF

> Select the attributes to work on based on a global condition.

```output: TF = subset(input: TF)```

Computes a subset of the items (e.g. key/value-mappings) mapped to in the ```input``` TF.

*For instance*, this could be used to compute a subset of a tuple, i.e. 'give me the k-smallest items present in that
tuple w.r.t. the condition specified'.

#### RF → RF

> Select the tuples to work on based on a global condition.

```output: RF = subset(input: RF)```

Computes a subset of the items (e.g. key/TF-mappings) mapped to in the ```input``` RF. Semantically equivalent to a
top-k operator in extended relational algebra or simulating the same thing in SQL using ORDER BY and LIMIT.

Note that for k=1, this operation is equivalent to a classical min or max-aggregation (but not mean, avg, median, count
as they compute a new value that does not have to exist in the input RF, see [aggregate](aggregate.md)).

*For instance*, this could be used to compute the subset of tuples of a given relation, i.e. 'give me
the k-smallest tuples based on the condition specified'.

#### DBF → DBF

> Select the relations to work on based on a global condition.

```output: DBF = subset(input: DBF)```

Computes a subset of the items (e.g. key/RF-mappings) mapped to in the ```input``` DBF.

*For instance*, this could be used to compute the subset of relations of a given database, i.e. 'give
me the k-smallest relations based on the condition specified'.

#### SDBF → SDBF

> Select the databases to work on based on a global condition.

```output: SDBF = subset(input: SDBF)```

Computes a subset of the items (e.g. key/DBF-mappings) mapped to in the ```input``` SDBF.

*For instance*, this could be used to compute the subset of databases of a given set of databases,
i.e. 'give me the k-smallest databases based on the condition specified'.
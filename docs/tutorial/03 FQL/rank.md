## rank_by — and the FDM-faithful answer to ORDER BY

In FDM, an attribute function is a mathematical function `f: K → V`. Functions
have **no inherent ordering** over their domain — asking "what is the third
key of `f`?" is a category error in the model. SQL's `ORDER BY` therefore
cannot be expressed as an operation that *mutates* an AF or attaches order
as metadata. We need a different reduction.

FQL provides two operators that, together, cover everything `ORDER BY` is
used for in SQL, while staying faithful to the model:

| Operator | Form | Stays in the FQL algebra? | When to use |
|:---------|:-----|:--------------------------|:------------|
| [`rank_by`](#rank_by-af--af) | AF → AF | yes | When the next step is more FQL |
| [`items_sorted_by`](#items_sorted_by-af--iteratoritem) | AF → Iterator[Item] | no — terminal sink | When the next step is presentation (print, CSV, paginated UI) |

### rank_by: AF → AF

```output: AF = rank_by(input: AF, ranking_key=…, reverse=False)```

Produces a **new** AF whose key domain is `ℕ` (the natural numbers `0, 1, 2, …`)
and whose values are the values of `input` in user-defined ranked order. Order
is therefore encoded **inside the domain of the resulting function** rather
than as a property of the input AF — which is the only way to reconcile
"order by" with the FDM postulate that functions are unordered.

```python
from fdm.attribute_functions import RF, TF
from fql.operators.rank import rank_by

users: RF = RF({
    1: TF({"name": "Horst", "yob": 1972}),
    2: TF({"name": "Tom",   "yob": 1983}),
    3: TF({"name": "John",  "yob": 2003}),
})

ranked: RF = rank_by(users, ranking_key=lambda i: i.value.yob).result
# → RF { 0: <Horst>, 1: <Tom>, 2: <John> }
```

#### Parameters

- `ranking_key`: a callable mapping an `Item` (key + value) to a comparable
  value. Analogous to Python's `sorted(..., key=…)`.
- `reverse=True`: rank descending (largest first).
- `output_factory`: optional, defaults to `RF` because the result's key
  domain is always `ℕ` regardless of the input type.

#### Closure under FQL: top-k, pagination, median for free

Because the result of `rank_by` is *itself* an AF, it composes with every
other FQL operator that consumes an AF with a natural-number key domain.
The classic SQL ordering use cases reduce to one-liners:

```python
from fql.operators.filters import filter_keys

# top-2 (k smallest by yob)
top_k = filter_keys(
    rank_by(users, ranking_key=lambda i: i.value.yob),
    filter_predicate=lambda k: k < 2,
).result

# pagination: page 2 of size 10
page = filter_keys(
    rank_by(users, ranking_key=lambda i: i.value.yob),
    filter_predicate=lambda k: 10 <= k < 20,
).result

# median lookup
median = rank_by(users, ranking_key=lambda i: i.value.yob).result[len(users) // 2]
```

This is the *non-trivial* property that makes `rank_by` more than just
"call `sorted()` for me": the FQL algebra is **closed under ranking**, so a
pipeline never has to leave the model just to express order.

> **Note.** The existing [`subset`](subset.md) operator already provides
> a declarative top-k. `rank_by` generalizes it: any top-k expressed via
> `subset(ranking_key=…, k=…)` can equivalently be expressed as
> `rank_by(...) | filter_keys(k < k_max)`. Both stay in the codebase;
> use whichever reads more clearly at the call site.

#### Caveat: ranking replaces the original key domain

The original keys of `input` are **not** preserved in the result of
`rank_by`. If you need to keep them — e.g. to join back on a surrogate
user id later in the pipeline — `project` the original key into the
value first, so it travels along as part of the value type, *then*
`rank_by`. Otherwise the surrogate key is gone.

#### Stable tie-breaking

`rank_by` uses Python's built-in `sorted`, which is guaranteed stable.
Items with equal `ranking_key` therefore retain their input iteration
order in the result. If you need a deterministic total order even
across runs of an unordered store, fold a tiebreaker into `ranking_key`
itself (e.g. return a tuple `(yob, name)`).

### items_sorted_by: AF → Iterator[Item]

```iterator: Iterator[Item] = items_sorted_by(input: AF, key=…, reverse=False)```

The **terminal** counterpart to `rank_by`. Yields the original `Item`
instances of `input` (with their **original keys preserved**) in
user-defined sorted order. Returns a plain Python `Iterator[Item]`, not
an AF — and that is the entire point: `items_sorted_by` is the place
where a FQL pipeline deliberately **steps out** of the algebra, because
the next consumer is presentation, not querying.

```python
from fql.operators.rank import items_sorted_by

for item in items_sorted_by(users, key=lambda i: i.value.yob):
    print(item.key, "->", item.value.name, item.value.yob)
# 1 -> Horst 1972
# 2 -> Tom 1983
# 3 -> John 2003
```

Use `items_sorted_by` when the next step is a Python loop, a `print`,
a CSV writer, a paginated UI, etc. Use `rank_by` when the next step is
another FQL operator. The defining difference between the two is
**closure under the FQL algebra** (`rank_by` is closed; `items_sorted_by`
is not), not eagerness — both materialize the sort internally.

#### Why a free function and not a method on `AttributeFunction`?

Hiding `items_sorted_by` as `users.sorted_items(...)` would blur the
boundary between "this stays in the model" and "this leaves it". Keeping
it as a standalone function in `fql.operators.rank` makes the step out
of the algebra **explicit at every call site**.

#### Parameters

- `key`: a callable mapping an `Item` to a comparable value. Required;
  must be callable or a `TypeError` is raised eagerly.
- `reverse=True`: yield descending.

#### Important properties

- Original keys are preserved in the yielded `Item`s (in contrast to
  `rank_by`, which replaces them with rank integers).
- The input AF is not mutated.
- Tie-breaking is stable (Python's `sorted`).
- `input_function` is assumed to be finite — every AF backing in funqDB
  satisfies this.
- A `TypeError` from `sorted` itself (e.g. when `key` returns mutually
  non-comparable values) propagates from the `items_sorted_by` call.

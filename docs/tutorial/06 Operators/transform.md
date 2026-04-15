## Transform

Transform operators apply arbitrary functions to AFs, either to the AF as a
whole or item-by-item.

### transform — whole-AF transformation

Applies a function to the entire input AF and returns the result. This is the
most general operator: any computation that maps one AF to another can be
expressed as a transform.

```python
from fdm.attribute_functions import TF, RF
from fql.operators.transforms import transform

scores = RF({
    1: TF({"name": "Alice", "score": 90}),
    2: TF({"name": "Bob",   "score": 75}),
    3: TF({"name": "Carol", "score": 60}),
})

# Compute a summary TF from the entire RF:
summary = transform[RF, TF](
    scores,
    transformation_function=lambda rf: TF({
        "count": len(rf),
        "max_score": max(item.value.score for item in rf),
    }),
).result

summary.count      # → 3
summary.max_score  # → 90
```

### transform_items — per-item mapping

Maps a function over each item in the input AF. The function receives an
``Item(key, value)`` and returns a (possibly modified) ``Item``, or ``None``
to drop the item.

```python
from fql.operators.transforms import transform_items
from fql.util import Item

# Double every score:
doubled = transform_items[RF, RF](
    scores,
    transformation_function=lambda item: Item(item.key, TF({
        "name": item.value.name,
        "score": item.value.score * 2,
    })),
    output_factory=lambda _: RF(),
).result

doubled[1].score  # → 180
doubled[2].score  # → 150
```

### Relationship to other operators

- **vs [filter](filter.md)**: filter selects a subset; transform can reshape
  or compute entirely new values.
- **vs [project](project.md)**: project is a special case of transform that
  retains only specified attributes.
- **vs [aggregate](aggregate.md)**: aggregate reduces an AF to a lower-level
  AF; transform can produce any output type.

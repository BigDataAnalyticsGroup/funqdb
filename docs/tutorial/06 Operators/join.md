## Join Operators

> **Note:** The join operators are currently being reworked. The API and
> semantics described here may change. See also the
> [subdatabase](subdatabase.md) operator, which is the foundation for join
> processing in FQL and is likewise being revised.

FQL provides two join operators in ``fql/operators/joins.py``:

- **``join``** — generic nested-loop join with an arbitrary predicate
- **``equi_join``** — specialized hash-based equi-join with join index

Both take a DBF as input and produce a flattened output. Unlike SQL, FQL
separates the *reduction* step ([subdatabase](subdatabase.md)) from the
*flattening* step (join), giving the user explicit control over when and
whether to denormalize.

Documentation with full examples will be added once the rework is complete.

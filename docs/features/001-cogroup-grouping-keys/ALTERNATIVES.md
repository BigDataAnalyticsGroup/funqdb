# Rejected alternatives — 001 cogroup grouping keys

- **Uniform set-leaf in the default (key-based) mode too** — rejected: in key-based mode the grouping key *is* the
  identity key, unique per relation, so the set would always hold exactly one tuple. Wrapping every value in a
  singleton RF adds an information-free nesting level and changes the result shape of the one existing use for no gain.

- **Overwrite + warning on collision (like the `union` operator)** — rejected: silently loses earlier matches on n:m
  grouping, contradicting the user's explicit requirement to preserve duplicates and `group_by`'s loss-free behaviour.

- **Missing attribute → NULL-group / skip** — rejected: imports SQL NULL semantics the project deliberately avoids.
  Fail-fast with the natural lookup error instead.

- **Positional `*aggregate_keys`, one per input relation** (`cogroup(db, "dept", "id", ...)`) — rejected by the user:
  the relation→attribute mapping is positionally coupled to the DBF insertion order, so reordering the DBF silently
  shifts the mapping. The map form (relation-name → attribute) is self-documenting.

- **Uniform `str` / `tuple` shorthand applying to all relations** (`cogroup(db, "name", ...)`) — rejected: cannot
  express the heterogeneous-schema join (different attribute names per relation), which is the core motivation. The
  map form subsumes the same-name case (`{"users": "name", "customers": "name"}`) with one clear parameter shape.

- **Single low-level callable `partitioning_function`** (like `partition`) — rejected: the user thinks in terms of
  attribute names per relation; a raw callable is lower-level and less faithful to the `group_by`-style API.

- **Accept only an AF (no plain `dict`)** — rejected: both forms support `spec[relation_name]` indexing, so accepting
  both is free and more convenient; the AF form remains available for the idiomatic case.

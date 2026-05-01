# funqDB — Feature Specification

Living document. The gold standard for `[✅]` is a passing test.
Update this file whenever a feature is started, completed, broken, or gains/loses test coverage.

| Status | Meaning |
|--------|---------|
| `[✅]` | Implemented and covered by a passing test |
| `[🔄]` | Currently in development (ONGOING) |
| `[❌]` | Should work but is broken |
| `[⚠️]` | Exists in code but has no confirming test |

Inheritance note: capabilities listed under `DictionaryAttributeFunction` apply to all
its subtypes unless a subtype entry explicitly says otherwise.

---

## Functional Data Model (FDM)

### AttributeFunction / DictionaryAttributeFunction

- `[✅]` *Create from a dictionary of key→value pairs*  
  Initialize an AF by passing a dictionary to the constructor (`data=`). The resulting AF stores all key-value pairs and makes them accessible through indexing, attribute, and call-style syntax. The constructor also accepts parameters for optional features like schema, computed attributes, defaults, and domains.

- `[✅]` *Read, write, and delete values by key (dict-style, attribute-style, and call-style)*  
  Access values using bracket notation (`af[key]`), dot notation (`af.key`), or call syntax (`af(key)`). Assign values with `af[key] = value` or `af.key = value`. Delete entries with `del af[key]` or `del af.key`. All three syntaxes are identical access patterns delegating to the same underlying implementation.

- `[✅]` *Membership test, iteration over keys and values, length*  
  Test membership with `key in af`, which returns True if the key exists in stored data, computed attributes, or the active domain. Iterate over all items with `for item in af:`, yielding Item objects. Get the total count via `len(af)`. Enumerate keys with `af.keys()` and values with `af.values()`, both reflecting the complete union of stored, computed, and domain-backed sources.

- `[✅]` *Equality comparison between two AFs*  
  Two AFs are equal if they contain the same set of items (key-value pairs), checked by materializing all items from both AFs and comparing the sets. The comparison includes stored, computed, and domain-backed items, making two AFs equal regardless of how their values are sourced internally.

- `[✅]` *Deep copy with new UUID (`af.copy()`)*  
  Creates a new AF with identical data but a fresh UUID. Implemented as a shallow copy of the object followed by UUID reassignment — preserves stored data, computed attributes, defaults, and domains.

- `[⚠️]` *Shallow copy preserving ephemeral state (`copy.copy(af)`)*  
  `copy.copy(af)` is implemented via `__copy__` and returns a shallow copy that reuses the same UUID. No dedicated test exists for this behaviour; only the deep-copy path (`af.copy()`) is explicitly tested.

- `[✅]` *Filter items by predicate (`where()` / `𝛔`) — accepts callables and Predicate objects*  
  Call `af.where(predicate)` or `af.𝛔(predicate)` to filter items by a condition. Plain callables receive an Item object and return True/False. Structured Predicate objects are applied to item values. Also accepts Django ORM-style kwargs (e.g., `salary__gte=50000`) as conjunctive filters. Returns a new AF of the same type containing only matching items.

- `[✅]` *Project to a subset of keys (`project()` / `π`) — flat and path-based keys*  
  Call `af.project(*keys)` or `af.π(*keys)` to retain only specified attributes in each value. Keys can be flat (e.g., `"name"`) or dot-separated paths (e.g., `"department.name"`). Path keys traverse intermediate segments and store the final value under the last segment name. Missing paths are silently skipped. Returns a new AF of the same type with projected values.

- `[✅]` *Rename keys (`rename()` / `ρ`) — rejects path-based keys*  
  Call `af.rename(**kwargs)` or `af.ρ(**kwargs)` with `old_key=new_key` pairs to rename attributes inside each value. For example, `rf.rename(name="first_name")` renames the `"name"` key to `"first_name"` in each TF. Path-based keys are explicitly rejected with `ValueError`. Returns a new AF of the same type with renamed keys in values.

- `[✅]` *Freeze and unfreeze (makes AF read-only)*  
  Call `af.freeze()` to make the AF read-only, blocking all write operations. Call `af.unfreeze()` to restore writability. Check read-only status with `af.frozen()`. Frozen AFs reject write operations with a `ReadOnlyError`. Freezing is used internally by partition operators to mark output partitions as immutable.

- `[✅]` *Computed attributes (derived values with declared dependencies)*  
  Define derived attributes by passing a dict of callable factories at construction (`computed=`) or by calling `af.add_computed(key, func)`. The callable receives the AF itself as an argument and is re-evaluated on every access, reflecting the current state of stored attributes. Computed attributes are read-only, ephemeral (stripped on pickling), and cannot coexist with a stored key of the same name.

- `[✅]` *Default fallback function for missing keys*  
  Define a default function via the constructor (`default=`) or by calling `af.add_default(func)`. The callable receives the requested key and returns a value. For keys not found in stored data or computed attributes, the default generates values on the fly, enabling AFs with potentially infinite key domains. The default is ephemeral (stripped on pickling) and scoped by the active domain if one is set.

- `[✅]` *Active domain declaration for default-key iteration*  
  Define a finite set of keys via the constructor (`domain=`) or by calling `af.set_domain(iterable)`. The domain does not restrict stored or computed keys — it only declares the scope of the default function. When both `default` and `domain` are set, iteration, length, and key enumeration include domain keys that are absent from stored data and computed attributes.

- `[✅]` *Schema constraints (type checking per key)*  
  Pass a schema dict at construction (`schema=`) mapping attribute names to their expected Python types. The schema becomes a `Schema` constraint object that type-checks every write, rejecting values that do not match their declared type with a `ConstraintViolationError`. Schema AFs used as reference targets automatically establish bidirectional `ForeignValueConstraint` / `ReverseForeignObjectConstraint` pairs.

- `[✅]` *Values constraints (per-key value whitelist/validator)*  
  Add arbitrary per-value validation rules via `af.add_values_constraint(constraint)`, where the constraint is a callable that receives a value and a `ChangeEvent` and returns True if the value is valid. Multiple constraints are checked in sequence on every write. If a constraint fails, the write is rolled back and a `ConstraintViolationError` is raised.

- `[✅]` *Observer pattern — register, notify, and remove observers*  
  Register an observer with `af.add_observer(observer)` to be notified of all changes (add, update, delete). Remove with `af.remove_observer(observer)`. Observers implement `receive_notification(af, item, event)` where event is a `ChangeEvent`. When a nested AF value changes and is itself Observable, the parent AF's constraints are re-triggered, enabling cascade consistency checks.

- `[✅]` *Lazy loading of values from the Store by UUID reference*  
  When an AF is initialized with a `store=` parameter, values that are `AttributeFunctionSentinel` instances (UUID placeholders) are lazily loaded from the store on first access. The sentinel is replaced with the actual AF instance and cached for future accesses. This enables efficient large datasets where only accessed AFs are loaded into memory.

- `[✅]` *Lineage tracking (`get_lineage()`, `add_lineage()`)*  
  Each AF maintains a lineage list recording its derivation history. Call `af.get_lineage()` to retrieve the list as a string array. Call `af.add_lineage(entry)` to append a provenance entry describing how the AF was derived (e.g., from an operator result). Lineage is preserved on copy but stripped during pickling.

- `[✅]` *Top-k / bottom-k / random item retrieval*  
  Call `af.top(k, key=func)` to get a new AF containing the k items with the smallest key values according to a ranking function. Call `af.bottom(k, key=func)` for the k items with the largest values. Call `af.random_item()` to retrieve one item uniformly at random. All three return new AFs or individual items without modifying the original.

- `[✅]` *Human-readable string representation and HTML printing*  
  Call `str(af)` to get a human-readable string showing all key-value pairs in a nested tree format. Call `af.print(flat=False)` to print to stdout with optional indentation. HTML rendering for Jupyter notebooks is supported via the `.my_str()` method, enabling formatted display in notebook environments.

- `[✅]` *Pickle serialization (`__getstate__` / `__setstate__`)*  
  AFs can be pickled for storage and transmission. `__getstate__()` converts the AF to a picklable form by replacing AF-valued entries with UUID sentinels and stripping ephemeral state (computed, default, domain, observers, store reference). `__setstate__()` restores the pickled state. Callers must re-attach ephemeral features and re-register observers after deserialization.

### Subtypes

- `[✅]` ***TF** (Tuple Function) — represents a single row; attribute name → scalar*  
  TF is an AF subtype that maps attribute names (strings) to scalar values or nested AFs. It represents a single tuple or row and is the innermost value type in a relation hierarchy. TFs support all standard AF operations and can be nested as values in RFs.

- `[✅]` ***RF** (Relation Function) — represents a relation; key → TF*  
  RF is an AF subtype that maps keys (typically surrogate IDs like integers) to TF instances, representing a table or relation. Each value is a TF representing one row. RFs support all standard AF operations and can be nested as values in DBFs.

- `[✅]` ***DBF** (Database Function) — represents a database; relation name → RF*  
  DBF is an AF subtype that maps relation names (strings) to RF instances, representing a full database or schema. Each value is an RF representing one table. DBFs support all standard AF operations and serve as the primary input type for join-related operators.

- `[✅]` ***SDBF** (Set-of-Databases Function) — database name → DBF*  
  SDBF is an AF subtype that maps database names (strings) to DBF instances, enabling queries over multiple independent databases as first-class values. Unlike relational databases which restrict queries to a single schema, SDBFs allow simultaneous access to multiple DBFs in a single pipeline.

- `[✅]` ***RSF** (Relationship Function) — N:M relationships via CompositeForeignObject keys; `related_values()` traversal*  
  RSF is an AF subtype that uses `CompositeForeignObject` instances as keys, modeling N:M relationships between multiple AFs. Call `rsf.related_values(match_index, subkey, return_index)` to find all AFs at one position in the composite key that are paired with a specific AF at another position. For example, a meetings RSF keyed on `(user, customer)` allows retrieving all customers that a specific user has met.

- `[✅]` ***Schema** — type-map AF that validates key→type conformance; usable as constraint*  
  Schema is a special AF subtype that maps attribute names to their expected Python types. It doubles as an `AttributeFunctionConstraint`: when used as a schema parameter at AF construction, it type-checks every write and, if any schema values are AF instances (references), automatically establishes bidirectional `ForeignValueConstraint` / `ReverseForeignObjectConstraint` pairs for referential integrity.

- `[⚠️]` ***Tensor** — n-dimensional tensor with `rank()`; element-wise add tested, subtract/multiply/matmul untested*  
  Tensor is an AF subtype for n-dimensional arrays with composite keys representing coordinates. Call `tensor.rank()` to get the number of dimensions. Element-wise addition via `t1 + t2` is tested and working. Subtraction, multiplication, and matrix multiplication are implemented but not reliably tested, as the `"dimensions"` metadata key stored alongside data creates collisions in arithmetic operations.

### CompositeForeignObject

- `[✅]` *Composite key from multiple AFs (`subkey(index)`, `__contains__`, `__len__`, `__hash__`, `__eq__`)*  
  `CompositeForeignObject` wraps a tuple of AF references, serving as a composite key for RSFs and Tensors. Access individual subkeys with `cfo.subkey(index)`. Test membership with `af in cfo` to check if an AF is part of the composite. Hash and equality are based on AF UUIDs, making CFO instances usable as dictionary keys. Two CFOs are equal if they reference the same AFs (by identity/UUID) in the same order.

### Constraints

- `[✅]` ***ForeignValueConstraint** — validates that a referenced value exists in a target AF*  
  This constraint checks that a specific key's value in an AF exists as an item in a target AF. Established automatically by `af.references(key, target_af)`, it is triggered on every write to the referencing AF and raises `ConstraintViolationError` if the referenced value is not found in the target.

- `[✅]` ***ReverseForeignObjectConstraint** — blocks deletion when referencing AFs still hold references*  
  The converse of `ForeignValueConstraint`, added to the target AF by `references()`. It prevents deletion of a target AF's item if other AFs still reference it, raising `ConstraintViolationError` on DELETE events. Together with FVC, it establishes bidirectional referential integrity without schema-level declarations.

- `[✅]` ***JoinPredicate** — cross-relation predicate; evaluated lazily by the join operator*  
  `JoinPredicate` encodes an arbitrary callable spanning two or more named relations in a DBF, capturing classical SQL-style join conditions that cannot be expressed as object-identity references (e.g., `a.salary < b.budget`). It is never evaluated at DBF mutation time; the join operator evaluates it via `evaluate(tuples)` when assembling candidate tuple combinations, receiving a TF keyed by relation name.

- `[✅]` ***attribute_name_equivalence** — checks that AF keys match a given set*  
  This constraint validates that the AF contains exactly the keys in a specified set, no more and no fewer. It materializes all keys (stored, computed, and domain-backed) and compares the result to the expected set. Useful for enforcing strict schema conformance where additional attributes are not permitted.

- `[✅]` ***max_count** — checks that AF item count stays below a maximum*  
  This constraint validates that `len(af)` does not exceed a specified limit. Returns True if the total item count (stored, computed, and domain-backed) is at or below the maximum. Useful for enforcing cardinality bounds on relations (e.g., a user cannot have more than 10 addresses).

- `[✅]` ***in_subset** — checks that a value belongs to a whitelist*  
  This constraint validates that a value exists within a predefined whitelist set via membership testing. Useful for enforcing enumerations (e.g., `status` must be one of `{'active', 'inactive', 'pending'}`) on individual attribute values.

---

## FQL Operators

### Filter

- `[✅]` ***filter_values** — keep items whose value satisfies a predicate (callable or Predicate)*  
  Returns a new AF containing only items whose values pass a given predicate. The predicate receives the value directly (not the full item) and may be any callable or a structured `Predicate` object. The output AF is of the same type as the input.

- `[✅]` ***filter_keys** — keep items whose key satisfies a predicate*  
  Returns a new AF containing only items whose keys pass a given predicate. The predicate receives the key directly and may be any callable or a structured `Predicate` object. The output AF is of the same type as the input.

- `[✅]` ***filter_items** — keep items where the full (key, value) pair satisfies a predicate*  
  Returns a new AF containing only items where the full (key, value) pair passes a predicate. The predicate receives an `Item` object containing both `key` and `value` fields. Items are materialized before filtering to avoid iterator mutation. The output AF is of the same type as the input.

- `[✅]` ***filter_items_scan_complement** — inverse of filter_items (complement set)*  
  Returns a new AF containing all items that would be excluded by `filter_items` — that is, all items for which the predicate returns false. This is the set complement: if `filter_items(af, p)` keeps items where `p(item)` is true, then `filter_items_scan_complement(af, p)` keeps items where `p(item)` is false.

### Projection & Transformation

- `[✅]` ***project** — select named attributes; supports flat and path-based (dot-notation) keys*  
  Returns a new AF containing only the specified attributes. Supports flat keys (e.g., `"name"`) and dot-separated nested paths (e.g., `"department.name"`). For path keys, intermediate AF-valued attributes are traversed and the scalar value stored under the final segment. Missing keys are silently skipped.

- `[✅]` ***transform** — apply an arbitrary function to the whole AF*  
  Returns the result of applying a transformation function to the entire input AF. The function receives the AF as its sole argument and may return any type. No assumptions are made about the output structure, allowing arbitrary transformations including type changes.

- `[✅]` ***transform_items** — map a function over each (key, value) item*  
  Returns a new AF whose items are the results of mapping a function over each `Item` in the input. The mapping function receives an `Item` and returns a transformed `Item`; returning `None` drops the item. Items are materialized before mapping to avoid modification during iteration.

### Join

- `[✅]` ***join** — materialise a reference-based join over an acyclic RF graph into nested TF rows*  
  Accepts a DBF decorated with `ForeignValueConstraint` metadata and materializes all surviving tuple combinations as an RF of nested rows. Each row is a TF mapping relation names to their tuple values. Referenced tuples are shared by object identity across rows — two rows whose reference chain leads to the same target TF share the exact same instance, preserving zero-redundancy. The join internally runs Yannakakis reduction to ensure only tuples participating in the full join are included.

- `[✅]` *Supports trivial (single relation), linear chain, and star schema topologies*  
  The join operator correctly handles three reference graph shapes: a single isolated relation (trivial, no references), a linear chain where each relation references exactly one other, and a star schema where one central relation is referenced by multiple leaf relations. All three preserve the zero-redundancy contract through shared TF object identity.

- `[✅]` *Shares target TF objects by identity across rows*  
  When multiple rows reference the same target tuple through a `ForeignValueConstraint`, all those rows receive the same TF instance (verifiable with Python's `is` operator). This eliminates redundant copying and ensures that modifications to the shared instance propagate to all referencing rows.

- `[✅]` *Validates graph topology; rejects multi-source and disconnected graphs*  
  The join operator inspects the reference graph extracted from `ForeignValueConstraint` metadata and raises `NotImplementedError` with an explicit hint if the graph has multiple pure-source relations, isolated relations with no references, or multiple disconnected components. These cases are out of scope for the current implementation.

- `[⚠️]` *JoinPredicate pushdown during join (deferred)*  
  If the input DBF carries any `JoinPredicate` constraints, the join operator raises `NotImplementedError` rather than silently ignoring them. Predicate pushdown — applying user-supplied join conditions during the tuple combination phase — is deferred to a follow-up implementation.

- `[⚠️]` *Non-tree acyclic graphs / diamond references (deferred)*  
  The join operator assumes the reference graph forms a tree rooted at a single pure-source relation. If a relation is reachable via two or more distinct paths (a diamond pattern or non-tree acyclic graph), the operator raises `NotImplementedError`. Handling such graphs would require multiple accumulators per walk and is deferred to a follow-up implementation.

### Semijoin & Subdatabase

- `[✅]` ***semijoin** — reduce one RF to tuples that participate in a join; auto-detects reference direction*  
  Filters one relation in a DBF to keep only tuples that participate in a join with another relation, based on FDM references installed via `add_reference`. The reference direction (which relation holds the foreign key) is determined automatically by inspecting `ForeignValueConstraint` metadata. Returns a new DBF with the reduced relation swapped in; constraints are copied to enable chaining.

- `[✅]` ***subdatabase** — compute minimal subdatabase via Yannakakis semi-join reduction; extracts join graph from ForeignValueConstraints*  
  Accepts a DBF and returns a new DBF containing only tuples that participate in the full join across all relations, computed via the Yannakakis semi-join reduction algorithm. The join graph is extracted automatically from `ForeignValueConstraint` metadata. The reduction is expressed as a cascade of `semijoin` operators, making the computation inspectable via the plan IR. Only acyclic graphs are supported; input constraints are preserved in the output.

### Flatten

- `[✅]` ***flatten** — convert nested join output to flat SQL-style rows with dot-separated keys*  
  Transforms a nested RF (typically from `join` output) into flat SQL-style rows by recursively collecting all scalar leaves and assigning them dot-separated path keys. For example, a nested row `{users: TF({name: "Alice", dept: TF({name: "Dev"})})}` becomes `{users.name: "Alice", users.dept.name: "Dev"}`. Output rows use sequential integer keys starting at 0.

- `[✅]` *Supports arbitrary nesting depth and star schemas*  
  The flatten operator recursively walks arbitrarily deep nesting of AF-valued attributes, producing dot-separated keys of any length. It correctly handles star schemas where multiple relations reference the same leaf relation, creating multiple paths to the same leaf in the flattened output.

- `[✅]` *Materialises computed and domain-backed attributes as static values*  
  During flattening, computed attributes are evaluated and frozen as plain Python values in the output; domain-backed default attributes are included when the source TF carries a finite domain. No callables or lazy-evaluation wrappers are carried forward into the flattened rows.

- `[✅]` *Cycle detection raises `ValueError`*  
  The flatten operator tracks visited AF identities (by UUID) along each root-to-leaf path and raises `ValueError` if a reference cycle is detected. Only acyclic reference graphs are supported.

- `[⚠️]` *Attributes containing dots in their name cause key collisions (known limitation)*  
  If an attribute name itself contains a dot (e.g., `"user.name"` as a single key), the flattened output may produce key collisions with recursively built paths from other nested attributes. This is a known limitation of the dot-notation scheme.

### Aggregation

- `[✅]` ***aggregate** — apply named aggregation functions to an RF, returning a TF*  
  Accepts an RF and one or more named aggregation functions, applies each to the RF, and returns a frozen TF with keys matching the aggregation names and values being the scalar results. For example, `aggregate(users, count=Count("id"), avg_age=Avg("age"))` returns a TF with two entries.

- `[✅]` *Built-in aggregators: **Max**, **Min**, **Count**, **Sum**, **Avg/Mean**, **Median***  
  Each aggregator is a callable class constructed with a single attribute name. When applied to an RF, it extracts that attribute from every item and computes the corresponding aggregate (maximum, minimum, count, sum, arithmetic mean, or median). Median performs a stable sort and handles both odd and even-sized inputs correctly.

### Partition & Grouping

- `[✅]` ***partition** — split RF into DBF by a custom partitioning function; partitions are frozen*  
  Takes an RF and a user-supplied function that maps each `Item` to a partition key. Returns a DBF where each key is a distinct partition key and each value is a frozen RF containing all items that produced that key. The original RF is unmodified; each partition is frozen to prevent mutation after assignment.

- `[✅]` ***group_by** — partition RF by equality of one or more attributes*  
  Partitions an RF using one or more attribute names as grouping keys, automatically deriving a partitioning function that extracts those attributes from each item's value. When multiple attributes are provided, the partition key is a tuple of their values; for a single attribute, it is the value directly. Returns a frozen DBF with one frozen RF per distinct group.

- `[✅]` ***group_by_aggregate** — group_by followed by aggregate per group*  
  Combines grouping and aggregation: partitions an RF by one or more attributes using `group_by`, then applies aggregation functions to each resulting partition independently. Returns an RF with one TF per group, where each TF contains the group's aggregated results.

- `[✅]` ***partition_by_aggregate** — custom partition followed by aggregate per partition*  
  Partitions an RF using a custom partitioning function, then applies a custom aggregation function to each partition's RF. Returns an RF where keys are partition keys and values are the aggregated results. More general than `group_by_aggregate` — allows arbitrary partitioning logic and a custom scalar aggregation function.

### Ranking & Ordering

- `[✅]` ***rank_by** — rank items by a key function; output uses sequential integer keys (ℕ)*  
  Produces a new AF with consecutive integer keys 0, 1, 2, … encoding rank position, and values being the input AF's values sorted by a user-supplied key function. Original keys are replaced by natural-number indices. Ties are broken stably by input order, matching Python's `sorted()` semantics. Composable with `filter_keys` to produce top-k or paginated results.

- `[✅]` ***items_sorted_by** — yield items as a sorted Python iterator (presentation, not algebraic)*  
  Returns a plain Python iterator (not an AF) over the input's items sorted by a key function. Unlike `rank_by`, this is intentionally a terminal operator signaling "leaving the FQL algebra for presentation" (printing, CSV export, pagination). The iterator is backed by an eagerly materialized sorted list.

- `[✅]` ***subset** — top-k by ranking key or arbitrary predicate-based subset*  
  Extracts a global subset of an AF in two modes: declarative top-k (via `ranking_key` and `k`) keeps the k items with the smallest ranking-key values (or largest if `reverse=True`); generic mode (via `subset_predicate`) accepts a function that receives the entire AF and returns an arbitrary subset. The two modes are mutually exclusive.

### Set Operations

- `[✅]` ***union** / **V** — combine ≥2 AFs by keys; warns on duplicate key overwrite*  
  Merges ≥2 AFs (held in an input DBF) by combining all key-value pairs. If a key appears in multiple input AFs, the value from the last occurrence is retained and a warning is emitted. Returns a new AF with all keys from all inputs (latest-wins on duplicates).

- `[✅]` ***intersect** / **Ʌ** — keep keys present in all ≥2 AFs*  
  Returns a new AF containing only keys that appear in every input AF (held in an input DBF). Values are taken from the first input AF for each surviving key. Non-matching keys are silently discarded.

- `[✅]` ***minus** / **difference** — remove keys found in subsequent AFs*  
  Starts with the first input AF and removes any keys that appear in the second and subsequent AFs (held in an input DBF). Returns a new AF containing only keys from the first AF that are absent from all later AFs, preserving the first AF's values for surviving keys.

- `[✅]` ***cogroup** — group ≥2 AFs by shared key into a nested AF per key*  
  Takes ≥2 AFs (in an input DBF) and groups them by key. The output AF maps each distinct key to a nested AF whose keys are the input AF UUIDs and whose values are the originals from each input AF at that key. For each key, the nested AF shows which input AFs contained that key and their respective values.

### Reference Management

- `[✅]` ***add_reference** — add a foreign-key reference between two RFs in a DBF (installs FVC + RFOC)*  
  Adds a foreign-key relationship within a DBF by installing a `ForeignValueConstraint` on a source RF (keyed by a reference attribute) pointing to a target RF, and a symmetric `ReverseForeignObjectConstraint` on the target. The input DBF is cloned with fresh RFs so the original remains unchanged; the returned DBF is self-consistent with rebound intra-DBF references.

- `[✅]` ***drop_reference** — remove a foreign-key reference from a DBF*  
  Removes a foreign-key reference from a DBF by dropping the `ForeignValueConstraint` on the source RF and the matching `ReverseForeignObjectConstraint` on the target RF. The input DBF is cloned before modification. Raises `ValueError` if the source relation, target relation, or the reference itself does not exist.

- `[✅]` ***add_join_predicate** — register a cross-relation predicate on a DBF (evaluated lazily by join)*  
  Registers a `JoinPredicate` constraint on a DBF spanning one or more named relations, accepting either a plain callable (lambda) or a structured predicate (e.g., `Gt("users.age", Ref("departments.min_age"))`). The predicate is never evaluated during DBF mutations; it is held for lazy evaluation by the join operator when assembling tuple combinations.

- `[✅]` ***drop_join_predicate** — remove a join predicate by description, identity, or custom matcher*  
  Removes one or more `JoinPredicate`s from a DBF using one of three mutually exclusive modes: by description string (raises `ValueError` if not found), by predicate object identity (raises `ValueError` if not found), or by a custom callable matcher (silent no-op if no match, enabling idempotent drops). The input DBF is cloned and results frozen before return.

---

## FQL Predicates

### Comparison predicates (Eq, Gt, Lt, Gte, Lte)

- `[✅]` *Structured, serialisable predicates with callable interface (drop-in for lambdas)*  
  These five predicate classes implement the `Predicate` interface: they are callable (receiving an object and returning a boolean) and fully serialisable to a structured dict with `"type": "predicate"`, `"op"`, `"attr"`, and `"value"` fields. This allows filter operators to accept them wherever lambdas would normally go, while still exposing the comparison operation to the plan IR for inspection by downstream backends.

- `[✅]` *Support nested attribute paths (dot or dunder notation)*  
  The attribute path normalizes both dot notation (e.g., `"department.name"`) and double-underscore notation (e.g., `"department__name"`) to a common dot form, then traverses the attribute chain segment-by-segment, supporting arbitrary nesting depth on `DictionaryAttributeFunction` values.

- `[✅]` *Support attribute-to-attribute comparison via `Ref`*  
  If the `value` parameter is a `Ref` instance, the predicate resolves it against the same object as the left-hand side at evaluation time rather than using it as a literal. This enables comparisons like `Gt("end_year", Ref("start_year"))` that compare two attributes of the same object dynamically.

- `[✅]` *Serialise to plan IR dict; JSON round-trip*  
  Comparison predicates implement `to_dict()` returning a plain dict, and `Predicate.from_dict()` dispatches to the correct subclass for reconstruction. `Ref` instances are preserved through serialization. The plan IR handles JSON serialization and deserialization, supporting full round-trips through `json.dumps` / `json.loads`.

### Like

- `[✅]` *SQL-style `LIKE` with `%` wildcards (prefix, suffix, contains, exact)*  
  The `Like` predicate matches a string attribute value against a pattern supporting four forms: `"prefix%"` (startswith), `"%suffix"` (endswith), `"%contains%"` (substring), `"exact"` (no wildcards), and `"%"` (match everything). The attribute is resolved via the standard path mechanism and coerced to a string before matching.

- `[⚠️]` *Internal `%` patterns (e.g. `"H%st"`) not supported*  
  The pattern matching only recognizes `%` at the start and/or end of a pattern. Patterns with internal wildcards like `"H%st"` are treated as exact literal matches and will not behave as expected. This is a documented limitation.

### In

- `[✅]` *Membership test against a list, tuple, or set; JSON-compatible serialisation*  
  The `In` predicate resolves an attribute path and tests whether the resulting value is a member of a collection (list, tuple, or set). The `to_dict()` method coerces `values` to a list for JSON compatibility; a JSON round-trip always produces a list regardless of the original container type.

### Boolean combinators (And, Or, Not)

- `[✅]` *Compose predicates with logical conjunction, disjunction, and negation*  
  `And` takes ≥2 predicates and returns true only if all are satisfied; `Or` returns true if at least one is satisfied; `Not` negates a single predicate. Each is callable and composes arbitrary `Predicate` instances including other combinators.

- `[✅]` *Arbitrary nesting; serialisable and JSON round-trip*  
  Combinators recursively serialize their child predicates (e.g., `"and": [p1.to_dict(), p2.to_dict()]`) and deserialize by calling `Predicate.from_dict()` on each child. Deeply nested structures like `And(Or(Eq(...), Like(...)), Not(In(...)))` survive a full JSON round-trip.

### Ref

- `[✅]` *Sentinel for attribute-to-attribute comparisons; supports nested paths*  
  `Ref` wraps a dot- or dunder-separated attribute path and exposes a `resolve(obj)` method that extracts the attribute value from an object using the same path traversal logic as comparison predicates. It serializes to `{"type": "ref", "attr": ...}` and is reconstructed by `from_dict()`, enabling attribute-to-attribute comparisons to survive plan IR serialization.

---

## Plan / IR

### LogicalPlan & IR nodes

- `[✅]` *Extract an un-executed FQL operator pipeline to a `LogicalPlan` without running it*  
  The `extract(node)` function walks an `Operator` tree by reading the `input_function` attribute directly — never executing operators or triggering `.result` — and returns a `PlanChild` (`PlanNode` or `LeafRef`). The `extract_plan()` wrapper returns a `LogicalPlan` suitable for serialization and inspection.

- `[✅]` *Represent each operator as a `PlanNode` (operator name + parameters + input subplan)*  
  Each `PlanNode` stores the operator's class name as a string (e.g., `"filter_values"`), a tuple of serialized input subplans, and a mapping of keyword parameters passed to the operator. The operator name is decoupled from the Python class so the IR does not require the classes to be importable on the consuming side.

- `[✅]` *Represent AF leaves as `LeafRef` (UUID + class name + optional schema name)*  
  `LeafRef` is an immutable dataclass with the AF's integer UUID, unqualified class name (e.g., `"TF"`), and an optional `schema_name` for human readability (typically the key under which the AF appears in an enclosing DBF). The UUID is the canonical identifier; the other fields are informational.

- `[✅]` *Serialize non-serialisable values (lambdas, callables) as `Opaque` nodes with `repr` and `py_id`*  
  The parameter serializer detects callables (lambdas, functions, classes with `__call__`) and unknown types, wrapping them in `Opaque(reason="lambda"|"callable"|"unknown", repr=repr(value), py_id=id(value))`. This allows plan extraction to succeed even when the operator tree contains unserializable values, while marking them so a local executor can look up the original object by identity.

- `[✅]` *Structured predicates serialise inline; lambdas become Opaque*  
  Structured `Predicate` instances are detected during serialization and included as-is via their `to_dict()` method (appearing as `{"type": "predicate", "op": ...}` in the IR). Unstructured callables become `Opaque` markers. A plan can contain a mix of both.

- `[✅]` *Serialise to JSON string and deserialise back (`from_json` / `from_dict`)*  
  `LogicalPlan.to_json()` calls `to_dict()` then `json.dumps()` with optional indentation. `LogicalPlan.from_json(s)` calls `json.loads(s)` then `from_dict()`. Deserialization reconstructs `Opaque`, `Predicate`, `PlanNode`, and `LeafRef` instances from their dict representations, preserving the full tree structure.

- `[✅]` *Validate IR version on deserialisation*  
  An `IR_VERSION` constant is stamped into every serialized plan. On deserialization, `from_dict()` reads `"ir_version"` and raises `ValueError` if it does not match the expected version, preventing silent misinterpretation of future format changes.

- `[✅]` *Human-readable `explain()` output with indented operator tree*  
  `LogicalPlan.explain()` recursively walks the plan tree and emits indented lines for each node: `LeafRef` nodes appear as `"leaf <af_class> <schema_name or uuid>"` and `PlanNode` nodes as `"<op>(<param=value, ...>)"`. Parameters are abbreviated for single-line readability (e.g., `Opaque` becomes `"<opaque lambda>"`).

### JoinGraph

- `[✅]` *Build join graph from DBF via `ForeignValueConstraint` inspection*  
  `JoinGraph.from_dbf(dbf)` iterates the DBF's named RFs and scans each RF's constraints for `ForeignValueConstraint` instances. For each constraint whose target AF is also a named relation in the DBF, an edge is added recording the source RF, target RF, and the reference attribute name (`ref_key`).

- `[✅]` *Detect acyclicity (Kahn's topological sort)*  
  `check_acyclicity()` computes in-degree for all edge-participating nodes, seeds a queue with zero-in-degree nodes, and repeatedly dequeues nodes while decrementing target in-degrees. If fewer nodes are visited than participate in edges, a cycle exists and `ValueError` is raised.

- `[✅]` *Identify source nodes, isolated nodes, trivial graphs*  
  `pure_sources()` returns nodes with at least one outgoing edge and no incoming edges; `isolated_nodes()` returns nodes with neither; `is_trivial()` returns true iff the graph has no edges. These methods support operator policy decisions such as choosing error messages or branching on topology.

- `[✅]` *Auto-select or accept explicit root for join tree*  
  `select_root(root)` accepts a user-provided root name (asserting it exists in the graph) or auto-selects by finding a node with no incoming references. If no such node exists, the first node in insertion order is chosen as a fallback. The root is the starting point for join-tree construction.

- `[✅]` *Build Yannakakis semijoin cascade (bottom-up + top-down passes)*  
  `build_semijoin_cascade(root)` performs a BFS from the root to construct a rooted spanning tree, then generates two passes: a bottom-up post-order traversal (reducing targets by sources) followed by a top-down pre-order traversal (reducing sources by targets). Each step is a `SemijoinStep(reduce, by, ref_key)` consumed by the `semijoin` operator to progressively filter the DBF.

- `[⚠️]` *Individual graph query methods tested only via integration, not in isolation*  
  Methods such as `pure_sources()`, `isolated_nodes()`, `is_trivial()`, `sole_relation_name()`, `outgoing_adjacency()`, and `connected_components()` are exercised through integration tests of operators that depend on them (e.g., `subdatabase`, `join`), but do not have dedicated unit tests that exercise each method independently.

---

## Store

- `[✅]` *Persist AFs to SQLite (key/blob store via SqliteDict)*  
  The `Store` class wraps a `SqliteDict` instance using the AF's integer UUID as the key. AF instances are pickled and stored as blobs, allowing them to be persisted and reloaded across process boundaries. An `atexit` handler ensures the store is closed cleanly when the process exits.

- `[✅]` *Register AF for persistence; load AF by UUID*  
  `register(af)` stores the AF under its UUID in the SQLite dict and caches it in an in-memory buffer. `load(afid)` retrieves from the persistent store, injects a reference to the store into the AF's `__dict__`, and caches it. `get(afid)` returns a cached copy if available, otherwise calls `load()`.

- `[✅]` *Lazy loading — AF values are fetched from the store on first access*  
  `get()` checks an in-memory buffer first and only fetches from disk if the AF is not cached. This allows large datasets to be referenced without loading all AFs into memory upfront — only AFs that are actually accessed are fetched from the SQLite store.

- `[✅]` *Pickle serialisation for stored blobs*  
  `SqliteDict` transparently handles pickle serialization and deserialization of stored blobs. The `Store` does not call pickle directly; all serialization is delegated to `SqliteDict`. The `__getstate__` / `__setstate__` protocol on AFs (see FDM section) controls what is included in the pickled representation.

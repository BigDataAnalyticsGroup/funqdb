## To Do List

## High prio:

- [ ] we need set operators where we can define the identity of items to be used for the set operation; this
  is also broken in relational algebra and SQL, let's fix that, could in theory be different projection functions for
  different input AFs? Or would that be a separate rename step?
- [ ] window functions, partition by (technically only syntactic sugar anyway)

## Medium prio:

- [ ] report page (`benchmarks/tools/report/report.html`): add a browser test (Playwright or jsdom)
  that loads a report with a case that contains markup and asserts that the page shows it as text.
  The page already has `data-testid` attributes for this test.
- [ ] subqueries — FQL's functional composition already subsumes SQL subqueries
  (uncorrelated = variable assignment, correlated = lambda closures, scalar =
  transforms). Needs a tutorial page demonstrating each SQL pattern in FQL.
- [ ] double-check observer semantic in the presence ov .where() and .project(). I think observers are removed, but
  should not. Do not simply copy the AF as the id used for the store may then be doubled. The AE needs a copy
  constructor (DONE, but breaks some tests when used in where()).

- [ ] bug: `Tensor` element-wise arithmetic (`+`, `-`, `*`) is broken because `dimensions`
  is stored in the same data dict as tensor entries. `+` silently corrupts the result's
  dimensions via list concatenation; `-` and `*` raise `TypeError`. Fix: store `dimensions`
  outside the data dict, or skip it when iterating keys in arithmetic operators.
- [ ] logical index operator `index_by(rf, key_function)` (paper Sec 2.5):
  create a new RF organized by a different key, sharing the same TF instances.
  Different RFs mapping to the same TFs already work by design, but there is no
  convenience operator to create an "index RF" from an existing one.
- [ ] in-place FQL operators (paper Sec 4.3): INSERT/UPDATE/DELETE as composable
  FQL operators, not just `__setitem__`/`__delitem__`. The paper envisions
  in-place usage across the entire Table 1 landscape, not limited to RF→RF.
- [ ] undefined-attribute handling in `cogroup`: the grouping extraction in
  `set_operations.py` reads `item.value[attribute]` and still raises on a tuple
  that does not map it. `partition` / `group_by` are done (008: the tuple is
  dropped); `cogroup` should follow the same rule. Sibling of feature 004.
- [ ] Python treats `True`, `1` and `1.0` as equal dict keys, so a partition key
  function returning bools for some tuples and numbers for others merges those
  partitions (also with lambdas; noted in 008).
- [ ] `partition`: a partition key value containing `__` (e.g. `"a__b"`) is split
  as a path when written into the result DBF and raises `AttributeError`
  (pre-existing; noted in 008).
- [ ] flattening joins — follow-up MR to lift the two NotImplementedError
  paths that MR 2's minimal POC explicitly declines: (a) JoinPredicate
  pushdown during the join walk, firing predicates as soon as all their
  participating relations are in the accumulator; (b) multi-source
  reference graphs (e.g. JOB's `ci→t`, `mc→t`) and diamond-shaped
  acyclic graphs via a BFS-spanning-tree walk with an on-the-fly
  reverse index. Also: Cartesian fallback for multi-RF DBFs with zero
  references, if anyone ever asks for it.
- [ ] foreign object constraints through the store (similar problem as observers)
- [ ] transactions
- [ ] ordering/order by (does not make sense conceptually on a function, but of course we could create a sorted items
  stream of
  the contents of an AF)
- [ ] TPC-H and/or TPC-C queries in FQL; so far one JOB query is written in FQL [here](benchmarks/job/queries/SQL%20vs%20FQL.md)
- [ ] query optimization, in particular Yannakakis-style query processing and optimization
- [x] semijoin/subdatabase: should output RFs carry over constraints (ForeignValueConstraint etc.) from the input?
- [ ] `__setitem__` / `__delitem__` roll back only on `ConstraintViolationError`; a values-constraint that
  raises another exception (e.g. `ForeignValueConstraint` on a value missing the ref key raises `AttributeError`)
  leaves a half-applied write. Pre-existing; newly reachable via unfrozen `where()` results that now carry FKs.
- [ ] semijoin: auto-detect ref_key when there is only one ForeignValueConstraint between the two relations
- [ ] need to wrap access to ItemValues such that when an AF is accessed, it checks if it is loaded, otherwise loads it
  from the store

## Low prio

- [ ] provide operators working on a DB/store, i.e. by pushing down selections and projections, BSc-Thesis?
- [ ] allow pipelines to switch between in-memory and DB-backed AEs
- [ ] unpickling untrusted data is not secure and may lead to code execution vulnerabilities, so this must be done with
  care, maybe only allow loading from trusted sources
- [ ] https://docs.python.org/3/library/pickle.html#pickling-and-unpickling-normal-class-instances hmac?
- [ ] façades in other languages
- [ ] backends in other languages, e.g. Rust, C++, etc.
- [ ] other non-flat data like tensors
- [ ] continuous domain constraints (paper Sec 2.4): constrain an RF's domain
  to a continuous range like `[7, 12]`, not just discrete sets. Practical use
  depends on computed attribute functions being implemented first.
- [ ] Table 1 coverage gaps: purpose-built operators for DBF→TF (aggregate
  entire database to a tuple), DBF→RF (reduce each relation in a DBF to
  produce one RF), TF→RF (generate relation from specification tuple). The
  generic `transform` can cover these, but dedicated operators would be cleaner.
- [ ] SDBF operators (paper Table 1): the SDBF class exists but no
  SDBF-specific operators. Low priority — the paper itself defers these.
- [ ] relationship predicates (paper Sec 3, Def 4): a relationship function
  with Y_RF == bool, indicating whether a relationship exists. Currently
  expressible via regular predicates/lambdas.
- [ ] active domain: `set_domain()` checks `TypeError` (str guard) before
  `ReadOnlyError` (frozen guard) — opposite order from other mutating methods.
  Defensible (type errors are more fundamental), but inconsistent convention.
- [ ] active domain: `set(domain)` eagerly materializes the iterable, turning
  O(1)-space `range` objects into O(n) sets. Consider keeping `range` as-is
  and only converting general iterables to `set`.
- [ ] persist `computed`, `default`, and `domain` across pickling: currently
  stripped because standard `pickle` cannot serialize lambdas/closures.
  `cloudpickle` or `dill` could handle this, but **deserializing callables
  from untrusted sources is a remote code execution vector** — same concern
  as the existing pickle security TODOs above. Needs a trust/signing model
  before enabling.

# Other thoughts and ideas: prio unclear

- [ ] operator: output a plan:
    - [ ] PR 4: demo backend dispatcher that partitions a plan into a
      backend-executable prefix and a local residual at the first `Opaque`
      boundary.
    - [x] `filter_values` wraps the user predicate in an internal lambda
      (`filters.py:111`), so the extractor sees the wrapper, not the
      original predicate. Stash the original on the instance so PR 2 can
      serialize it. Done: stored as `_user_predicate` and exposed via
      `_plan_params` (also in `filter_keys`, `filter_items_scan_complement`).
    - [ ] `_value_{to,from}_dict` use a bare `"type"` discriminator; a user
      dict param whose keys include `"type"` with value in
      {`"leaf"`,`"node"`,`"opaque"`,`"literal"`,`"predicate"`,`"aggregation"`,
      `"expression"`}
      would be mis-rehydrated. Namespace to e.g. `"__funqdb_type__"`.
    - [ ] `Predicate.from_dict` raises `KeyError`/`TypeError` on malformed
      input instead of `ValueError`; validate like `AggregationFunction.from_dict`.
    - [ ] structured alternatives for the remaining genuine lambdas, so plans
      become SQL-translatable: case distinctions (`CASE WHEN`), string
      functions / concatenation, constant and nested `Record` fields, unary
      minus, join predicates (`add_join_predicate`). Ranking keys (007),
      partition keys (008), `transform_items` with `Record` + arithmetic (009)
      and `partition_by_aggregate` with keyword aggregates (010) are done;
      each new consumer of an expression must define its rule for
      `UNDEFINED`. Whole-AF `transform` stays opaque by design (general escape
      hatch; `aggregate` / `project` / `transform_items` are its structured
      alternatives).
    - [ ] ranking by several keys with a direction per key
      (`ORDER BY a, b DESC`), on top of `fql.expressions`.
- [ ] reject ambiguous attribute paths with a run of three or more underscores
  (`"a___b"` is read as `a` -> `_b` today) in `_split_attr_path` and `.where()`
  path splitting alike; affects predicates, aggregates and `Attr` (007).
- [ ] `aggregate`, `group_by_aggregate`, `partition_by_aggregate`: reject non-aggregation
  values given as keyword aggregates (e.g. `big=Gt("salary", 75)` or `Attr(...)`):
  a predicate is evaluated on the partition RF and silently yields `False`, yet the
  plan shows it as structured (found in the 010 review; same gap in all three).
- [ ] `group_by` / `partition` over tuple-valued keys (a reference such as `"customer"`, or a
  joined relation such as `"O"`): raises `TypeError: unhashable type: 'TF'` because TFs define
  `__eq__` but no `__hash__`. Reproduction:
  `c = TF({"n": 1}); group_by(RF({1: TF({"customer": c}), 2: TF({"customer": c})}), "customer").result`.
  Decide identity vs. content grouping there; content grouping would make `AggregateBox` edges
  unsound. Blocks executing every `AggregateBox` that groups by tuples (011).
- [ ] `AggregateBox` follow-ups (011, see its DECISION.md): derive boxes from FQL pipelines
  (`LogicalPlan` -> `JoinGraph`; likely needs a node kind pointing at a plan subtree instead of a
  live RF); derive box edges automatically from reference-valued grouping keys; execute a box
  (`group_by_aggregate(join(...))`, with grouping keys carried as result attributes and `join`
  accepting a `JoinGraph` / edge list instead of a DBF); aggregation without fan-out over the
  flattened join; render nested boxes (ASCII / `funqdb-viz`); serialize the graph; edges with join
  predicates; subqueries / correlated edges across box boundaries; edges through a grouping key of
  an inner box; selection predicates on nodes / boxes (WHERE / HAVING); key-preserving `project`
  as a node annotation.
- [ ] `repr()` of an `RF` raises `TypeError: __repr__ returned non-string (type ABCMeta)`, so
  any `JoinGraph` / `AggregateBox` repr fails too (unreadable pytest failure output).
- [ ] `fql/plan/join_graph.py` lacks the AGPL header and a module docstring.
- [ ] promote the definedness check onto the FDM type (`tf.defined(path)` /
  `tf.get(path, default)`), deferred in 004 until a third consumer appears;
  with `Attr` (007) there are three (predicates, aggregates, expressions).
  Should also make "is the key present?" and "read the key" agree: today an AF
  with a default value function but no declared domain returns a value for
  `af[key]` while `key in af` is `False`, so predicates and `Attr` treat it as
  absent although a lambda reads the default.
- [ ] observer semantics for AFs not in the store, maybe queue or load, when queuing it may break semantics, e.g. for
  other AFs in main memory that should be informed but are not as part of the observer chain is not in main memory


- [ ] maybe:
    1. the first get() to an item in the AE triggers the actual computation
    2. get the lineage(aka the logical plan of the AE)
    3. ...
    4. that determines at the same time the root of the computation

---

### DONE
- [x] SQL-style flat join results (`flatten` operator, MR 3 of the join-rework):
  standalone `flatten` class in `fql/operators/flatten.py`. Accepts any RF
  whose row values are nested TFs (typically the output of `join`) and emits
  a new RF of flat TFs with dot-separated `"relation.attribute"` keys.
  Recursively expands AF-valued attributes to arbitrary depth; computed
  attributes are materialised at flatten time; reference cycles raise
  `ValueError` via a visited-set guard. Key-collision behaviour (last write
  wins) is documented. The FDM-native nested shape from `join` is unchanged
  and remains the default.
- [x] flattening `join` operator (MR 2 of the join-rework): minimal POC
  in `fql/operators/joins.py`. Consumes a constraint-decorated DBF,
  runs `subdatabase` for Yannakakis reduction, walks outgoing
  `ForeignValueConstraint` edges from the unique pure-source relation,
  and emits an RF of **nested** per-row TFs of the form
  `TF({relation_name: relation_tf, …})` — no value denormalization;
  relation TFs are shared across rows by object identity. Explicit
  scope limits (JoinPredicate pushdown, multi-source / non-tree
  graphs, Cartesian over edgeless multi-RF DBFs) raise
  `NotImplementedError` and are tracked as a follow-up above.
- [x] dead `from docutils.nodes import target` import at
  `fql/plan/join_graph.py:6` — removed alongside MR 2.
- [x] constraint operators (MR 1 of the join rework): four specialized
  operators in `fql/operators/constraints.py` — `add_reference` /
  `drop_reference` for `ForeignValueConstraint`s and `add_join_predicate` /
  `drop_join_predicate` for the new `JoinPredicate` (`fdm/schema.py`).
  Users assemble a DBF whose constraints fully describe a join — references
  and arbitrary cross-RF predicates alike. Join predicates accept both
  lambdas and structured predicates from `fql.predicates.predicates` (e.g.
  `Gt("a.x", Ref("b.y"))`); the constraint wraps its tuples dict in a TF at
  evaluate time so `getattr`-path predicates just work. The forthcoming
  `join` operator (MR 2) will consume that DBF and flatten it.
- [x] computed attribute functions (paper Sec 2.6): any AF can generate values
  on the fly for unstored keys via the `default=` fallback function on
  `DictionaryAttributeFunction`. Covers potentially infinite domains.
- [x] computed attribute values (paper Sec 2.3): any AF can return computed values
  (e.g. `salary = 1000 * t1('age')`), indistinguishable from stored attributes.
  Implemented via `computed=` constructor parameter and `add_computed()` method
  on `DictionaryAttributeFunction`.
- [x] schema definitions for larger examples
- [x] looking up relationship functions, e.g. set of related items for a given item, e.g. all items that are related to
  item X through relationship function Y
- [x] bug: semijoin's `_find_ref_direction` matched constraints by ref_key only and silently
  picked the first hit, so two relations carrying the same ref_key (e.g. `"next"` on every hop
  of a chain) were reduced in the wrong direction — fixed: `SemijoinStep` carries the edge's
  source, `subdatabase` passes `source=`, and the auto-detection raises when it is ambiguous.
- [x] bug: `filter_items_scan_complement` (`filters.py`) had `lambda x: not filter_predicate`
  instead of `lambda x: not filter_predicate(x)` — fixed, test now covers actual filtering.
- [x] bug: `RSF.related_values(subkey_index, subkey)` filters and returns at the **same**
  index — it returns the matched AF back, not the related AF at another position. The
  docstring example (`returns all customers that have a meeting with user1`) is misleading.
  Fixed: method now accepts separate `match_index` and `return_index` parameters.
- [x] tighten `Operator` input typing: the `input_function: INPUT_AttributeFunction`
  parameter in the operator subclasses (`filter_items`, `filter_values`, …)
  silently accepts another `Operator` at runtime (via `_resolve_input`), but
  the static type annotation does not reflect that. Fixed: introduced
  `OperatorInput[T] = T | Operator[Any, T]` type alias in `APIs.py` and
  propagated to all operator subclasses.
- [x] operator: output a plan — PR 1 landed: `fql/plan/` + `Operator.to_plan()`
  walks the un-executed operator tree into a serializable `LogicalPlan`
  (LeafRef/PlanNode/Opaque) without triggering `_compute`. JSON roundtrip
  works; lambdas become `Opaque` markers.
    - [x] PR 2: structured predicates (Eq/Gt/Like/In/And/Or/Not) so that
      filter/join predicates are no longer forced to be opaque lambdas;
      needed for any real backend dispatcher.
    - [x] 006: no accidental `Opaque` — factories omitted from plans,
      `project`/`group_by` expose their attributes, built-in aggregation
      functions serialize (`IR_VERSION` 2).
- [x] sync docu and tutorial for new operators
- [x] PR 3: consolidate existing per-operator `explain()` strings to be
      derived from `to_plan()` so there is a single source of truth.
- [x] pipelining
- [x] some schema/constraint visualization, i.e. through .references(), graphviz, vue.js?
- [x] top-k/limit queries, in a single operator! parameters are k and the ranking attribute(s); this is a variant of a
  transform operator, i.e. the input RF is mapped to a new RF containing only the top-k elements
- [x] rename: rename keys of an AF, e.g. rename the key "name" to "first_name", etc.; really required? could als be a
  method of AFs
- [x] sync docu and tutorial for `where()` and `project()`
- [x] allow all FQL operators to be called via the constructor directly or via the additional __call__: in general both
  syntaxes should be possible
- [x] other "__"-syntax for filters, e.g. in-equality, <, <=, etc. where() maybe better in a filter operator being
  called from where()
- [x] union
- [x] intersect
- [x] minus/difference/except on AF's keys ("except" not possible due to name clash with reserved Python keyword)
- [x] minus/difference/except on AF's values to simulate RA/SQL
- [x] co-group operator
- [x] maybe a projection operator for AFs that allows to specify the output schema, e.g. by renaming attributes, or even
  computing new attributes based on the existing ones, e.g. by applying a function to them
- [x] maybe a special projection method for AFs: project() and 𝜋()
- [x] n:m relationships
- [x] add support for composite primary keys, low prio
- [x] relationship functions
- [x] unit test for group_by_aggregate, clean-up and unify tests for grouping and aggregation
- [x] restructure group-by, partitioning, and aggregation operators
- [x] FIX: filter values vs filter items vs filter keys
- [x] fix reverse fks
- [x] simpler additional filter syntax on attribute functions where()
- [x] basic "__"-syntax for filters, e.g. equality
- [x] directly specify query graph

---

### Discarded

- [ ] view()-method for afs, handy for all kinds of nesting? NOT required in FDM
- [ ] ~~get rid of `__call__` in operators and integrate into `__init__`~~
  No, actually good to keep it apart. Other option: make these classes functions instead of classes. Everything in one
  call.


### Github

- [x] mirroring from gitlab
- [x] license: AGPL
- [x] add license header to every file
- [x] write decent README.md, see template in the wiki

### POC

- [x] provide an AF backed by a DB/store, SqliteDict looks like a good start, see `test_sqlitedict`
  yet as it is used as a key/blob-store, we then cannot push down query processing.

some thoughts on this:

- [x] each af should have a globally unique identifier (uuid or simpler, must only be unique within the instance)?
- [x] we need an object store that can organize mappings from that id to the pickled object/blob where all
  references to other AF were swizzled to their unique id
- [x] user should not see this stuff, this is internal
- user creates AFS: TF: RF: DBF, whatever, puts them together in operators, builds pipelines, etc.
- [x] the store saves that to disk/db
- [x] when loading back, the store reconstructs the AFs from the blobs, re-linking the references, however link
  traversal
  should be done on demand, i.e. once an AF is needed from an Item, only then it is unpickled and its references are
  re-linked
- [x] it requires to swizzle/un-swizzle references to other AFs when pickling/unpickling

- maybe start with a POC using ~~SqliteDict~~ as the backing store that maps from uuid to pickled blob
- start even simpler:

1. [x] use a dict as the backing store, i.e. in-memory object store
2. [x] implement serialization/pickling of AFs with swizzling/un-swizzling of references to other AFs
3. [x] connect SqliteDict as the backing store

## To Do List

## High prio:


- [ ] we need set operators where we can define the identity of items to be used for the set operation; this
  is also broken in relational algebra and SQL, let's fix that, could in theory be different projection functions for
  different input AFs? Or would that be a separate rename step?

- [ ] window functions, partition by (technically only syntactic sugar anyway)
- [ ] subqueries

## Medium prio:

- [ ] double-check observer semantic in the presence ov .where() and .project(). I think observers are removed, but
  should not. Do not simply copy the AF as the id used for the store may then be doubled. The AE needs a copy
  constructor (DONE, but breaks some tests when used in where()).

- [ ] bug: `filter_items_scan_complement` (`filters.py`) has `lambda x: not filter_predicate`
  instead of `lambda x: not filter_predicate(x)` — evaluates truthiness of the
  function object (always `True`), so the complement is always empty. The existing
  test only checks `explain()`, not filtering behavior, so the bug is masked.
- [ ] tighten `Operator` input typing: the `input_function: INPUT_AttributeFunction`
  parameter in the operator subclasses (`filter_items`, `filter_values`, …)
  silently accepts another `Operator` at runtime (via `_resolve_input`), but
  the static type annotation does not reflect that. Type checkers therefore
  flag every nested pipeline like `filter_items(filter_values(af, ...), ...)`
  as a type error. Fix direction: change the annotation to
  `INPUT_AttributeFunction | Operator[Any, INPUT_AttributeFunction]` in the
  base class and propagate to subclasses. Surfaced by the IDE while adding
  type hints to `tests/fql/plan/test_extract.py`.
- [ ] flattening joins (revisit: the ones in the code base are outdated)
- [ ] foreign object constraints through the store (similar problem as observers)
- [ ] transactions
- [ ] ordering/order by (does not make sense conceptually on a function, but of course we could create a sorted items
  stream of
  the contents of an AF)
- [ ] TPC-H and/or TPC-C queries in FQL, one JOB query exists [here](benchmarks/job/queries/SQL%20vs%20FQL.md)
- [ ] query optimization, in particular Yannakakis-style query processing and optimization
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

# Other thoughts and ideas: prio unclear

- [ ] looking up relationship functions, e.g. set of related items for a given item, e.g. all items that are related to
  item X through relationship function Y
- [x] operator: output a plan, how?  PR 1 landed: `fql/plan/` + `Operator.to_plan()`
  walks the un-executed operator tree into a serializable `LogicalPlan`
  (LeafRef/PlanNode/Opaque) without triggering `_compute`. JSON roundtrip
  works; lambdas become `Opaque` markers.
    - [ ] PR 2: structured predicates (Eq/Gt/Like/In/And/Or/Not) so that
      filter/join predicates are no longer forced to be opaque lambdas;
      needed for any real backend dispatcher.
    - [ ] PR 3: consolidate existing per-operator `explain()` strings to be
      derived from `to_plan()` so there is a single source of truth.
    - [ ] PR 4: demo backend dispatcher that partitions a plan into a
      backend-executable prefix and a local residual at the first `Opaque`
      boundary.
    - [ ] `filter_values` wraps the user predicate in an internal lambda
      (`filters.py:111`), so the extractor sees the wrapper, not the
      original predicate. Stash the original on the instance so PR 2 can
      serialize it.
    - [ ] `_value_{to,from}_dict` use a bare `"type"` discriminator; a user
      dict param whose keys include `"type"` with value in
      {`"leaf"`,`"node"`,`"opaque"`,`"literal"`} would be mis-rehydrated.
      Namespace to e.g. `"__funqdb_type__"`.
    - [ ] alternative / future work: tainting mechanism via the AF being
      passed through and collecting information along the pipeline — search
      for "lineage" in the codebase for the earlier start.
- [ ] observer semantics for AFs not in the store, maybe queue or load, when queuing it may break semantics, e.g. for
  other AFs in main memory that should be informed but are not as part of the observer chain is not in main memory


- [ ] maybe:
    1. the first get() to an item in the AE triggers the actual computation
    2. get the lineage(aka the logical plan of the AE)
    3. ...
    4. that determines at the same time the root of the computation

---

### DONE
- [ ] sync docu and tutorial for new operators
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

---

DONE:

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

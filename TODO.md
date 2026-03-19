## To Do List

## High prio:

- [ ] allow all FQL operators to be called via the constructor directly or via the additional __call__
- [x] union
- [x] intersect
- [x] minus/difference/except (except not possible due to name clash with reserved keyword)
- [ ] we need set operators where we can define the identity of items to be used for the set operation; this
is also broken in relational algebra and SQL, let's fix that
- [ ] rename
- [ ] window functions, partition by (technically only syntactic sugar anyway)
- [ ] subqueries

## Medium prio:

- [ ] double-check observer semantic in the presence ov .where() and .project(). I think observers are removed, but
  should not. Do not simply copy the AF as the id used for the store may then be doubled. The AE needs a copy
  constructor (DONE, but breaks some tests when used in where()).
- [ ] some schema/constraint visualization, i.e. through .references(), graphviz, vue.js?
- [ ] full-fledged subdatabase operator (revisit: the ones in the code base are outdated)
- [ ] flattening joins (revisit: the ones in the code base are outdated)
- [ ] foreign object constraints through the store (similar problem as observers)
- [ ] other "__"-syntax for filters, e.g. in-equality, <, <=, etc. where() maybe better in a filter operator being
  called from where()
- [ ] transactions
- [ ] ordering/order by (does not make sense conceptually on a function, but of course we could create a sorted items
  stream of
  the contents of an AF)
- [ ] top-k queries, in a single operator! parameters are k and the ranking attribute(s); this is a variant of a
  transform
  operator, i.e. the input RF is mapped to a new RF containing only the top-k elements
- [ ] TPC-H and/or TPC-C queries in FQL, one JOB query exists [here](benchmarks/job/queries/SQL%20vs%20FQL.md)
- [ ] pipelining
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
- [ ] operator: output a plan, how?
    - [ ] as everything is functions and the input to an operator is not another operator
      -> explain must traverse through the call chain including attribute functions!
    - [ ] maybe through a tainting mechanism, i.e. make the AF being passed through and let it collect information on
      the way
      through the pipeline!  STARTED: search for "lineage" in the codebase
- [ ] observer semantics for AFs not in the store, maybe queue or load, when queuing it may break semantics, e.g. for
  other AFs in main memory that should be informed but are not as part of the observer chain is not in main memory


- [ ] maybe:
    1. the first get() to an item in the AE triggers the actual computation
    2. get the lineage(aka the logical plan of the AE)
    3. ...
    4. that determines at the same time the root of the computation

---

### DONE

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

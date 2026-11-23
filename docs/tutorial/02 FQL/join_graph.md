## Join Graph, Filters and Views

> **Note:** Filters and views are a **logical representation** only. They describe a query; nothing executes them
> yet (see *Limitations* below).

### The join graph

In a DBF, relations are connected by **references**: a tuple holds another tuple as an attribute value
(`order.customer is alice`), declared with `.references(ref_key, target)` or [`add_reference`](constraints.md). The
**join graph** makes this structure visible:

- one **node** per relation of the DBF,
- one directed **edge** per reference, from the relation whose tuples hold the reference to the relation they point
  at. The edge `O -customer-> C` reads: every tuple of `O` holds, in its attribute `customer`, a tuple of `C`.

The [join](join.md) and [subdatabase](subdatabase.md) operators build this graph from their input DBF and read their
join structure from it. You can build it yourself to look at a DBF's structure, and to describe a query on it with
filters and views (below).

#### The running example

All examples on this page use a small sales schema: line items belong to orders, orders to customers, customers to
nations, and support tickets also point at customers.

```
   L (lineitems) --order--> O (orders) --customer--> C (customers) --nation--> N (nations)
                                                         ^
                                     T (tickets) --customer--+
```

```python
from fdm.attribute_functions import TF, RF, DBF
from fdm.schema import JoinPredicate
from fql.operators.aggregates import Count, Max, Sum
from fql.plan.join_graph import JoinGraph, View
from fql.predicates.predicates import Eq, Gt, Ref

nations = RF({"de": TF({"name": "Germany"})}, frozen=False)
customers = RF({"c1": TF({"name": "Alice", "signup": "2025-05-01", "nation": nations["de"]}),
                "c2": TF({"name": "Bob", "signup": "2026-02-01", "nation": nations["de"]})},
               frozen=False).references("nation", nations)
orders = RF({"o1": TF({"date": "2026-10-01", "customer": customers["c1"]})},
            frozen=False).references("customer", customers)
lineitems = RF({"l1": TF({"price": 20, "order": orders["o1"]}),
                "l2": TF({"price": 5, "order": orders["o1"]})},
               frozen=False).references("order", orders)
tickets = RF({"t1": TF({"topic": "refund", "customer": customers["c2"]})},
             frozen=False).references("customer", customers)
for rf in (nations, customers, orders, lineitems, tickets):
    rf.freeze()   # a relation can only be the target of .references(...) while it is not frozen yet
```

#### Building and inspecting the graph

```python
def edge_labels(graph: JoinGraph) -> list[str]:
    """The graph's edges as readable labels "source -ref_key-> target"."""
    return [f"{e.source.name} -{e.ref_key}-> {e.target.name}" for e in graph.edges]

graph = JoinGraph.from_dbf(DBF({"L": lineitems, "O": orders, "C": customers, "N": nations}, frozen=True))

assert list(graph) == ["L", "O", "C", "N"]   # node names, in DBF order
assert len(graph) == 4
assert edge_labels(graph) == ["L -order-> O", "O -customer-> C", "C -nation-> N"]
assert graph.nodes["O"].rf is orders         # a node holds its relation
```

The relations that come from the DBF are called **base relations**, to tell them apart from the views introduced
below.

Each edge knows its `source` and `target` node and its `ref_key`, the attribute that holds the reference. Compare
edges and nodes by name, not with `==`: `==` compares the relations they hold by content.

The adjacency maps list the neighbours of each relation, in the direction of the references and against it:

```python
out_map = graph.outgoing_adjacency()   # relation -> the relations its tuples point at
in_map = graph.incoming_adjacency()    # relation -> the relations whose tuples point at it
assert [(n.name, n.ref_key) for n in out_map["O"]] == [("C", "customer")]
assert [(n.name, n.ref_key) for n in in_map["O"]] == [("L", "order")]
```

#### Which references become edges

Only references between relations **of this DBF** become edges. Leave a relation out, and the references into it
disappear from the graph:

```python
no_customers = JoinGraph.from_dbf(DBF({"L": lineitems, "O": orders, "N": nations}, frozen=True))
assert edge_labels(no_customers) == ["L -order-> O"]
assert no_customers.isolated_nodes() == {"N"}   # O.customer points at C, which is not in this DBF
```

A relation's reference **to itself** is no edge: a reference is a pointer, not a request to join the relation with
itself (a self-join needs a second relation, see the paragraph on aliases below):

```python
employees = RF({"e1": TF({"name": "Ann"})}, frozen=False)
employees["e2"] = TF({"name": "Ben", "boss": employees["e1"]})
employees = employees.references("boss", employees)
employees.freeze()

staff = JoinGraph.from_dbf(DBF({"E": employees}, frozen=True))
assert staff.edges == [] and staff.sole_relation_name() == "E"
```

A **filtered copy** of a relation is a new relation. References of other relations still point at the original, so
the edge into the copy is lost. To restrict a relation without losing its edges, attach a filter to its node instead
(see *Filters on nodes* below):

```python
alice_only = customers.where(lambda item: item.value.name == "Alice")   # a new relation
lost = JoinGraph.from_dbf(DBF({"O": orders, "C": alice_only}, frozen=True))
assert lost.edges == []                                                 # O.customer points at `customers`

kept = JoinGraph.from_dbf(DBF({"O": orders, "C": customers}, frozen=True))
kept.add_filter("C", Eq("name", "Alice"))                               # same restriction, edge kept
assert len(kept.edges) == 1
```

A reference into one relation entered under **several names** (aliases, as in SQL's `FROM t AS a, t AS b`) is
refused with `NotImplementedError`, because nothing tells which alias it means. Enter every name as its own relation,
for each name including the first (e.g. `rf.where(lambda i: True)`, so the original relation is not in the DBF), and
bind each reference with `add_reference`.

#### The shape of the graph

The operators care about the **shape** of the graph, and the graph answers these **topology queries**. Our chain
`L -> O -> C -> N` is connected and no relation can be reached on two different ways, i.e. it is a **tree** (with
the edge directions ignored). A **pure source** is a relation that points at others but is pointed at by none (see
also the [glossary in join](join.md)):

```python
assert graph.is_tree()                        # one piece, and 3 edges for 4 nodes
assert graph.pure_sources() == {"L"}          # L points at others, nobody points at L
assert graph.isolated_nodes() == set()        # every relation takes part in an edge
assert graph.connected_components() == [{"L", "O", "C", "N"}]
graph.check_acyclicity()                      # raises ValueError on a directed cycle
```

Two relations pointing at the same relation (orders and tickets both point at customers) still form a tree, now
with two pure sources:

```python
star = JoinGraph.from_dbf(DBF({"O": orders, "T": tickets, "C": customers}, frozen=True))
assert star.is_tree()
assert star.pure_sources() == {"O", "T"}
assert [n.name for n in star.incoming_adjacency()["C"]] == ["O", "T"]
```

A relation that is reached on two paths makes a **diamond**. A diamond has no directed cycle, but it is no tree:

```
   A --b--> B --d--> D
   |                 ^
   +--c---> C --d----+
```

```python
d = RF({"d1": TF({"x": 1})}, frozen=False)
b = RF({"b1": TF({"d": d["d1"]})}, frozen=False).references("d", d)
c = RF({"c1": TF({"d": d["d1"]})}, frozen=False).references("d", d)
a = RF({"a1": TF({"b": b["b1"], "c": c["c1"]})}, frozen=False).references("b", b).references("c", c)
for rf in (d, b, c, a):
    rf.freeze()

diamond = JoinGraph.from_dbf(DBF({"A": a, "B": b, "C": c, "D": d}, frozen=True))
assert not diamond.is_tree()          # 4 edges for 4 nodes: one too many for a tree
diamond.check_acyclicity()            # does not raise: no directed cycle
```

The graph from the example above without customers falls apart into two pieces:

```python
assert not no_customers.is_tree()
assert no_customers.connected_components() == [{"L", "O"}, {"N"}]
```

What the operators accept:

| Graph shape | [join](join.md) | [subdatabase](subdatabase.md) |
|---|---|---|
| a single relation | yes | yes |
| several relations, no edges at all | `NotImplementedError` | yes |
| tree, in any edge direction (chain, star, several pure sources) | yes | yes |
| diamond or other non-tree without a directed cycle | `NotImplementedError` | yes |
| directed cycle | `NotImplementedError` | `ValueError` |
| an isolated relation next to a connected rest | `NotImplementedError` | yes |
| two or more pieces with edges | `NotImplementedError` | `ValueError` |

The topology queries at a glance:

| Method | Answers |
|---|---|
| `is_tree()` | Is the graph one piece, with no relation reachable on two ways (edge directions ignored)? |
| `connected_components()` | Which relations hang together (edge directions ignored)? |
| `pure_sources()` | Which relations point at others but are pointed at by none? |
| `isolated_nodes()` | Which relations take part in no edge? |
| `is_trivial()` / `sole_relation_name()` | Has the graph no edges? / The name of the only relation, or `None` |
| `outgoing_adjacency()` / `incoming_adjacency()` | Neighbours of each relation, with and against the references |
| `check_acyclicity()` | Raises `ValueError` on a directed cycle |
| `build_semijoin_cascade(root)` | The semijoin steps `subdatabase` runs (Yannakakis reduction) |

### Filters on nodes

A filter on **one** relation hangs at that relation's node. It applies to the relation's tuples, exactly like
[`filter_values`](filter.md), and may be a [structured predicate](predicates.md) or a lambda. Several filters on one
node must all hold (AND):

```python
graph.add_filter("C", Eq("name", "Alice"))                 # WHERE C.name = 'Alice'
graph.add_filter("O", lambda o: o.date >= "2026-01-01")    # a lambda works too
graph.add_filter("O", Gt("customer.signup", "2025-01-01")) # a second filter on O: both must hold

assert list(graph.filters) == ["C", "O"]
assert len(graph.filters["O"]) == 2
```

Paths in a node filter start at the relation's tuple (`"name"`, `"customer.signup"`), not with the relation name;
they may follow references (`customer.signup` reads the signup date of the order's customer). The filters are part
of the graph only: the relations themselves stay unchanged, and so do the graph's edges.

A filter on a name that is no node of the graph is rejected:

```python
try:
    graph.add_filter("X", Eq("name", "Alice"))
except ValueError as error:
    assert str(error) == "'X' is not a node of the graph"
```

### Views

A join graph alone cannot express aggregation, conditions over several relations or the shape of the output. A
**view** draws a box around a subgraph and turns it into a new relation. That relation is a node of the enclosing
graph, so the enclosing graph can filter it, join over it, and put it into another view. In SQL terms, a view is a
derived table `(SELECT ... FROM <join of the subgraph> WHERE ... [GROUP BY ...])`.

```
   +--- View A: customer = O.customer ; revenue = Sum("L.price") --+
   |        L --order--> O                                          |
   +--------------------------------+-------------------------------+
                                    | result A (customer, revenue)
                         A --customer--> C --nation--> N
                         HAVING: filter on node A, e.g. Gt("revenue", 100)
```

Paths in a view start with a relation name of its subgraph: `"O.customer"` is relation `O`, then its attribute
`customer` (`"O__customer"` is the same path). They are read in each row of the joined subgraph (one row per matching
combination of tuples, see [join](join.md)). A path without a dot (`"O"`) means the `O` tuples themselves; the first
part may also name an inner view (`"A.revenue"`). A view can carry:

- **Join predicates** (`join_predicates`): conditions over **several** relations of the subgraph, as
  [`JoinPredicate`](constraints.md)s (the type `add_join_predicate` uses), e.g. `JoinPredicate(("O", "C"),
  Gt("O.date", Ref("C.signup")))`. A condition on one relation belongs on that node instead.
- **A grouping** (`group_keys`, `aggregates`): grouping keys map an output name to a path, aggregates map an output
  name to an aggregation over a path (`Sum("L.price")`, `Count()`). Grouping keys without aggregates are a
  **DISTINCT projection** (`is_distinct()`); aggregates without grouping keys are a **global aggregate** with one
  result tuple.
- **A projection** (`projection`): output name to path, e.g. `{"date": "O.date"}`. One output tuple per joined row,
  duplicates are kept. A view has a projection or a grouping, never both.

A view with neither grouping nor projection **passes the joined rows on** (a *pass-through view*); its outputs are its
subgraph's relation names (`passes_rows_on()`). A filter on a view's node is **HAVING**: it applies to the view's
result tuples and may only read the view's outputs. Filters on the nodes inside a view's subgraph are its WHERE.

#### Views by example

All examples use the relations of the running example above.

**A grouping view with WHERE, an edge and HAVING.** View `A` computes the revenue per customer over `L -order-> O`,
counting only orders from 2026 on. In the enclosing graph, `A` is joined to the customers and filtered by its revenue:

```python
inner = JoinGraph.from_dbf(DBF({"O": orders, "L": lineitems}, frozen=True))
inner.add_filter("O", Gt("date", "2025-12-31"))   # WHERE, on a node inside the view
revenue = View(
    name="A",
    subgraph=inner,
    group_keys={"customer": "O.customer"},        # output name -> path into the joined rows
    aggregates={"revenue": Sum("L.price")},       # output name -> aggregation
)
assert revenue.output_names() == ["customer", "revenue"]

outer = JoinGraph.from_dbf(DBF({"C": customers, "N": nations}, frozen=True))
outer.add_view(revenue)
outer.add_edge("A", "C", "customer")              # allowed: "O.customer" references C
outer.add_filter("A", Gt("revenue", 10))          # HAVING revenue > 10
assert outer.is_tree()                            # topology queries treat the view like any relation
```

A HAVING may only read the view's outputs; a typo is caught when the filter is added, not later as an empty result:

```python
try:
    outer.add_filter("A", Gt("revenu", 10))
except ValueError as error:
    assert "names no output of the view: ['customer', 'revenue']" in str(error)
```

**A view around a whole graph.** A condition over two relations and the shape of the output:

```python
orders_after_signup = View(
    name="W",
    subgraph=JoinGraph.from_dbf(DBF({"O": orders, "C": customers}, frozen=True)),
    join_predicates=[JoinPredicate(("O", "C"), Gt("O.date", Ref("C.signup")))],
    projection={"date": "O.date", "customer": "O.customer"},
)
assert orders_after_signup.output_names() == ["date", "customer"]
```

**DISTINCT and global aggregates.** Grouping keys without aggregates give each distinct value once; aggregates
without grouping keys give one result tuple:

```python
customers_with_orders = View(
    name="P",
    subgraph=JoinGraph.from_dbf(DBF({"O": orders}, frozen=True)),
    group_keys={"customer": "O.customer"},
)
assert customers_with_orders.is_distinct()

n_items = View(
    name="G",
    subgraph=JoinGraph.from_dbf(DBF({"L": lineitems}, frozen=True)),
    aggregates={"n": Count()},
)
assert n_items.output_names() == ["n"]
```

**Nesting.** View `B` aggregates over the graph that contains view `A`; a path into `A` must name one of its outputs:

```python
best = View(
    name="B",
    subgraph=outer,
    group_keys={"nation": "C.nation"},
    aggregates={"top_revenue": Max("A.revenue")},
)
assert best.output_names() == ["nation", "top_revenue"]
```

**A pass-through view.** It has no grouping and no projection; its outputs are its subgraph's relations, so an
enclosing view reads through it with a path that starts with the view, then a relation (`"R.O.date"`):

```python
orders_with_customers = View(
    name="R",
    subgraph=JoinGraph.from_dbf(DBF({"O": orders, "C": customers}, frozen=True)),
)
assert orders_with_customers.passes_rows_on()
assert orders_with_customers.output_names() == ["O", "C"]

around_r = JoinGraph()
around_r.add_view(orders_with_customers)
per_date = View(
    name="S",
    subgraph=around_r,
    group_keys={"date": "R.O.date"},              # relation O inside view R
    aggregates={"orders": Count()},
)
assert per_date.output_names() == ["date", "orders"]
```

**An edge through a projection name.** A projection name whose path is a reference can carry an edge, like a grouping
key:

```python
order_view = View(
    name="V",
    subgraph=JoinGraph.from_dbf(DBF({"O": orders}, frozen=True)),
    projection={"date": "O.date", "customer": "O.customer"},
)
with_customers = JoinGraph.from_dbf(DBF({"C": customers}, frozen=True))
with_customers.add_view(order_view)
with_customers.add_edge("V", "C", "customer")     # "O.customer" references C
assert edge_labels(with_customers) == ["V -customer-> C"]
```

### Rules

- **Edges added with `add_edge` start at a view** and end at a base relation (edges between base relations come from
  `from_dbf`; a pass-through view cannot be an edge source yet). They go through a grouping key or projection name
  that really is a reference to the target: `X.attr` where `X` carries a reference on `attr` to the target, or `X`
  when the target is `X` itself. Relations count as the same only if they are the same object (each relation carries
  an internal id, its `uuid`); two relations with equal content are still different.
  `outer.add_edge("A", "N", "customer")` raises
  `ValueError: Grouping key 'customer' of view 'A' (path 'O.customer') is not a reference to 'N'`.
- **No edge crosses a view boundary:** an edge connects nodes of the same graph only. The same relation may appear
  inside and outside a view.
- **Views are validated when they are built** (paths start at a relation of the subgraph, names are non-empty and
  contain neither `.` nor `__`, aggregates are `AggregationFunction`s, projection and grouping are not combined, ...).
  A join predicate must declare at least two relations of the subgraph, and a built-in structured one must read
  exactly the relations it declares (*built-in* means the predicate classes funqDB ships, such as `Eq`, `Gt`, `And`,
  not your own `Predicate` subclasses). A path into an inner view must name one of its outputs, also through views
  that pass rows on (`"R.O.date"`). `add_view` rejects name clashes and a view that would contain the graph it is
  added to.
- **`add_filter`** takes a node of this graph only (a node inside a view: `view.subgraph.add_filter(...)`) and refuses
  a `JoinPredicate`. On a view node, a built-in structured predicate may only read the view's outputs (on a view that
  passes rows on, also into its inner views); lambdas and user-defined `Predicate` subclasses are not checked, and
  neither is an `And` / `Or` / `Not` with such a part.

### Limitations

- **Not executable yet.** A grouping view means `group_by_aggregate(join(subgraph), ...)` (without grouping keys:
  `aggregate(join(subgraph), ...)`), but `group_by` cannot group by tuples today (`TypeError: unhashable type: 'TF'`),
  and `group_by_aggregate` does not write the grouping keys into the result tuples, so a HAVING on a grouping key has
  nothing to read yet. An edge from a grouping view assumes one group per referenced tuple (one per customer), even
  when two customers have identical attribute values.
- Aggregates run over the rows of the [join](join.md): an order with three line items appears in three rows, so an
  aggregate over an order attribute counts that order three times (as in SQL, `GROUP BY` over a join).
- If the view's subgraph and the outer graph are built from different copies of a relation (e.g. one of them
  filtered), the relations count as different, and `add_edge` rejects the edge even though it would make sense.
- `And(Eq("O.x", 1), Eq("C.y", 2))` declared over `O` and `C` is accepted as a join predicate, although both parts
  belong on the nodes.
- A filter on a view that passes rows on may read its relations: one (`"O.date"`, a second place for a WHERE) or
  several (`Gt("O.date", Ref("C.signup"))`, a second place for a join predicate, but without the check of declared
  relations). Prefer the node inside the subgraph or the view's join predicates.
- A relation whose name contains `__` or `.` can be put into a view, but no path can reach it, because paths split on
  `__` and `.`.
- To hide an aggregate that only a HAVING needs, put a view with a projection around the view. Such an outer view
  cannot be joined through `"A.customer"`, because an edge path must start at a base relation.
- Filters and views are built by hand; deriving them from FQL pipelines, rendering, and executing them are future
  work. `from_dbf` ignores a DBF's own `JoinPredicate`s.

Design records:
[`docs/changes/011-join-graph-aggregate-boxes/DECISION.md`](../../changes/011-join-graph-aggregate-boxes/DECISION.md),
[`docs/changes/015-join-graph-views/DECISION.md`](../../changes/015-join-graph-views/DECISION.md).

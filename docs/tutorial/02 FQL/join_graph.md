## Join Graph and Aggregate Boxes

> **Note:** Aggregate boxes are a **logical representation** only. They describe a query; nothing executes them yet
> (see *Limitations* below).

### The join graph

Every DBF has a **join graph**: one node per relation, one directed edge per reference (`.references(ref_key,
target)` or [`add_reference`](constraints.md)); a DBF without references has a graph without edges. A relation's
reference to itself (e.g. `title.episode_of_id → title`) is no edge, and a reference into one relation entered under
several names (aliases) is refused, because nothing tells which alias it means. The
[join](join.md) and [subdatabase](subdatabase.md) operators read their join structure from it:

```python
from fql.plan.join_graph import JoinGraph

graph = JoinGraph.from_dbf(dbf)   # nodes: relation names, edges: references
```

### Aggregate boxes

A join graph alone cannot express aggregation. An **aggregate box** draws a box around a subgraph and annotates it with
**grouping keys** and **aggregates**. The box's result is again a relation, so the enclosing graph can join over it,
and a subgraph may itself contain boxes. In SQL terms, a box is a derived table
`(SELECT keys, aggregates FROM <join of the subgraph> GROUP BY keys)` that the outer query joins with.

```
   +--- Box A: customer = O.customer ; revenue = Sum("L.price") ---+
   |        L --order--> O                                          |
   +--------------------------------+-------------------------------+
                                    | result A (customer, revenue)
                         A --customer--> C --nation--> N
```

- **Grouping keys** map an output name to a dotted path such as `"O.customer"`: relation `O`, then its attribute
  `customer`. The path is read in each row of the joined subgraph (one row per matching combination of tuples, see
  [join](join.md)). A path without a dot (`"O"`) groups by the `O` tuples themselves; the first part may also name an
  inner box (`"A.revenue"`).
- **Aggregates** map an output name to an aggregation over such a path (`Sum("L.price")`, `Count()`).
- Without aggregates, a box is a **DISTINCT projection**; without grouping keys, it is a **global aggregate** with one
  result tuple.

### Example

```python
from fdm.attribute_functions import TF, RF, DBF
from fql.operators.aggregates import Count, Max, Sum
from fql.plan.join_graph import AggregateBox, JoinGraph

nations = RF({"de": TF({"name": "Germany"})}, frozen=False)
customers = RF({"c1": TF({"name": "Alice", "nation": nations["de"]})},
               frozen=False).references("nation", nations)
orders = RF({"o1": TF({"date": "2026-10-01", "customer": customers["c1"]})},
            frozen=False).references("customer", customers)
lineitems = RF({"l1": TF({"price": 20, "order": orders["o1"]}),
                "l2": TF({"price": 5, "order": orders["o1"]})},
               frozen=False).references("order", orders)
for rf in (nations, customers, orders, lineitems):
    rf.freeze()

# box A: revenue per customer over the subgraph L -order-> O
inner: JoinGraph = JoinGraph.from_dbf(DBF({"O": orders, "L": lineitems}, frozen=True))
revenue = AggregateBox(
    name="A",
    subgraph=inner,
    group_keys={"customer": "O.customer"},   # output name -> path into the joined rows
    aggregates={"revenue": Sum("L.price")},  # output name -> aggregation
)
assert revenue.output_names() == ["customer", "revenue"]

# the enclosing graph C -nation-> N, extended by the box and an edge from it
outer: JoinGraph = JoinGraph.from_dbf(DBF({"C": customers, "N": nations}, frozen=True))
outer.add_box(revenue)
outer.add_edge("A", "C", "customer")   # allowed: "O.customer" references C
assert outer.is_tree()                 # graph queries like is_tree() treat the box like any relation

# a DISTINCT projection: grouping keys, no aggregates
customers_with_orders = AggregateBox(
    name="P",
    subgraph=JoinGraph.from_dbf(DBF({"O": orders}, frozen=True)),
    group_keys={"customer": "O.customer"},
)
assert customers_with_orders.is_projection()

# a global aggregate: no grouping keys, one result tuple
n_items = AggregateBox(
    name="G",
    subgraph=JoinGraph.from_dbf(DBF({"L": lineitems}, frozen=True)),
    aggregates={"n": Count()},
)
assert n_items.output_names() == ["n"]

# nesting: box B aggregates over the graph that contains box A
best = AggregateBox(
    name="B",
    subgraph=outer,
    group_keys={"nation": "C.nation"},
    aggregates={"top_revenue": Max("A.revenue")},   # "revenue" must be an output of A
)
```

### Rules

- **Edges added with `add_edge` start at a box** and end at a plain relation (edges between plain relations come from
  `from_dbf`). They go through a grouping key that really is a reference to the target: `X.attr` where `X` carries a
  reference on `attr` to the target, or `X` when the target is `X` itself. Relations count as the same only if they
  are the same object (each relation carries an internal id, its `uuid`); two relations with equal content are still
  different. `outer.add_edge("A", "N", "customer")` raises
  `ValueError: Grouping key 'customer' of box 'A' (path 'O.customer') is not a reference to 'N'`.
- **No edge crosses a box boundary:** an edge connects nodes of the same graph only. The same relation may appear
  inside and outside a box.
- **Boxes are validated when they are built** (paths start at a relation of the subgraph, names contain no `.`,
  aggregates are `AggregationFunction`s, ...). `add_box` rejects name clashes and a box that would contain the graph
  it is added to.

### Limitations

- **Not executable yet.** A box means `group_by_aggregate(join(subgraph), ...)` (without grouping keys:
  `aggregate(join(subgraph), ...)`), but `group_by` cannot group by tuples today (`TypeError: unhashable type: 'TF'`),
  and `group_by_aggregate` does not write the grouping keys into the result tuples. An edge from a box assumes one
  group per referenced tuple (one per customer), even when two customers have identical attribute values.
- Aggregates run over the rows of the [join](join.md): an order with three line items appears in three rows, so an
  aggregate over an order attribute counts that order three times (as in SQL, `GROUP BY` over a join).
- If the box's subgraph and the outer graph are built from different copies of a relation (e.g. one of them
  filtered), the relations count as different, and `add_edge` rejects the edge even though it would make sense.
- Boxes are built by hand; deriving them from FQL pipelines, rendering, and selection predicates (WHERE / HAVING) are
  future work.

Design record:
[`docs/changes/011-join-graph-aggregate-boxes/DECISION.md`](../../changes/011-join-graph-aggregate-boxes/DECISION.md).

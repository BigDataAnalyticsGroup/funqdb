# Podcast Script: "Everything is a Function"

**[Listen to the podcast](https://bigdata.uni-saarland.de/publications/Replacing_SQL_Tables_With_Pure_Functions_01.mp3)**

> *Disclaimer: Both this script and the podcast audio were generated with AI assistance.
> The content is based on the paper [A Functional Data Model and Query Language is All You Need](https://bigdata.uni-saarland.de/publications/Dittrich%20-%20A%20Functional%20Data%20Model%20and%20Query%20Language%20is%20All%20You%20Need%20@EDBT2026.pdf) (EDBT 2026) by Jens Dittrich.*

## A Conversation About Rethinking Databases from Scratch

**Hosts:**
- **Alex** — a software engineer and podcast host, curious but skeptical
- **Jordan** — a database researcher, enthusiastic about the ideas in the paper

---

### INTRO

**Alex:** Welcome back to the show, everyone. Today we're diving into something that, honestly, when I first read about it, I thought was either brilliant or completely insane. Maybe both. We're talking about a paper called *"A Functional Data Model and Query Language is All You Need"* by Jens Dittrich from Saarland University, published at EDBT 2026. Jordan, you've been raving about this paper for weeks. What's the big idea?

**Jordan:** The big idea is deceptively simple: what if we modeled *everything* in a database — tuples, tables, entire databases — as *functions*? Not sets, not tables, not rows. Functions. And then what if the query language was just... calling and composing those functions inside your normal programming language?

**Alex:** Okay, so when you say "function," you mean like a mathematical function? Input goes in, output comes out?

**Jordan:** Exactly. Think of it this way. What is a tuple, really? You give it an attribute name — say, "age" — and it gives you back a value — say, 30. That's a function. What is a table? You give it a primary key — say, employee ID 42 — and it gives you back a tuple. That's also a function. What is a database? You give it a table name — "employees" — and it gives you back a table. Again, a function.

**Alex:** So you're saying there's this one concept — a function — and it works at every level?

**Jordan:** That's exactly it. The paper calls these *attribute functions*. A tuple function, or TF, maps attribute names to values. A relation function, or RF, maps keys to tuple functions. A database function, or DBF, maps relation names to relation functions. And you can even have sets of databases — SDBF — which map database names to database functions. Same concept, all the way up and all the way down.

---

### THE PROBLEMS WITH SQL

**Alex:** Alright, but we have SQL. It works. Billions of dollars of infrastructure run on it. Why would we want to replace it?

**Jordan:** Great question. And the paper doesn't shy away from it — it lists a whole catalogue of problems with SQL and the relational model. Let me walk through the big ones.

**Alex:** Hit me.

**Jordan:** Number one: **NULL values and three-valued logic**. In SQL, if a value is missing, you get NULL. And NULL doesn't behave like you'd expect. NULL equals NULL? That's not true — it's *unknown*. NULL plus five? That's NULL. You end up with this weird three-valued logic — true, false, unknown — that trips up even experienced developers.

**Alex:** Oh yeah, I've been burned by that. `WHERE column != 'x'` doesn't return rows where the column is NULL.

**Jordan:** Exactly. In the functional data model, there are no NULLs. If an attribute doesn't exist for a tuple, the function is simply *not defined* for that input. You don't get some magic poison value that propagates through your entire query. You get an error — which is honest and debuggable.

**Alex:** What's problem number two?

**Jordan:** **Everything gets crammed into a single flat table.** When you write a SQL query — even a complex join across seven tables — the result is always one single, possibly denormalized table. That's a fundamental limitation baked into SQL since the 1970s.

**Alex:** And what's wrong with that?

**Jordan:** Think about it. You join customers with orders with products. The result? One giant table where the customer name "Alice" is repeated on every single row that has one of her orders. That's redundant data. It wastes memory, it wastes network bandwidth, and it violates the very normal forms that the relational model is supposed to uphold.

**Alex:** Huh, I never thought about it that way. SQL queries *violate* normal forms by design.

**Jordan:** Right! The paper actually references earlier work by Dittrich's group — a SIGMOD 2025 paper — where they showed how to extend SQL to return a *subdatabase* instead. But FDM and FQL go much further. In FQL, operators can return anything — a tuple, a relation, a database, a set of databases. There's no single-table straitjacket.

**Alex:** Okay, what else?

**Jordan:** **SQL injection.** As of 2025, it's the second most dangerous software weakness in the world, according to the CWE Top 25 list. And the reason it exists is fundamentally architectural: SQL is a *text string* that you embed inside your programming language. There's a boundary between "this is code" and "this is user input," and attackers exploit that boundary.

**Alex:** And parameterized queries, prepared statements — those are just patches?

**Jordan:** They're afterthoughts. They work, but they have to be used correctly every single time. One missed parameter binding and you're vulnerable. In FQL, there is no text boundary. The query language is just function calls in your programming language. There's no string to inject into. SQL injection becomes *impossible by design*, not by discipline.

**Alex:** That's a strong claim.

**Jordan:** It is. But think about it — if your query is `filter(users, Eq("name", user_input))`, there's no way for `user_input` to change the *structure* of the query. It's just a value passed to a function. The semantics are fixed at compile time.

---

### THE FOURTH PROBLEM: THE IMPEDANCE MISMATCH

**Alex:** You mentioned four or five problems. What's next?

**Jordan:** **The impedance mismatch** — or what I'd call the "two languages" problem. When you write a web application, you think in Python or Java or Rust. But to talk to your database, you switch to SQL. Different syntax, different semantics, different type system. And the glue between them — ORMs like Django ORM or SQLAlchemy — they add another layer of abstraction that leaks in all sorts of ways.

**Alex:** The old "object-relational impedance mismatch." That's been a complaint for decades.

**Jordan:** And for good reason. FQL eliminates it entirely. The query language *is* the programming language. In the Python implementation — which is an open-source project called funqDB — a query looks like this:

```python
result = aggregate(
    join(
        DBF({
            "ci": cast_info.where(note__like="%(producer)%"),
            "cn": company_name.where(country_code="[us]"),
            "t": title.where(production_year__gt=1990),
        })
    ),
    character=Min("chn.name"),
    movie_with_american_producer=Min("t.title"),
)
```

That's pure Python. No string interpolation, no ORM magic. Just function calls.

**Alex:** And if you wanted to write the same thing in Rust, it would use Rust syntax but express the same semantics?

**Jordan:** Exactly. The paper calls these *function costumes*. The FQL operator is the abstract concept; its appearance in a specific language is the costume it wears. The key insight is that these function calls don't have to be executed by the programming language. The runtime can detect that this is a database query and *push it down* to a database engine for optimization.

---

### HOW THE DATA MODEL ACTUALLY WORKS

**Alex:** Let's get concrete. Walk me through how you'd actually model data in FDM.

**Jordan:** Sure. Let's say we have a small company with departments and employees. In SQL, you'd write:

```sql
CREATE TABLE departments (
    id TEXT PRIMARY KEY,
    name TEXT,
    budget INTEGER
);

CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT,
    salary INTEGER,
    department_id TEXT REFERENCES departments(id)
);
```

In FDM, you'd write:

```python
departments = RF({
    "d1": TF({"name": "Engineering", "budget": 11_000_000}),
    "d2": TF({"name": "Sales", "budget": 5_000_000}),
})

employees = RF({
    1: TF({"name": "Alice", "salary": 90_000, "dept": departments["d1"]}),
    2: TF({"name": "Bob", "salary": 70_000, "dept": departments["d1"]}),
    3: TF({"name": "Carol", "salary": 60_000, "dept": departments["d2"]}),
})
```

**Alex:** Wait — `departments["d1"]` — that's not a foreign key ID. That's a *direct reference* to the actual tuple function?

**Jordan:** Yes! That's one of the most powerful ideas. In SQL, you store a foreign key — just a number or string — and then you *join* to navigate the reference. In FDM, the value of the "dept" attribute *is* the department tuple function itself. No join needed for navigation. You just write `employees[1]["dept"]["name"]` and you get "Engineering" directly.

**Alex:** So it's like an object reference in an OOP language?

**Jordan:** Very similar, but with an important difference: there's no separate "object world" and "database world." It's all functions. And the system handles persistence transparently — when you store this to disk, the reference gets *swizzled* into a lightweight sentinel with just a UUID, and when you access it, it gets *unswizzled* back to the full object lazily.

**Alex:** Lazy loading built into the data model. Nice. But what about schemas and integrity constraints? In SQL, you have `CREATE TABLE` with column types, `NOT NULL`, `FOREIGN KEY`, `CHECK` constraints... Does FDM just throw all of that away?

**Jordan:** Not at all — but it integrates it much more naturally. You can declare a schema for a relation function, and it uses the same attribute function abstraction. Here's what it looks like:

```python
employees = RF({
    1: TF({"name": "Alice", "salary": 90_000, "dept": departments["d1"]}),
    2: TF({"name": "Bob", "salary": 70_000, "dept": departments["d1"]}),
}, schema={"name": str, "salary": int, "dept": departments})
```

See how `"dept": departments` works? That single declaration says: the value of "dept" must be a tuple function that exists in the `departments` relation function. That's your foreign key constraint — but expressed as a *type constraint* on the function's codomain, not as a separate `FOREIGN KEY` clause.

**Alex:** So the schema is just... another function describing the shape of the data?

**Jordan:** Exactly. And you can add explicit foreign value constraints too:

```python
employees.references("dept", departments)
```

This does two things at once: it constrains employees so that every "dept" value must point to a valid entry in `departments`, and it prevents you from deleting a department that's still referenced. Bidirectional integrity, declared once, enforced everywhere. In SQL, you'd need a `FOREIGN KEY` declaration plus `ON DELETE RESTRICT` — and if you forget the latter, you get dangling references.

**Alex:** And type checking happens on write?

**Jordan:** Yes. If you try to store a string where the schema says `int`, you get an immediate error. No silent coercion, no "it'll blow up at query time." The schema is enforced at the boundary — when data enters the function.

---

### BLURRING THE LINES

**Alex:** You said earlier that the same concept works at every level. Can you give me an example of why that matters?

**Jordan:** Here's where it gets really interesting. In the relational model, there are hard walls between levels. A tuple contains atomic values — that's first normal form. A relation is a set of tuples. A database is a set of relations. Each level has different operations.

In FDM, those walls are gone. Consider this:

```python
t = TF({"name": "Metadata Record", "data": some_relation_function})
```

Here, a tuple function has an attribute whose value is a *relation function*. Is `t` a tuple or a database? In FDM, it doesn't matter — it's just a function, and you can query it with the same operators regardless.

**Alex:** So you could have a tuple that contains a table, or a table whose rows are themselves databases?

**Jordan:** Exactly. And you query all of these with the same operators. No need for special `ARRAY` or `MAP` types or `JSON` column workarounds like in modern SQL extensions. The model is *inherently* hierarchical and composable.

**Alex:** That's... actually elegant.

---

### INDEXES AS A FIRST-CLASS CONCEPT

**Jordan:** There's another beautiful consequence. Think about what a "logical index" is. In SQL, an index is a physical optimization — you create it after the fact to speed up lookups, and it's not part of the data model at all.

In FDM, an index is just *another relation function* over the same tuple functions, but organized by a different key. Say you have employees keyed by employee ID. You can define a second relation function keyed by department name that returns the same tuple functions, just organized differently.

**Alex:** So the concept of an index is baked into the mathematical foundation?

**Jordan:** Yes. A function, by definition, maps each input to exactly one output. That gives you the uniqueness constraint for free. If you want non-unique indexes — multiple employees per department — you define the function to return a *set* of tuple functions. That's exactly what a non-unique index does in a traditional database, but here it's part of the conceptual model, not an afterthought.

---

### THE QUERY LANGUAGE: UNARY OPERATORS ALL THE WAY DOWN

**Alex:** Let's talk about FQL itself. How do queries work?

**Jordan:** The core principle is this: every FQL operator is a *unary, higher-order function*. It takes one function as input and produces one function as output. That's it.

**Alex:** Wait — a join takes two tables as input. How is that unary?

**Jordan:** Great question. In FQL, you don't pass two tables to a join operator. Instead, you first construct a *database function* that contains both tables, and then you pass that single database function to the join operator. The join takes one input — a DBF — and produces one output.

**Alex:** So instead of `JOIN table_a WITH table_b`, you'd write something like `join(DBF({"a": table_a, "b": table_b}))`?

**Jordan:** Exactly. And this generalizes beautifully. A three-way join? Put three tables in the DBF. An n-way join? Put n tables in. The operator signature never changes. Compare that to SQL, where adding a table to a join changes the query structure dramatically — you need another `JOIN ... ON` clause, more conditions in the `WHERE` clause...

**Alex:** And the join predicates?

**Jordan:** In many cases, they're *implicit*. If your schema already declares that employees reference departments — that foreign value constraint we talked about — the join operator knows how to connect them. You don't repeat that information in every query. In SQL, you write `employees.department_id = departments.id` in every single query, even though the schema already defines this relationship.

**Alex:** So the schema is actually *useful* at query time, not just for validation?

**Jordan:** Precisely. The paper shows a side-by-side comparison with a Join Order Benchmark query — seven tables, seven join predicates, three filters, two aggregates in SQL. The FQL version is about half the size, and more importantly, there are no redundant join predicates. It just says "join this database" and the system knows how the tables relate.

---

### POWERFUL UPDATES

**Alex:** What about writes? INSERT, UPDATE, DELETE?

**Jordan:** This is another area where FQL is fundamentally more powerful than SQL. In SQL, your DML operations are limited to modifying rows in a single table. You can INSERT rows, UPDATE rows, DELETE rows. That's it. You can't, say, replace an entire table atomically, or transform a database structure.

**Alex:** What can you do in FQL?

**Jordan:** The paper introduces two modes: *out-of-place* and *in-place*. Out-of-place is like a SELECT — you get a view, a new perspective on the data, without changing anything. In-place means you *replace* the underlying function with the result of the operation.

And since operators work at every level — tuples, relations, databases — you can do in-place modifications at every level too. You can redefine a tuple, replace an entire relation, restructure a database. The same operator that you use for reading can be used for writing.

**Alex:** The paper mentions that SQL's update capabilities cover only a tiny corner of the possible operation space?

**Jordan:** Yes — Table 1 in the paper maps out this space as a matrix. The rows are input types (TF, RF, DBF, SDBF), the columns are output types. SQL covers the diagonal plus one small additional area. FQL covers the *entire* matrix. That's a massive expansion of what's expressible.

---

### COMPUTED DATA AND THE DISAPPEARING BOUNDARY

**Alex:** You mentioned something earlier about computed values. What's that about?

**Jordan:** This is one of my favorite parts. In SQL, there's a hard distinction between stored data and computed data. A column value is stored; a view is computed. They look different, they behave differently, they're managed differently.

In FDM, that boundary vanishes. A tuple function can have *computed* attributes that are calculated on every access:

```python
employee = TF(
    {"name": "Alice", "base_salary": 50_000},
    computed={
        "bonus": lambda tf: tf["base_salary"] * 0.1,
        "total_pay": lambda tf: tf["base_salary"] + tf["bonus"],
    },
)
```

When you access `employee["bonus"]`, it's computed from `base_salary`. When you access `employee["total_pay"]`, it's computed from `base_salary` and `bonus`. And here's the thing — from the outside, you can't tell which attributes are stored and which are computed. They're indistinguishable.

**Alex:** So if you change `base_salary`, the bonus automatically updates?

**Jordan:** Instantly. No materialized view refresh, no trigger, no cache invalidation. It's just a function call.

**Alex:** And this works at the relation level too?

**Jordan:** Yes, and this is where it gets really powerful. The paper introduces the `default=` parameter — a fallback function that generates values on the fly for keys that have no stored data:

```python
users = RF(
    {1: TF({"name": "Alice"})},
    default=lambda key: TF({"name": f"User-{key}"}),
)
```

Now `users[1]` returns the stored Alice tuple, but `users[999]` generates a tuple on the fly. From the caller's perspective, both look identical — you can't tell which is stored and which is computed. This is Section 2.6 of the paper: a *computed relation function*.

**Alex:** But if the default generates values for *any* key, how do you iterate over it? You can't enumerate all integers.

**Jordan:** Exactly — and that's where *active domains* come in. You can scope the default with a `domain=` parameter that defines the finite set of keys for which the function is defined:

```python
settings = TF(
    {"theme": "dark"},
    default=lambda key: "unset",
    domain={"theme", "language", "timezone"},
)
```

Now `settings` has three keys: "theme" returns "dark" — the stored value takes precedence — while "language" and "timezone" both return "unset" from the default. And crucially, `len(settings)` returns 3, and you can iterate over all three keys. The domain makes the function *enumerable* without losing the power of computed fallbacks.

**Alex:** So the domain acts like a declaration of "these are the valid inputs to this function"?

**Jordan:** Right. It's the FDM equivalent of constraining a function's domain in the mathematical sense. And it composes nicely with the rest of the model — you can use it at any level. A relation function with a domain and a default is, conceptually, a table that has a finite set of known keys but can generate sensible default tuples for each one. Think configuration tables, calendar dimensions, or sparse data where most entries follow a pattern and only the exceptions are stored explicitly.

---

### RELATIONSHIP FUNCTIONS: BEYOND FOREIGN KEYS

**Alex:** What about many-to-many relationships? In SQL, you need a junction table.

**Jordan:** In FDM, there's a concept called a *relationship function*. Given k functions with domains X₁ through Xₖ, a relationship function maps from the Cartesian product X₁ × X₂ × ... × Xₖ to some codomain. If the codomain is boolean, it's called a *relationship predicate* — it just says "does this relationship exist?"

In practice, for a many-to-many relationship between users and projects, you'd use a composite key:

```python
meetings = RSF()
meetings[CompositeForeignObject(alice, project_alpha)] = TF({"role": "lead"})
meetings[CompositeForeignObject(bob, project_alpha)] = TF({"role": "member"})
```

**Alex:** So the relationship can carry its own data — like "role" in this case — and the key is a composite of the actual referenced objects, not just their IDs?

**Jordan:** Exactly. And the relationship function can express relationships not just between tuples, but between *any* level. You can have a relationship between a relation and a tuple, or between a database and a relation. The paper gives an example: "which relation in this database is accessed by which user?" That's a relationship between a relation function and a tuple function — something you can't even express cleanly in SQL without metadata hacks.

---

### THE BIGGER PICTURE: OPTIMIZATION OPPORTUNITIES

**Alex:** Let's zoom out. If FQL is just function calls in the programming language, what does that mean for optimization?

**Jordan:** It opens up a completely new optimization space. Today, you have two separate optimizers: the compiler or interpreter for your programming language, and the query optimizer inside the database. They can't see each other's work. If you have a loop in Python that calls a SQL query on each iteration, neither optimizer can recognize that this could be a single batch operation.

With FQL, the boundary is gone. The programming language and the query language are the same thing. So in principle, a sufficiently smart runtime could look at your entire program and decide: "These three function calls can be pushed down to the database as a single optimized query, but this other one should stay in the programming language because it involves application logic."

**Alex:** So the UDF boundary disappears too?

**Jordan:** Yes! In SQL, a User-Defined Function is a black box to the optimizer. It can't inline it, can't reorder around it, can't push predicates through it. In FQL, since everything is a function in the same language, the optimizer can potentially see through all of them.

**Alex:** That's ambitious. Is anyone actually building this?

**Jordan:** The paper's author started an open-source implementation called funqDB in January 2026. It's a Python prototype — deliberately not optimized for performance, since it's a proof of concept. But it implements the full data model, a rich set of query operators, persistence with automatic swizzling, structured predicates that are serializable...

---

### WHAT'S NOT SQL ABOUT THIS

**Alex:** I want to make sure our listeners understand the philosophical shift here. Can you summarize what's fundamentally different?

**Jordan:** Sure. Let me give you the ten design principles from funqDB, and for each one, the paper explicitly states: "None of this is true for SQL."

One: **Purely functional data model** — everything is a key-value function. Not sets, not bags, not tables.

Two: **Same modeling concept at all levels** — whether you're looking at a tuple, a relation, a database, or a set of databases, it's the same abstraction.

Three: **All operators are unary** — input is a function, output is a function. No binary joins, no special-case operators.

Four: **The query language is part of the programming language** — not a separate string-based language embedded inside it.

Five: **No SQL injection** — by design, not by discipline.

Six: **No shoehorning results into a single table** — groups, partitions, outer joins, subdatabases all return the natural result type.

Seven: **No NULL values** — no NULLs in data, no three-valued logic. Missing is missing, not "unknown."

Eight: **Same power for updates as for reading** — every operator works in-place, not just a limited DML subset.

Nine: **Easily extensible** — any function defined in the host language can be an FQL operator.

Ten: **Indexes are built into the data model** — a logical index is just another function over the same data, not a physical optimization bolt-on.

---

### THE ADOPTION PATH

**Alex:** This all sounds great in theory. But how do you actually get from "the world runs on SQL" to this?

**Jordan:** The paper proposes four concrete adoption paths, from least to most ambitious:

First: a **Functional Relational Mapper** — like an ORM, but mapping FDM models and FQL queries to an existing relational database under the hood. You get the nicer programming model while your data stays in PostgreSQL.

Second: opening up an **existing DBMS to accept FQL expressions** through an API, mapping them to SQL internally.

Third: modifying the **storage layer** of an open-source database to natively store and optimize for FDM.

Fourth: building a **native FDM/FQL system** from scratch. That's what funqDB is — approach four, the most ambitious one.

**Alex:** And the idea is that people could start with approach one — just a better ORM — and gradually move toward native?

**Jordan:** Exactly. The paper actually mentions Django specifically — replacing Django ORM with an FQL-based mapper is one of the stated mid-term goals.

---

### THE RESEARCH AGENDA

**Alex:** Where does this go from here?

**Jordan:** The paper lays out an extensive research agenda. Joint optimization between compilers and databases. Joint transactional spaces — what does concurrency control look like when your programming language and your database share the same execution model? How do you integrate database optimizers architecturally when the query language is the programming language? How can FDM handle tensors — which are just functions with composite numeric keys, when you think about it?

And there's the usability question. The relational model has fifty years of tooling, education, and muscle memory behind it. FDM needs to prove it's not just theoretically superior but practically usable by working developers.

**Alex:** That's the real test, isn't it?

**Jordan:** It is. But I think the elegance of the model gives it a real shot. When a student sees that a tuple, a table, and a database are all the same thing — just a function — there's this "aha" moment. And for practicing developers, the elimination of the impedance mismatch alone might be worth the switch. No more fighting your ORM. No more writing SQL strings inside Python code. No more NULL surprises. Just... functions.

---

### CLOSING

**Alex:** So to sum it up for our listeners: the paper proposes replacing the relational model — tuples, tables, schemas — with functions at every level. It replaces SQL with function calls in your normal programming language. And it claims this solves NULLs, SQL injection, the impedance mismatch, the single-table result limitation, weak update capabilities, and the artificial separation between stored and computed data.

**Jordan:** And it's not just a thought experiment anymore. There's a working open-source prototype. It's early — alpha stage, Python only — but it's real code that you can run, with a test suite, a tutorial, and schema visualization. The paper is published at a top database conference. This isn't someone's weekend project — it's a serious research vision backed by a concrete implementation.

**Alex:** Last question: if a listener wanted to explore this, where should they start?

**Jordan:** Read the paper first — it's only eight pages and remarkably clear for an academic paper. Then look at the funqDB repository. The tests are written as tutorial-style examples, so they're very readable. And the SQL vs FQL comparison in the benchmarks directory is a great "aha" moment — seeing the same query in both languages side by side makes the advantages visceral.

**Alex:** Jordan, thanks for walking us through this. Listeners, the paper is *"A Functional Data Model and Query Language is All You Need"* by Jens Dittrich, EDBT 2026. We'll link it in the show notes.

**Jordan:** Thanks for having me. And remember: everything is a function.

**Alex:** Everything is a function. See you next week.

*[End of episode]*

---

### SHOW NOTES

- Paper: Jens Dittrich. *A Functional Data Model and Query Language is All You Need.* EDBT 2026.
  [PDF](https://bigdata.uni-saarland.de/publications/Dittrich%20-%20A%20Functional%20Data%20Model%20and%20Query%20Language%20is%20All%20You%20Need%20@EDBT2026.pdf)
- Extended version with code examples: [arXiv:2507.20671](https://arxiv.org/abs/2507.20671)
- funqDB open-source project: [GitLab](https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb)
- Related: Nix & Dittrich. *Extending SQL to Return a Subdatabase.* SIGMOD 2025.
- Related: Rink & Dittrich. *Query Optimization for Database-Returning Queries.* SIGMOD 2026.

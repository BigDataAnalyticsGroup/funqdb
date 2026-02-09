# funqDB

## Background

<blockquote>
The goal of funqDB is to replace the relational model, relational algebra, ORMs, and SQL in the longrun.
</blockquote>
funqDB is built around the central ideas of the vision paper:
<a id="1">[Dit26]</a> Dittrich, Jens. "A Functional Data Model and Query Language is All You Need." In Proceedings of
the
25th International Conference on Extending Database Technology **([EDBT 2026](https://edbticdt2026.github.io))**.

<blockquote>
Abstract:

We propose the vision of a functional data model (FDM) and an associated functional query
language (FQL). Our proposal has far-reaching consequences: we show a path to come up with
a modern query language (QL) that solves (almost if not) all problems of SQL (NULL-values,
type marshalling, SQL injection, missing querying capabilities for updates, etc.). FDM and
FQL are much more expressive than the relational model and SQL. In addition, in contrast to
SQL, FQL integrates smoothly into existing programming languages. In our approach both QL
and PL become the ‘same thing’, thus opening up several interesting holistic optimization
opportunities between compilers and databases.
</blockquote>

We **highly** recommend reading that paper to understand the motivation and the ideas behind funqDB: **FDM**,
and **FQL**.
This README is not meant to be a replacement for the paper, but rather a guide to the project and its current state,
how to use it, and how to contribute.

That paper, [Dit26], was preceded by a couple of previous version of that paper (with quite some variation in content):

<a id="3">[Dit25b]</a> Jens
Dittrich. [A Functional Data Model and Query Language is All You Need](https://arxiv.org/abs/2507.20671). arXiv:
2507.20671 [cs.DB].
*This paper also contains a lot of Python/FQL code examples.*

<a id="2">[Dit25a]</a> Jens
Dittrich. [How to get Rid of SQL, Relational Algebra, the Relational Model, ERM, and ORMs in a Single Paper -- A Thought Experiment](http://arxiv.org/abs/2504.12953)
arXiv:2504.12953 [cs.DB]
*Initially, this was a thought experiment to explore the ideas that eventually led to the vision papers [Dit25b] and then [Dit26]. It
contains a lot of code examples and is a good read to understand the motivation and the ideas behind funqDB.*

<blockquote>
Abstract:

Without any doubt, the relational paradigm has been a huge success. At the same time, we believe that the time is ripe
to rethink how database systems could look like if we designed them from scratch. Would we really end up with the same
abstractions and techniques that are prevalent today? This paper explores that space. We discuss the various issues with
both the relational model(RM) and the entity-relationship model (ERM). We provide a unified data model: the relational
map type model (RMTM) which can represent both RM and ERM as special cases and overcomes all of their problems. We
proceed to identify seven rules that an RMTM query language (QL) must fulfill and provide a foundation of a language
fulfilling all seven rules. Our QL operates on maps which may represent tuples, relations, databases or sets of
databases. Like that we dramatically expand the existing operational abstractions found in SQL and relational algebra (
RA)
which only operate on relations/tables. In fact, RA is just a special case of our much more generic approach. This
work has far-reaching consequences: we show a path how to come up with a modern QL that solves (almost if not) all
problems of SQL. Our QL is much more expressive than SQL and integrates smoothly into existing programming languages (
PL). We also show results of an initial experiment showcasing that just by switching to our data model, and without
changing the underlying query processing algorithms, we can achieve speed-ups of up to a factor 3. We will conclude
that, if we build a database system from scratch, we could and should do this without SQL, RA, RM, ERM, and ORMs.
</blockquote>


The following papers are also related. In these works we proposed to change SQL to return a subdatabase and how to
perform query processing and optimization accordingly:

<a id="4">[ND25]</a> Joris Nix, Jens
Dittrich. [Extending SQL to Return a Subdatabase](https://bigdata.uni-saarland.de/publications/Nix,%20Dittrich%20-%20Extending%20SQL%20to%20Return%20a%20Subdatabase.pdf).
**SIGMOD 2025**.

<blockquote>
Abstract:

Every SQL statement is limited to return a single, possibly denormalized table. This approximately 50-year-old design
decision has far-reaching consequences. The most apparent problem is the redundancy introduced through denormalization,
which can result in long transfer times of query results and high memory usage for materializing intermediate results.
Additionally, regardless of their goals, users are forced to fit query computations into one single result, mixing the
data retrieval and transformation aspect of SQL. Moreover, both problems violate the principles and core ideas of normal
forms. In this paper, we argue for eliminating the single-table limitation of SQL. We extend SQL's SELECT clause by the
keyword `RESULTDB' to support returning a result subdatabase. Our extension has clear semantics, i.e., by annotating any
existing SQL statement with the RESULTDB keyword, the DBMS returns the tables participating in the query, each
restricted to the relevant tuples that occur in the traditional single-table query result. Thus, we do not denormalize
the query result in any way. Our approach has significant, far-reaching consequences, impacting the querying of
hierarchical data, materialized views, and distributed databases, while maintaining backward compatibility. In addition,
our proposal paves the way for a long list of exciting future research opportunities. We propose multiple algorithms to
integrate our feature into both closed-source and open-source database systems. For closed-source systems, we provide
several SQL-based rewrite methods. In addition, we present an efficient algorithm for cyclic and acyclic join graphs
that we integrated into an open-source database system. We conduct a comprehensive experimental study. Our results show
that returning multiple individual result sets can significantly decrease the result set size. Furthermore, our rewrite
methods and algorithm introduce minimal overhead and can even outperform single-table execution in certain cases.
</blockquote>

<a id="5">[RD25]</a> Simon Rink, Jens
Dittrich. [Query Optimization for Database-Returning Queries](https://bigdata.uni-saarland.de/publications/p353-rink.pdf).
**SIGMOD 2026**.

<blockquote>
Abstract:

Recently, the novel concept of database-returning SQL queries (DRQs) was introduced. Instead of a single, (potentially)
denormalized result table, DRQs return an entire subdatabase with a single SQL query. This subdatabase represents a
subset of the original database, reduced to the relations, tuples, and attributes that contribute to the traditional
join result. DRQs offer several benefits: they reduce network traffic in client-server settings, can lower memory
requirements for materializing results, and significantly simplify querying hierarchical data. Currently, two
state-of-the-art algorithms exist to compute DRQs: (1.) ResultDB<sub>Semi-Join</sub> builds upon Yannakakis’ semi-join
reduction algorithm by adding support for cyclic queries. (2.) ResultDB_{Decompose} computes the standard single-table
result and projects the result to the base tables to obtain the resulting subdatabase. However, multiple issues can be
identified with these algorithms. First, ResultDB<sub>Semi-Join</sub> employs simple heuristics to greedily solve the underlying
enumeration problems, often leading to suboptimal query plans. Second, each algorithm performs best under different
conditions, so it is up to the user to choose the appropriate one for a given scenario. In this paper, we address these
two issues. We propose two enumeration algorithms for ResultDB<sub>Semi-Join</sub> to handle the Root Node Enumeration Problem (
RNEP) and the Tree Folding Enumeration Problem (TFEP). Further, we present a unified enumeration
algorithm, TD<sub>ResultDB</sub>, to automatically decide between plans generated by our new enumeration algorithms for ResultDB<sub>Semi-Join</sub>
and ResultDB<sub>Decompose</sub>. Through a comprehensive experimental evaluation, we show that the enumeration time overhead
introduced by our methods remains minimal. Furthermore, by effectively solving the RNEP and TFEP, we achieve up to a 6x
speed-up in query execution time for ResultDB<sub>Semi-Join</sub>, whereas TD<sub>ResultDB</sub> consistently selects the best available
plans.
</blockquote>

## Current State of the Project

This project is in an early alpha state (we started end of January 2026) and not ready for production use. It is a proof of
concept for the ideas in the paper. Yet our goal is to make it as complete as possible, and make it usable in production
environments.

### Supported features include:

- [x] attribute functions (AFs) as replacements for tuples, relations, and databases, and sets of databases
- [x] operators including simple relational algebra operators such as selection, projection, join, etc., but also
  more complex ones such as subdatabase, group-by, etc.
- [x] an observer mechanism for AFs, i.e. when an AF is updated, all AFs that depend on it are informed and can react to
  the change
- [x] support for composite primary keys
- [x] relationship functions (RFs) as replacements for n:m-relationships, i.e. they can be used to express relationships
  between AFs, e.g. one-to-many, many-to-many, etc.
- [x] a store for AFs, currently using SqliteDict as a key/blob-store, yet as it is used as a key/blob-store, we then
  cannot push down query processing
- [x] automatic swizzlling/unswizzling of references

### TODO and ongoing work:

- [ ] pipelining
- [ ] tensors
- [ ] other non-Python backends
- [ ] query optimization
- [ ] backends in other languages, e.g. Rust, C++, etc.

## Installation

for the moment there is the option to clone or download a zip of the repository and install the dependencies
through [poetry](https://python-poetry.org/), e.g. through `poetry install` in the project directory.

## Usage

see the [tutorial](docs/tutorial.md)

## Contributing

If you are interested in contributing to the project, have questions, or want to discuss anything related to funqDB,
please reach out to us.

## Authors and acknowledgment

Show your appreciation to those who have contributed to the project.

## License

This project is licenses under the AGPL-3.0 License. See the [LICENSE](LICENSE.txt) file for more details.

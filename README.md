
[![coverage report](https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/badges/main/coverage.svg)](https://gitlab.cs.uni-saarland.de/bigdata/funqdb/funqdb/-/commits/main)

# funqDB

*The goal of funqDB is to replace the relational model, relational algebra, ORMs, and SQL in the long run.*

## Core Ideas:
1. purely functional (key/value) data model
2. same modeling concept at all levels, no matter whether we are looking at “tuples“, “relations“, or “databases“, "sets of databases", etc.
3. all operators are unary: input is a function, output is a function
4. query language is a façade and part of the embedding programming language
5. no SQL injection
6. no shoehorning of groups, partitions, grouping sets, cube, outer joins, etc. into a single output relation
7. no problems with NULL: no NULL values in data, no three-valued logic
8. same power for updates as for reading
9. easily extensible
10. the notion of an "index" is built into the data model
   
**None** of this is true for SQL.


## Getting Started

### Installation

For the moment there is the option to clone or download a zip of the repository and install the dependencies
through [poetry](https://python-poetry.org/), e.g. through `poetry install` in the project directory.

### Tutorial

see the [tutorial](docs/tutorial/README.md) which is work in progress

### TLDR

see the [SQL vs FQL](benchmarks/job/queries/SQL%20vs%20FQL.md) comparison

## Background

funqDB is built around the central ideas of the vision paper:

<a id="1">[Dit26]</a> Dittrich,
Jens. <a href="https://bigdata.uni-saarland.de/publications/Dittrich%20-%20A%20Functional%20Data%20Model%20and%20Query%20Language%20is%20All%20You%20Need%20@EDBT2026.pdf">
A Functional Data Model and Query Language is All You Need.</a> In Proceedings of
the 25th International Conference on Extending Database Technology ([EDBT 2026](https://edbticdt2026.github.io)).

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

```latex
@inproceedings{dittrich2026FDMFQL,
title={A Functional Data Model and Query Language is All You Need},
author={Jens Dittrich},
booktitle = {EDBT},
year={2026},
} 
```

</blockquote>

We **highly** recommend reading that paper to understand the motivation and the ideas behind funqDB: **FDM**,
and **FQL**.
This README is not meant to be a replacement for the paper, but rather a guide to the project and its current state,
how to use it, and how to contribute.

## State of this Project

### Summary

This project is in an **early alpha state** (I started implementing end of January 2026). This project is **not** (yet)
ready for production use.
It is a proof of concept for the ideas in the paper and a playground and thought experiment and exercise how a
functional data model (FDM) and functional query language (FQL) may look like. Currently, everything is implemented in
Python (I love Python).
Hence, performance-wise the current version will obviously not be a match against data processing done in C++ or Rust.
However,
keep in mind that most data management problems are "small". If you are wondering whether your data is "big" or "small",
the likelihood is high (>95%) that your data is "small". By "small" I mean that it can be processed in memory on a
single machine, using Python, and that will be just fine: you won't feel a performance bottleneck. The latter is
actually one of the reasons why Python has become so popular for data processing in the past decade.

Anyway, notice, and as outlined in the paper, the ideas of FDM and FQL are not bound to one particular programming
language: *FQL is just a programming language façade* (in this particular case a *Python façade*) for a backend. In
the long run, I would like to have façades in other languages, as well as a C++ or Rust backend.

### Open Source

I believe that such fundamental software as data management and query processing should be open source. That is why I am
publishing the project under an [AGPL license](LICENSE.txt). As my main job is being
a [Professor of Computer Science at Saarland University](https://bigdata.uni-saarland.de/), I
have the freedom to do this. However, I also have other obligations, e.g. [teaching](https://www.youtube.com/@jensdit),
[research](https://bigdata.uni-saarland.de/publications/), administration, developing
the [Masterhorst application system](https://apply.cs.uni-saarland.de), etc., so
at this point, this cannot be a full time job (unfortunately).


### Supported features

See [`SPEC.md`](SPEC.md) for the current feature specification.
The gold standard for a supported feature is a passing test.

### Project Goals

My **mid term** goals are:

1. to have a complete implementation of the ideas in
   the <a href="https://bigdata.uni-saarland.de/publications/Dittrich%20-%20A%20Functional%20Data%20Model%20and%20Query%20Language%20is%20All%20You%20Need%20@EDBT2026.pdf">[Dit26]
   paper</a>.
2. to have a complete [tutorial](docs/tutorial) and documentation for the project.
3. to have minimal transactional processing capabilities (in Python, but this can also be done in non-Python backends):
    1. e.g. CRUD support for concurrent updates and ACID
    2. MVCC
    3. recovery
    4. database versioning as of [this paper](https://vldb.org/cidrdb/papers/2025/p24-yilmaz.pdf)
4. to have the project capable of replacing ORMs in production environments, to proof the point...:
    1. sample Django project using FDM and FQL rather than sqlite
    2. data model definitions without resorting to automatic and manual migrations
    3. POC for ResultDB extension

5. educational slide sets
6. educational videos that can be used for lectures

My **long term** goals are:

1. other backends in other languages, e.g. Rust, C++, etc. (volunteers needed)
2. other attribute functions beyond tabular data, e.g. tensors, etc.  (volunteers needed)
3. to have a complete implementation of the ideas in the [ND25] and [RD25] papers, i.e. support for database-returning
   queries and query optimization for database-returning  (volunteers needed)

### Tests

All tests are located in the `tests` directory. You can run all tests through `pytest` in the project directory, e.g.
through
`pytest tests`. You can also run individual test files, e.g. `pytest tests/test_attribute_functions.py`.

The tests also serve as a good starting point to understand how to use the project, as they contain a lot of
textbook-style code examples. I often re-use the examples from the tests in the [tutorial](docs/tutorial/README.md).

### Podcast

[Listen to the podcast](https://bigdata.uni-saarland.de/publications/Replacing_SQL_Tables_With_Pure_Functions_01.mp3) — an English-language dialogue summarizing the core ideas of the [Dit26] paper ([script](docs/podcast/podcast_script.md))

*Note: Both the podcast audio and script were generated with AI assistance.*

### Talks

see [talks](docs/Talks.md)

### Videos

will be published [here](https://www.youtube.com/jensdit)


---

## Project History

The <a href="https://bigdata.uni-saarland.de/publications/Dittrich%20-%20A%20Functional%20Data%20Model%20and%20Query%20Language%20is%20All%20You%20Need%20@EDBT2026.pdf">[Dit26]
paper</a> was preceded by a couple of previous version of that paper (with quite some variation in content):

<a id="3">[Dit25b]</a> Jens
Dittrich. [A Functional Data Model and Query Language is All You Need](https://arxiv.org/abs/2507.20671). arXiv:
2507.20671 [cs.DB].
*This paper also contains a lot of Python/FQL code examples.*

<blockquote>
Abstract:

We propose the vision of a functional data model (FDM) and an associated functional query language (FQL). Our proposal
has far-reaching consequences: we show a path to come up with a modern QL that solves (almost if not) all problems of
SQL (NULL-values, impedance mismatch, SQL injection, missing querying capabilities for updates, etc.). FDM and FQL are
much more expressive than the relational model and SQL. In addition, in contrast to SQL, FQL integrates smoothly into
existing programming languages. In our approach both QL and PL become the "same thing", thus opening up some interesting
holistic optimization opportunities between compilers and databases. In FQL, we also do not need to force application
developers to switch to unfamiliar programming paradigms (like SQL or datalog): developers can stick with the
abstractions provided by their programming language.

```latex
@misc{dittrich2025functionaldatamodelquery,
title={A Functional Data Model and Query Language is All You Need},
author={Jens Dittrich},
year={2025},
eprint={2507.20671},
archivePrefix={arXiv},
primaryClass={cs.DB},
url={https://arxiv.org/abs/2507.20671},
}
```

</blockquote>

But all of this started with this one: this was a thought experiment to explore the ideas that eventually led to the
vision papers [Dit25b] and then [Dit26]. It
contains a lot of code examples and is a good read to understand the motivation and the ideas behind funqDB.

<a id="2">[Dit25a]</a> Jens
Dittrich. [How to get Rid of SQL, Relational Algebra, the Relational Model, ERM, and ORMs in a Single Paper -- A Thought Experiment](http://arxiv.org/abs/2504.12953)
arXiv:2504.12953 [cs.DB]

<blockquote>
Abstract:

Without any doubt, the relational paradigm has been a huge success. At the same time, we believe that the time is ripe
to rethink how database systems could look like if we designed them from scratch. Would we really end up with the same
abstractions and techniques that are prevalent today? This paper explores that space. We discuss the various issues with
both the relational model(RM) and the entity-relationship model (ERM). We provide a unified data model: the relational
map type model (RMTM) which can represent both RM and ERM as special cases and overcomes all of their problems. We
proceed to identify seven rules that an RMTM query language (QL) must fulfill and provide a foundation of a language
fulfilling all seven rules. Our QL operates on maps which may represent tuples, relations, databases or sets of
databases. Like that we dramatically expand the existing operational abstractions found in SQL and relational algebra
(RA) which only operate on relations/tables. In fact, RA is just a special case of our much more generic approach. This
work has far-reaching consequences: we show a path how to come up with a modern QL that solves (almost if not) all
problems of SQL. Our QL is much more expressive than SQL and integrates smoothly into existing programming languages (
PL). We also show results of an initial experiment showcasing that just by switching to our data model, and without
changing the underlying query processing algorithms, we can achieve speed-ups of up to a factor 3. We will conclude
that, if we build a database system from scratch, we could and should do this without SQL, RA, RM, ERM, and ORMs.

```latex
@misc{dittrich2025ridsqlrelationalalgebra,
title={How to get Rid of SQL, Relational Algebra, the Relational Model, ERM, and ORMs in a Single Paper -- A Thought Experiment},
author={Jens Dittrich},
year={2025},
eprint={2504.12953},
archivePrefix={arXiv},
primaryClass={cs.DB},
url={https://arxiv.org/abs/2504.12953},
}
```

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

```latex
@inproceedings{Nix2025ExtendingSQL,
title={Extending SQL to Return a Subdatabase},
author={Joris Nix and Jens Dittrich},
year={2025}
booktitle = {SIGMOD},
publisher = {ACM}
}
```

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
reduction algorithm by adding support for cyclic queries. (2.) ResultDB<sub>Decompose</sub> computes the standard
single-table
result and projects the result to the base tables to obtain the resulting subdatabase. However, multiple issues can be
identified with these algorithms. First, ResultDB<sub>Semi-Join</sub> employs simple heuristics to greedily solve the
underlying
enumeration problems, often leading to suboptimal query plans. Second, each algorithm performs best under different
conditions, so it is up to the user to choose the appropriate one for a given scenario. In this paper, we address these
two issues. We propose two enumeration algorithms for ResultDB<sub>Semi-Join</sub> to handle the Root Node Enumeration
Problem (
RNEP) and the Tree Folding Enumeration Problem (TFEP). Further, we present a unified enumeration
algorithm, TD<sub>ResultDB</sub>, to automatically decide between plans generated by our new enumeration algorithms for
ResultDB<sub>Semi-Join</sub>
and ResultDB<sub>Decompose</sub>. Through a comprehensive experimental evaluation, we show that the enumeration time
overhead
introduced by our methods remains minimal. Furthermore, by effectively solving the RNEP and TFEP, we achieve up to a 6x
speed-up in query execution time for ResultDB<sub>Semi-Join</sub>, whereas TD<sub>ResultDB</sub> consistently selects
the best available
plans.

```latex
@inproceedings{RinkD2026,
title={Query Optimization for Database-Returning Queries},
author={Simon Rink and Jens Dittrich},
issue_date = {December 2025},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
volume = {3},
number = {6},
journal = {Proc. ACM Manag. Data},
url = {https://doi.org/10.1145/3769818},
doi = {10.1145/3769818},
}
```

</blockquote>

## License

This project is licenses under the AGPL-3.0 License. See the [LICENSE](LICENSE.txt) file for more details.

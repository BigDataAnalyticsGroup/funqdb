# funqDB

## Background

funqDB is a project build around the central ideas of the vision paper:

<a id="1">[Dit26]</a> Dittrich, Jens. "A Functional Data Model and Query Language is All You Need." In Proceedings of
the
25th International Conference on Extending Database Technology (**[EDBT 2026](https://edbticdt2026.github.io/)**).

<details>

<summary>Abstract</summary>
<div style="background-color: lightgray!important">
We propose the vision of a functional data model (FDM) and an associated functional query
language (FQL). Our proposal has far-reaching consequences: we show a path to come up with
a modern query language (QL) that solves (almost if not) all problems of SQL (NULL-values,
type marshalling, SQL injection, missing querying capabilities for updates, etc.). FDM and
FQL are much more expressive than the relational model and SQL. In addition, in contrast to
SQL, FQL integrates smoothly into existing programming languages. In our approach both QL
and PL become the ‘same thing’, thus opening up several interesting holistic optimization
opportunities between compilers and databases.</div>
</details>

We **highly** recommend reading that paper to understand the motivation and the ideas behind funqDB, FDM, and FQL.
This README is not meant to be a replacement for the paper, but rather a guide to the project and its current state,
how to use it, and how to contribute.

That paper,[Dit26], was preceded by a couple of other papers, including:

<a id="2">[Dit25b]</a> Jens
Dittrich. [How to get Rid of SQL, Relational Algebra, the Relational Model, ERM, and ORMs in a Single Paper -- A Thought Experiment](http://arxiv.org/abs/2504.12953)
arXiv:2504.12953 [cs.DB]
*Initially, this was a thought experiment to explore the ideas that eventually led to the vision paper [Dit26]. It
contains a lot of code examples and is a good read to understand the motivation and the ideas behind funqDB.*

<a id="3">[Dit25a]</a>Jens
Dittrich. [A Functional Data Model and Query Language is All You Need](https://arxiv.org/abs/2507.20671). arXiv:
2507.20671 [cs.DB].
*This paper also contains a lot of code examples.*

The following papers are also related, in these works we proposed to change SQL to return a subdatabase and how to
perform query processing and optimization accordingly:

<a id="4">[ND25]</a> Joris Nix, Jens
Dittrich. [Extending SQL to Return a Subdatabase](https://bigdata.uni-saarland.de/publications/Nix,%20Dittrich%20-%20Extending%20SQL%20to%20Return%20a%20Subdatabase.pdf).
**SIGMOD 2025**.

<a id="5">[RD25]</a> Simon Rink, Jens
Dittrich. [Query Optimization for Database-Returning Queries](https://bigdata.uni-saarland.de/publications/p353-rink.pdf).
**SIGMOD 2026**.

## Current State

This project is in an early alpha state (started end of January 2026) and not ready for production use. It is a proof of
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

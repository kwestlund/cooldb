# CoolDB: Embedded Database for Java

[![Latest release](https://img.shields.io/github/release/kwestlund/cooldb.svg)](https://github.com/kwestlund/cooldb/releases/latest)
[![javadoc](https://javadoc.io/badge2/com.cooldb/cooldb/javadoc.svg)](https://javadoc.io/doc/com.cooldb/cooldb)
[![build](https://github.com/kwestlund/cooldb/workflows/build/badge.svg)](https://github.com/kwestlund/cooldb/actions?query=workflow%3Abuild)

CoolDB is a pure java, zero dependency, embedded database system.

Some technical highlights:

* [ACID](https://en.wikipedia.org/wiki/ACID) compliant transactions.

* [B+tree](https://en.wikipedia.org/wiki/B%2B_tree) indexing (multi-column and non-unique support) with support for ACID
  transactions.

* Intelligent paging using combination of clocked-LRU and
  hint-based [replacement strategy](https://en.wikipedia.org/wiki/Page_replacement_algorithm#Clock). Enforces
  the [WAL](https://en.wikipedia.org/wiki/Write-ahead_logging) protocol by force-writing logs prior to page flushing.

* Recovery manager/log writer implements
  the [ARIES](https://en.wikipedia.org/wiki/Algorithms_for_Recovery_and_Isolation_Exploiting_Semantics)
  recovery method. Handles transaction rollback during normal processing and guarantees the atomicity and durability
  properties of transactions in the fact of process, transaction, system and media failures.

* Mixed method [2PL](https://en.wikipedia.org/wiki/Two-phase_locking)
  and [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) concurrency control. Transactions may run
  at two [levels of isolation](https://en.wikipedia.org/wiki/Isolation_(database_systems)): Serializable and Repeatable
  Read.

* Thread safe: each transaction session can be run in its own thread.

> CoolDB was designed to serve as the foundation for an RDBMS.

## Adding CoolDB to your build

CoolDB's Maven group ID is `com.cooldb`, and its artifact ID is `cooldb`.

To add a dependency on CoolDB using Maven, use the following:

```xml
<dependency>
    <groupId>com.cooldb</groupId>
    <artifactId>cooldb</artifactId>
    <version>1.0.0</version>
</dependency>
```

To add a dependency using Gradle:

```gradle
dependencies {
  implementation("com.cooldb:cooldb:1.0.0")
}
```

## Usage

See the latest [CoolDB API](https://javadoc.io/doc/com.cooldb/cooldb) javadocs for an overview and examples.

## History

CoolDB was originally written in Objective-C on a [NeXTSTEP slab](https://en.wikipedia.org/wiki/NeXTstation) back in
1992, then ported to C++, and finally to its current form in Java.

> CoolDB was developed for research purposes

The technology contained in this library is based on countless hours spent in the stacks of
MIT's [Barker Engineering Library](https://libraries.mit.edu/barker/) devouring years
of [ACM SIGMOD](https://sigmod.org) and [VLDB](https://dl.acm.org/conference/vldb) publications concerning then
state-of-the-art database technology.

## Fun Fact

CoolDB, by twist of fate, ran (and may still be currently running) in
the [algorithmic trading](https://en.wikipedia.org/wiki/Algorithmic_trading) systems of a large Wall Street brokerage
accounting for up to 2% of the US Equity Market daily trading volume.

## Future Work

Ideas for future research and work on CoolDB:

1. Grouping functions
1. Joining functions (nested-loop, sort-merge, hash)
1. SQL parser and query optimizer
1. Partitioning and distributed transactions
1. Various access and storage methods

# Influences

1. [Relational Model of Data for Large Shared Data Banks](http://avid.cs.umass.edu/courses/645/s2006/codd.pdf)
1. [System R: Relational Approach to Database Management](https://dl.acm.org/doi/pdf/10.1145/320455.320457)
1. [Buffer Management in Relational Database Systems](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.137.8362&rep=rep1&type=pdf)
1. [Granularity of Locks and Degrees of Consistency in a Shared Data Base](https://citeseerx.ist.psu.edu/viewdoc/download;jsessionid=FDF91C045A64BF3AEA3000AD006818A7?doi=10.1.1.92.8248&rep=rep1&type=pdf)
1. [An Evaluation of Buffer Management Strategies for Relational Database Systems](http://www.vldb.org/conf/1985/P127.PDF)
1. [Starburst Mid-Flight: As the Dust Clears](http://citeseerx.ist.psu.edu/viewdoc/download;jsessionid=368F5072014DFD0DA7EC7D1FB6FC8A7D?doi=10.1.1.874.9020&rep=rep1&type=pdf)
1. [ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging](https://web.stanford.edu/class/cs345d-01/rl/aries.pdf)
1. [ARIES/lM: An Efficient and High Concurrency index Management Method Using Write-Ahead Logging](https://www.ics.uci.edu/~cs223/papers/p371-mohan.pdf)
1. [An In-Depth Analysis of Concurrent B-Tree Algoritms](https://apps.dtic.mil/sti/pdfs/ADA232287.pdf)
1. [Order Preserving Key Compression](http://bitsavers.trailing-edge.com/pdf/dec/tech_reports/CRL-94-3.pdf)

## Licensing

This project is licensed under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).

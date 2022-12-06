# jepsen.history

[Jepsen](https://jepsen.io) tests distributed and concurrent systems safety. To
analyze these systems, Jepsen uses a *history*: a totally ordered log of
concurrent logical operations. This Clojure library provides support for
various tools (including Jepsen itself) that work with histories. Its
documentation also describes the structure and semantics of histories, so that
other people can interpret them.

[![Clojars Project](https://img.shields.io/clojars/v/io.jepsen/history.svg)]

## What's Here

- [jepsen.history](src/jepsen/history.clj) provides an Op datatype and a family
  of history types with efficient get-by-index, invocation-completion mapping,
  optimized concurrent folds, and memory-efficient, lazy map/filter, plus
  dependency-aware futures. It uses...
- [jepsen.history.fold](src/jepsen/history/fold.clj) is a stateful executor for
  running linear and concurrent folds over large chunked collections (like
  histories). It automatically fuses together concurrent folds into fewer
  passes over the underlying dataset.
- [jepsen.history.task](src/jepsen/history/task.clj) offers a dependency-aware,
  transactional ThreadPoolExecutor for compute-bound tasks (like analysis of
  histories).

## Example

A short, real history from a transactional test of a system might be:

```clj
[{:type :invoke, :f :txn, :value [[:w 2 1]], :time 3291485317, :process 0, :index 0}
 {:type :invoke, :f :txn, :value [[:r 0 nil] [:w 1 1] [:r 2 nil] [:w 1 2]], :time 3296209422, :process 2, :index 1}
 {:type :fail, :f :txn, :value [[:r 0 nil] [:w 1 1] [:r 2 nil] [:w 1 2]], :time 3565403674, :process 2, :index 2, :error [:duplicate-key "etcdserver: duplicate key given in txn request"]}
 {:type :ok, :f :txn, :value [[:w 2 1]], :time 3767733708, :process 0, :index 3}]
```

This shows two concurrent transactions executed by process 0 and 2,
respectively; process 0's transaction attempted to write key 2 = 1 and
succeeded, but process 2's transaction tried to read 0, write key 1 = 1, read
2, then write key 1 = 2, and failed.

## Operation Structure

Each element of a history is called an *operation*. Operations are maps with
several well-defined keys:

The `:type` must be one of `:invoke`, `:ok`, `:info`, or `:fail`. `:invoke`
denotes the start of a logical operation; `:ok` denotes its successful
completion. `:fail` denotes its definite failure. `:info` denotes an
indeterminate result: the operation either may or may not have completed. For
historical reasons, `:info` is also used for all operations performed by the
`:nemesis` process, which performs fault injection. This has been somewhat
awkward, and may change in the future.

The `:time` is a long: The time since the start of the test, in nanoseconds.

The `:process` is a logical identifier for the process executing this
operation, like `2` or `:nemesis`. Processes are logically singlethreaded, and
can execute only one thing at a time. If a process crashes (e.g. by beginning
an `:invoke` followed by an `:info`) it can never invoke an operation again.

The `:f` is a function being applied. This may be any object, but is often
something like `:read`, `:write`, `:txn`, `:dequeue`, `:kill`, etc. The
interpretation of `:f` is generally up to the user.

The `:value` stores arguments to and/or return values from the operation. For
instance, the value of a read might be the value read from the database (and
for the invocation of the read, is typically left `nil`. For a write, it might
be the value written *to* the database.

The `:index` is a unique, monotonically ascending integer identifying this
operation in the history. In most histories these are usually 0, 1, 2, ...,
which is helpful for encoding operations in an array or integer-based data
structure, then mapping back to full operations later. However, users may omit
indices (e.g. for testing), or filter a history to a subset of operations
(which makes indices sparse). We make efforts to support both cases.

An `:error` field encodes error information, and is often helpful for debugging
why an operation returned `:info` or `:fail`.

Operations may contain any number of additional fields, which users are free to
interpret as they like. For instance, one might store `:debug` information
related to the operation's execution, or the `:version` of the database
interacted with.

## Operation Semantics

There are two basic flavors of process: *clients*, which have integer
`:process` identifiers, and nemeses, which have other `:process`
identifiers--for instance `:process :nemesis`. Clients interact with the
logical state of the system. Nemeses introduce faults and other meta-level
operations into a system, and are not *supposed* to have any impact on logical
safety.

An operation which completes with `:ok` *must* logically affect the system. An operation which completes with `:fail` *must* have no logical effect on the system. An `:info` may do either.

For historical reasons, nemeses use `:info` when they invoke, and `:info` when
they complete an operation. This is sort of awkward, but changing it will
require reviewing huge swaths of checker code that assumes `:info`s can't
affect system state.

The logical semantics of an operation--what it would mean to execute that
operation in an abstract model of the system--should, in general, depend only
on its `:f` and `:value`.

## What Do Checkers Need?

Checkers often need to do things like:

- Compute a derived history based on an input history, perform an analysis on
  that derived history, then map the operations we find *back* to operations in
  the original history for presentation.

- Reindex a history which has sparse or missing indices

- Access an operation in a history by index, even if indices are sparse

- Map each operation of a history into a new kind of operation

- Filter a history to just operations from clients, or strip out failed
  operations

- Reduce over a history in as few passes as possible, computing auxiliary data
  structures

- Partition a history into several histories, one per key, and analyze each
  partition separately.

- Perform a series of reductions over a history, ideally in parallel, using
  data from each reduction stage to influence the second stage, and possibly a
  third, etc.

## Library Goals

Overall this library aims for a small-but-useful footprint which de-duplicates
analysis routines that are often split up between Jepsen itself and checkers
like Knossos.

Operations in a history are always represented by defrecords, which offer
improved performance and map-like semantics. For ease-of-use at the REPL and
when serializing to EDN, they print like normal maps.

Histories are essentially "vectors, plus some auxiliary data structures to make
querying faster". They are immutable, persistent structures. They transparently
wrap an ordered collection of operations and behave like vectors of operations
in terms of count, nth, seq, reduce, and so on. Like other Clojure collections,
their hashes and equality semantics are based purely on their elements, not
their internal structure or metadata. This makes writing tests and working at
the REPL easy. Histories are drop-in replacements for existing Jepsen checkers
written to work on vectors or seqs.

However, unlike vectors, you can ask a history to do things like "find the
invocation corresponding to this completion", and it'll do that fast.

This library tries to be reasonably fast (at least for Clojure)--supporting
parallel techniques, memoization, and compact data structures.

Realistic history sizes are up to, say, a hundred million operations. It might
be nice to go bigger, but right now it's not critical.

In general, analyses of Jepsen histories is done in-memory. Recent work in
Jepsen has brough us to streaming-capable on-disk storage, but basically every
checker is going to immediately materialize huge chunks of the history in RAM.
I'd like to explore doing analyses meaningfully *without* materializing the
whole history in memory. This library has been written with an eye towards
writing checkers that never materialize the whole dataset in RAM, and to fuse
together multiple reductions into a single IO pass over the data.

## License

Copyright © 2022 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.

(ns jepsen.history
  "Support functions for working with histories.

  We provide a dedicated Op defrecord which speeds up the most commonly
  accessed fields.

  We also provide a special History datatype. This type wraps an ordered
  collection, and acts like a vector. However, it also provides
  automatically-computed, memoized auxiliary structures, like a pair index
  which allows callers to quickly map between invocations and completions.

  ## Scheduling

  There are several challenges for Jepsen checkers.

  1. We have no idea how many other checkers are executing, or what kind of
     data they intend to compute. This causes duplicated work and many passes
     over the underlying history.

  2. We're running on fixed hardware with a finite pool of CPUs, and want to
     spawn a reasonable number of tasks, take advantage of at least *some*
     memory and cache locality, etc.

  3. Checkers want to be able to ask for the results of a fold at any time, and
     block until it's ready.

  4. But a regular function call like `reduce` won't work: a caller might do
     (reduce a history) (reduce b history) and we wouldn't know that a and b
     could have been executed in one pass.

  5. Sometimes we want delay-like behavior. Histories can declare, for
     instance, that they have a pair index or a count available, but those
     computations shouldn't be performed until someone asks for them.

  6. Sometimes we want future-like behavior: we know we'll use the results of a
     computation, and starting it now is more efficient than waiting for
     someone to ask for it.

  All of this suggests to me that the normal approaches to concurrent execution
  (e.g. just spawning a bunch of futures or delays) are not going to work: we
  need a new, richer kind of control flow here. The need to coordinate between
  callers who are not aware of each other also tells us that whatever executes
  folds should be a shared, mutable thing rather than a pure, immutable
  structure. For example, we might want:

                  +-------------------------+   +-----------------------+
                  |    Original History     |---|   Dataflow Executor   |
                  +-------------------------+   +-----------------------+
                            ^     ^
                  +---------|     |---------+
                  |                         |
    +-------------------------+    +--------------------------+
    | History w/just clients  |    |  History w/just writes   |
    +-------------------------+    +--------------------------+
        ^                               ^
        +--Build a pair index           +--Build a pair index
        |                               |
        +--With that, find G1a          +--With that, compute latencies
        |
        +--Count ok ops

  Two different checkers construct different histories derived from the main
  history--the left checker makes a view just with client ops, and the right
  checker makes a view with just writes. Both are implemented as lazy views
  over the original history.

  Then the clients start performing queries. They submit these queries to their
  individual histories, which in turn pass them up (with some translation) to
  the original history, which hands them to the dataflow executor. Let's say:

  1. The left checker asks for G1a first. The dataflow executor realizes it
     needs to compute a pair index first, so it begins that pass over the raw
     history.

  2. The left checker asks for a count of OK ops. The executor completes its
     pair-index pass of the first chunk, checks its state, and realizes that
     since neither has a dependency on the other, these operations can be
     unified into a single pass. It counts the first chunk, then merges the
     pair-index and count folds into a single fold and begins the remaining
     chunks.

  3. The right checker asks for latencies. Since this depends on the pair
     index, which is currently under computation, the executor defers the fold
     for later.

  4. The executor completes its first pass and delivers the results of the pair
     index and count. It discovers that the G1a and latencies can also be
     computed in a single pass, constructs a merged fold, and begins executing
     them. Once they complete, their results are delivered to the two checkers.

  This pre-emption and merging of fold passes is important: without it, we
  would need callers to block and coordinate when they actually asked for
  results."
  (:require [clojure.core.reducers :as r]
            [dom-top.core :refer [assert+ loopr]]
            [jepsen.history.core :refer [AbstractVector]]
            [potemkin :refer [def-abstract-type
                              definterface+
                              deftype+]])
  (:import (clojure.core.reducers CollFold)
           (clojure.lang Associative
                         Counted
                         IHashEq
                         IPersistentCollection
                         IPersistentStack
                         IPersistentVector
                         IReduce
                         IReduceInit
                         Indexed
                         Reversible
                         Seqable
                         Sequential)))

;; Operations

(defrecord Op [^:int index ^:long time type process f value])

(defn op
  "Constructs an operation. With one argument, expects a map, and turns that
  map into an Op record, which is somewhat faster to work with."
  [op]
  (map->Op op))

(defn invoke?
  "Is this op an invocation?"
  [op]
  (identical? :invoke (:type op)))

(defn ok?
  "Is this op OK?"
  [op]
  (identical? :ok (:type op)))

(defn fail?
  "Is this op a failure?"
  [op]
  (identical? :fail (:type op)))

(defn info?
  "Is this op an informational message?"
  [op]
  (identical? :info (:type op)))

(defn Op->map
  "Turns an Op back into a plain old map"
  [^Op op]
  (when op (into {} op)))

(defn client-op?
  "Is this an operation from a client? e.g. does it have an integer process."
  [op]
  (integer? (:process op)))

;; Histories

(definterface+ History
  (dense-indices? [history]
                  "Returns true if indexes in this history are 0, 1, 2, ...")

  (get-index [history ^long index]
             "Returns the operation with the given index in this history.
             For densely indexed histories, this is just like `nth`. For sparse
             histories, it may not be the nth op!")

  (completion [history invocation]
              "Takes an invocation operation belonging to this history, and
              returns the operation which invoked it, or nil if none did.")

  (invocation [history completion]
              "Takes a completion operation and returns the operation which
              invoked it, or nil if none did."))

;; Dense histories. These have indexes 0, 1, ..., and allow for direct,
;; efficient traversal.

(deftype+ DenseHistory
  [; Any vector-like collection
   ops
   ; A delayed int array mapping invocations to completions, or -1 where no
   ; link exists.
   pair-index]

  AbstractVector

  clojure.lang.Counted
  (count [this]
    (count ops))

  clojure.lang.IReduceInit
  (reduce [this f init]
          (reduce ops f init))

  clojure.lang.Indexed
  (nth [this i not-found]
    (nth ops i not-found))

  clojure.lang.IPersistentVector
  (assocN [this i op]
          ; You're not allowed to alter the process, f, or index
          (assert+ (= i (:index op))
                   {:type  ::wrong-index
                    :index i
                    :op    op})
          (let [extant (nth this i)]
            (assert+ (= (:process extant) (:process op))
                     {:type ::can't-change-process
                      :op   extant
                      :op'  op})
            (assert+ (= (:f extant) (:f op))
                     {:type ::can't-change-f
                      :op   extant
                      :op'  op}))
          ; But obey those rules, and the pair index is still valid!
          (DenseHistory. (assoc op i op) pair-index))

  clojure.lang.Reversible
  (rseq [this]
    (rseq ops))

  clojure.lang.Seqable
  (seq [this]
    (seq ops))

  CollFold
  (coll-fold [this n combinef reducef]
    (r/coll-fold ops n combinef reducef))

  History
  (dense-indices? [this]
    true)

  (get-index [this index]
    (nth ops index))

  (completion [this invocation]
    (assert+ (or (invoke? completion)
                 ; Non-clients are allowed to invoke with :info
                 (and (not (client-op? completion))
                      (info? completion)))
             IllegalArgumentException)
    (let [i (aget ^ints @pair-index (:index invocation))]
      (when-not (= -1 i)
        (nth ops i))))

  (invocation [this completion]
    (assert+ (not (invoke? completion)) IllegalArgumentException)
    (let [i (aget ^ints @pair-index (:index completion))]
      (when-not (= -1 i)
        (nth ops i))))

  Iterable
  (forEach [this consumer]
    (.forEach ^Iterable ops consumer))

  (iterator [this]
    (.iterator ^Iterable ops))

  (spliterator [this]
    (.spliterator ^Iterable ops))

  Object
  (equals [this other]
    (.equiv this other))

  (hashCode [this]
    (.hashCode ops))

  (toString [this]
    (.toString ops)))


(defn ^ints dense-history-pair-index
  "Computes an array mapping indexes back and forth for a dense history of ops.
  For non-client operations, we map pairs of :info messages back and forth."
  [ops]
  (let [pair-index (int-array (count ops))]
    (loopr [; A map of processes to the index of the op they just
            ; invoked
            invokes (transient {})]
           [op ops]
           (let [p (:process op)]
             (case (:type op)
               :invoke
               (if-let [invoke (get invokes p)]
                 (throw (ex-info
                          (str "Process " p " is still executing "
                               (pr-str invoke) " and cannot invoke "
                               (pr-str op))
                          {:type       ::double-invoke
                           :op         op
                           :running-op invoke}))
                 (recur (assoc! invokes p op)))

               :info
               (if (client-op? op)
                 ; For clients, you need to have invoked something
                 (if-let [invoke (get invokes p)]
                   (do (aset-int pair-index (:index invoke) (:index op))
                       (aset-int pair-index (:index op) (:index invoke))
                       (recur (dissoc! invokes p)))
                   (throw (ex-info (str "Client " p " logged an :info without invoking anything: " (pr-str op))
                          {:type    ::info-without-invoke
                           :op      op})))
                 ; For non-clients, match successive pairs of infos
                 (if-let [invoke (get invokes p)]
                   ; Second
                   (do (aset-int pair-index (:index invoke) (:index op))
                       (aset-int pair-index (:index op) (:index invoke))
                       (recur (dissoc! invokes p)))
                   ; First
                   (recur (assoc! invokes p op))))

               (:ok, :fail)
               (if-let [invoke (get invokes p)]
                 (do (aset-int pair-index (:index invoke) (:index op))
                     (aset-int pair-index (:index op) (:index invoke))
                     (recur (dissoc! invokes p)))
                 (throw (ex-info (str "Process " p " can not complete "
                                     (pr-str op) "without invoking it first")
                                 {:type ::complete-without-invoke
                                  :op   op})))))
           ; Any remaining invokes, fill in -1
           (doseq [op (-> invokes persistent! vals)]
             (aset-int pair-index (:index op) -1)))
    pair-index))

(defn check-dense-indices
  "Checks to make sure that a history is densely indexed, and throws if not.
  Returns history."
  [history]
  (if (and (instance? History history)
           (dense-indices? history))
    ; Already checked
    history
    ; Gotta double-check
    (loopr [i 0]
           [op history]
           (if (= i (:index op))
             (recur (inc i))
             (throw (ex-info (str "History not densely indexed! At index " i
                                  ", op was" op)
                    {:type ::not-densely-indexed
                     :i    i
                     :op   op})))
           history)))

(defn dense-history
  "A dense history has indexes 0, 1, 2, ..., and can encode its pair index in
  an int array. You can provide a history, or a vector (or any
  IPersistentVector), or a reducible, in which case the reducible is
  materialized to a vector."
  [ops]
  (let [; Materialize if necessary.
        ops (if (vector? ops)
              ops
              (into [] ops))]
    (check-dense-indices ops)
    (DenseHistory. ops (delay (dense-history-pair-index ops)))))

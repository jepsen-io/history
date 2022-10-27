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
  (:refer-clojure :exclude [map filter remove])
  (:require [clojure [core :as c]
                     [pprint :refer [pprint]]]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ loopr]]
            [jepsen.history [core :refer [AbstractVector]]
                            [fold :as f]]
            [potemkin :refer [def-abstract-type
                              definterface+
                              deftype+]]
            [tesser [core :as tesser]
                    [utils :as tesser.utils]])
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
                         Sequential)
           (io.lacuna.bifurcan IEntry
                               IList
                               IMap
                               IntMap
                               Map
                               Maps
                               List)
           (java.util Arrays)))

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

;; Common folds

(def pair-index-fold
  "A fold which builds a pair index, as an IntMap."
  ; In our reduce pass, we build up an IntMap partial pair index, as
  ; well as an unmatched head and tail, each a map of processes to
  ; completions/invocations, which should be stitched together with
  ; neighboring chunks. Non-client ops are stored for use by the
  ; combiner.
  {:reducer-identity (fn reducer-identity []
                       [(.linear (IntMap.)) ; pairs
                        (.linear (Map.))    ; head
                        (.linear (Map.))    ; tail
                        (.linear (List.))]) ; non-client
   :reducer
   (fn reducer [[pairs head tail non-client] op]
     (if-not (client-op? op)
       [pairs head tail (.addLast non-client op)]
       ; Client op
       (let [p (:process op)]
         (if (invoke? op)
           ; Invoke
           (let [invoke (.get tail p nil)]
             (assert+ (nil? invoke)
                      {:type    ::double-invoke
                       :op      op
                       :running invoke})
             [pairs head (.put tail p op) non-client])
           ; Complete
           (let [invoke (.get tail p nil)
                 i0     (:index invoke)
                 i1     (:index op)]
             (if invoke
               ; Have invocation
               [(.. pairs (put i0 i1) (put i1 i0))
                head (.remove tail p) non-client]
               ; Probably in the previous chunk. Put it in the head
               (do (assert+ (not (.contains head p))
                            {:type      ::double-complete
                             :op        op
                             :completed (.get head p nil)})
                   [pairs (.put head p op) tail non-client])))))))
   :combiner-identity (fn combiner-identity []
                        [(.linear (IntMap.)) ; pairs
                         (.linear (Map.))])  ; running
   :combiner
   (fn combiner [[^IntMap pairs, ^IMap running]
                 [chunk-pairs head tail non-client]]
     (let [; Merge pairs
           pairs (.merge pairs chunk-pairs Maps/MERGE_LAST_WRITE_WINS)
           ; Complete running operations using head
           [pairs running]
           (loopr [pairs'   pairs
                   running' running]
                  [op (.values head)]
                  (let [p      (:process op)
                        invoke (.get running p nil)
                        i0     (:index invoke)
                        i1     (:index op)]
                    (assert+ invoke {:type ::not-running, :op op})
                    (recur (.. pairs' (put i0 i1) (put i1 i0))
                           (.remove running' p))))
           ; Handle non-client ops
           [pairs running]
           (loopr [pairs'   pairs
                   running' running]
                  [op non-client]
                  (let [p (:process op)]
                    (if-let [invoke (.get running p nil)]
                      ; Complete
                      (let [i0 (:index invoke)
                            i1 (:index op)]
                        (recur (.. pairs' (put i0 i1) (put i1 i0))
                               (.remove running p)))
                      ; Begin
                      (recur pairs' (.put running p op)))))

           ; Begin running operations using tail
           running (.merge running tail Maps/MERGE_LAST_WRITE_WINS)]
       [pairs running]))
   :post-combiner
   (fn post-combiner [[pairs running]]
     ; Finish off running ops with nil
     (loopr [pairs' pairs]
            [op (.values running)]
            (recur (.put pairs' (:index op) nil))
            (.forked pairs')))})

;; Histories

(definterface+ History
  (dense-indices? [history]
                  "Returns true if indexes in this history are 0, 1, 2, ...")

  (get-index [history ^long index]
             "Returns the operation with the given index in this history.
             For densely indexed histories, this is just like `nth`. For sparse
             histories, it may not be the nth op!")

  (^long pair-index [history index]
         "Given an index, returns the index of that operation's
         corresponding invocation or completion. -1 means no match.")

  (completion [history invocation]
              "Takes an invocation operation belonging to this history, and
              returns the operation which invoked it, or nil if none did.")

  (invocation [history completion]
              "Takes a completion operation and returns the operation which
              invoked it, or nil if none did.")

  (fold [history fold]
        "Executes a fold on this history. See history.fold/fold for details.")

  (tesser [history tesser-fold]
          "Executes a Tesser fold on this history. See history.fold/tesser for
          details."))

;; Dense histories. These have indexes 0, 1, ..., and allow for direct,
;; efficient traversal.

(deftype+ DenseHistory
  [; Any vector-like collection
   ops
   ; A folder over ops
   folder
   ; A delayed int array mapping invocations to completions, or -1 where no
   ; link exists.
   pair-index]

  AbstractVector

  clojure.lang.Counted
  (count [this]
    (count ops))

  clojure.lang.IReduceInit
  (reduce [this f init]
          (reduce f init ops))

  clojure.lang.Indexed
  (nth [this i not-found]
    (nth ops i not-found))

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

  (pair-index [this index]
    (aget ^ints @pair-index index))

  (completion [this invocation]
    (assert+ (or (invoke? invocation)
                 ; Non-clients are allowed to invoke with :info
                 (and (not (client-op? invocation))
                      (info? invocation)))
             IllegalArgumentException
             (str "Expected an invocation, but got " (pr-str invocation)))
    (let [i (.pair-index this (:index invocation))]
      (when-not (= -1 i)
        (nth ops i))))

  (invocation [this completion]
    (assert+ (not (invoke? completion))
             IllegalArgumentException
             (str "Can't ask for the invocation of an invocation: "
                  (pr-str completion)))
    (let [i (aget ^ints @pair-index (:index completion))]
      (when-not (= -1 i)
        (nth ops i))))

  (fold [this fold]
        (f/fold folder fold))

  (tesser [this tesser-fold]
          (f/tesser folder tesser-fold))

  Iterable
  (forEach [this consumer]
    (.forEach ^Iterable ops consumer))

  (iterator [this]
    (.iterator ^Iterable ops))

  (spliterator [this]
    (.spliterator ^Iterable ops))

  Object
  (hashCode [this]
            (.hashCode ops))

  (toString [this]
    (.toString ops)))

(defn ^ints dense-history-pair-index
  "Given a folder of ops, computes an array mapping indexes back and forth for
  a dense history of ops. For non-client operations, we map pairs of :info
  messages back and forth."
  [folder]
  (let [^IntMap m (f/fold folder pair-index-fold)
        ; Translate back to ints
        ary (int-array (.size m))]
    (loopr []
           [^IEntry kv m]
           (let [i (.key kv)
                 j (.value kv)]
             (if j
               (do (aset-int ary i j)
                   (aset-int ary j i))
               (aset-int ary i -1))
             (recur)))
    ary))

(defn index-ops
  "Adds sequential indices to a series of operations. Throws if there are
  existing indices that wouldn't work with this."
  [ops]
  (if (and (instance? History ops)
           (dense-indices? ops))
    ; Already checked
    ops
    ; Go through em
    (loopr [ops' (transient [])
            i    0]
           [op ops]
           (if-let [index (:index op)]
             (if (not= index i)
               (throw (ex-info {:type ::existing-different-index
                                :i    i
                                :op   op}))
               (recur (conj! ops' op) (inc i)))
             (recur (conj! ops' (assoc op :index i))
                    (inc i)))
           (persistent! ops'))))

(defn dense-history*
  "Constructs a dense history around a collection of operations. Does not check
  to make sure the ops are already dense; you have to do that yourself."
  [ops]
  (let [folder (f/folder ops)]
    (DenseHistory. ops
                   folder
                   (delay (dense-history-pair-index folder)))))

(defn dense-history
  "A dense history has indexes 0, 1, 2, ..., and can encode its pair index in
  an int array. You can provide a history, or a vector (or any
  IPersistentVector), or a reducible, in which case the reducible is
  materialized to a vector."
  [ops]
  (let [; Materialize if necessary.
        ops (if-not (indexed? ops)
              ops
              (into [] ops))
       ops (index-ops ops)]
    (dense-history* ops)))

(deftype+ MappedHistory [history map-fn]
  AbstractVector

  clojure.lang.Counted
  (count [this]
    (count history))

  clojure.lang.IReduceInit
  (reduce [this f init]
          (reduce ((c/map map-fn) f) init history))

  clojure.lang.Indexed
  (nth [this i not-found]
    (map-fn (nth history i not-found)))

  clojure.lang.Reversible
  (rseq [this]
    (c/map map-fn (rseq history)))

  clojure.lang.Seqable
  (seq [this]
       ; Map always returns a LazySeq (), rather than nil.
       (let [s (c/map map-fn history)]
         (when (seq s)
           s)))

  CollFold
  (coll-fold [this n combinef reducef]
    (r/coll-fold history n combinef ((c/map map-fn) reducef)))

  History
  (dense-indices? [this]
    (dense-indices? history))

  (get-index [this index]
    (map-fn (get-index history index)))

  ; Because map-fn preserves indices, we can use our parent's pair index.
  (pair-index [this index]
              (.pair-index history index))

  ; Because map-fn preserves indices, we can use our parent's pair index.
  (completion [this invocation]
    (assert+ (or (invoke? invocation)
                 ; Non-clients are allowed to invoke with :info
                 (and (not (client-op? invocation))
                      (info? invocation)))
             IllegalArgumentException
             (str "Expected an invocation, but got " (pr-str invocation)))
    (let [i (pair-index this (:index invocation))]
      (when-not (= -1 i)
        (.nth this i))))

  (invocation [this completion]
    (assert+ (not (invoke? completion))
             IllegalArgumentException
             (str "Can't ask for the invocation of an invocation: "
                  (pr-str completion)))
    (let [i (pair-index this (:index completion))]
      (when-not (= -1 i)
        (.nth this i))))

  (fold [this fold]
        (let [fold    (f/make-fold fold)
              reducer (:reducer fold)
              fold    (assoc fold :reducer
                             (fn fold-map [acc x]
                               (reducer acc (map-fn x))))]
          (.fold history fold)))

  (tesser [this tesser-fold]
          (let [fold' (into (tesser/map map-fn) tesser-fold)]
            (tesser history fold')))

  Iterable
  (forEach [this consumer]
           (.forEach (c/map map-fn history) consumer))

  (iterator [this]
            (.iterator (c/map map-fn history)))

  (spliterator [this]
    (.spliterator (c/map map-fn history))))

(defn map
  "Specialized, lazy form of clojure.core/map which acts on histories
  themselves. Wraps the given history in a new one which has its operations
  transformed by (f op). Applies f on every access to an op. This is faster
  when you intend to traverse the history only a few times, or when you want to
  keep memory consumption low.

  f is assumed to obey a few laws, but we don't enforce them, in case it turns
  out to be useful to break these rules later. It should return operations just
  like its inputs, and with the same :index, so that pairs are preserved. It
  should also be injective on processes, so that it preserves the concurrency
  structure of the original. If you violate these rules, get-index, invocation,
  and completion will likely behave strangely."
  [f history]
  (MappedHistory. history f))

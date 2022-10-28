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
                     [pprint :as pprint :refer [pprint
                                                pprint-logical-block
                                                pprint-newline
                                                print-length-loop]]]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ loopr]]
            [jepsen.history [core :refer [AbstractVector]]
                            [fold :as f]
                            [task :as task]]
            [potemkin :refer [def-abstract-type
                              definterface+
                              deftype+]]
            [slingshot.slingshot :refer [try+ throw+]]
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
           (java.io Writer)
           (java.util Arrays)))

;; Operations

(defrecord Op [^:int index ^:long time type process f value])

(alter-meta! #'pprint/*current-length* #(dissoc % :private))
(defn pprint-kv
  "Helper for pretty-printing op fields."
  [^Writer out k v]
  (pprint/write-out k)
  (.write out " ")
  ;(pprint-newline :linear)
  (set! pprint/*current-length* 0)
  (pprint/write-out v)
  (.write out ", ")
  (pprint-newline :linear))

; We're going to print a TON of these, and it's really just noise to see the Op
; record formatting.
; Why is this not public if you need it to print properly?
(defmethod pprint/simple-dispatch jepsen.history.Op
  [^Op op]
  (let [^Writer out *out*]
  ; See https://github.com/clojure/clojure/blob/5ffe3833508495ca7c635d47ad7a1c8b820eab76/src/clj/clojure/pprint/dispatch.clj#L105
    (pprint-logical-block :prefix "{" :suffix "}"
      (pprint-kv out :process (.process op))
      (pprint-kv out :type    (.type op))
      (pprint-kv out :f       (.f op))
      (pprint-kv out :value   (.value op))
      ; Other fields
      (print-length-loop [pairs (seq (.__extmap op))]
        (when pairs
          (pprint-logical-block
            (pprint/write-out (ffirst pairs))
            (.write out " ")
            (pprint-newline :linear)
            (pprint/write-out (fnext (first pairs))))
          (when-let [pairs' (next pairs)]
            (.write out ", ")
            (pprint-newline :linear)
            (recur pairs'))))
      ; Always at the end
      (pprint-kv out :index   (.index op))
      (pprint-kv out :time    (.time op))
    )))

(defmethod print-method jepsen.history.Op [op ^java.io.Writer w]
  (.write w (str (into {} op))))

(defn op
  "Constructs an operation. With one argument, expects a map, and turns that
  map into an Op record, which is somewhat faster to work with. If op is
  already an Op, returns it unchanged."
  [op]
  (if (instance? Op op)
    op
    (map->Op op)))

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

(defn assert-invoke+
  "Throws if something is not an invocation, or, for non-client operations, an
  info. Otherwise returns op."
  [op]
  (assert+ (or (invoke? op)
               ; Non-clients are allowed to invoke with :info
               (and (not (client-op? op))
                    (info? op)))
           IllegalArgumentException
           (str "Expected an invocation, but got " (pr-str op)))
  op)

(defn assert-complete
  "Throws if something is not a completion. Otherwise returns op."
  [op]
  (assert+ (not (invoke? op))
           IllegalArgumentException
           (str "Expected a completion, but got " (pr-str op)))
  op)

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
   (fn reducer [[^IntMap pairs, ^IMap head, ^IMap tail, ^IList non-client],
                ^Op op]
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
                 [^IntMap chunk-pairs, ^IMap head, ^IMap tail, non-client]]
     (let [; Merge pairs
           pairs (.merge pairs chunk-pairs Maps/MERGE_LAST_WRITE_WINS)
           ; Complete running operations using head
           [pairs running]
           (loopr [^IntMap pairs'   pairs
                   ^IMap   running' running]
                  [op (.values head)]
                  (let [p      (:process op)
                        invoke (.get running p nil)
                        i0     (:index invoke)
                        i1     (:index op)]
                    (assert+ invoke {:type ::not-running, :op op})
                    (recur (.. pairs' (put i0 i1) (put i1 i0))
                           (.remove running' p))))
           ; Handle non-client ops
           [pairs ^IMap running]
           (loopr [^IntMap pairs'   pairs
                   ^IMap   running' running]
                  [op non-client]
                  (let [p (:process op)]
                    (if-let [invoke (.get running' p nil)]
                      ; Complete
                      (let [i0 (:index invoke)
                            i1 (:index op)]
                        (recur (.. pairs' (put i0 i1) (put i1 i0))
                               (.remove running' p)))
                      ; Begin
                      (recur pairs' (.put running' p op)))))

           ; Begin running operations using tail
           running (.merge running tail Maps/MERGE_LAST_WRITE_WINS)]
       [pairs running]))
   :post-combiner
   (fn post-combiner [[pairs ^IMap running]]
     ; Finish off running ops with nil
     (loopr [^IntMap pairs' pairs]
            [^Op op (.values running)]
            (recur (.put pairs' (.index op) (Integer. -1)))
            (.forked pairs')))})

;; Histories

(definterface+ IHistory
  (dense-indices? [history]
                  "Returns true if indexes in this history are 0, 1, 2, ...")

  (get-index [history ^long index]
             "Returns the operation with the given index in this history, or
             nil if that operation is not present. For densely indexed
             histories, this is just like `nth`. For sparse histories, it may
             not be the nth op!")

  (^long pair-index [history ^long index]
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

(definterface+ Taskable
  (executor [this]
            "Returns the history's task executor. See jepsen.history.task for
            details. All histories descending from the same history
            (e.g. via map or filter) share the same task executor.")

  (task! [this name f]
         [this name deps f]
         [this name data deps f]
         "Launches a Task on this history's task executor. Use this to perform
         parallel, compute-bound processing of a history in a dependency
         tree--for instance, to perform multiple folds over a history at the
         same time. See jepsen.history.task/submit! for details.")

  (catch-task! [this name dep f]
               [this name data dep f]
               "Adds a catch task to this history which handles errors on the
               given dep. See jepsen.history.task/catch!.")

  (cancel-task! [this task]
                "Cancels a task on the this history's task executor. Returns
                task."))


(def-abstract-type AbstractHistory
  IHistory
  (completion [this invocation]
              (assert-invoke+ invocation)
              (let [i (.pair-index this (:index invocation))]
                (when-not (= -1 i)
                  (.get-index this i))))

  (invocation [this completion]
              (assert-complete completion)
              (let [i (.pair-index this (:index completion))]
                (when-not (= -1 i)
                  (.get-index this i))))

  Taskable
  (executor [this] executor)

  (task! [this name f]
         (task/submit! executor name f))

  (task! [this name deps f]
         (task/submit! executor name deps f))

  (task! [this name data deps f]
         (task/submit! executor name data deps f))

  (catch-task! [this name dep f]
               (task/catch! executor name dep f))

  (catch-task! [this name data dep f]
               (task/catch! executor name data dep f))

  (cancel-task! [this task]
                (task/cancel! executor task)))

(defn add-dense-indices
  "Adds sequential indices to a series of operations. Throws if there are
  existing indices that wouldn't work with this."
  [ops]
  (if (and (instance? IHistory ops)
           (dense-indices? ops))
    ; Already checked
    ops
    ; Go through em
    (loopr [ops' (transient [])
            i    0]
           [op ops]
           (if-let [index (:index op)]
             (if (not= index i)
               (throw+ {:type ::existing-different-index
                        :i    i
                        :op   op})
               (recur (conj! ops' op) (inc i)))
             (recur (conj! ops' (assoc op :index i))
                    (inc i)))
           (persistent! ops'))))

(defn assert-indices
  "Ensures every op has an :index field. Throws otherwise."
  [ops]
  (if (instance? IHistory ops)
    ops
    (loopr []
           [op ops]
           (do (when-not (integer? (:index op))
                 (throw+ {:type ::no-integer-index
                          :op op}))
               (recur))
           ops)))

(defn preprocess-ops
  "When we prepare a history around some operations, we need to ensure they
  have indexes, belong to indexed collections, and so on. This takes a
  collection of ops, and optionally an option map, and returns processed ops.
  These ops are guaranteed to:

  - Have :index fields
  - Be records
  - Be in a Clojure Indexed collection

  Options are:

    :have-indices?  If true, these ops already have :index fields.
    :already-ops?   If true, these ops are already Op records."
  ([ops]
   (preprocess-ops ops {}))
  ([ops {:keys [have-indices? already-ops?]}]
   (let [; First, lift into an indexed collection
         ops (if (indexed? ops)
               ops
               (vec ops))
         ; Then ensure they're Ops
         ops (if already-ops?
               ops
               (mapv op ops))
         ; And that they have indexes
         ops (if have-indices?
               ops
               (assert-indices ops))]
     ops)))

;; Dense histories. These have indexes 0, 1, ..., and allow for direct,
;; efficient traversal.

(deftype+ DenseHistory
  [; Any vector-like collection
   ops
   ; A folder over ops
   folder
   ; Our executor
   executor
   ; A delayed int array mapping invocations to completions, or -1 where no
   ; link exists.
   pair-index]

  AbstractVector
  AbstractHistory

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

  IHistory
  (dense-indices? [this]
    true)

  (get-index [this index]
    (nth ops index))

  (pair-index [this index]
    (aget ^ints @pair-index index))

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
             (aset-int ary i j)
             (when (not= -1 j)
               (aset-int ary j i))
             (recur)))
    ary))

(defn dense-history
  "A dense history has indexes 0, 1, 2, ..., and can encode its pair index in
  an int array. You can provide a history, or a vector (or any
  IPersistentVector), or a reducible, in which case the reducible is
  materialized to a vector. Options are:

    :have-indices?  If true, these ops already have :index fields.
    :already-ops?   If true, these ops are already Op records.
    :dense-indices? If true, indices are already dense, and need not be
                    checked.

  If given a history without indices, adds them."
  ([ops]
   (dense-history ops {}))
  ([ops {:keys [dense-indices?] :as options}]
   (let [ops    (preprocess-ops ops options)
         ops    (if dense-indices?
                  ops
                  (add-dense-indices ops))
         folder (f/folder ops)]
     (DenseHistory. ops
                    folder
                    (task/executor)
                    (delay (dense-history-pair-index folder))))))

;; Sparse histories

(deftype+ SparseHistory
  [ops         ; A sparse vector-like of operations; e.g. one where indexes
               ; aren't 0, 1, ...
   folder      ; A folder over those ops
   executor    ; Our task executor
   by-index    ; A delay of an IntMap which takes an op index to an offset in
               ; ops.
   pair-index] ; A delay of a pair index, as an IntMap.

  AbstractVector
  AbstractHistory

  clojure.lang.Counted
  (count [this]
         (count ops))

  clojure.lang.IReduceInit
  (reduce [this f init]
          (.reduce ^IReduceInit folder f init))

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
             (.coll-fold ^CollFold folder n combinef reducef))

  IHistory
  (dense-indices? [this] false)

  (get-index [this index]
             (let [i (.get ^IntMap @by-index index (Integer. -1))]
               (when-not (= i -1)
                 (nth ops i))))

  (pair-index [this index]
              (.get ^IntMap @pair-index index (Integer. -1)))

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

(def sparse-history-by-index-fold
  "A fold which computes the by-index IntMap, taking op indexes to collection
  indexes."
  (let [; In the reducer, we build a list of every :index encountered
        reducer
        (fn reducer
          ([] (.linear (List.)))
          ([^IList indices, op]
           (.addLast indices (:index op))))
        ; And in the combiner, we stitch those into a reverse-indexed map,
        ; keeping track of the starting index of each chunk.
        combiner
        (fn combiner
          ([] [0 (.linear (IntMap.))])
          ([[i, ^IntMap m]] (.forked m))
          ([[^long i, ^IntMap m] chunk-indices]
           (loopr [j  0
                   ^IntMap m' m]
                  [index chunk-indices]
                  (recur (inc j) (.put m' (long index) ^Object (+ i j)))
                  [(+ i j) m])))]
    {:name     :sparse-history-by-index
     :reducer  reducer
     :combiner combiner}))

(defn sparse-history
  "Constructs a sparse history backed by the given collection of ops. Options:

    :have-indices?  If true, these ops already have :index fields.
    :already-ops?   If true, these ops are already Op records."
  ([ops]
   (sparse-history ops {}))
  ([ops options]
   (let [ops        (preprocess-ops ops options)
         folder     (f/folder ops)
         by-index   (delay (f/fold folder sparse-history-by-index-fold))
         pair-index (delay (f/fold folder pair-index-fold))]
     (SparseHistory. ops folder (task/executor) by-index pair-index))))

(defn history
  "Just make a history out of something. Figure it out. Options are:

    :have-indices?  If true, these ops already have :index fields.
    :already-ops?   If true, these ops are already Op records.
    :dense-indices? If true, indices are already dense.

  With :dense-indices?, we'll assume indices are dense and construct a dense
  history. With have-indices? we'll use existing indices and construct a sparse
  history. Without either, we examine the first op and guess: if it has no
  :index, we'll assign sequential ones and construct a dense history. If the
  first op does have an :index, we'll use the existing indices and construct a
  sparse history."
  ([ops]
   (history ops {}))
  ([ops {:keys [dense-indices? have-indices?] :as options}]
   (cond dense-indices?
         (dense-history ops options)

         have-indices?
         (sparse-history ops options)

         ; Guess about indices
         (integer? (:index (first ops)))
         (sparse-history ops options)

         ; No indices; we'll give them dense ones.
         true
         (dense-history ops options))))

;; Mapped histories

(deftype+ MappedHistory [^IHistory history map-fn executor]
  AbstractVector
  AbstractHistory

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

  IHistory
  (dense-indices? [this]
    (dense-indices? history))

  (get-index [this index]
    (map-fn (get-index history index)))

  ; Because map-fn preserves indices, we can use our parent's pair index.
  (pair-index [this index]
              (.pair-index history index))

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
           (.forEach ^Iterable (c/map map-fn history) consumer))

  (iterator [this]
            (.iterator ^Iterable (c/map map-fn history)))

  (spliterator [this]
    (.spliterator ^Iterable (c/map map-fn history))))

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
  and completion will likely behave strangely.

  A mapped history inherits the same task executor as the original history."
  [f history]
  (MappedHistory. history f (executor history)))

;; Filtered histories

(deftype+ FilteredHistory [^IHistory history ; Underlying history
                           pred    ; Filtering predicate
                           executor ; Task executor
                           count-  ; Delayed count
                           indices ; Delayed int array of all op indices
                           ]
  AbstractVector
  AbstractHistory

  CollFold
  (coll-fold [this n combinef reducef]
             (r/coll-fold history n combinef ((c/filter pred) reducef)))

  ; We're not counted, so we'll do IPersistentCollection instead
  clojure.lang.IPersistentCollection
  (count [this] @count-)

  IReduceInit
  (reduce [this f init]
          (.reduce ^IReduceInit history ((c/filter pred) f) init))

  Reversible
  (rseq [this]
        (c/filter pred (rseq history)))

  Seqable
  (seq [this]
       ; Might be empty!
       (let [s (c/filter pred (seq history))]
         (when (seq s)
           s)))

  IHistory
  (dense-indices? [this] false)

  (get-index [this index]
             (when-let [op (get-index history index)]
               (when (pred op)
                 op)))

  (pair-index [this index]
              ; We need to constrain the pair index to not return values for
              ; elements not in the set, so we have to actually fetch the
              ; element and check it here.
              (let [i (pair-index history index)]
                (if (= -1 i)
                  -1
                  (let [op (.get-index this index)]
                    (if (pred op) i -1)))))

  ; To avoid double-fetching during our safety check in pair-index, we provide
  ; custom logic here too rather than relying on AHistory.
  (completion [this invocation]
              (assert-invoke+ invocation)
              (let [i (.pair-index this (:index invocation))]
                (when-not (= -1 i)
                  (let [op (.get-index this i)]
                    (when (pred op) op)))))

  (invocation [this completion]
              (assert-complete completion)
              (let [i (.pair-index this (:index completion))]
                (when-not (= -1 i)
                  (let [op (.get-index this i)]
                    (when (pred op) op)))))

  (fold [this fold]
        (let [fold    (f/make-fold fold)
              reducer (:reducer fold)
              fold (assoc fold :reducer
                          (fn fold-filter [acc x]
                            (if (pred x)
                              (reducer acc x)
                              acc)))]
          (.fold history fold)))

  (tesser [this tesser-fold]
          (let [fold (into (tesser/filter pred) tesser-fold)]
            (tesser history fold)))

  clojure.lang.Indexed
  (nth [this i not-found]
       (let [^ints a @indices]
         (when-not (< -1 i (alength a))
           (throw (IndexOutOfBoundsException. (str "Index " i " out of bounds"))))
         (.get-index this (aget a i))))

  Iterable
  (forEach [this consumer]
            (.forEach ^Iterable (c/filter pred history) consumer))

  (iterator [this]
            (.iterator ^Iterable (c/filter pred history)))

  (spliterator [this]
               (.spliterator ^Iterable (c/filter pred history))))

(def filtered-history-indices-fold
  "A fold which computes an int array of all the indexes in a filtered
  history."
  (let [; Build up lists
        reducer (fn reducer
                  ([] (.linear (List.)))
                  ([l] l)
                  ([^IList indices, op]
                   (.addLast indices (:index op))))
        ; Concat together, then convert to array
        combiner (fn combiner
                   ([] (.linear (List.)))
                   ([^IList indices]
                    (let [ary (int-array (.size indices))]
                      (loopr [i 0]
                             [^Integer index indices]
                             (do (aset ary i index)
                                 (recur (unchecked-inc-int i))))
                      ary))
                   ([^IList indices, ^IList chunk-indices]
                    (.concat indices chunk-indices)))]
        {:name     :filtered-history-indices
         :reducer  reducer
         :combiner combiner}))

(defn filter
  "Specialized, lazy form of clojure.core/filter which acts on histories and
  returns histories. Wraps the given history in a new one which only has
  operations passing (pred op).

  A filtered history inherits the task executor of the original."
  [f history]
  (let [count- (delay (->> (tesser/filter f)
                           (tesser/count)
                           (tesser history)))
        indices (delay (fold history
                             (update filtered-history-indices-fold
                                     :reducer
                                     (c/filter f))))]
    (FilteredHistory. history f (task/executor) count- indices)))

(defn remove
  "Inverse of filter"
  [f history]
  (filter (complement f) history))

(defn client-ops
  "Filters a history to just clients."
  [history]
  (filter client-op? history))

(defn invokes
  "Filters a history to just invocations."
  [history]
  (filter invoke? history))

(defn oks
  "Filters a history to just :ok ops"
  [history]
  (filter ok? history))

(defn infos
  "Filters a history to just :info ops."
  [history]
  (filter info? history))

(defn fails
  "Filters a history to just :fail ops"
  [history]
  (filter fail? history))

(defn possible
  "Filters a history to just :ok or :info ops"
  [history]
  (filter (fn possible? [op]
            (or (ok? op) (fail? op)))
          history))

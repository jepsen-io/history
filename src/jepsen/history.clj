(ns jepsen.history
  "Support functions for working with histories. This provides two things you
  need for writing efficient checkers:

  1. A dedicated Op defrecord which speeds up the most commonly accessed
  fields and reduces memory footprint.

  2. A History datatype which generally works like a vector, but also supports
  efficient fetching of operations by index, mapping back and forth between
  invocations and completions, efficient lazy map/filter, fusable concurrent
  reduce/fold, and a dependency-oriented task executor.

  ## Ops

  Create operations with the `op` function. Unlike most defrecords, we
  pretty-print these as if they were maps--we print a LOT of them.
  ```
    (require '[jepsen.history :as h])
    (def o (h/op {:process 0, :type :invoke, :f :read, :value [:x nil],
                  :index 0, :time 0}))
    (pprint o)
    ; {:process 0,
    ;  :type :invoke,
    ;  :f :read,
    ;  :value [:x nil],
    ;  :index 0,
    ;  :time 0}
  ```
  We provide a few common functions for interacting with operations:
  ```
    (invoke? o)    ; true
    (client-op? o) ; true
    (info? o)      ; false
  ```
  And of course you can use fast field accessors here too:
  ```
    (.process o) ; 0
  ```
  ## Histories

  Given a collection of operations, create a history like so:
  ```
    (def h (h/history [{:process 0, :type :invoke, :f :read}
                       {:process 0, :type :ok, :f :read, :value 5}]))
  ```
  `history` automatically lifts maps into Ops if they aren't already, and adds
  indices (sequential) and times (-1) if you omit them. There are options to
  control how indices are added; see `history` for details.
  ```
    (pprint h)
    ; [{:process 0, :type :invoke, :f :read, :value nil, :index 0, :time -1}
    ;  {:process 0, :type :ok, :f :read, :value 5, :index 1, :time -1}]
  ```
  If you need to convert these back to plain-old maps for writing tests, use
  `as-maps`.
  ```
    (h/as-maps h)
    ; [{:index 0, :time -1, :type :invoke, :process 0, :f :read, :value nil}
    ;  {:index 1, :time -1, :type :ok, :process 0, :f :read, :value 5}]
  ```
  Histories work almost exactly like vectors (though you can't assoc or conj
  into them).
  ```
    (count h)
    ; 2
    (nth h 1)
    ; {:index 1, :time -1, :type :ok, :process 0, :f :read, :value 5}
    (map :type h)
    ; [:invoke :ok]
  ```
  But they have a few extra powers. You can get the Op with a particular :index
  regardless of where it is in the collection.
  ```
    (h/get-index h 0)
    ; {:index 0, :time -1, :type :invoke, :process 0, :f :read, :value nil}
  ```
  And you can find the corresponding invocation for a completion, and
  vice-versa:
  ```
    (h/invocation h {:index 1, :time -1, :type :ok, :process 0, :f :read,
                     :value 5})
    ; {:index 0, :time -1, :type :invoke, :process 0, :f :read, :value nil}

    (h/completion h {:index 0, :time -1, :type :invoke, :process 0, :f :read, :value nil})
    ; {:index 1, :time -1, :type :ok, :process 0, :f :read, :value 5}
  ```
  We call histories where the :index fields are 0, 1, 2, ... 'dense', and other
  histories 'sparse'. With dense histories, `get-index` is just `nth`. Sparse
  histories are common when you're restricting yourself to just a subset of the
  history, like operations on clients. If you pass sparse indices to `(history
  ops)`, then ask for an op by index, it'll do a one-time fold over the ops to
  find their indices, then cache a lookup table to make future lookups fast.
  ```
    (def h (history [{:index 3, :process 0, :type :invoke, :f :cas,
                      :value [7 8]}]))
    (h/dense-indices? h)
    ; false
    (get-index h 3)
    ; {:index 3, :time -1, :type :invoke, :process 0, :f :cas, :value [7 8]}
  ```
  Let's get a slightly more involved history. This one has a concurrent nemesis
  crashing while process 0 writes 3.
  ```
    (def h (h/history
             [{:process 0, :type :invoke, :f :write, :value 3}
              {:process :nemesis, :type :info, :f :crash}
              {:process 0, :type :ok, :f :write, :value 3}
              {:process :nemesis, :type :info, :f :crash}]))
  ```
  Of course we can filter this to just client operations using regular seq
  operations...
  ```
    (filter h/client-op? h)
    ; [{:process 0, :type :invoke, :f :write, :value 3, :index 0, :time -1}
    ;  {:process 0, :type :ok, :f :write, :value 3, :index 2, :time -1}]
  ```
  But `jepsen.history` also exposes a more efficient version:
  ```
    (h/filter h/client-op? h)
    ; [{:index 0, :time -1, :type :invoke, :process 0, :f :write, :value 3}
    ;  {:index 2, :time -1, :type :ok, :process 0, :f :write, :value 3}]
  ```
  There are also shortcuts for common filtering ops: `client-ops`, `invokes`,
  `oks`, `infos`, and so on.
  ```
    (def ch (h/client-ops h))
    (type ch)
    ; jepsen.history.FilteredHistory
  ```
  Creating a filtered history is O(1), and acts as a lazy view on top of the
  underlying history. Like `clojure.core/filter`, it materializes elements as
  needed. Unlike Clojure's `filter`, it does not (for most ops) cache results
  in memory, so we can work with collections bigger than RAM. Instead, each
  seq/reduce/fold/etc applies the filtering function to the underlying history
  on-demand.

  When you ask for a count, or to fetch operations by index, or to map between
  invocations and completions, a FilteredHistory computes a small, reduced data
  structure on the fly, and caches it to make later operations of the same type
  fast.
  ```
    (count ch) ; Folds over entire history to count how many match the predicate
    ; 2
    (count ch) ; Cached

    ; (h/completion ch (first ch)) ; Folds over history to pair up ops, caches
    {:index 2, :time -1, :type :ok, :process 0, :f :write, :value 3}

    ; (h/get-index ch 2) ; No fold required; underlying history does get-index
    {:index 2, :time -1, :type :ok, :process 0, :f :write, :value 3}
  ```
  Similarly, `h/map` constructs an O(1) lazy view over another history. These
  compose just like normal Clojure `map`/`filter`, and all share structure with
  the underlying history.

  ### Folds

  All histories support `reduce`, `clojure.core.reducers/fold`, Tesser's
  `tesser`, and `jepsen.history.fold/fold`. All four mechanisms are backed by
  a `jepsen.history.fold` Folder, which allows concurrent folds to be joined
  together on-the-fly and executed in fewer passes over the underlying data.
  Reducers, Tesser, and history folds can also be executed in parallel.

  Histories created with `map` and `filter` share the folder of their
  underlying history, which means that two threads analyzing different views of
  the same underlying history can have their folds joined into a single pass
  automatically. This should hopefully be invisible to users, other than making
  things Automatically Faster.

  If you filter a history to a small subset of operations, or are comfortable
  working in-memory, it may be sensible to materialize a history. Just use
  `(vec h)` to convert a history a plain-old Clojure vector again.

  ### Tasks

  Analyzers often perform several independent reductions over a history, and
  then compute new values based on those previous reductions. You can of course
  use `future` for this, but histories also come with a shared,
  dependency-aware threadpool executor for executing compute-bound concurrent
  tasks. All histories derived from the same history share the same executor,
  which means multiple checkers can launch tasks on it without launching a
  bazillion threads. For instance, we might need to know if a history includes
  crashes:
  ```
    (def first-crash (h/task h find-first-crash []
      (->> h (h/filter (comp #{:crash} :f)) first)))
  ```
  Like futures, deref'ing a task yields its result, or throws.
  ```
    @first-crash
    {:index 1, :time -1, :type :info, :process :nemesis, :f :crash,
     :value nil}
  ```
  Unlike futures, tasks can express *dependencies* on other tasks:
  ```
    (def ops-before-crash (h/task h writes [fc first-crash]
      (let [i (:index first-crash)]
        (into [] (take-while #(< (:index %) i)) h))))
  ```
  This task won't run until first-crash has completed, and receives the result
  of the first-crash task as its argument.
  ```
    @ops-before-crash
    ; [{:index 0, :time -1, :type :invoke, :process 0, :f :write, :value 3}]
  ```
  See [[jepsen.history.task]] for more details."
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
                         Murmur3
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

(definterface+ IOp
  (index= [this, other]
          "Equality comparison by index only. Useful for speeding up large
          structures of operations.")
  (index-hash [this]
              "Hash by index only. Useful for speeding up large structures of
              operations."))

(defrecord Op [^long index ^long time type process f value]
  IOp
  (index= [this other]
    (assert (instance? Op other)
            (str "Can only compare an Op to another Op, not a "
                 (type other) ": " (pr-str other)))
    (= index (.index ^Op other)))

  (index-hash [this]
    (Murmur3/hashLong index))

  Object
  (toString [this]
    (pr-str this)))

(defn op
  "Constructs an operation. With one argument, expects a map, and turns that
  map into an Op record, which is somewhat faster to work with. If op is
  already an Op, returns it unchanged.

  Ops *must* have an index. Ops may be missing a :time; if so, we give them
  time -1."
  [op]
  (if (instance? Op op)
    op
    (do (assert+ (:index op) "Ops require a long :index field")
        (Op/create (if (contains? op :time)
                     op
                     (assoc op :time -1))))))

(alter-meta! #'pprint/*current-length* #(dissoc % :private))
(defn pprint-kv
  "Helper for pretty-printing op fields."
  ([^Writer out k v]
   (pprint/write-out k)
   (.write out " ")
   ;(pprint-newline :linear)
   (set! pprint/*current-length* 0)
   (pprint/write-out v)
   (.write out ", ")
   (pprint-newline :linear))
  ([^Writer out k v last?]
   (pprint/write-out k)
   (.write out " ")
   ;(pprint-newline :linear)
   (set! pprint/*current-length* 0)
   (pprint/write-out v)))

; We're going to print a TON of these, and it's really just noise to see the Op
; record formatting.
; Why is this not public if you need it to print properly?
(defmethod pprint/simple-dispatch jepsen.history.Op
  [^Op op]
  (let [^Writer out *out*]
  ; See https://github.com/clojure/clojure/blob/5ffe3833508495ca7c635d47ad7a1c8b820eab76/src/clj/clojure/pprint/dispatch.clj#L105
    (pprint-logical-block :prefix "{" :suffix "}"
      (let [pairs (seq (.__extmap op))]
        (pprint-kv out :process (.process op))
        (pprint-kv out :type    (.type op))
        (pprint-kv out :f       (.f op))
        (pprint-kv out :value   (.value op))
        ; Other fields
        (print-length-loop [pairs pairs]
                           (when pairs
                             (pprint-logical-block
                               (pprint/write-out (ffirst pairs))
                               (.write out " ")
                               (pprint-newline :linear)
                               (pprint/write-out (fnext (first pairs))))
                               (.write out ", ")
                               (pprint-newline :linear)
                               (recur (next pairs))))
        ; Always at the end
        (pprint-kv out :index   (.index op))
        (pprint-kv out :time    (.time op) false)))))

(defmethod print-method jepsen.history.Op [op ^java.io.Writer w]
(.write w (str (into {} op))))

(defn op?
  "Is this op an Op defrecord?"
  [op]
  (instance? Op op))

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

(defn has-f?
  "Constructs a function which takes ops and returns true if the op has the
  given :f, or, given a set, any of the given :fs."
  [f-or-fs]
  (cond (set? f-or-fs)     (fn set [^Op op] (contains? f-or-fs (.f op)))
        (keyword? f-or-fs) (fn kw [^Op op] (identical? f-or-fs (.f op)))
        true               (fn equals [^Op op] (= f-or-fs (.f op)))))

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

(defn as-maps
  "Turns a collection of Ops back into plain old Clojure maps. Helpful for
  writing tests."
  [ops]
  (c/map (partial into {}) ops))

(defn strip-indices
  "Strips off indices from a history, returning a sequence of plain maps.
  Helpful for writing tests."
  [ops]
  (c/map (fn [op] (dissoc op :index)) ops))

(defn strip-times
  "Strips off times from a history, returning a sequence of plain maps. Helpful
  for writing tests."
  [ops]
  (c/map (fn [op] (dissoc op :time)) ops))

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

  (ensure-pair-index [history]
                     "Ensures the pair index exists. Helpful when you want to
                     use the pair index during a fold, because loading the pair
                     index itself would require another fold pass. Returns
                     history.")

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

  (task-call [this name f]
             [this name deps f]
             [this name data deps f]
             "Launches a Task on this history's task executor. Use this to
             perform parallel, compute-bound processing of a history in a
             dependency tree--for instance, to perform multiple folds over a
             history at the same time. See jepsen.history.task/submit! for
             details.")

  (catch-task-call [this name dep f]
                   [this name data dep f]
                   "Adds a catch task to this history which handles errors on
                   the given dep. See jepsen.history.task/catch!.")

  (cancel-task [this task]
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

  (ensure-pair-index [this]
                     (when-let [o ^Op (first this)]
                       (.pair-index this (.index o)))
                     this)

  Taskable
  (executor [this] executor)

  (task-call [this name f]
             (task/submit! executor name f))

  (task-call [this name deps f]
             (task/submit! executor name deps f))

  (task-call [this name data deps f]
             (task/submit! executor name data deps f))

  (catch-task-call [this name dep f]
                   (task/catch! executor name dep f))

  (catch-task-call [this name data dep f]
                   (task/catch! executor name data dep f))

  (cancel-task [this task]
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
  - Have :time  fields
  - Be Op records
  - Be in a Clojure Indexed collection

  Options are:

    :have-indices?  If true, these ops already have :index fields.
    :already-ops?   If true, these ops are already Op records."
  ([ops]
   (preprocess-ops ops {}))
  ([ops {:keys [have-indices? already-ops?]}]
   (let [; Ensure they have indexes
         ops (cond have-indices? ops
                   ; Guess
                   (integer? (:index (first ops)))
                   (assert-indices ops)

                   ; Add
                   true
                   (add-dense-indices ops))
         ; Ensure they're Ops
         ops (if already-ops?
               ops
               (mapv op ops))
         ; Finally lift into an indexed collection, in case we get a lazy seq
         ops (if (indexed? ops)
               ops
               (vec ops))]
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
           (loopr [j          0
                   ^IntMap m' m]
                  [index chunk-indices]
                  (recur (inc j) (.put m' (long index) (Long. (+ i j))))
                  [(+ i j) m])))]
    {:name     :sparse-history-by-index
     :reducer  reducer
     :combiner combiner}))

(defn sparse-history
  "Constructs a sparse history backed by the given collection of ops. Options:

    :have-indices?  If true, these ops already have :index fields.
    :already-ops?   If true, these ops are already Op records.

  Adds dense indices if the ops don't already have their own indexes."
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
  sparse history.

  Operations with missing :time fields are given :time -1."
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
  (assert+ (instance? IHistory history)
           {:type         ::not-history
            :history-type (type history)})
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

  clojure.lang.Reversible
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
  (assert+ (instance? IHistory history)
           {:type         ::not-history
            :history-type (type history)})
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
            (or (ok? op) (info? op)))
          history))

(defn filter-f
  "Filters to a specific :f. Or, given a set, a set of :fs."
  [f-or-fs history]
  (filter (has-f? f-or-fs) history))

(defn parse-task-args
  "Helper for task and catch-task which parses varargs and extracts the data,
  dep names, dep tasks, and body."
  [args]
  (let [; Figure out where the binding vector is
        could-be-binding-name? (fn [form]
                                 (or (symbol? form)
                                     (vector? form)
                                     (map? form)))
        could-be-bindings? (fn [form]
                             (and (vector? form)
                                  (even? (count form))
                                  (every? could-be-binding-name?
                                          (take-nth 2 form))))
        bindices (keep-indexed (fn [i form]
                                 (when (could-be-bindings? form)
                                   i))
                               (take 2 args))
        bindex (if (= 1 (count bindices))
                 (first bindices)
                 (throw (IllegalArgumentException.
                            "Can't guess where your binding vector is")))
        data      (when (= 1 bindex)
                    (first args))
        bindings  (nth args bindex)
        dep-names (take-nth 2 bindings)
        deps      (take-nth 2 (drop 1 bindings))
        body      (drop (inc bindex) args)]
    {:deps      deps
     :dep-names dep-names
     :data      data
     :body      body}))

(defmacro task
  "A helper macro for launching new tasks. Takes a history, a symbol for your
  task name, optional data, a binding vector of names to dependency tasks you'd
  like to depend on, and a body. Wraps body
  in a function and submits it to the task executor.
  ```
    (task history find-anomalies []
       ... go do stuff)

    (task history furthermore [as find-anomalies]
      ... do stuff with as)

    (task history data-task {:custom :data} [as anomalies, f furthermore]
      ... do stuff with as and f)
   ```"
  [history task-name & args]
  (let [{:keys [dep-names deps data body]} (parse-task-args args)]
    `(task-call ~history '~task-name ~data [~@deps]
                (fn ~(symbol (str task-name "-task")) [[~@dep-names]]
                  ~@body))))

(defmacro catch-task
  "A helper for launching new catch tasks. Takes a history, a symbol for your
  catch task's name, optional data, and a binding vector of [arg-name task],
  followed by a body. Wraps body in a function and submits it as a new catch
  task to the history's executor.

    (catch-task history oh-no [err some-task]
  (handle err ...))"
  [history task-name & args]
  (let [{:keys [dep-names deps data body]} (parse-task-args args)]
    (assert (= 1 (count deps)))
    `(catch-task-call ~history '~task-name ~data ~@deps
                      (fn ~task-name [~@dep-names]
                        ~@body))))

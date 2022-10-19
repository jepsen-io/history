(ns jepsen.history.fold
  "Represents and executes graphs of folds over histories.

  A fold represents a reduction over a history, which can optionally be
  executed over chunks concurrently. It's a map with the following fields:

    {; Metadata

     :name              The unique name of this fold. May be any object, but
                        probably a keyword.
     :dependencies      A set of names which this fold needs to have execute
                        first.

     ; How to reduce a chunk

     :reducer-identity  A function (f history) which generates an identity
                        object for a reduction over a chunk.

     :reducer           A function (f history acc op) which takes a history, a
                        chunk accumulator, and an operation from the history,
                        and returns a new accumulator.

     :post-reducer      A function (f history acc) which takes the final
                        accumulator from a chunk and transforms it before being
                        passed to the combiner

     ; How to combine chunks together

     :combiner-identity A function (f history) which generates an identity
                        object for combining chunk results together.

     :combiner          A function (f history acc chunk-result) which folds
                        the result of a chunk into the combiner's accumulator.

     :post-combiner     A function (f history acc) which takes the final acc
                        from merging all chunks and produces the fold's return
                        value.

     ; Execution hints

     :associative?      If true, we can start reducing chunks concurrently. If
                        false, we must reduce chunk 0, then 1, etc.}

  Folds should be pure functions of their histories, though reducers and
  combiners are allowed to use in-memory mutability; each is guaranteed to be
  single-threaded. The final return value from a fold should be immutable so
  that other readers or folds can use it safely in a concurrent context.

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
  results.

  ## Handles

  Users need a way to receive the results of folds, start them off, cancel
  them, and so on. To do this we introduce a new datatype: a Handle. Submitting
  a fold to the executor returns a Handle. Handles support the usual IDeref to
  block on results and receive exceptions. They also support additional
  control-flow functions for launching and cancelling folds."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen.history [core :as hc]
                            [task :as task]])
  (:import (io.lacuna.bifurcan DirectedGraph
                               IEdge
                               IGraph
                               Graph)))

; We want to run multiple folds concurrently. This FusedFold datatype allows
; that, and can be extended with new folds later. It returns a vector of
; results for each individual fold it contains.
(defrecord FusedFold
  [; These fields let us work transparently like any other fold map
   reducer-identity
   reducer
   post-reducer
   combiner-identity
   combiner
   post-combiner
   associative?
   ; But we also track a vector of the original folds we're now fusing.
   folds])

(defn fused?
  "Is this fold a fused fold?"
  [fold]
  (instance? FusedFold fold))

(defn fuse
  "Takes a fold (possibly a FusedFold) and fuses a new fold into it. Also
  provides a means of joining in-process reducer/combiner state together, so
  that you can zip together two independent folds and continue with the fused
  fold halfway through. Returns a map of:

    :fused              A new FusedFold which performs everything the original
                        fold did *plus* the new fold.

    :join-accs          A function which joins together accumulators from the
                        original and new fold. Works on both reduce and combine
                        accumulators. Returns an accumulator for the new fold.

  If this isn't fast enough, we might try doing some insane reflection and
  dynamically compiling a new class to hold reducer state with primitive
  fields."
  [old-fold new-fold]
  (let [; If we're fusing into a fused fold, we slot in our fold at the end of
        ; its existing folds.
        folds' (if (fused? old-fold)
                 (conj (:folds old-fold) new-fold)
                 ; We have no insight here; just make a pair.
                 [old-fold new-fold])
        ; The reducer identity constructs an array of reducer identities
        reducer-identity
        (fn reducer-identity []
          (->> folds'
               (map (fn [fold]
                      ((:reducer-identity fold))))
               object-array))
        ; Reducers update each field in the array. This is a *very* hot path.
        ; We pre-materialize an array of reducer functions to speed up
        ; traversal. We clobber reducer array state in-place. This SHOULD, I
        ; think, be OK because of the memory happens-before effects of the task
        ; executor's queue.
        reducers (object-array (map :reducer folds'))
        n        (alength reducers)
        reducer (fn reducer [^objects accs x]
                  (loop [i 0]
                    (if (< i n)
                      (let [reducer (aget reducers i)
                            acc     (aget accs i)
                            acc'    (reducer acc x)]
                        (aset accs i acc')
                        (recur (unchecked-inc-int i)))
                      ; Done
                      accs)))
        ; Post-reducers again update each field in the array. We can clobber it
        ; in-place.
        post-reducers (object-array (map :post-reducer folds'))
        post-reducer  (fn post-reducer [^objects accs]
                        (loop [i 0]
                          (if (< i n)
                            (let [post-reducer (aget post-reducers i)
                                  acc          (aget accs i)
                                  acc'         (post-reducer acc)]
                              (aset accs i acc')
                              (recur (unchecked-inc-int i)))
                            ; Done
                            accs)))
        ; Combiners: same deal
        combiner-identity (fn combiner-identity []
                            (->> folds'
                                 (map (fn [fold]
                                        ((:combiner-identity fold))))
                                 object-array))
        combiners (object-array (map :combiner folds'))
        combiner (fn combiner [^objects accs ^objects xs]
                   (loop [i 0]
                     (if (< i n)
                       (let [combiner (aget combiners i)
                             acc (aget accs i)
                             x   (aget xs i)
                             acc' (combiner acc x)]
                         (aset accs i acc')
                         (recur (unchecked-inc-int i)))
                       accs)))
        post-combiners (object-array (map :post-combiner folds'))
        ; Turn things back into vectors on the way out
        post-combiner (fn post-combiner [^objects accs]
                        (loop [i     0
                               accs' (transient [])]
                          (if (< i n)
                            (let [post-combiner (aget post-combiners i)
                                  acc (aget accs i)
                                  acc' (post-combiner acc)]
                              (recur (unchecked-inc-int i)
                                     (conj! accs' acc')))
                            (persistent! accs'))))
        ; Now we need functions to join together reducer and combiner state.
        ; The shape here is going to depend on whether the original fold was a
        ; FusedFold (in which case it has an array of accs) or a normal fold
        ; (in which case it has a single acc).
        join-accs (if (fused? old-fold)
                    ; Old fold has an array; new fold has a single acc.
                    (fn join-fused [^objects old-accs, new-acc]
                      (let [old-n (alength old-accs)
                            accs' (object-array (inc old-n))]
                        (System/arraycopy old-accs 0 accs' 0 old-n)
                        (aset accs' old-n new-acc)
                        accs'))
                    ; Construct tuples
                    (fn join-unfused [old-acc new-acc]
                      (object-array [old-acc new-acc])))
        fused (map->FusedFold
                {:reducer-identity  reducer-identity
                 :reducer           reducer
                 :post-reducer      post-reducer
                 :combiner-identity combiner-identity
                 :combiner          combiner
                 :post-combiner     post-combiner
                 :associative?      (and (:associative? old-fold)
                                         (:associative? new-fold))
                 :folds             folds'})]
    {:fused     fused
     :join-accs join-accs}))

(deftype Executor
  [task-executor
   history])

(defn executor
  "Starts a new executor for folds over the given chunkable history"
  [history]
  (Executor. (task/executor) history))

(defn empty-fold
  "Runs a fold over zero elements."
  [{:keys [combiner-identity post-combiner]}]
  (post-combiner (combiner-identity)))

(defn linear-fold!
  "Takes a history executor, a fold, and a chunkable collection. Expands the
  fold into a series of reductions with no combines."
  [^Executor e
   {:keys [reducer-identity
           reducer
           post-reducer
           combiner-identity
           combiner
           post-combiner]}]
  (let [exec    (.task-executor e)
        history (.history e)
        chunks  (hc/chunks history)
        ; First reduction task
        r0 (task/submit! exec [:reduce 0]
                         []
                         (fn first-reduce [_]
                           (reduce
                             reducer
                             (reducer-identity)
                             (first chunks))))
        reduces
        (loopr [; A vector of reduction tasks, one per chunk
                reduces [r0]
                ; Which chunk are we on?
                i 1]
               [chunk (rest chunks)]
               (let [; Reduction tasks
                     prev-reduce     (peek reduces)
                     prev-reduce-id  (task/id prev-reduce)
                     r (task/submit! exec
                                     [:reduce i]
                                     ; Depends only on the previous reduce
                                     [prev-reduce]
                                     (fn reduce-task [inputs]
                                       (let [acc (get inputs prev-reduce-id)]
                                         (reduce reducer acc chunk))))]
                 (recur (conj reduces r)
                        (inc i)))
               reduces)
        ; Post-reduce
        last-reduce    (peek reduces)
        last-reduce-id (task/id last-reduce)
        pr (task/submit! exec
                         :post-reduce
                         [last-reduce]
                         (fn post-reduce [inputs]
                           (let [acc (get inputs last-reduce-id)]
                             (post-reducer acc))))
        ; Combine
        combine (if (and (identical? reducer-identity
                                      combiner-identity)
                          (identical? reducer combiner))
                   ; Wouldn't be any effect; skip it
                   pr
                   ; Gotta do a singleton combine
                   (task/submit! exec
                                 :single-combine
                                 [pr]
                                 (fn single-combine [inputs]
                                   (combiner
                                     (combiner-identity)
                                     (get inputs (task/id pr))))))
        ; post-combine
        post-combine (task/submit! exec
                                   :post-combine
                                   [combine]
                                   (fn post-combine [inputs]
                                     (post-combiner
                                       (get inputs (task/id combine)))))]
    {:type         :linear
     :post-reduce  pr
     :reduces      reduces
     :combine      combine
     :post-combine post-combine
     :result       post-combine}))

(defn concurrent-fold!
  "Takes a history executor and a fold. Expands the fold into concurrent
  reduce and serial combine tasks."
  [^Executor e
   {:keys [reducer-identity
           reducer
           post-reducer
           combiner-identity
           combiner
           post-combiner]}]
  (let [exec    (.task-executor e)
        history (.history e)
        chunks  (hc/chunks history)
        ; Independent reduce tasks
        reduces
        (loopr [i       0
                reduces []]
               [chunk chunks]
               (let [task (task/submit! exec
                                        [:reduce i]
                                        nil
                                        (fn reduce-task [_]
                                          (post-reducer
                                            (reduce reducer
                                                    (reducer-identity)
                                                    chunk))))]
                 (recur (inc i) (conj reduces task)))
               reduces)
        ; Serial combines
        combines (loopr [combines []]
                        [i        (range (count reduces))]
                        (let [; Reduce and combine task we want to combine
                              r      (nth reduces i)
                              prev-c (peek combines)
                              task (task/submit!
                                     exec
                                     [:combine i]
                                     ; First combine only requires first
                                     ; reduce, later combines require earlier
                                     ; combines
                                     (if (= 0 i)
                                       [r]
                                       [r prev-c])
                                     (fn combine-task [in]
                                       (let [c-res (if (= 0 i)
                                                     (combiner-identity)
                                                     (get in (task/id prev-c)))
                                             r-res (get in (task/id r))
                                             ; Do combine
                                             res (combiner c-res r-res)]
                                         (if (= i (dec (count reduces)))
                                           ; Final stage; post-combine
                                           (post-combiner res)
                                           res))))]
                          (recur (conj combines task))))]
    {:type     :concurrent
     :reduces  reduces
     :combines combines
     :result   (peek combines)}))

(defn fold
  "Executes a fold on the given executor and returns its result."
  [^Executor executor fold]
  (let [chunks (hc/chunks (.history executor))
        chunk-count (count chunks)]
    (cond ; No chunks
          (= 0 chunk-count)
          (empty-fold fold)

          ; Linear fold
          (or (= 1 chunk-count)
              (not (:associative? fold)))
          @(:result (linear-fold! executor fold))

          ; Associative fold
          true
          @(:result (concurrent-fold! executor fold)))))

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
  (:require [clojure [pprint :as pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ loopr]]
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
   name
   reducer-identity
   reducer
   post-reducer
   combiner-identity
   combiner
   post-combiner
   associative?
   ; But we also track a vector of the original folds we're now fusing.
   folds])

; Debugging with full fn values is exhausting
(defmethod pprint/simple-dispatch jepsen.history.fold.FusedFold
  [fold]
  (.write ^java.io.Writer *out*
          (str "(FusedFold " (pr-str (:name fold))
               (when (:associative? fold)
                 " :assoc")
               ;(pr-str (:folds fold)))
               ")"
               )))

(defn fused?
  "Is this fold a fused fold?"
  [fold]
  (instance? FusedFold fold))

(defn ary->vec
  "Turns arrays into vectors recursively."
  [x]
  (if (= "[Ljava.lang.Object;" (.getName (class x)))
    (mapv ary->vec x)
    x))

(defn fuse
  "Takes a fold (possibly a FusedFold) and fuses a new fold into it. Also
  provides a means of joining in-process reducer/combiner state together, so
  that you can zip together two independent folds and continue with the fused
  fold halfway through. Returns a map of:

    :fused              A new FusedFold which performs everything the original
                        fold did *plus* the new fold. Its post-combined results
                        are a pair of [old-res new-res].

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
        split-accs (if (fused? old-fold)
                     ; Old fold has an array; new fold has a single acc.
                     (fn split-fused [^objects accs]
                       (let [old-n    (dec n)
                             old-accs (object-array old-n)]
                         (System/arraycopy accs 0 old-accs 0 old-n)
                         [old-accs (aget accs old-n)]))
                     ; Both simple folds
                     (fn split-unfused [^objects accs]
                       [(aget accs 0) (aget accs 1)]))
        ; Turn things back into a pair of [old-res new-res] on the way
        ; out.
        old-post-combiner (:post-combiner old-fold)
        new-post-combiner (:post-combiner new-fold)
        post-combiner (fn post-combiner [^objects accs]
                        (let [[old-acc new-acc] (split-accs accs)]
                          [(old-post-combiner old-acc)
                           (new-post-combiner new-acc)]))
        fused (map->FusedFold
                {:name              [(:name old-fold) (:name new-fold)]
                 :reducer-identity  reducer-identity
                 :reducer           reducer
                 :post-reducer      post-reducer
                 :combiner-identity combiner-identity
                 :combiner          combiner
                 :post-combiner     post-combiner
                 :associative?      (and (:associative? old-fold)
                                         (:associative? new-fold))
                 :folds             folds'})]
    {:fused               fused
     :join-accs           join-accs
     :split-accs          split-accs}))

; Task construction

(defn task-workers
  "Takes a Task and returns a vector of tasks which actually do work (e.g. call
  a reducer or combiner) for this particular chunk. We need this to figure out
  if tasks are safely cancellable, or if some of their work has begun."
  [task]
  (case (first (task/name task))
    (:reduce, :combine, :deliver)
    [task]

    (:split-reduce, :join-reduce, :split-combine, :join-combine)
    (:workers (task/data task))))

(def concurrent-reduce-task-unlock-factor
  "This factor controls how quickly concurrent reduce tasks 'unlock' reductions
  of later chunks. 8 means that each task unlocks roughly 8 later tasks."
  8)

(defn make-reduce-task
  "Takes a task executor state, a fold, chunks, and a vector of
  previously launched reduce tasks for this fold, and an index into the chunks
  i. Returns [state' task]: a new task to reduce that chunk.

  This is less speedy for single folds, but for multiple folds we actually want
  to *defer* starting work until later--that way the later folds have a chance
  to join and cancel our tasks. So even though we *could* rush ahead and launch
  every reduce concurrently, we inject dependencies between reduce
  tasks forming a tree: the first chunk unlocks the second and third, which
  unlock the fourth through seventh, and so on."
  [state {:keys [reducer-identity, reducer, post-reducer]} chunks
   prev-reduce-tasks i]
  (let [ordering-dep (when (seq prev-reduce-tasks)
                       (nth prev-reduce-tasks
                            (long (/ (count prev-reduce-tasks)
                                     concurrent-reduce-task-unlock-factor))))]
    (task/submit state
                 [:reduce i]
                 (when ordering-dep [ordering-dep])
                 (fn task [_]
                   ; (info :reduce i)
                   (post-reducer
                     (reduce reducer
                             (reducer-identity)
                             (nth chunks i)))))))

(defn make-combine-task
  "Takes a task executor state, a fold, chunks, a chunk index, and either:

  1. A reduce task (for the first combine)
  2. A previous combine task and a reduce task (for later combines)

  Returns [state' task]: a task which combines that chunk with earlier
  combines."
  ([state {:keys [combiner-identity combiner]} chunks i reduce-task]
   (let [n (count chunks)]
     (task/submit state
                  [:combine i]
                  [reduce-task]
                  (fn first-task [[post-reduced]]
                    ; (info :first-combine i post-reduced)
                    (let [res (combiner (combiner-identity) post-reduced)]
                      ;(info :combine i res)
                      res)))))
  ([state {:keys [combiner post-combiner]} chunks i
    prev-combine-task reduce-task]
   (let [n (count chunks)]
     (task/submit state
                  [:combine i]
                  [prev-combine-task reduce-task]
                  (fn task [[prev-combine post-reduced]]
                    ; (info :combine i prev-combine post-reduced)
                    (let [res (combiner prev-combine post-reduced)]
                      ;(info :combine i res)
                      res))))))

(defn make-deliver-task
  "Takes a task executor state, a final combine task, and a function which
  delivers results to the output of a fold. Returns [state' task], where task
  applies the post-combiner of the fold and calls deliver-fn with the results"
  [state {:keys [post-combiner]} task deliver-fn]
  (task/submit state
               [:deliver]
               [task]
               (fn deliver [[combined]]
                 ; (info :deliver combined)
                 (deliver-fn (post-combiner combined)))))

(defn make-split-task
  "Takes a task executor state, a name, a function that splits an accumulator,
  an index to extract from that accumulator, and a task producing that
  accumulator. Returns [state' task], where task returns the given index in the
  accumulator task."
  [state name split-accs i acc-task]
  (task/submit state
               name
               {:workers (task-workers acc-task)}
               [acc-task]
               (fn split [[acc]]
                 ; (info :split acc)
                 (nth (split-accs acc) i))))

(defn make-join-task
  "Takes a task executor state, a name, a function that joins two accumulator,
  and accumulator tasks a and b. Returns [state' task], where task returns the
  two accumulators joined. Join tasks keep a vector of :worker tasks they
  depend on."
  [state name join-accs a-task b-task]
  (task/submit state
               name
               {:workers (into (task-workers a-task)
                               (task-workers b-task))}
               [a-task b-task]
               (fn join [[a b]]
                 ; (info :join a b)
                 (join-accs a b))))

(defn task-work-pending?
  "We may have a join task which unifies work from two different reducers or
  combiners, or one which splits a reducer or combiner. If we cancel just the
  join (split), the reducers (combiners) will still run. This takes a state and
  a task and returns true if all the actual work involved in this particular
  chunk is still pending. You can also pass :cancelled as a task; this is
  always pending."
  [state task]
  (or (identical? task :cancelled)
      (every? (partial task/pending? state) (task-workers task))))

(defn cancel-task-workers
  "Takes a state and a reduce, combine, split, or join task. Cancels not only
  this task, but all of its dependencies which actually perform the work for
  its particular chunk. Returns state'.

  A task of :cancelled yields no changes."
  [state task]
  (condp identical? task
    ; Already cancelled
    :cancelled state
    ; Unknown, definitely not cancellable!
    nil (throw (ex-info {:type :impossible-cancellation
                         :task task}))
    (reduce task/cancel state (task-workers task))))

; Pass construction
(defn concurrent-pass
  "Constructs a new concurrent pass record over the given chunks, using the
  given fold, and delivering results to the given function."
  [chunks fold deliver-fn]
  {:type    :concurrent
   :chunks  chunks
   :fold    fold
   :deliver deliver-fn})

(defn launch-concurrent-pass
  "Takes a task executor State and an unstarted Pass. Launches all the tasks
  required to execute this pass. Returns [state' pass']."
  [state {:keys [chunks fold deliver] :as pass}]
  (let [n (count chunks)]
    (assert (pos? n))
    ; (info "Launching concurrent pass of" n "chunks")
    (loop [i             0
           state         state
           reduce-tasks  []
           combine-tasks []]
      (if (< i n)
        ; Spawn tasks
        (let [[state reduce-task]
              (make-reduce-task state fold chunks reduce-tasks i)
              [state combine-task]
              (if (= i 0)
                (make-combine-task state fold chunks i reduce-task)
                (make-combine-task state fold chunks i
                                   (nth combine-tasks (dec i)) reduce-task))]
          (recur (inc i)
                 state
                 (conj reduce-tasks  reduce-task)
                 (conj combine-tasks combine-task)))
        ; Done!
        (let [[state deliver-task]
              (make-deliver-task state fold (nth combine-tasks (dec n))
                                 deliver)]
          [state
           (assoc pass
                  :reduce-tasks  (mapv task/id reduce-tasks)
                  :combine-tasks (mapv task/id combine-tasks)
                  :deliver-task  (task/id deliver-task))])))))

(defn print-join-plan
  "Draws an ASCII diagram of a concurrent pass. Helpful for debugging join
  plans"
  ([state old-pass new-pass pass]
   (let [chunks (:chunks old-pass)
         get-task          (fn [id] (when id (task/get-task state id)))
         old-combine-tasks (mapv get-task (:combine-tasks old-pass))
         old-reduce-tasks  (mapv get-task (:reduce-tasks old-pass))
         combine-tasks     (mapv get-task (:combine-tasks pass))
         reduce-tasks      (mapv get-task (:reduce-tasks pass))
         new-combine-tasks (mapv get-task (:new-combine-tasks pass))
         new-reduce-tasks  (mapv get-task (:new-reduce-tasks pass))]
     (print-join-plan (count chunks) state old-reduce-tasks old-combine-tasks
                      reduce-tasks combine-tasks
                      new-reduce-tasks new-combine-tasks)))
  ([chunk-count state
   old-reduce-tasks old-combine-tasks
   reduce-tasks     combine-tasks
   new-reduce-tasks new-combine-tasks]
  (let [sb (StringBuilder.)]
    (loopr []
           [[name tasks]
            [[" old-reduce" old-reduce-tasks]
             ["old-combine" old-combine-tasks]
             ["     reduce" reduce-tasks]
             ["    combine" combine-tasks]
             ["new-combine" new-combine-tasks]
             [" new-reduce" new-reduce-tasks]]]
           (do (.append sb name)
               (.append sb " [")
               (loopr [i 0]
                      [task tasks]
                      (do ;(when (pos? i)
                          ;  (.append sb " "))
                          (cond (= :cancelled task)
                                (.append sb "x ")

                                (not (task/has-task? state task))
                                (.append sb ". ")

                                true
                                (do (.append sb
                                             (case (first (task/name task))
                                               :join-combine  ">"
                                               :split-combine "<"
                                               :join-reduce   ">"
                                               :split-reduce  "<"
                                               :combine       "C"
                                               :reduce        "R"))
                                    (.append sb (if (task/pending? state task)
                                                  " "
                                                  "!"))))
                          (recur (inc i))))
               (.append sb "]\n")
               (recur)))
    (str sb))))

(defn join-concurrent-pass
  "Takes a task executor State, and a pair of Passes; the first (potentially)
  running, the second fresh. Joins these passes into a new pass which tries to
  merge as much work as possible into fused reduce/combine operations. Returns
  [state' joined-pass]."
  [^jepsen.history.task.State state
   old-pass
   new-pass]
  ; Different ways we can derive joined reducer/combiner tasks from old+new
  ; ones and vice-versa. Chunks go right to left. Within a single chunk, we can
  ; join or split accumulators.
  ;
  ;               join    split
  ;
  ;  old reducers  +-r      r<+
  ;                |          |
  ; old combiners  | c-+  +>c |
  ;                |   |  |   |
  ;      reducers  +>r |  | r-+
  ;                |   |  |   |
  ;     combiners  | c<+  +-c |
  ;                |   |  |   |
  ; new combiners  | c-+  +>c |
  ;                |          |
  ;  new reducers  +-r      r<+
  ;
  ; Our goal is to produce a single final combiner task as efficiently as
  ; possible.
  (let [;_ (assert+ (instance? jepsen.history.task.State state))
        ; First, we're going to need a fused fold, and a function to join
        ; accumulators/reducers.
        chunks               (:chunks old-pass)
        n                    (count chunks)
        _                    (assert (identical? chunks (:chunks new-pass)))
        old-fold             (:fold old-pass)
        new-fold             (:fold new-pass)
        ; Fuse folds
        {:keys [fused join-accs split-accs split-post-combined]}
        (fuse old-fold new-fold)
        _ (when-not (:name old-fold)
            (warn :malformed-old-fold
                  (with-out-str (pprint old-fold))))
        ;_ (info "Joining" (:name old-fold) "with" (:name new-fold)
        ;        "over" n "chunks")
        old-reduce-task-ids  (:reduce-tasks old-pass)
        old-combine-task-ids (:combine-tasks old-pass)
        get-task          (fn get-task [task-id]
                            (when task-id
                              (task/get-task state task-id)))
        ; Array of tasks, one per chunk, for the old fold's reduce/combine
        ; tasks, derived the current task executor state. Basically lazy
        ; caches. nil means we don't know, :cancelled means we cancelled the
        ; task. We mutate these two arrays as we go.
        old-reduce-tasks  (object-array (map get-task old-reduce-task-ids))
        old-combine-tasks (object-array (map get-task old-combine-task-ids))
        ; Array of tasks, one per chunk, for the new fold's reduce/combine
        ; tasks. Elements created as needed, never modified
        new-reduce-tasks  (object-array n)
        new-combine-tasks (object-array n)
        ; Same arrays, same rules, but for the joined reduce/combine tasks.
        reduce-tasks      (object-array n)
        combine-tasks     (object-array n)
        old-deliver-task  (get-task (:deliver-task old-pass))

        ; Now, like twitter, the goal of the game is to produce a pass which is
        ; maximally cancellable. The more of our reductions we can cancel
        ; later, the less duplicate work gets done, and the more we take
        ; advantage of our new, efficient fused tasks. However, some of those
        ; old tasks are already in progress. Others have been completed and
        ; their results are no longer available to us. We work our way through
        ; the old pass's reducers and combiners, at each stage trying to
        ; compute new reducers and combiners that ignore as much of the old
        ; pass as we can, but also don't get stuck not having access to data we
        ; need.
        ;
        ; I know for a fact that this is less than optimal, but this is also
        ; damn hard to think about and I need to keep it understandable.
        ;
        ; There's a *lot* of backtracking/recursion here involving creating
        ; tasks as needed, and I think mutability is simpler and faster than
        ; doing a zillion lazy delays/fn trampolines and destructuring
        ; everywhere.
        vstate            (volatile! state)
        ; Creates a task and mutates the state, returning task.
        task!             (fn task! [f & args]
                            (let [[state' task] (apply f @vstate args)]
                              (vreset! vstate state')
                              (assert (not (false? task)))
                              task))
        ; Cancels a task in the mutable state. We also cancel all of the work
        ; for that chunk, in case it's a split/join task.
        cancel!           (fn cancel! [task]
                            (vswap! vstate cancel-task-workers task))
        ; When we cancel a combine, we need to mark every later combine as
        ; cancelled, and we don't want to repeat that work.
        combines-cancelled-i (volatile! n)
        ; Cancel an old combine task
        cancel-old-combine-task! (fn cancel-old-combine-task! [i]
                                   (when (<= i @combines-cancelled-i)
                                     (loop [j i]
                                       (when (< j n)
                                         (let [ct (aget old-combine-tasks j)]
                                           (assert+ ct)
                                           (cancel! ct)
                                           (aset old-combine-tasks j :cancelled)
                                           (recur (inc j)))))
                                     (vreset! combines-cancelled-i i)))
        ; Cancel an old reduce task
        cancel-old-reduce-task! (fn cancel-old-reduce-task! [i]
                                  (let [rt (aget old-reduce-tasks i)]
                                    (assert+ rt)
                                    (cancel! rt)
                                    (aset old-reduce-tasks i :cancelled)
                                    (cancel-old-combine-task! i)))

        ; We need to cancel the old deliver task if we're going to preserve
        ; exactly-once execution. Should do early return here later--maybe
        ; split up fn?
        _ (when old-deliver-task (cancel! old-deliver-task))

        ;_ (info (str "Original folds:\n"
        ;             (print-join-plan n @vstate
        ;                              old-reduce-tasks old-combine-tasks
        ;                              reduce-tasks combine-tasks
        ;                              new-reduce-tasks new-combine-tasks)))
        ;  old-reduce [.  R  . ]
        ; old-combine [C  C  C ]
        ;
        ; A wrinkle: here reducer R2 has already been started, but we lost the
        ; reference to it. If we cancel R1, it'll also cancel C2, and we *need*
        ; C2 since it's our only way to see effects of R2. We scan back and
        ; find the last missing reducer; we can't cancel anything prior. Ditto,
        ; last missing combiner.
        last-missing-fn (fn last-missing-fn [^objects ary]
                          (loop [i (dec n)]
                            (if-not (aget ary i)
                              i
                              (if (= i 0)
                                ; Some of our comparisons are simpler if we
                                ; can safely distinguish "first missing"
                                ; from "none missing"
                                -2
                                (recur (dec i))))))
        last-missing-reduce-i  (last-missing-fn old-reduce-tasks)
        last-missing-combine-i (last-missing-fn old-combine-tasks)

        ; First pass: reduces. These are easy because they have no
        ; dependencies. We need to ensure that every reduce executes exactly
        ; once. That gives us two possible options: either we keep the old
        ; reduce and create a new one, OR we cancel the old reduce and create a
        ; joined reduce.
        ;_ (info :last-missing-reduce last-missing-reduce-i
        ;        :last-missing-combine last-missing-combine-i)
        _ (loop [i 0]
            (when (< i n)
              (let [old-rt (aget old-reduce-tasks i)]
                (if (and (< last-missing-reduce-i i)
                         (< last-missing-combine-i (dec i))
                         (task-work-pending? state old-rt))
                  (do ; Replace with a fused reduce
                      (cancel-old-reduce-task! i)
                      (aset reduce-tasks i
                            (task! make-reduce-task fused chunks
                                   (vec (remove nil? reduce-tasks)) i)))
                  ; We need to start a new reduce task instead.
                  (aset new-reduce-tasks i
                        (task! make-reduce-task new-fold chunks
                               (vec (remove nil? new-reduce-tasks)) i))))
              (recur (inc i))))

        ;_ (info (str "New reduce plan:\n"
        ;             (print-join-plan n @vstate
        ;                              old-reduce-tasks old-combine-tasks
        ;                              reduce-tasks combine-tasks
        ;                              new-reduce-tasks new-combine-tasks)))

        ; Looks up the old combine task at i, or creates a split from the
        ; existing fused combine tasks.
        old-combine-task!
        (fn old-combine-task! [i]
          (let [ct (aget old-combine-tasks i)]
            (if (or (nil? ct) (identical? ct :cancelled))
              ; We don't have it or it was cancelled, but we could maybe split
              (when-let [ct (aget combine-tasks i)]
                (aset old-combine-tasks i
                      (task! make-split-task [:split-combine i] split-accs 0
                             ct)))
              ; We have it
              ct)))

        ; Looks up the old reduce task at i, or creates a split from the
        ; existing fused reduce task.
        old-reduce-task!
        (fn old-reduce-task! [i]
          (let [rt (aget old-reduce-tasks i)]
            (if (or (nil? rt) (identical? rt :cancelled))
              ; We don't have it, but we can split safely.
              (when-let [rt (aget reduce-tasks i)]
                (aset old-reduce-tasks i
                      (task! make-split-task [:split-reduce i] split-accs 0
                             rt)))
              rt)))

        ; Looks up the new combine task at i, or creates a split from the
        ; existing fused combine task.
        new-combine-task!
        (fn new-combine-task! [i]
          (or (aget new-combine-tasks i)
              (when-let [ct (aget combine-tasks i)]
                (aset new-combine-tasks i
                    (task! make-split-task [:split-combine i] split-accs 1
                           ct)))))

        ; Looks up the new reduce task at i, or creates a split from the
        ; existing fused reduce task.
        new-reduce-task!
        (fn new-reduce-task! [i]
          (or (aget new-reduce-tasks i)
              (when-let [rt (aget reduce-tasks i)]
                (aset new-reduce-tasks i
                      (task! make-split-task [:split-reduce i] split-accs 1
                             rt)))))

        ; Either looks up the reduce task at index i, or creates a join from
        ; the existing old/new combine tasks.
        reduce-task!
        (fn reduce-task! [i]
          (or (aget reduce-tasks i)
              (let [old-rt (aget old-reduce-tasks i)
                    new-rt (aget new-reduce-tasks i)]
                ; Can we join?
                (when (and old-rt
                           new-rt
                           (not (identical? :cancelled old-rt))
                           (or (task/ran? old-rt)
                               (task/has-task? @vstate old-rt)))
                  (aset reduce-tasks i
                        (task! make-join-task [:join-reduce i] join-accs
                               old-rt new-rt))))))

        ; Either looks up the combine task at index i, or creates a join from
        ; the existing old/new combine tasks.
        combine-task!
        (fn combine-task! [i]
          (or (aget combine-tasks i)
              ; We can't start fusing combines until we get past missing
              ; reducers/combiners.
              (when
                (< last-missing-combine-i i)
                ; Can we join?
                (let [old-ct (aget old-combine-tasks i)
                      new-ct (aget new-combine-tasks i)]
                  (when (and old-ct
                             new-ct
                             (not (identical? :cancelled old-ct))
                             (or (task/ran? old-ct)
                                 (task/has-task? @vstate old-ct)))
                    ; Join
                    (aset combine-tasks i
                          (task! make-join-task [:join-combine i] join-accs
                                 old-ct new-ct)))))))

        ; Second pass: combines. These are trickier because each combine relies
        ; on every previous combine. However, because we want to enforce our
        ; single-execution invariant, there are really only two modes per
        ; chunk. Like reduce, we're either replacing the original combine with
        ; a fused combine, or we're creating a new combine alongside the old
        ; one.
        ;
        ; Each combine needs the current chunk's reduction
        ; result as input. We split up the fused reduction if we need to
        ; execute separate combines, and join old and new if we want to do a
        ; fused combine.
        ;
        ; Start off with the first combine.
        _ (let [old-ct (aget old-combine-tasks 0)]
            (or (and ; Can't start cancelling until we get past last missing.
                     (< last-missing-reduce-i 0)
                     (< last-missing-combine-i 0)
                     ; Can only cancel if work is pending
                     (task-work-pending? state old-ct)
                     ; And we need the reduce task
                     (when-let [rt (reduce-task! 0)]
                       ; We can replace this with an initial fused combine task
                       (cancel-old-combine-task! 0)
                       (aset combine-tasks 0
                             (task! make-combine-task fused chunks 0 rt))))
                (do ; Gotta run an independent new initial combine task
                    (aset new-combine-tasks 0
                          (task! make-combine-task new-fold chunks 0
                                 (assert+ (new-reduce-task! 0)))))))
        ; For later combines: each combine also needs the previous chunk's
        ; combine result. When that's unavailable, we create join nodes in the
        ; *previous* chunk.
        _ (loop [i 1]
            (when (< i n)
              ;(info (str "Combine plan up to i = " i "\n"
              ;           (print-join-plan n @vstate
              ;                            old-reduce-tasks old-combine-tasks
              ;                            reduce-tasks combine-tasks
              ;                            new-reduce-tasks new-combine-tasks)))
              (let [old-ct (aget old-combine-tasks i)]
                ; We'd like to cancel the old combine and do a fused one
                ; instead.
                (or (and ; Can't start cancelling until we get past last missing
                         (< last-missing-reduce-i i)
                         (< last-missing-combine-i (dec i))
                         ; We need the old ct to be pending...
                         (task-work-pending? state old-ct)
                         ; We need both the current reduce task and the
                         ; previous combine task
                         (when-let [rt (reduce-task! i)]
                           (when-let [prev-ct (combine-task! (dec i))]
                             (cancel-old-combine-task! i)
                             (aset combine-tasks i
                                   (task! make-combine-task fused chunks i
                                          prev-ct rt)))))
                    ; Fall back to separate old/new combine tasks. Do we need
                    ; to create an old combine to replace one we cancelled?
                    (do (when (identical? old-ct :cancelled)
                          (aset old-combine-tasks i
                                (task! make-combine-task old-fold chunks i
                                       (assert+ (old-combine-task! (dec i)))
                                       (assert+ (old-reduce-task! i)))))
                        ; And create our new combine task
                        (aset new-combine-tasks i
                              (task! make-combine-task new-fold chunks i
                                     (assert+ (new-combine-task! (dec i)))
                                     (assert+ (new-reduce-task! i)))))))
              (recur (inc i))))

        ; Final combine task
        combine-task (combine-task! (dec n))
      ]

    ;(info (str "Combine plan:\n"
    ;           (print-join-plan n @vstate
    ;                            old-reduce-tasks old-combine-tasks
    ;                            reduce-tasks combine-tasks
    ;                            new-reduce-tasks new-combine-tasks)))
    ; We could have gotten something completely unmergeable--for instance, the
    ; last combiner or reducer could have been missing.
    (if (or (nil? combine-task)
            (not (task/pending? state old-deliver-task)))
      ; If this happens, just launch the new pass on the original state.
      (launch-concurrent-pass state new-pass)
      ; Right, new plan is a go.
      (let [; side effects: delivering results
            old-deliver (:deliver old-pass)
            new-deliver (:deliver new-pass)
            ; The deliver fn takes post-combined results, which means a pair
            ; of [old-result new-result]
            deliver-fn (fn deliver-acc [[old-res new-res]]
                         ;(info :joint-delivery (:name fused) old-res new-res)
                         (old-deliver old-res)
                         (new-deliver new-res))
            state' @vstate
            [state' deliver-task]
            (make-deliver-task state' fused combine-task deliver-fn)

            ; Right, now we go back and garbage collect every task *not*
            ; involved in producing our output. Should be able to drop this now
            ; that we're conservative w/pruning.
            state' (->> (concat old-reduce-task-ids old-combine-task-ids)
                        (remove nil?)
                        (keep (partial task/get-task state'))
                        (task/gc state' [deliver-task]))

            ; Log resulting plan
            ;_ (info (str "Final join plan\n"
            ;        (print-join-plan n
            ;                         state'
            ;                         old-reduce-tasks
            ;                         old-combine-tasks
            ;                         reduce-tasks
            ;                         combine-tasks
            ;                         new-reduce-tasks
            ;                         new-combine-tasks)))

            ; Turn our tasks back into task IDs
            task-id (fn task-id [task]
                      (when task
                        (task/id task)))]
        ; And construct our new pass.
        [state'
         {:type          :concurrent
          :joined?       true
          :fold          fused
          :chunks        chunks
          :reduce-tasks  (mapv task-id reduce-tasks)
          :combine-tasks (mapv task-id combine-tasks)
          ; We don't actually need these for execution, but they're helpful for
          ; testing/debugging.
          :new-reduce-tasks  (mapv task-id new-reduce-tasks)
          :new-combine-tasks (mapv task-id new-combine-tasks)
          :deliver-task      (task/id deliver-task)
          :deliver           deliver-fn}]))))

(deftype Executor
  [task-executor
   history
   passes])

(defn executor
  "Starts a new executor for folds over the given chunkable history"
  [history]
  (Executor. (task/executor)
             history
             (atom {:concurrent nil
                    :linear     nil})))

(defn clear-old-passes!
  "Takes an Executor and clears out any old pass state."
  [^Executor e]
  (let [task-executor (.task-executor e)]
    (locking e
      (swap! @(.passes e)
             (fn clear [passes]
               (let [state (task/executor-state task-executor)]
                 (loopr [passes' passes]
                        [[type pass] passes]
                        (case type
                          :concurrent
                          (if (task/pending? state
                                               (peek (:combine-tasks pass)))
                            passes
                            (assoc passes type nil))

                          :linear
                          passes))))))))

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
                                     (fn reduce-task [[acc]]
                                       (reduce reducer acc chunk)))]
                 (recur (conj reduces r)
                        (inc i)))
               reduces)
        ; Post-reduce
        last-reduce    (peek reduces)
        last-reduce-id (task/id last-reduce)
        pr (task/submit! exec
                         :post-reduce
                         [last-reduce]
                         (fn post-reduce [[acc]]
                           (post-reducer acc)))
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
                                 (fn single-combine [[post-reduced]]
                                   (combiner
                                     (combiner-identity)
                                     post-reduced))))
        ; Post-combine
        post-combine (task/submit! exec
                                   :post-combine
                                   [combine]
                                   (fn post-combine [[combined]]
                                     (post-combiner combined)))]
    {:type         :linear
     :post-reduce  pr
     :reduces      reduces
     :combine      combine
     :post-combine post-combine
     :result       post-combine}))

(defn concurrent-fold!
  "Takes a history executor and a fold. Expands the fold into concurrent
  reduce and serial combine tasks."
  [^Executor e fold]
  (let [exec       (.task-executor e)
        history    (.history e)
        passes     (.passes e)
        chunks     (hc/chunks history)
        result     (promise)
        deliver-fn (fn [r]
                     (deliver result r)
                     ; TODO: make this async (use task executor!)
                     ;(clear-old-passes! e)
                     )
        new-pass   (concurrent-pass chunks fold deliver-fn)
        resulting-pass (atom nil)]
    ; We're about to go do some VERY heavy computation on the executor, and
    ; don't want anyone else clobbering the pass state while we're here.
    (locking e
      (let [old-pass (:concurrent @passes)]
        (task/txn! exec
                   (fn txn [state]
                     (let [[state pass]
                           (if old-pass
                             (join-concurrent-pass state old-pass new-pass)
                             (launch-concurrent-pass state new-pass))]
                       (reset! resulting-pass pass)
                       state))))
      (swap! passes assoc :concurrent @resulting-pass))
    (let [pass @resulting-pass]
      ;(if (:joined? pass)
      ;  (info "Joined!" (:name (:fold pass)))
      ;  (info "Didn't join!" (:name (:fold pass))))
      (assoc pass :result result))))

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
          (let [res (:result (concurrent-fold! executor fold))]
            @res
            ;(let [res (deref res 5000 ::timeout)]
            ;  (when (= ::timeout res)
            ;    (warn "Timed out!" (:name fold) (with-out-str (pprint (-> executor .task-executor task/executor-state))))
            ;    (throw (ex-info "timeout"
            ;                    {:name  (:name fold)
            ;                     :state (-> executor .task-executor task/executor-state)})))
            ; res)
            ))))

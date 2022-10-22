(ns jepsen.history.task
  "Supports jepsen.history.dataflow by providing a low-level executor for tasks
  with dependencies. Handles threadpool management, wraps queue operations,
  etc."
  (:refer-clojure :exclude [name])
  (:require [clojure [pprint :as pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ loopr]])
  (:import (clojure.lang IDeref
                         IBlockingDeref
                         IPending)
           (io.lacuna.bifurcan DirectedGraph
                               IEdge
                               IGraph
                               IList
                               IMap
                               ISet
                               Graphs
                               List
                               Map
                               Set)
           (java.util OptionalLong)
           (java.util.concurrent ArrayBlockingQueue
                                 BlockingQueue
                                 ExecutorService
                                 Executors
                                 Future
                                 LinkedBlockingQueue
                                 ThreadPoolExecutor
                                 TimeUnit)
           (java.util.concurrent.locks Condition
                                       ReentrantLock)
           (java.util.function Function)))

(declare finish-task!)

; Represents low-level tasks. For speed, we hash and compare equality strictly
; by ID. Tasks should not be compared across different executors!
(deftype Task
  [; This task's ID
   ^long id
   ; A friendly name for this task
   name
   ; A collection of tasks we depend on. TODO: retaining the outputs of these
   ; tasks forever means we retain EVERY task in a chain until the final task
   ; is done.
   deps
   ; A function which takes a map of task IDs (our dependencies) to their
   ; results, and produces a result
   f
   ; The promise we should deliver our own result to.
   ^IDeref output
   ; The executor we should update when we complete
   executor]

  clojure.lang.IDeref
  (deref [this]
    (let [out @output]
      (if (instance? Throwable out)
        (throw out)
        out)))

  clojure.lang.IBlockingDeref
  (deref [this ms timeout-value]
    (let [out (.deref ^IBlockingDeref output ms timeout-value)]
      (cond (identical? timeout-value out) out
            (instance? Throwable out) (throw out)
            true out)))

  clojure.lang.IPending
  (isRealized [this]
    (.isRealized ^IPending output))

  Runnable
  (run [this]
       (try
         (let [inputs (loopr [m (transient {})]
                             [^Task task deps]
                             (recur (assoc! m (.id task) @task))
                             (persistent! m))]
           (deliver output (f inputs)))
         (catch Throwable t
           (deliver output t)))
       ; Inform executor we're done
       (when executor
         (finish-task! executor this)))

  Object
  (hashCode [this]
    (hash id))

  (equals [this other]
    (or (identical? this other)
      (and (instance? Task other)
           (= id (.id ^Task other)))))

  (toString [this]
    (str "(Task " name " " id ")")))

; pprint will try to block on the deref of tasks forever; makes it impossible
; to debug stuff
(defmethod pprint/simple-dispatch jepsen.history.task.Task
  [task]
  (.write ^java.io.Writer *out* (str task)))
(prefer-method pprint/simple-dispatch
               jepsen.history.task.Task clojure.lang.IDeref)

; Ditto prn
(defmethod print-method jepsen.history.task.Task [t ^java.io.Writer w]
  (.write w (str t)))

(defn id
  "Returns the ID of a task."
  [^Task t]
  (.id t))

(defn task-deps
  "Returns the deps of a task."
  [^Task t]
  (.deps t))

(defn all-task-deps
  "Returns an iterable of all deps of a task."
  [^Task task]
  (let [deps (reify Function
               (apply [_ task]
                 (or (task-deps task)
                     [])))]
    (Graphs/bfsVertices task deps)))

(defn name
  "Returns the name of a task."
  [^Task t]
  (.name t))

; An immutable representation of our executor state. By happy circumstance, it
; is *also* an immutable representation of a queue, which the executor can pull
; work from.
(defrecord State
  [^long          next-task-id
   executor
   ^DirectedGraph dep-graph
   ^ISet          ready-tasks
   ^ISet          running-tasks
   ^IList         effects])

(defn state
  "Constructs a new executor state. States have six parts:

  - The next task ID we'll hand out

  - The executor wrapping this state. We need this to close the loop on task
    side effects.

  - A graph of dependencies between tasks. Iff a -> b, b depends on a's
    results. Once tasks are completed, they're removed from this graph.

  - A set of ready tasks which have no pending dependencies, but have not yet
    begun. These tasks are eligible to be executed by the threadpool.

  - A set of running tasks we're currently executing. When the executor begins
    executing a task, it moves from the ready set to the running set.

  - A list of unapplied side effects. Pure functions of the state add effects
    to the end of this list. They're applied by a mutable function
    apply-effects!) later, and popped off the list as this occurs."
  []
  (State. 0
          nil
          (.forked (DirectedGraph.))
          (.forked (Set.))
          (.forked (Set.))
          (.forked (List.))))

(defn ^Task get-task
  "Fetches a Task from a state by ID."
  [^State state, task-id]
  ;(info :get-task (.dep-graph state) task-id)
  (let [^DirectedGraph dep-graph (.dep-graph state)
        ; Taking advantage of our weird-ass equality/hashing semantics
        ^OptionalLong i (.indexOf dep-graph
                                  (Task. task-id nil nil nil nil nil))]
    (when (.isPresent i)
      (.nth dep-graph (.getAsLong i)))))

(defn has-task?
  "Takes a state and a task. Returns true iff the state knows about this task."
  [^State state, task]
  (.contains (.vertices (.dep-graph state)) task))

(defn pending?
  "Takes a state and a task. Returns true iff that task is still pending
  execution, and can be safely cancelled."
  [^State state, task]
  (let [^DirectedGraph dep-graph (.dep-graph state)
        ^ISet          running   (.running-tasks state)]
    (and (not (.contains running task))
         (.contains ^ISet (.vertices dep-graph) task))))

(defn all-deps-pending?
  "Takes a state and a task. Walks that task's dependency graph, returning true
  iff every dependency is pending."
  [^State state, task]
  (let [^DirectedGraph  dep-graph (.dep-graph state)
        ^ISet           tasks     (.vertices dep-graph)
        ^ISet           running   (.running-tasks state)
        ; A Function which finds dependencies of a task
        deps (reify Function
               (apply [_ task]
                 (or (task-deps task)
                     [])))]
    (loopr []
           [t (Graphs/bfsVertices task deps)]
           (and (not (.contains running t))
                (.contains tasks t)
                (recur))
           true)))

(defn state-done?
  "Returns true when a state has nothing ready, nothing running, and nothing in
  the dependency graph; e.g. there is nothing more for it to do."
  [^State state]
  (and (= 0 (.size ^DirectedGraph (.dep-graph state)))
       (= 0 (.size ^ISet (.ready-tasks state)))
       (= 0 (.size ^ISet (.running-tasks state)))))

(defn new-task
  "Given a state, constructs a fresh task with the given name, collection of
  Tasks as dependencies, a function (f dep-results) which takes a map of
  dependency IDs to their results. Returns the state with the new task
  integrated, including a [:new-task task], which you can use to read the
  created task.

  If the task has no dependencies, it may be ready immediately; the :new-task
  effect will be followed by a [:ready-task task] effect. In either case, the
  final effect *will* contain the newly created task."
  [^State state name deps f]
  (let [id         (.next-task-id state)
        task       (Task. id name deps f (promise) (.executor state))
        ; Update dependency graph
        ^DirectedGraph dep-graph (.dep-graph state)
        ^DirectedGraph dep-graph'
        (loopr [^DirectedGraph g (.. dep-graph
                                     linear
                                     (add task))]
               [dep deps]
               (do (assert+
                     (instance? Task dep)
                     IllegalArgumentException
                     (str "Dependencies must be tasks, but got "
                          (pr-str deps)))
                   ; We don't want to add things back to the dep
                   ; graph if they're already finished
                   (if (.contains (.vertices g) dep)
                     (recur (.link g dep task))
                     (recur g)))
               (.forked g))
        ; Is the task ready now?
        ready? (= 0 (.size ^ISet (.in dep-graph' task)))
        ; If it's ready, we add it to the ready set
        ready-tasks  ^ISet (.ready-tasks state)
        ready-tasks' (if ready?
                       (.add ready-tasks task)
                       ready-tasks)
        ; And record effects
        effects ^IList (.effects state)
        effects' (.addLast effects [:new-task task])
        effects' (if ready?
                   (.addLast effects' [:ready-task task])
                   effects')]
    ;(info :new-task task)
    (assoc state
           :next-task-id  (inc id)
           :dep-graph     dep-graph'
           :ready-tasks   ready-tasks'
           :effects       effects')))

(defn new-task+
  "Like new-task, but returns [state new-task]. Useful for transactional task
  creation."
  [state name deps f]
  (let [^State state' (new-task state name deps f)]
    [state' (nth (.last ^IList (.effects state')) 1)]))

(defn finish-task
  "Takes a state and a task which has been executed, and marks it as completed.
  This deletes the state from the running set and returns the resulting state.
  It may also result in new tasks being ready."
  [^State state, ^Task task]
  (let [; Definitely shouldn't be ready
        ^ISet ready    (.ready-tasks state)
        _ (assert (not (.contains ready task)))
        ; Remove from running
        ^ISet running  (.running-tasks state)
        running'       (.remove running task)
        ; Remove from dep graph
        ^DirectedGraph dep-graph (.dep-graph state)
        dep-graph'     (.remove dep-graph task)
        ; Completing this task may make others ready
        ^IList effects (.effects state)
        [ready' effects']
        (if (.contains ^ISet (.vertices dep-graph) task)
          (loopr [^ISet ready'    (.linear ready)
                  ^IList effects' (.linear effects)]
                 [dependent (.out dep-graph task)]
                 ; If it has no deps and isn't ready or running, we can
                 ; add it to the ready set and log an effect
                 (if (and (= 0 (.size (.in dep-graph' dependent)))
                          (not (.contains running' dependent))
                          (not (.contains ready' dependent)))
                   (recur (.add ready' dependent)
                          (.addLast effects' [:ready-task dependent]))
                   (recur ready' effects'))
                 [(.forked ready')
                  (.forked effects')])
          ; Already removed from the dep graph; there won't be any deps.
          [ready effects])]
    (assoc state
           :ready-tasks   ready'
           :running-tasks running'
           :dep-graph     dep-graph'
           :effects       effects')))

(defn cancel-task
  "Takes a state and a task to cancel. Deletes the task, and any tasks which
  depend on it, from every part of the state."
  [^State state, ^Task task]
  (let [^DirectedGraph dep-graph (.dep-graph state)
        ; A Function which finds tasks depending on this one
        dependents (reify Function
                     (apply [_ task]
                       (.out dep-graph task)))]
    (if (.contains ^ISet (.vertices dep-graph) task)
      (loopr [^DirectedGraph dep-graph' (.linear dep-graph)
              ^ISet ready-tasks'        (.linear ^ISet (.ready-tasks state))
              ^ISet running-tasks'      (.linear ^ISet (.running-tasks state))]
             [t (Graphs/bfsVertices task dependents)]
             (recur (.remove dep-graph'     t)
                    (.remove ready-tasks'   t)
                    (.remove running-tasks' t))
             (assoc state
                    :dep-graph     (.forked dep-graph')
                    :ready-tasks   (.forked ready-tasks')
                    :running-tasks (.forked running-tasks')))
      state)))

(defn gc
  "Takes a state, a collection of goal tasks you'd like to achieve, and a set of
  tasks you might like to cancel. Cancels all tasks not contributing to the
  goal, returning a new state."
  [^State state, goal, to-delete]
  (let [^DirectedGraph dep-graph (.dep-graph state)
        deps (reify Function
               (apply [_ task]
                 (try (.in dep-graph task)
                      (catch IllegalArgumentException e
                        (throw (ex-info {:type ::no-such-task
                                         :task task
                                         :state state}))))))
        ; Walk the graph backwards from goal, cancelling tasks
        to-delete (loopr [to-delete (.linear (Set/from to-delete))]
                         [t (Graphs/bfsVertices ^Iterable goal deps)]
                         (recur (.remove to-delete t)))]
    (info "GCing" (.size to-delete) "unneeded tasks")
    (reduce cancel-task state to-delete)))

(defn state-queue-claim-task*!
  "A support function for StateQueue.run. Definitely don't call this yourself.

  Takes a state and a volatile. Tries to move a task from the ready set to the
  running set, and if that works, sets the volatile to that task. Otherwise
  sets the volatile to nil."
  [^State state, vol]
  (let [^ISet ready (.ready-tasks state)]
    (if (= 0 (.size ready))
      (do (vreset! vol nil)
          state)
      (let [task (.nth ready 0)]
        (when (.contains ^ISet (.running-tasks state) task)
          (warn "Task" task "is both ready AND running:"
                (with-out-str (pprint state))))
        (assert (not (.isLinear ^ISet (.running-tasks state))))
        (assert (not (.isLinear ^ISet (.ready-tasks state))))
        (vreset! vol task)
        (assoc state
               :ready-tasks   (.remove ready task)
               :running-tasks (.add ^ISet (.running-tasks state) task))))))

(deftype StateQueue
  ; We don't actually NEED to lock anything, but it's the only way to get a
  ; Condition I've found. Maybe we just back off to Object.await?
  [state, ^ReentrantLock lock, ^Condition not-empty seen?]
  BlockingQueue
  (isEmpty [this]
    (= 0 (.size this)))

  (size [this]
    (.size ^ISet (.ready-tasks ^State @state)))

  ; Adapted from https://github.com/openjdk/jdk/blob/9583e3657e43cc1c6f2101a64534564db2a9bd84/src/java.base/share/classes/java/util/concurrent/LinkedBlockingQueue.java#L449
  (poll [this timeout time-unit]
    (let [task (volatile! nil)]
      (try (.lock lock)
           (loop [nanos (.toNanos time-unit timeout)]
             ; Try moving a task from ready to running
             (let [^State state' (swap! state state-queue-claim-task*! task)]
               (if-let [task @task]
                 (do ; We got a task! Any left?
                     ; (info :polled task :state (with-out-str (pprint state')))
                     (when (< 0 (.size ^ISet (.ready-tasks state')))
                       ; Someone else might be waiting
                       (.signal not-empty))
                     (when (@seen? task)
                       ; Oh no
                       (warn "Task dequeued twice:" task "\n"
                             (with-out-str (pprint state'))))
                     (swap! seen? conj task)
                     task)
                 ; No tasks ready. Sleep more?
                 (if (< 0 nanos)
                   (recur (.awaitNanos not-empty nanos))
                   nil))))
           (finally
             (.unlock lock)))))

  (take [this]
    (.poll this Long/MAX_VALUE TimeUnit/NANOSECONDS))

  (offer [this task]
    ; skskskskksks
    (try (.lock lock)
         (.signal not-empty)
         (finally
           (.unlock lock)))
    true))

(defn state-queue
  "A queue for our ThreadPoolExecutor which wraps an atom of a State. Tasks are
  pulled off the state's queue and placed into the running set. This lets us
  get around issues with handing off records from our immutable state (which is
  basically a queue!) to a standard mutable executor queue (which we can't
  mutate safely).

  We're doing something *kind of* evil here: to avoid mutating the state
  twice, we actually:

  1. Put the task into our state directly, using our own transactional fns
  2. Call executor.execute(task)
  3. The executor tries to put that task onto this queue; we return true and do
     nothing because we *know* we already have it.
  4. The executor threads poll us, and we hand them tasks from the queue."
  [state-atom]
  (let [lock (ReentrantLock. false)]
    (StateQueue. state-atom lock (.newCondition lock) (atom #{}))))


(deftype Executor [; The ExecutorService which actually does work
                   ^ExecutorService executor-service
                   ; An atom containing our current state.
                   state])

(defn executor
  "Constructs a new Executor for tasks. Executors are mutable thread-safe
  objects."
  []
  (let [procs (-> (Runtime/getRuntime) .availableProcessors)
        state (atom (state))
        state-queue (state-queue state)
        exec (doto (ThreadPoolExecutor. procs ; core pool
                                        procs ; max pool
                                        1 TimeUnit/SECONDS ; keepalive
                                        state-queue)
               (.allowCoreThreadTimeOut true))
        this (Executor. exec state)]
    ; Tie the knot: the state needs to have a reference back to the executor so
    ; tasks created on it can clear themselves from our state when finished
    (swap! state assoc :executor this)
    this))

(defn executor-state
  "Reads the current state of an Executor."
  [^Executor e]
  @(.state e))

(def void-runnable
  "A dummy runnable which only exists to wake up a ThreadPoolExecutor."
  (reify Runnable
    (run [this])))

(defn apply-effects!
  "Attempts to apply all pending effects from an Executor's state. Side effects
  may be interleaved in any order. When an effect is applied it is removed from
  the effect queue. Returns executor.

  In the two-arity form, performs a specific effect."
  ([^Executor executor]
   (let [; Claim effects to execute
         effects (volatile! nil)
         state'  (swap! (.state executor)
                        (fn claim-effects [^State state]
                          (vreset! effects (.effects state))
                          (assoc state :effects (List.))))]
     (doseq [effect @effects]
      (apply-effects! executor effect))))
  ; Actually *do* a single side effect
  ([^Executor executor, [type, ^Task task]]
   ;(info :effect type task)
   (case type
     ; Nothing to do for a new task--it's just an effect so callers can read
     ; off their created tasks.
     :new-task   nil
     :ready-task (let [^ExecutorService exec (.executor-service executor)]
                   ; The *only* purpose of this is to wake up the executor
                   ; service in case it's asleep. The work is already in the
                   ; state atom, and therefore the state queue.
                   (.execute exec void-runnable)))))

(defn txn!
  "Executes a transaction on an executor. Takes an executor and a function
  which transforms that executor's State. Applies that function to the
  executor's state, then applies any pending side effects. Returns the state
  the function returned."
  [^Executor executor, f]
  (let [state' (swap! (.state executor) f)]
    ;(info :txn! (with-out-str (pprint @caller-state)))
    ; Apply side effects
    (apply-effects! executor)
    ; Return caller's generated state
    state'))

(defn finish-task!
  "Tells an executor that a task is finished."
  [executor task]
  (txn! executor #(finish-task % task)))

(defn submit!
  "Submits a new task to an Executor. Takes a task name, a collection of Tasks
  as dependencies, and a function `(f dep-results)` which receives a map of
  dependency IDs to their results. Returns a newly created Task object."
  [executor name deps f]
  (let [; Create the task
        ^State state' (txn! executor #(new-task % name deps f))
        [_ task] (.last ^IList (.effects state'))]
    task))

(ns jepsen.history.task
  "A transactional, dependency-graph oriented task scheduler. Provides a
  stateful executor for CPU-bound tasks backed by a num-cores
  ThreadPoolExecutor, and allows you to submit tasks to be run on that
  executor.

    (require '[jepsen.history.task :as task])
    (def e (task/executor))

  At a very high level, a task is a named function of optional dependencies
  (inputs) which returns an output. Here's a task with no dependencies:

    (def pet (task/submit! e :pet-dog (fn [_] :petting-dog)))

  Tasks are de-refable with the standard blocking and nonblocking calls.
  Derefing a task returns its output. You can ask completion with `realized?`

    @pet             ; :petting-dog
    (realized? pet)  ; true

  If a task throws, its output is the Throwable it threw. Derefing that
  throwable also throws, like Clojure futures. Exceptions propagate to
  dependencies: dependencies will never execute, and if derefed, will throw as
  well.

    (def doomed (task/submit! e :doomed (fn [_] (assert false))))
    ; All fine, until
    @doomed    ; throws Assert failed: false

  Each task is assigned a unique long ID by its executor. Tasks should never be
  used across executors; their hashcodes and equality semantics are, for
  performance reasons, by ID *alone*.

    (task/id pet)   ; 0

  Tasks also have names, which can be any non-nil object, and are used for
  debugging & observability. Tasks can also carry an arbitrary data object,
  which can be anything you like. You can use this to build more sophisticated
  task management systems around this executor.

    (task/name pet)   ; :pet-dog

    (def train (task/submit! e :train {:tricks [:down-stay :recall]} nil
                 (fn [_] :training-dog)))
    (task/data train)  ; {:tricks [:down-stay :recall]}

  When submitted, tasks can depend on earlier tasks. When it executes, a task
  receives a vector of the outputs of its dependencies. A task only executes
  once its dependencies have completed, and will observe their memory effects.

    (def dog-promise (promise))
    (def dog-task    (task/submit! e :make-dog    (fn [_] @dog-promise)))
    (def person-task (task/submit! e :make-person (fn [_] :nona)))
    (def adopt-task  (task/submit! e :adopt [person-task dog-task]
                       (fn [[person dog]]
                         (prn person :adopts dog)
                         :adopted!)))
    ; Adopt pauses, waiting on dog, which in turn is waiting on our
    ; dog-promise.
    (realized? adopt-task) ; false

    (task/dep-ids adopt-task) ; [5 4]

    ;Person completed immediately:
    @person-task   ; :nona

    ; Let's let the dog task run. Once it does, adopt can run too.
    (deliver dog-promise :noodle)
    ; Immediately prints :nona :adopts :noodle

    ; Now we can deref the adoption task.
    @adopt-task    ; :adopted!

  Tasks may be cancelled. Cancelling a task also cancels all tasks which depend
  on it. Unlike normal ThreadPoolExecutors, cancellation is *guaranteed* to be
  safe: if a task is still pending, it will never run. Cancelling a task which
  is running or has already run has no effect, other than removing it from the
  executor state. This may not be right for all applications; it's important
  for us.

    (task/cancel! e adopt-task)

  Tasks either run to completion or are cancelled; they are never interrupted.
  If they are, who knows what could happen? Almost certainly the executor will
  stall some tasks forever. Hopefully you're throwing away the executor and
  moving on with your life.

  Unlike standard j.u.c executors, tasks in this system may be created,
  queried, and cancelled *transactionally*. The executor's state (`State`) is a
  persistent, immutable structure. You perform a transaction with `(txn!
  executor (fn [state] ...))`, which returns a new state. Any transformations
  you apply to the state take place atomically. Note that these functions use
  pure functions without `!`: `submit` instead of `submit!`, etc.

    ; Create a task which blocks...
    (def dog-promise (promise))
    (def dog-task (task/submit! e :find-dog (fn [_] @dog-promise)))
    ; And one that depends on it
    (def adopt-task (task/submit! e :adopt-dog [dog-task]
      (fn [dog] (println :adopting dog) [:adopted dog])))

    ; adopt-task hasn't run because dog-task is still pending.
    (task/txn! e (fn [state]
      ; Make sure the adoption hasn't happened yet
      (if-not (task/pending? state adopt-task)
        state
        ; If it hasn't happened yet, cancel the dog task. Adoption will be
        ; cancelled automatically.
        (let [state' (task/cancel state dog-task)
              ; And do something else
              [state' new-task] (task/submit state' :enter-tomb
                                  (fn [_]
                                    (prn :oh-no)
                                    :tomb-entered))]
          state'))))
  ; prints :oh-no
  (deliver dog-promise :noodle)
  ; Adoption never happens!"
  (:refer-clojure :exclude [name])
  (:require [clojure [pprint :as pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn fatal]]
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
   ; Arbitrary metadata you might like to provide with a task
   data
   ; An array of task IDs we depend on
   dep-ids
   ; A function which takes a map of task IDs (our dependencies) to their
   ; results, and produces a result
   f
   ; An array of input promises from our dependencies. We clear this as soon as
   ; the task begins to avoid retaining memory.
   ^{:tag "[Ljava.lang.Object;", :unsynchronized-mutable true} inputs
   ; The promise we should deliver our own result to
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
         (let [n             (alength inputs)
               input-results (object-array n)]
           ; Fetch input results as an array.
           (loop [i 0]
             (when (< i n)
               (let [res @(aget inputs i)]
                 (when (instance? Throwable res)
                   (throw res))
                 (aset input-results i res))
               (recur (unchecked-inc-int i))))
           ; Immediately discard our input tasks so we don't retain memory.
           ; Note that we have to be in a tail-position try block for this to
           ; work.
           (set! inputs nil)
           ; Run function and deliver output
           (let [out (f (vec input-results))]
             (deliver output out)))
         ; TODO: sort out exactly what classes we should catch & propagate
         ; here. Java people love to do Weird Things in the exception
         ; hierarchy. Check real-pmap from dom-top?
         (catch Throwable t
           (deliver output t))
         (finally
           ; Inform executor we're done
           (try
             (when executor
               (finish-task! executor this))
             (catch Throwable t
               (fatal t "Error finishing task!" this))))))

  Object
  (hashCode [this]
    (hash id))

  (equals [this other]
    (or (identical? this other)
      (and (instance? Task other)
           (= id (.id ^Task other)))))

  (toString [this]
    (str "(Task " name " " id ")")))

(defn pseudotask
  "One of the weird tricks we use (programmers HATE him!) is to abuse the
  id-only equality semantics of Task objects for efficient lookups in our
  internal graphs and sets. This constructs an empty task with the given ID."
  [^long id]
  (Task. id nil nil nil nil nil nil nil))

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

(defn name
  "Returns the name of a task."
  [^Task t]
  (.name t))

(defn data
  "Returns the custom data associated with a task."
  [^Task t]
  (.data t))

(defn dep-ids
  "Returns a vector of dependency IDs of a task."
  [^Task t]
  (vec (.dep-ids t)))

(defn ran?
  "Returns true iff a task ran already."
  [^Task t]
  (realized? (.output t)))

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
  "Fetches a Task from a state by ID. Returns nil if task is not known."
  [^State state, ^long task-id]
  ;(info :get-task (.dep-graph state) task-id)
  (let [^DirectedGraph dep-graph (.dep-graph state)
        ; Taking advantage of our weird-ass equality/hashing semantics
        ^OptionalLong i (.indexOf dep-graph (pseudotask task-id))]
    (when (.isPresent i)
      (.nth dep-graph (.getAsLong i)))))

(defn has-task?
  "Takes a state and a task. Returns true iff the state knows about this task."
  [^State state, task]
  (.contains ^ISet (.vertices ^DirectedGraph (.dep-graph state)) task))

(defn deps
  "Given a State and a Task, returns a vector of Tasks that this task depends
  on. Tasks are nil where the task is no longer known."
  [state ^Task task]
  (mapv (partial get-task state) (.dep-ids task)))

(defn all-deps
  "Returns an iterable of all deps of a task: a flat series of Tasks. Some may
  be nil if they're no longer in the state."
  [state ^Task task]
  (let [deps (reify Function
               (apply [_ task]
                 (or (vals (deps task))
                     [])))]
    (Graphs/bfsVertices task deps)))

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
                 (or (deps task) [])))]
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

(defn submit*
  "Given a state, constructs a fresh task with the given name, optional data,
  optional list of Tasks as dependencies, and a function (f dep-results) which
  takes a vector of dependency results. Returns the state with the new task
  integrated, including a [:new-task task], which you can use to read the
  created task.

  Dependencies must either be in the graph already, or realized, to prevent
  deadlock.

  If the task has no dependencies, it may be ready immediately; the :new-task
  effect will be followed by a [:ready-task task] effect. In either case, the
  final effect *will* contain the newly created task."
  ([state name f]
   (submit* state name nil nil f))
  ([state name deps f]
   (submit* state name nil deps f))
  ([^State state name data deps f]
   (assert+ (not (nil? name)))
   (let [id         (.next-task-id state)
         dep-count  (count deps)
         dep-ids    (long-array dep-count)
         inputs     (object-array dep-count)
         _          (loopr [i 0]
                           [dep deps]
                           (do (aset dep-ids i (.id dep))
                               (aset inputs i (.output dep))
                               (recur (inc i))))
         task       (Task. id name data dep-ids f inputs (promise)
                           (.executor state))
         ; Update dependency graph
         ^DirectedGraph dep-graph (.dep-graph state)
         ^DirectedGraph dep-graph'
         (loopr [^DirectedGraph g (.. dep-graph
                                      linear
                                      (add task))]
                [^Task dep deps]
                (do (assert+
                      (instance? Task dep)
                      IllegalArgumentException
                      (str "Dependencies must be tasks, but got "
                           (pr-str deps)))
                    ; We don't want to add things back to the dep
                    ; graph if they're already finished
                    (if (.contains (.vertices g) dep)
                      (recur (.link g dep task))
                      ; Oh hang on, you're trying to add something that we don't
                      ; have. Is it finished?
                      (do (assert+ (realized? (.output dep))
                                   IllegalStateException
                                   (str "Task " (pr-str task) " dependency "
                                        (pr-str dep)
                                        " is unknown and has not finished. This could cause deadlock!"))
                          (recur g))))
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
            :effects       effects'))))

(defn submit
  "Like submit*, but also returns the created task: [state new-task]. This form
  is more convenient for transactional task creation.

  Given a state, constructs a fresh task with the given name, optional data,
  optional list of Tasks as dependencies, and a function (f dep-results) which
  takes a vector of dependency results. Returns the state with the new task
  integrated, including a [:new-task task], which you can use to read the
  created task.

  Dependencies must either be in the graph already, or realized, to prevent
  deadlock."
  ([state name f]
   (submit state name nil nil f))
  ([state name deps f]
   (submit state name nil deps f))
  ([state name deps data f]
   (let [^State state' (submit* state name data deps f)]
     [state' (nth (.last ^IList (.effects state')) 1)])))

(defn finish
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

(defn cancel
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
  ;(info "GC targets" to-delete)
  (let [^DirectedGraph dep-graph (.dep-graph state)
        deps (reify Function
               (apply [_ task]
                 (try (.in dep-graph task)
                      (catch IllegalArgumentException e
                        (throw (ex-info {:type ::no-such-task
                                         :task task
                                         :state state}))))))
        ; Walk the graph backwards from goal, cancelling tasks
        ^ISet to-delete
        (loopr [^ISet to-delete (.linear (Set/from ^Iterable to-delete))]
               [t (Graphs/bfsVertices ^Iterable goal deps)]
               (recur (.remove to-delete t)))]
    (when (< 0 (.size to-delete))
      (info "GCing" (.size to-delete) "unneeded tasks:" to-delete))
    (reduce cancel state to-delete)))

(defn state-queue-claim-task*!
  "A support function for StateQueue.run. Definitely don't call this yourself.
  I mean, *I* call it, but I've come to terms with my sins.

  Takes a state and a volatile. Tries to move a task from the ready set to the
  running set, and if that works, sets the volatile to that task. Otherwise
  sets the volatile to nil."
  [^State state, vol]
  (let [^ISet ready (.ready-tasks state)]
    (let [size (.size ready)]
    (if (= 0 size)
      ; Empty
      (do (vreset! vol nil)
          state)
      (let [; We do something a bit sneaky here: if there's more than a handful
            ; of tasks, we sample several and pick the one with the lowest ID.
            ; This gives us a good chance of leaving later dependency tasks for
            ; later, which makes joining folds more efficient. Later we might
            ; want an actual prioqueue? Or timing hints?
            task (if (< size 5)
                    (.nth ready 0)
                    (loop [i    1
                           task (.nth ready 0)]
                      (if (= i 5)
                        task
                        (let [task' (.nth ready i)]
                          (recur (unchecked-inc-int i)
                                 (if (< (id task) (id task'))
                                   task
                                   task'))))))]
        (when (.contains ^ISet (.running-tasks state) task)
          (warn "Task" task "is both ready AND running:"
                (with-out-str (pprint state))))
        (assert (not (.isLinear ^ISet (.running-tasks state))))
        (assert (not (.isLinear ^ISet (.ready-tasks state))))
        (vreset! vol task)
        (assoc state
               :ready-tasks   (.remove ready task)
               :running-tasks (.add ^ISet (.running-tasks state) task)))))))

(deftype StateQueue
  ; We don't actually NEED to lock anything, but it's the only way to get a
  ; Condition I've found. Maybe we just back off to Object.await?
  [state, ^ReentrantLock lock, ^Condition not-empty]
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
    (StateQueue. state-atom lock (.newCondition lock))))


(deftype Executor [; The ExecutorService which actually does work
                   ^ExecutorService executor-service
                   ; An atom containing our current state.
                   state
                   ; Our state queue
                   ^StateQueue queue])

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
        this (Executor. exec state state-queue)]
    ; Tie the knot: the state needs to have a reference back to the executor so
    ; tasks created on it can clear themselves from our state when finished
    (swap! state assoc :executor this)
    this))

(defn executor-state
  "Reads the current state of an Executor."
  [^Executor e]
  @(.state e))

(def void-runnable
  "A dummy runnable which only exists to wake up a ThreadPoolExecutor. Might
  also be handy if you want to create tasks that do nothing, e.g. for
  dependency graph sequencing tricks."
  (reify Runnable
    (run [this])))

(defn apply-effects!
  "Attempts to apply all pending effects from an Executor's state. Side effects
  may be interleaved in any order. When an effect is applied it is removed from
  the effect queue. Returns executor. This is generally called automatically as
  tasks complete; you shouldn't need to do it yourself.

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
  the function returned.

  Locks executor queue, since txns may be ~expensive~ and the threadpool is
  constantly mutating it."
  [^Executor executor, f]
  (let [^ReentrantLock lock (.lock ^StateQueue (.queue executor))
        state' (try (.lock lock)
                    (swap! (.state executor) f)
                    (finally (.unlock lock)))]
    ;(info :txn! (with-out-str (pprint @caller-state)))
    ; Apply side effects
    (apply-effects! executor)
    ; Return caller's generated state
    state'))

(defn finish-task!
  "Tells an executor that a task is finished. You probably don't need to call
  this yourself; tasks call this automatically."
  [executor task]
  (txn! executor #(finish % task)))

(defn submit!
  "Submits a new task to an Executor. Takes a task name, an optional collection
  of Tasks as dependencies, optional data, and a function `(f dep-results)`
  which receives a map of dependency IDs to their results. Returns a newly
  created Task object."
  ([executor name f]
   (submit! executor name nil nil f))
  ([executor name deps f]
   (submit! executor name nil deps f))
  ([executor name data deps f]
   (let [; Create the task
         ^State state' (txn! executor #(submit* % name data deps f))
         [_ task] (.last ^IList (.effects state'))]
     task)))

(defn cancel!
  "Cancels a task on an executor. Returns task."
  [executor task]
  (txn! executor #(cancel % task))
  task)

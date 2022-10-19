(ns jepsen.history.task
  "Supports jepsen.history.dataflow by providing a low-level executor for tasks
  with dependencies. Handles threadpool management, wraps queue operations,
  etc."
  (:require [clojure [pprint :as pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ loopr]])
  (:import (clojure.lang IDeref
                         IBlockingDeref
                         IPending)
           (io.lacuna.bifurcan DirectedGraph
                               IEdge
                               IGraph
                               IMap
                               ISet
                               Graphs
                               Map
                               Set)
           (java.util.concurrent ArrayBlockingQueue
                                 ExecutorService
                                 Executors
                                 Future
                                 LinkedBlockingQueue
                                 ThreadPoolExecutor
                                 TimeUnit)
           (java.util.function Function)))

(declare finish-task!
         queue-tasks!)

; Represents low-level tasks. For speed, we hash and compare equality strictly
; by ID. Tasks should not be compared across different executors!
(deftype Task
  [; This task's ID
   ^long id
   ; A friendly name for this task
   name
   ; A collection of tasks we depend on
   deps
   ; A function which takes a map of task IDs (our dependencies) to their
   ; results, and produces a result
   f
   ; A promise of a Future for this task's execution. Delivered only when the
   ; task is queued up on the executor. Wish we could have this earlier so it
   ; didn't need to be stateful, but no dice.
   future
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

; An immutable representation of our executor state
(defrecord State
  [; The next task ID we'll hand out
   ^long next-task-id
   ; A graph of dependencies between tasks. Iff a -> b, b depends on a's
   ; results. Once tasks are completed, they're removed from this graph.
   ^DirectedGraph dep-graph
   ; A set of tasks we've queued for execution. Once tasks are completed,
   ; they're removed from this set.
   ^ISet queued-tasks
   ; A vector of unapplied side effects. Pure functions of the state build up
   ; this vector, and it's applied and emptied by mutable functions like txn!.
   effects])

(defn state
  "Constructs a new executor state"
  []
  (State. 0
          (.forked (DirectedGraph.))
          (.forked (Set.))
          []))

(defn state-done?
  "Returns true when a state has nothing queued and nothing in the dependency
  graph; e.g. there is nothing more for it to do."
  [^State state]
  (and (= 0 (.size ^DirectedGraph (.dep-graph state)))
       (= 0 (.size ^ISet (.queued-tasks state)))))

(defn new-task
  "Given a state, constructs a fresh task with the given name, collection of
  Tasks as dependencies, a function (f dep-results) which takes a map of
  dependency IDs to their results, and an Executor to inform when the task is
  done. Returns new state with a [:new-task t] effect."
  [^State state name deps f executor]
  (let [id         (.next-task-id state)
        task       (Task. id name deps f (promise) (promise) executor)
        ; Update dependency graph
        dep-graph  ^DirectedGraph (.dep-graph state)
        dep-graph' (loopr [^DirectedGraph g (.. dep-graph
                                                linear
                                                (add task))]
                          [dep deps]
                          (do (assert+
                                (instance? Task dep)
                                IllegalArgumentException
                                (str "Dependencies must be tasks, but got "
                                     (pr-str deps)))
                            (recur (.link g dep task)))
                          (.forked g))]
    ;(info :new-task task)
    (assoc state
           :next-task-id (inc id)
           :dep-graph dep-graph'
           :effects (conj (.effects state) [:new-task task]))))

(defn queue-task
  "Takes a state and looks for a task that can be executed. If so, adds the
  task to queued-tasks, and returns the resulting state with with a
  [:queue-task task] effect. If no tasks can be queued, returns state
  unchanged."
  [^State state]
  ; Traverse the graph looking for a node with no edges in to it which hasn't
  ; been queued yet.
  (let [^DirectedGraph dep-graph    (.dep-graph state)
        ^ISet          queued-tasks (.queued-tasks state)]
    ; This is... n^2 inefficient but whatever, just a prototype. We'll profile
    ; and if it's a problem do something smarter, like keeping track of likely
    ; candidates as the graph changes.
    (loopr []
           [^Task t (.vertices dep-graph)]
           (do ;(info :checking t)
               (if (and (not (.contains queued-tasks t))
                        (= 0 (.size (.in dep-graph t))))
                 ; Found a task with no deps!
                 (assoc state
                        :queued-tasks (.add ^ISet (.queued-tasks state) t)
                        :effects (conj (.effects state) [:queue-task t]))
                 (recur)))
           ; Nothing to do
           state)))

(defn queue-tasks
  "Takes a state and queues as many tasks as possible."
  [state]
  (let [state' (queue-task state)]
    (if (identical? state state')
      ; Done!
      state'
      (recur state'))))

(defn finish-task
  "Takes a state and a task which has been executed, and marks it as completed.
  This deletes the state from the queued set and returns the resulting state
  with a [:finish-task t] effect."
  [^State state, ^Task task]
  (assoc state
         :queued-tasks (.remove ^ISet (.queued-tasks state) task)
         :dep-graph    (.remove ^DirectedGraph (.dep-graph state) task)
         :effects      (conj (.effects state) [:finish-task task])))

(defn cancel-task
  "Takes a state and a task to cancel. Deletes the task, and any tasks which
  depend on it, from the graph and queued tasks set. Adds :cancel-task
  effects to the state for each task cancelled."
  [^State state, ^Task task]
  (let [^DirectedGraph dep-graph (.dep-graph state)
        ; A Function which finds tasks depending on this one
        dependents (reify Function
                     (apply [_ task]
                       (.out dep-graph task)))]
    (loopr [dep-graph'    (.linear dep-graph)
            queued-tasks' (.linear (.queued-tasks state))
            effects'      (transient (.effects state))]
           [t (Graphs/bfsVertices task dependents)]
           (recur (.remove dep-graph' t)
                  (.remove queued-tasks' t)
                  (conj! effects' [:cancel-task t]))
           (assoc state
                  :dep-graph    (.forked dep-graph')
                  :queued-tasks (.forked queued-tasks')
                  :effects      (persistent! effects')))))

(deftype Executor [; The ExecutorService which actually does work
                   ^ExecutorService executor-service
                   ; An atom containing our current state.
                   ^State state])

(defn executor
  "Constructs a new Executor for tasks. Executors are mutable thread-safe
  objects."
  []
  (let [procs (-> (Runtime/getRuntime) .availableProcessors)
        ; We use an unbounded queue with a large core pool size, but allow core
        ; threads to time out--we want this executor to shrink to nothing when
        ; not in use.
        exec (doto (ThreadPoolExecutor. procs ; core pool
                                        procs ; max pool
                                        1 TimeUnit/SECONDS ; keepalive
                                        (LinkedBlockingQueue.))
               (.allowsCoreThreadTimeOut))]
    (Executor. exec (atom (state)))))

(defn executor-state
  "Reads the current state of an Executor."
  [^Executor e]
  @(.state e))

(defn apply-effects!
  "Takes an executor and a state, and applies that state's side effects.
  Returns state."
  [^Executor executor, ^State state]
  (doseq [[type ^Task task] (.effects state)]
    (case type
      :new-task    nil
      :queue-task  (let [^ExecutorService exec (.executor-service executor)
                         fut (.submit exec task)]
                     (deliver (.future task) fut))
      :finish-task nil
      :cancel-task (when (realized? (.future task))
                     (.cancel ^Future @(.future task) true))
  state)))

(defn txn!
  "Executes a transaction on an executor. Takes an executor and a function
  which transforms that executor's State. Applies that function to the
  executor's state, then applies any pending side effects. Returns the state
  the function returned.

  Note that this txn may immediately advance the state in other way, and
  execute other side effects. For instance, it might queue up newly-added tasks
  whose dependencies are satisfiable. These side effects are not included in
  this function's return value: it's as if they took place 'automatically' just
  after the transaction finished."
  [^Executor executor, f]
  (let [caller-state (atom nil)
        final-state  (atom nil)]
    (swap! (.state executor)
           (fn txn [state]
             (let [state' (f state)]
               ; Save the state the caller returned
               (reset! caller-state state')
               ; Now we may have tasks to queue as a result of their actions
               (let [state' (queue-tasks state')]
                 (reset! final-state state')
                 ; Now we're going to apply these effects, so we clear the
                 ; effects list.
                 (assoc state' :effects [])))))
    (apply-effects! executor @final-state)
    ; Return caller's generated state
    @caller-state))

(defn finish-task!
  "Tells an executor that a task is finished."
  [executor task]
  (txn! executor #(finish-task % task)))

(defn queue-tasks!
  "Steps an Executor forward by queuing up as many tasks as possible. Returns
  executor."
  [executor]
  (txn! queue-tasks))

(defn submit!
  "Submits a new task to an Executor. Takes a task name, a collection of Tasks
  as dependencies, and a function `(f dep-results)` which receives a map of
  dependency IDs to their results. Returns a newly created Task object."
  [executor name deps f]
  (let [; Create the task
        ^State state' (txn! executor #(new-task % name deps f executor))
        ;_ (prn :submit! :state)
        ;_ (pprint state')
        [_ task] (peek (.effects state'))]
    task))

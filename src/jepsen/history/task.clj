(ns jepsen.history.task
  "Supports jepsen.history.dataflow by providing a low-level executor for tasks
  with dependencies. Handles threadpool management, wraps queue operations,
  etc."
  (:require [clojure [pprint :as pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]])
  (:import (clojure.lang IDeref
                         IBlockingDeref
                         IPending)
           (io.lacuna.bifurcan DirectedGraph
                               IEdge
                               IGraph
                               IMap
                               ISet
                               Map
                               Set)
           (java.util.concurrent ArrayBlockingQueue
                                 ExecutorService
                                 Executors
                                 Future
                                 LinkedBlockingQueue
                                 ThreadPoolExecutor
                                 TimeUnit)))

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
       ; Inform executor we're done, and try queuing any other tasks we can
       (when executor
         (finish-task! executor this)
         (queue-tasks! executor)))

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
   ; The side effect of the last state transition. This object is applied by
   ; calls to step!
   effect])

(defn state
  "Constructs a new executor state"
  []
  (State. 0
          (.forked (DirectedGraph.))
          (.forked (Set.))
          nil))

(defn state-done?
  "Returns true when a state has nothing queued and nothing in the dependency
  graph; e.g. there is nothing more for it to do."
  [^State state]
  (and (= 0 (.size ^DirectedGraph (.dep-graph state)))
       (= 0 (.size ^ISet (.queued-tasks state)))))

(defn state-new-task
  "Given a state, constructs a fresh task with the given name, collection of
  Tasks as dependencies, a function (f dep-results) which takes a map of
  dependency IDs to their results, and an Executor to inform when the task is
  done. Returns new state with a [:new-task t] effect."
  [^State state name deps f executor]
  (let [id         (.next-task-id state)
        task       (Task. id name deps f (promise) executor)
        ; Update dependency graph
        dep-graph  ^DirectedGraph (.dep-graph state)
        dep-graph' (loopr [^DirectedGraph g (.. dep-graph
                                                linear
                                                (add task))]
                          [dep deps]
                          (recur (.link g dep task))
                          (.forked g))]
    ;(info :new-task task)
    (assoc state
           :next-task-id (inc id)
           :dep-graph dep-graph'
           :effect [:new-task task])))

(defn state-queue-task
  "Takes a state and looks for a task that can be executed. If so, adds the
  task to queued-tasks, and returns the resulting state with with a
  [:queue-task task] effect. If no tasks can be queued, returns state with a
  nil effect."
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
                        :effect [:queued-task t])
                 (recur)))
           ; Nothing to do
           (assoc state :effect nil))))

(defn state-finish-task
  "Takes a state and a task which has been executed, and marks it as completed.
  This deletes the state from the queued set and returns the resulting state
  with a [:finished-task t] effect."
  [^State state, ^Task task]
  (assoc state
         :queued-tasks (.remove ^ISet (.queued-tasks state) task)
         :dep-graph    (.remove ^DirectedGraph (.dep-graph state) task)
         :effect       [:finished-task task]))

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

(defn finish-task!
  "Tells an executor that a task is finished."
  [^Executor executor, task]
  (let [state' (swap! (.state executor) state-finish-task task)]
    ;(prn :finish-task! :state)
    ;(pprint state')
    state'))

(defn queue-tasks!
  "Steps an Executor forward by queuing up as many tasks as possible. Returns
  executor."
  [^Executor executor]
  (let [state' (swap! (.state executor) state-queue-task)]
    ;(prn :queue-tasks! :state)
    ;(pprint state')
    (if-let [[_ task] (:effect state')]
      (do
        ; We queued up this task! Hand it to the executor.
        (.submit ^ExecutorService (.executor-service executor) ^Runnable task)
        (recur executor))
      executor)))

(defn submit!
  "Submits a new task to an Executor. Takes a task name, a collection of Tasks
  as dependencies, and a function `(f dep-results)` which receives a map of
  dependency IDs to their results. Returns a newly created Task object."
  [^Executor executor name deps f]
  (let [; Create the task
        state' (swap! (.state executor) state-new-task name deps f executor)
        ;_ (prn :submit! :state)
        ;_ (pprint state')
        [_ task] (:effect state')]
    ; And try to queue it up
    (queue-tasks! executor)
    task))

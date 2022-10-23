(ns jepsen.history.fold-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]
                                [results :as results :refer [Result]]
                                [rose-tree :as rose]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ letr loopr real-pmap]]
            [jepsen.history [core :as hc]
                            [fold :as f]
                            [task :as t]])
  (:import (io.lacuna.bifurcan ISet
                               IList
                               IMap
                               List)
             (java.io StringWriter)))

(def n
  "How aggressive should tests be?"
  1000)

(defn reporter-fn
  "A reporter that pretty-prints failures, because oh my god these are so hard
  to read"
  [{:keys [type] :as args}]
  (with-test-out
    (case type
      :complete (println "Trials complete.")
      ; Don't need to see every trial
      :trial nil
      ; We want to know a failure occurred
      :failure (println "Trial failed, shrinking...")
      ; Don't need to know about shrinking in detail
      :shrink-step (print ".")
      ; Don't care about the original fail; show me the shrunk one
      :shrunk (do (println "\n\n# Shrunk result\n")
                  ;(pprint args)
                  (let [shrunk (:shrunk args)
                        data   (:result-data shrunk)]
                    (pprint data)
                    (when-let [print-fn (:print data)]
                      (println "\n## Printing results\n")
                      (print-fn)))))))

(use-fixtures :once
              (fn [run-tests]
                (binding [clojure.test.check.clojure-test/*default-opts*
                          {:reporter-fn reporter-fn}]
                  (run-tests))))

(def small-pos-int
  "A nonzero positive integer."
  (gen/sized (fn [size] (gen/choose 1 (inc size)))))

(def dog-gen
  "Makes a dog"
  (gen/hash-map :age      small-pos-int
                :legs     small-pos-int
                :cuteness small-pos-int))

(def dogs-gen
  "Makes a vector of dogs"
  (gen/vector dog-gen))

; Some example folds. We give each fold a :model which defines how it *ought*
; to behave when applied to a collection.
(def fold-count
  "A fold which just counts elements."
  {:name :count
   :associative? true
   :model count
   :reducer-identity (constantly 0)
   :reducer (fn [count dog] (+ count 1))
   :post-reducer identity
   :combiner-identity (constantly 0)
   :combiner +
   :post-combiner identity})

(def fold-count-gen
  "Generates a fold that counts elements."
  (gen/let [assoc? (gen/elements [true false])]
    (assoc fold-count
           :associative? assoc?)))

(defn fold-type
  "A fold which always ignores inputs and returns name, and checks that name is
  threaded through correctly at every turn."
  [name]
  {:name         name
   :associative? true
   :model       (fn [_] name)
   :reducer-identity (constantly [:r name])
   :reducer (fn [acc _]
              (assert (= acc [:r name]))
              acc)
   :post-reducer (fn [acc]
                   (assert (= acc [:r name]))
                   [:pr name])
   :combiner-identity (constantly [:combine name])
   :combiner (fn [acc reduced-acc]
               (assert (= acc [:combine name]))
               (assert (= reduced-acc [:pr name]))
               acc)
   :post-combiner (fn [acc]
                    (assert (= acc [:combine name]))
                    name)})

(def fold-type-gen
  "Generates folds which ignore input and return name, checking type safety."
  (gen/let [name (gen/elements [:a :b :c :d])
            assoc? (gen/elements [true false])]
    (assoc (fold-type name)
           :associative? assoc?)))

(def fold-mean-cuteness
  "A fold designed to stress all the reduce/combine steps"
  {:name          :mean-cuteness
   :associative?  true
   :model (fn [dogs]
            (if (seq dogs)
              (/ (reduce + 0 (map :cuteness dogs))
                 (count dogs))
              :nan))
   ; [sum count]
   :reducer-identity (constantly [0 0])
   :reducer (fn [[sum count] dog]
              [(+ sum (:cuteness dog)) (inc count)])
   ; Flip representation to [count sum]
   :post-reducer (fn [[sum count]] [count sum])
   ; Then combine into {:sum s :count c} maps
   :combiner-identity (constantly {:sum 0, :count 0})
   :combiner (fn [acc [count sum]]
               {:sum   (+ (:sum acc) sum)
                :count (+ (:count acc) count)})
   :post-combiner (fn [{:keys [sum count]}]
                    (if (pos? count)
                      (/ sum count)
                      :nan))})

(def fold-mean-cuteness-gen
  "Generates a fold that finds the average cuteness of all dogs"
  (gen/let [assoc? (gen/elements [true false])]
    (assoc fold-mean-cuteness
           :associative? assoc?)))

(declare fold-gen)

(defn fold-fuse-gen
  "Takes two generators of folds, and returns a generator of folds that fuses
  both together."
  [old-fold-gen new-fold-gen]
  (gen/let [old-fold old-fold-gen
            new-fold new-fold-gen]
    (assoc (:fused (f/fuse old-fold new-fold))
           :name [(:name old-fold) (:name new-fold)]
           :model (fn [dogs]
                    [((:model old-fold) dogs)
                     ((:model new-fold) dogs)]))))

(def basic-fold-gen
  "Makes a random, basic fold over dogs"
  (gen/one-of
    [fold-type-gen
     fold-count-gen
     fold-mean-cuteness-gen]))

(def fold-gen
  "Makes a (possibly recursively fused) fold over dogs."
  (gen/recursive-gen (fn [fold-gen]
                       (fold-fuse-gen fold-gen fold-gen))
                     basic-fold-gen))

(defn slow-fold-gen
  "Takes a fold gen and makes its reducers slow."
  [fold-gen]
  (gen/let [fold fold-gen]
    (assoc fold
           :name [:slow (:name fold)]
           :reducer (fn slow [acc x]
                      (Thread/sleep 100)
                      ((:reducer fold) acc x)))))

(defn concurrent-pass
  "Constructs a fresh concurrent pass over chunks. Always does a type pass."
  [chunks]
  (let [result (promise)]
    (assoc (f/concurrent-pass chunks (fold-type (rand-nth [:a :b :c :d]))
                              (fn [x] ;(info :concurrent-pass-deliver x)
                                (deliver result x)))
           :result result)))

(defn concurrent-pass-gen
  "Generates a fresh concurrent pass over chunks."
  [chunks]
  (concurrent-pass chunks))

(defn launch-concurrent-pass-gen
  "Simulates launching a concurrent pass on state. Returns a generator of
  [state' pass] pairs."
  [state chunks]
  (gen/let [pass (concurrent-pass-gen chunks)]
    (f/launch-concurrent-pass state pass)))

(def chunk-scale
  "A scaling parameter for number of chunks in concurrent-pass-join tests"
  25)

(def task-state-steps-gen
  "Generates a sequence of steps to apply to a task executor state."
  (gen/let [n small-pos-int]
    (gen/vector (gen/elements [:run :finish]) n)))

(defn apply-task-state-step
  "Takes a state and a step (either :run or :finish), and applies that step to
  the state, returning the new state (with empty effects). If no changes, returns state itself."
  [state step]
  (let [; Side effect channel for pulling tasks off state
        last-task (volatile! nil)]
    (case step
      :run (let [state' (t/state-queue-claim-task*! state last-task)]
             (if (identical? state state')
               state
               (do ;(info "Claim task" @last-task)
                   (assoc state' :effects (List.)))))

      :finish (let [^ISet running (.running-tasks state)]
                (if (pos? (.size running))
                  ; Finish task
                  (let [task (.nth running 0)]
                    ;(info "Executing" task)
                    (.run ^Runnable task)
                    ; If it crashes, we want to blow up here!
                    (assert (realized? (.output task)))
                    @task
                    (-> (t/finish-task state task)
                        (assoc :effects (List.))))
                  ; Nothing running
                  state)))))

(defn apply-task-state-steps
  "Takes a state and a sequence of steps, and applies those steps to the
  state, returning the new state (with empty effects)."
  [state steps]
  (reduce apply-task-state-step state steps))

(defn exhaust-task-state
  "Drives a task state forward to completion, possibly for side effects?
  Returns final state."
  [state]
  (loop [state state]
    (let [state' (apply-task-state-step state :run)]
      (if-not (identical? state state')
        (recur state')
        (let [state' (apply-task-state-step state :finish)]
          (if-not (identical? state state')
            (recur state')
            ; Nothing to do
            (do (assert+ (t/state-done? state)
                         {:type    :deadlock
                          :running (.running-tasks state)
                          :ready   (.ready-tasks state)
                          :deps    (.dep-graph state)})
                (assoc state :effects (:effects (t/state))))))))))

(defn join-concurrent-passes
  "Joins a series of concurrent passes together. Takes a list of chunks, then
  takes a vector of task executor steps to take before joining each new pass.

  Returns a seq of maps, one for each pass launched, of the form:

    {:chunks
     :state0   The state before the executor did anything
     :state    The state going in to the pass join
     :state'   The state after the pass join
     :old-pass
     :new-pass
     :pass}"
  [{:keys [chunks task-steps]}]
  ; First, get an initial pass launched
  (let [[state0 pass0] (f/launch-concurrent-pass
                         (t/state)
                         (concurrent-pass chunks))
        pass-count (count task-steps)]
    ; Now step through passes
    (loop [results    []
           pass-i     0
           state0     state0
           pass       pass0]
      (if (= pass-i pass-count)
        ; Done
        results
        (let [; Apply steps
              state (apply-task-state-steps state0 (nth task-steps pass-i))
              ; Join new pass
              new-pass (concurrent-pass chunks)
              [state' pass'] (f/join-concurrent-pass
                               state pass new-pass)]
          (recur (conj results {:chunks   chunks
                                :state0   state0
                                :state    state
                                :state'   state'
                                :old-pass pass
                                :new-pass new-pass
                                :pass     pass'})
                 (inc pass-i)
                 state'
                 pass'))))))

(def join-concurrent-passes-inputs-gen
  "Generates a map of inputs for join-concurrent-passes. Keeping the massive
  states test.check generates REPL-accessible is a bear."
  (gen/scale (fn [size]
               ; No real diff between 5 chunks and 50, and ditto, we want small
               ; numbers of steps.
               (inc (/ size chunk-scale 2)))
             (gen/let [chunks (gen/not-empty (gen/vector (gen/return [])))
                       pass-count small-pos-int
                       ; Before each pass, some task steps to take
                       task-steps (gen/vector task-state-steps-gen pass-count)]
               {:chunks     chunks
                :task-steps task-steps})))

(defn passing-result
  "A simple passing result."
  []
  (reify Result
    (pass? [_] true)
    (result-data [_] {})))

(defn err
  "A simple error."
  ([message actual]
   (reify Result
     (pass? [_] false)
     (result-data [_]
       {:message message
        :actual  actual})))
  ([message expected actual]
   (reify Result
     (pass? [_] false)
     (result-data [_]
       {:message message
        :expected expected
        :actual actual}))))

(defn all-results
  "A Result from a seq of Results"
  [results]
  (reify Result
    (pass? [_]
      (every? results/pass? results))

    (result-data [_]
      (results/result-data (first (remove results/pass? results))))))

(defn will-run?
  "Will a task eventually complete? Either it already ran, or its in the state,
  *and* all its deps will complete."
  [state task]
  (or (t/ran? task)
      (and (t/has-task? state task)
           (every? (partial will-run? state) (t/task-deps task)))))

(defn could-run?
  "Either it already ran or it's in the state."
  [state task]
  (or (t/ran? task)
      (t/has-task? state task)))

(defn worker?
  "Is a task actually going to do work, or is it just a split/join?"
  [task]
  (when task
    (let [type (first (t/name task))]
      (or (= type :combine)
          (= type :reduce)))))

(defn actual-workers
  "Returns a collection of all tasks which are actual workers for this
  particular chunk in a given task. Traverses split/join dependencies."
  [task]
  (if (nil? task)
    #{}
    (let [type (first (t/name task))]
      (case type
        (:combine :reduce) #{task}
        (:split-combine :split-reduce :join-combine :join-reduce)
        (into #{} (mapcat actual-workers (t/task-deps task)))))))

(defn join-concurrent-pass-result*
  "Checks to see if a concurrent pass join is OK, with early return"
  [chunks state0 state state' old-pass new-pass pass return!]
  (let [n                  (count chunks)
        get-task           (fn [id] (when id (t/get-task state' id)))
        get-task0          (fn [id] (when id (t/get-task state0 id)))
        old-reduce-tasks0  (mapv get-task0 (:reduce-tasks old-pass))
        old-combine-tasks0 (mapv get-task0 (:combine-tasks old-pass))
        old-combine-tasks  (mapv get-task (:combine-tasks old-pass))
        old-reduce-tasks   (mapv get-task (:reduce-tasks old-pass))
        combine-tasks      (mapv get-task (:combine-tasks pass))
        reduce-tasks       (mapv get-task (:reduce-tasks pass))
        new-combine-tasks  (mapv get-task (:new-combine-tasks pass))
        new-reduce-tasks   (mapv get-task (:new-reduce-tasks pass))

        ; Should be concurrent
        _ (when (not= :concurrent (:type pass))
            (return! (err :wrong-type :concurrent (:type pass))))

        ; Should have a final combine of *some* kind
        _ (when (not (nth combine-tasks (dec n)))
            (return! (err :no-final-combine combine-tasks)))

        ; Which must complete in the new state
        _ (when-not (will-run? state' (peek combine-tasks))
            (return! (err :won't-complete (peek combine-tasks))))

        ; If you gave up, fine
        _ (when-not (:joined? pass)
            (return! (passing-result)))

        ; Let's look at reducers
        _ (doseq [i (range n)]
            (let [; Original old reduce task. Might be missing by now!
                  old-rt0  (nth old-reduce-tasks0 i)
                  ; Old reduce task if present in result state
                  old-rt   (nth old-reduce-tasks i)
                  ; Fused reduce task, if present
                  rt       (nth reduce-tasks i)
                  ; New reduce task, if present
                  new-rt   (nth new-reduce-tasks i)
                  ; Ditto, combiners
                  old-ct0 (nth old-combine-tasks0 i)
                  old-ct  (nth old-combine-tasks i)
                  ct      (nth combine-tasks i)
                  new-ct  (nth new-combine-tasks i)
                  ; Does a task have any side effects besides the given tasks?
                  effects? (fn effects?
                             ([task] (effects? task #{}))
                             ([task besides]
                              (->> (actual-workers task)
                                   (remove besides)
                                   (filter (partial could-run? state'))
                                   seq)))
                  _ (if (and (worker? rt) (could-run? state' rt))
                      ; We have an actual worker for the new reduce task. Old
                      ; and new reduce tasks must be side-effect free (besides
                      ; our own worker)
                      (when (or (effects? old-rt0 #{rt})
                                (effects? old-rt #{rt})
                                (effects? new-rt #{rt}))
                        (return! (err [:duplicate-reduces i]
                                      {:old0 {:ran?      (t/ran? old-rt0)
                                              :effects? (effects? old-rt0)}
                                       :old  (effects? old-rt)
                                       :fused (effects? rt)
                                       :new  (effects? new-rt)})))
                      ; We need a new reducer. Old reducer could be completely
                      ; gone by this point, so we don't require it.
                      (when-not (effects? new-rt #{rt})
                        (return! (err [:missing-new-reduce i]
                                       {:new  (effects? new-rt)}))))
                  ; We don't want to duplicate work on the old reducer.
                  _ (when-not (= old-rt0 old-rt)
                      (when (and (effects? old-rt0) (effects? old-rt))
                        (return! (err [:duplicate-old-reduces i]
                                      {:old0 (effects? old-rt0)
                                       :old  (effects? old-rt)}))))

                  ; OK, let's look at combiners
                  _ (if (and (worker? ct) (could-run? state' ct))
                      ; Doing a fused combine. Should be no effects (other than
                      ; our fused combine itself) for old/new tasks.
                      (when (or (effects? old-ct0 #{ct})
                                (effects? old-ct #{ct})
                                (effects? new-ct #{ct}))
                        (return! (err [:duplicate-combines i]
                                      {:old0 (effects? old-ct0)
                                       :old  (effects? old-ct)
                                       :fused (effects? ct)
                                       :new  (effects? new-ct)})))
                      ; Not doing a fused combine. We need at least a new
                      ; combiner; old one might be unknown.
                      (when-not (effects? new-ct #{ct})
                        (return! (err [:missing-combines i]
                                      {:old0 (effects? old-ct0)
                                       :old  (effects? old-ct)
                                       :fused (effects? ct)
                                       :new  (effects? new-ct)}))))
                  ; We don't want to duplicate work on the old combiner
                  _ (when-not (= old-ct0 old-ct)
                      (when (and (effects? old-ct0) (effects? old-ct))
                        (return! (err [:duplicate-old-combines i]
                                      {:old0 (effects? old-ct0)
                                       :old  (effects? old-ct)}))))

                  ]))
        ]
    (return! (passing-result))))

(defn join-concurrent-pass-result
  "Checks to see if a concurrent pass join is OK."
  [{:keys [chunks state0 state state' old-pass new-pass pass]}]
  ; Mechanics for early return; this code is a BEAR to write functionally
  (let [return! (fn [x]
                  (throw (ex-info nil {:type :early-return
                                       :return x})))
        res (try
              (join-concurrent-pass-result* chunks state0 state state'
                                            old-pass new-pass pass return!)
              (catch clojure.lang.ExceptionInfo e
                (if (= :early-return (:type (ex-data e)))
                  (:return (ex-data e))
                  (throw e))))]
    (reify Result
      (pass? [_] (results/pass? res))
      (result-data [_]
        (assoc (results/result-data res)
               :chunk-count (count chunks)
               :state  state
               :state' state'
               ; The folds are huge and not important
               :old-pass (dissoc old-pass :fold :deliver)
               :new-pass (dissoc new-pass :fold :deliver)
               :pass     (dissoc pass :fold :deliver)
               :print    (fn []
                           (println
                             (f/print-join-plan
                               state' old-pass new-pass pass))))))))

(defn join-concurrent-passes-result
  "Takes inputs for join-concurrent-passes and returns a test.check result.
  Basically checks each step of the join process independently, then finishes
  the scheduler and asserts all folds terminate."
  [inputs]
  (let [steps (join-concurrent-passes inputs)
        ; Check passes individually
        pass-results (mapv join-concurrent-pass-result steps)
        ; Finish execution of final pass
        state (:state' (last steps))
        state' (exhaust-task-state state)]
    ;(prn)
    ;(prn :exhausting-state)
    ;(prn :state state')
    (let [term-results
          (->> steps
               (map (fn [step]
                      (let [pass   (:new-pass step)
                            result (:result pass)]
                        (prn :result result)
                        (if (realized? result)
                          (passing-result)
                          (err :nontermination result))))))]
      (all-results (concat pass-results term-results)))))

; We try to verify that concurrent pass join plans, you know, make *sense*
;
; Cheat sheet for REPL work here:
#_ (do
    (require '[clojure.test.check [generators :as gen] [results :as res]])
    (require '[jepsen.history [fold-test :as ft] [fold :as fold] [task :as t]] :reload)
    ; Run test
    (def qc (ft/join-concurrent-passes-spec))
    ; See error
    (-> qc :shrunk :result-data pprint)
    ; Print final plan
    (-> qc :shrunk :result-data :print (apply []))
    ; See input that failed
    (-> qc :shrunk :smallest first)
    ; Re-run with analysis
    (-> qc :shrunk :smallest first ft/join-concurrent-passes-result res/pass?)
    (-> qc :shrunk :smallest first ft/join-concurrent-passes-result res/result-data pprint)
    )

(defspec join-concurrent-passes-spec {:num-tests n
                                      ;:seed 1666379400858
                                      }
  (prop/for-all [inputs join-concurrent-passes-inputs-gen]
                (prn)
                (prn)
                (println "# RUN " inputs)
                (prn)
                (join-concurrent-passes-result inputs)))

(defn apply-fold-with-reduce
  "Applies a fold naively using reduce"
  [{:keys [reducer-identity
           reducer
           post-reducer
           combiner-identity
           combiner
           post-combiner] :as fold}
   coll]
  (let [reduced (reduce reducer (reducer-identity) coll)
        ;_    (info :reduced reduced)
        post-reduced (post-reducer reduced)
        ;_ (prn :post-reduced (f/ary->vec post-reduced))
        combined (combiner (combiner-identity) post-reduced)
        ;_ (prn :combined (f/ary->vec combined))
        post-combined (post-combiner combined)
        ;_ (prn :post-combined post-combined)
        ]
    post-combined))

(defspec fold-equiv-serial n
  (prop/for-all [dogs       dogs-gen
                 chunk-size small-pos-int
                 fold       fold-gen]
                (let [stdout     (StringWriter.)]
                  (binding [;*out* stdout
                            ;*err* stdout
                            ]
                    (let [executor   (f/executor (hc/chunked chunk-size dogs))
                          model-res  ((:model fold) dogs)
                          reduce-res (apply-fold-with-reduce fold dogs)
                          exec-res   (f/fold executor fold)]
                      (reify Result
                        (pass? [_]
                          (= model-res reduce-res exec-res))

                        (result-data [_]
                          {:fold   (:name fold)
                           :model  model-res
                           :reduce reduce-res
                           :exec   exec-res
                           :log    (str stdout)})))))))

(defn secs
  "Nanos to seconds"
  [nanos]
  (float (/ nanos 1000000000)))

(defn test-fold
  "Runs fold on executor, returning a test.check Result"
  [executor dogs fold]
  (let [t0 (System/nanoTime)
        exec-res   (f/fold executor fold)
        t1 (System/nanoTime)
        model-res  ((:model fold) dogs)
        t2 (System/nanoTime)
        reduce-res (apply-fold-with-reduce fold dogs)
        t3 (System/nanoTime)]
    (info "Tested fold" (:name fold)
          "- exec"  (secs (- t1 t0))
          " reduce" (secs (- t3 t2))
          " model"  (secs (- t2 t1)))
    (reify Result
      (pass? [_]
        (= model-res reduce-res exec-res))

      (result-data [_]
        {:name   (:name fold)
         :model  model-res
         :reduce reduce-res
         :exec   exec-res}))))

; Submits a bunch of folds at the same time to a single executor. Stress-tests
; all the concurrency safety!
(defspec fold-equiv-parallel n
  (prop/for-all [dogs       dogs-gen
                 chunk-size (gen/scale (fn [size]
                                         (inc (/ size 10)))
                                         small-pos-int)
                 ; folds    (gen/vector (slow-fold-gen basic-fold-gen))
                 folds      (gen/vector basic-fold-gen)]
                ; Just for now, cuz all I implemented was concurrent folds
                (let [folds (mapv #(assoc % :associative? true) folds)]
                  (info "parallel dogs" (count dogs) "chunk-size" chunk-size
                        "folds" (mapv :name folds))
                  (let [executor (f/executor (hc/chunked chunk-size dogs))
                        results  (real-pmap (partial test-fold executor dogs)
                                            folds)]
                    (all-results results)))))

; LORGE DATA
(defspec ^:perf ^:focus fold-equiv-parallel-perf 10
  (let [dog-count 100000000]
    (prop/for-all [dogs       (gen/not-empty dogs-gen)
                   ; folds    (gen/vector (slow-fold-gen basic-fold-gen))
                   folds      (gen/not-empty (gen/vector basic-fold-gen))]
                  ; Just for now, cuz all I implemented was concurrent folds
                  (let [folds (mapv #(assoc % :associative? true) folds)
                        dogs  (vec (take dog-count (cycle dogs)))
                        chunk-size (long (/ (count dogs) 32))]
                    (info "parallel dogs" (count dogs) "chunk-size" chunk-size
                          "folds" (mapv :name folds))
                    (let [executor (f/executor (hc/chunked chunk-size dogs))
                          results  (real-pmap (partial test-fold executor dogs)
                                              folds)]
                      (all-results results))))))

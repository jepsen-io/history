(ns jepsen.history.fold-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]
                                [results :as results :refer [Result]]
                                [rose-tree :as rose]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [letr loopr real-pmap]]
            [jepsen.history [core :as hc]
                            [fold :as f]
                            [task :as t]])
  (:import (java.io StringWriter)))

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

(def fold-mean-cuteness
  "A fold designed to stress all the reduce/combine steps"
  {:name :mean-cuteness
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
                    (let [old-res ((:model old-fold) dogs)
                          new-res ((:model new-fold) dogs)]
                      (if (f/fused? old-fold)
                        ; Fusing into a fused fold yields a flat vector.
                        (conj old-res new-res)
                        ; Pair of plain folds
                        [old-res new-res]))))))

(def basic-fold-gen
  "Makes a random, basic fold over dogs"
  (gen/one-of
    [fold-count-gen
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

(defn concurrent-pass-gen
  "Generates a fresh concurrent pass over chunks."
  [chunks]
  (gen/let [fold basic-fold-gen]
    {:type    :concurrent
     :chunks  chunks
     :fold    fold
     :deliver (fn [x])}))

(defn launch-concurrent-pass-gen
  "Simulates launching a concurrent pass on state. Returns a generator of
  [state' pass] pairs."
  [state chunks]
  (gen/let [pass (concurrent-pass-gen chunks)]
    (f/launch-concurrent-pass state pass)))

(def chunk-scale
  "A scaling parameter for number of chunks in concurrent-pass-join tests"
  25)

(defn running-concurrent-pass-gen
  "A generator of a running concurrent pass on a fresh state, which returns
  [state0 state' pass]. State0 is the state with all tasks still registered, so
  you can look up those tasks. State' is the running state. Some of the pass's
  tasks will have started."
  [chunks]
  (gen/let [[state0 pass] (launch-concurrent-pass-gen (t/state) chunks)
            ops           (gen/scale (fn [size]
                                       (inc (/ size chunk-scale)))
                                     (gen/vector
                                       (gen/elements [:run :finish])))]
    ; Run some of the tasks
    (let [; Side effect channel for pulling tasks off state
          last-task (volatile! nil)]
      (loopr [state   state0
              running #{}]
             [op ops]
             (case op
               :run (let [state' (t/state-queue-claim-task*! state last-task)]
                      (if (identical? state state')
                        (recur state' running)
                        (recur state' (conj running @last-task))))
               :finish (if-let [task (first running)]
                         (do (.run ^Runnable task)
                             (recur (t/finish-task state task)
                                    (disj running task)))
                         (recur state running)))
             [; We don't need side effects, and they take up debugging space
              (assoc state0 :effects (:effects (t/state)))
              (assoc state :effects (:effects (t/state)))
              pass]))))

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

(defn ran?
  "Did a task already run?"
  [^jepsen.history.task.Task task]
  (realized? (.output task)))

(defn will-run?
  "Will a task eventually complete? Either it already ran, or its in the state,
  *and* all its deps will complete."
  [state task]
  (or (ran? task)
      (and (t/has-task? state task)
           (every? (partial will-run? state) (t/task-deps task)))))

(defn could-run?
  "Either it already ran or it's in the state."
  [state task]
  (or (ran? task)
      (t/has-task? state task)))

(defn worker?
  "Is a task actually going to do work, or is it just a split/join?"
  [task]
  (when task
    (let [type (first (t/name task))]
      (or (= type :combine)
          (= type :reduce)))))

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
            (let [; Original old reduce task which we're GUARANTEED to have
                  old-rt0  (nth old-reduce-tasks0 i)
                  ; Old reduce task if present in result state
                  old-rt   (nth old-reduce-tasks i)
                  ; Fused reduce task, if present
                  rt       (nth reduce-tasks i)
                  ; New reduce task, if present
                  new-rt   (nth new-reduce-tasks i)
                  ; Which of these could possibly execute and do work?
                  runnable? (fn [task]
                              (and (worker? task)
                                   (could-run? state' task)))
                  _ (if (runnable? rt)
                      (when (or (runnable? old-rt0)
                                (runnable? old-rt)
                                (runnable? new-rt))
                        (return! (err [:duplicate-reduces i]
                                      {:old0 (runnable? old-rt0)
                                       :old  (runnable? old-rt)
                                       :fused (runnable? rt)
                                       :new  (runnable? new-rt)})))
                      ; Separate reducers
                      (when-not (and (or (runnable? old-rt0)
                                         (runnable? old-rt))
                                     (runnable? new-rt))
                        (return! (err [:missing-reduces i]
                                      {:old0 (runnable? old-rt0)
                                       :old  (runnable? old-rt)
                                       :fused (runnable? rt)
                                       :new  (runnable? new-rt)}))))
                  ; We don't want to duplicate work on the old reducer.
                  _ (when-not (= old-rt0 old-rt)
                      (when (and (runnable? old-rt0) (runnable? old-rt))
                        (return! (err [:duplicate-old-reduces i]
                                      {:old0 (runnable? old-rt0)
                                       :old  (runnable? old-rt)}))))

                  ; OK, let's look at combiners
                  old-ct0 (nth old-combine-tasks0 i)
                  old-ct  (nth old-combine-tasks i)
                  ct      (nth combine-tasks i)
                  new-ct  (nth new-combine-tasks i)

                  _ (if (runnable? ct)
                      ; Doing a fused combine
                      (when (or (runnable? old-ct0)
                                (runnable? old-ct)
                                (runnable? new-ct))
                        (return! (err [:duplicate-combines i]
                                      {:old0 (runnable? old-ct0)
                                       :old  (runnable? old-ct)
                                       :fused (runnable? ct)
                                       :new  (runnable? new-ct)})))
                      ; Separate reducers
                      (when-not (and (or (runnable? old-ct0)
                                         (runnable? old-ct))
                                     (runnable? new-ct))
                        (return! (err [:missing-combines i]
                                      {:old0 (runnable? old-ct0)
                                       :old  (runnable? old-ct)
                                       :fused (runnable? ct)
                                       :new  (runnable? new-ct)}))))
                  ; We don't want to duplicate work on the old combiner
                  _ (when-not (= old-ct0 old-ct)
                      (when (and (runnable? old-ct0) (runnable? old-ct))
                        (return! (err [:duplicate-old-combines i]
                                      {:old0 (runnable? old-ct0)
                                       :old  (runnable? old-ct)}))))


                  ]))
        ]
    (return! (passing-result))))

(defn join-concurrent-pass-result
  "Checks to see if a concurrent pass join is OK."
  [chunks state0 state state' old-pass new-pass pass]
  ; Mechanics for early return; this code is a BEAR to write functionally
  (let [return! (fn [x]
                  (throw (ex-info nil {:type :early-return
                                       :return x})))]
    (try
      (join-concurrent-pass-result* chunks state0 state state'
                                    old-pass new-pass pass return!)
      (catch clojure.lang.ExceptionInfo e
        (if (= :early-return (:type (ex-data e)))
          (:return (ex-data e))
          (throw e))))))

; We try to verify that concurrent pass join plans, you know, make *sense*
(defspec ^:focus join-concurrent-pass-spec {:num-tests n
                                            ;:seed 1666379400858
                                            }
  (prop/for-all [{:keys [chunks old-pass new-pass state0 state]}
                 (gen/let [chunks (gen/not-empty
                                    ; No real diff between 5 chunks and 50
                                    (gen/scale (fn [size]
                                                 (inc (/ size chunk-scale 2)))
                                               (gen/vector (gen/return []))))
                           new-pass         (concurrent-pass-gen chunks)
                           [state0 state old-pass] (running-concurrent-pass-gen
                                                     chunks)]
                           {:chunks   chunks
                            :state0   state0
                            :state    state
                            :old-pass old-pass
                            :new-pass new-pass})]
                (let [[state' pass]
                      (f/join-concurrent-pass state old-pass new-pass)
                      r (join-concurrent-pass-result
                          chunks state0 state state' old-pass new-pass pass)]
                  ; Wrap result with more data
                  (reify Result
                    (pass? [_] (results/pass? r))
                    (result-data [_]
                      (assoc (results/result-data r)
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
                                           state' old-pass new-pass pass)))))))))

(defn apply-fold-with-reduce
  "Applies a fold naively using reduce"
  [{:keys [reducer-identity
           reducer
           post-reducer
           combiner-identity
           combiner
           post-combiner]}
   coll]
  (->> coll
       (reduce reducer (reducer-identity))
       post-reducer
       (combiner (combiner-identity))
       post-combiner))

(defspec fold-equiv-serial n
  (prop/for-all [dogs       dogs-gen
                 chunk-size small-pos-int
                 fold       fold-gen]
                (let [stdout     (StringWriter.)]
                  (binding [;*out* stdout
                            ;*err* stdout
                            ]
                    ;(prn :active (Thread/activeCount))
                    (let [executor   (f/executor (hc/chunked chunk-size dogs))
                          model-res  ((:model fold) dogs)
                          reduce-res (apply-fold-with-reduce fold dogs)
                          exec-res   (f/fold executor fold)]
                      (reify Result
                        (pass? [_]
                          (= model-res reduce-res exec-res))

                        (result-data [_]
                          {:model  model-res
                           :reduce reduce-res
                           :exec   exec-res
                           :log    (str stdout)})))))))

(defn test-fold
  "Runs fold on executor, returning a test.check Result"
  [executor dogs fold]
  (let [model-res  ((:model fold) dogs)
        reduce-res (apply-fold-with-reduce fold dogs)
        exec-res   (f/fold executor fold)]
    (reify Result
      (pass? [_]
        (= model-res reduce-res exec-res))

      (result-data [_]
        {:name   (:name fold)
         :model  model-res
         :reduce reduce-res
         :exec   exec-res}))))

(defspec fold-equiv-parallel n
  (prop/for-all [dogs       dogs-gen
                 chunk-size small-pos-int
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

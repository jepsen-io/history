(ns jepsen.history.fold-test
  (:require [cheshire.core :as json]
            [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [clojure.java [io :as io]
                          [shell :refer [sh]]]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]
                                [results :as results :refer [Result]]
                                [rose-tree :as rose]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ letr loopr real-pmap]]
            [jepsen.history [core :as hc]
                            [fold :as f]
                            [task :as t]
                            [test-support :refer [n]]]
            [tesser.core :as tesser])
  (:import (io.lacuna.bifurcan ISet
                               IList
                               IMap
                               List)
           (java.io StringWriter)
           (java.util ArrayList)))

;; Simple example-based tests

(deftest basic-test
  (let [dogs [{:legs 6, :name :noodle},
              {:legs 4, :name :stop-it},
              {:legs 4, :name :brown-one-by-the-fish-shop}]
        chunked-dogs (hc/chunked 2 dogs)
        f (f/folder chunked-dogs)]
    ;(testing "reduce"
    ;  (is (= 6 (reduce (fn reduce-test
    ;                     ([] 0)
    ;                     ([max-legs dog]
    ;                      (max max-legs (:legs dog))))
    ;                   f))))
    (testing "reduce init"
      (is (= 6 (reduce (fn reduce-init-test [max-legs dog]
                         (max max-legs (:legs dog)))
                       0
                       f))))

    (testing "transduce"
      (let [xf (comp (map :name)
                     (remove #{:stop-it}))]
        (is (= [:noodle :brown-one-by-the-fish-shop]
               (transduce xf conj [] f)))))

    (testing "into"
      (is (= #{4 6} (into #{} (map :legs) f))))))

(deftest reduced-test
  (let [f (f/folder (hc/chunked 2 [1 2 3 4 5 6]))]
    (testing "linear"
      (is (= 2
             (reduce (fn [_ x]
                       (if (even? x)
                         (reduced x)
                         x))
                     nil
                     f))))

    (testing "concurrent"
      (is (= 4
             (f/fold f {:reducer-identity (constantly nil)
                        :reducer (fn [_ x]
                                   (if (< 3 x)
                                     (reduced x)
                                     x))
                        ; Reductions yield [2 4 5]
                        :combiner-identity (constantly nil)
                        :combiner (fn [_ x]
                                    (if (< 2 x)
                                      (reduced x)
                                      x))}))))
    ))

(deftest make-fold-test
  (let [xs (hc/chunked 2 [1 3 5 2 4])
        f  (f/folder xs)]
    (testing "just f"
      ; + has all 3 arities
      (is (= 15 (f/fold f +)))
      ; But we can also provide a fn without a post-reducer
      (is (= #{1 3 5 2 4} (f/fold f (fn ([] #{}) ([s x] (conj s x)))))))

    (testing "two fns"
      (is (= ",(3 1),(2 5),(4)!"
             (f/fold f (f/make-fold
                         (fn ([] ())
                           ([xs x] (conj xs x))
                           ([xs] (str xs)))
                         (fn ([] "")
                           ([s x] (str s "," x))
                           ([s] (str s "!"))))
                     ))))

    (testing "map"
      (is (= '(4 2 5 3 1)
             (f/fold f{:reducer-identity list
                       :reducer          conj}
                     )))
      (is (= 6
             (f/fold f {:reducer-identity (constantly 0)
                        :reducer          max
                        :post-reducer     inc}
                     ))))

    (testing "tesser"
      ; A little weird: Tesser's take is unordered, but we're relying on the
      ; particular way its reducer/combiner work here.
      (is (= #{0 1 2}
             (->> (tesser/map dec)
                  (tesser/take 3)
                  (tesser/into #{})
                  (f/tesser f)))))
    ))

;; Generative tests

(defn reporter-fn
  "A reporter that pretty-prints failures, because oh my god these are so hard
  to read"
  [{:keys [type] :as args}]
  (with-test-out
    (case type
      :complete (do (prn) (info "Trials complete."))
      ; Don't need to see every trial
      :trial (do (print ".") (flush))
      ; We want to know a failure occurred
      :failure (do (prn) (info "Trial failed, shrinking..."))
      ; Don't need to know about shrinking in detail
      :shrink-step (do (print ".") (flush))
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
  {:name              :count
   :associative?      true
   :asap?             true
   :model             (fn model [fold chunk-size dogs] (count dogs))
   :reducer-identity  (constantly 0)
   :reducer           (fn [count dog] (+ count 1))
   :post-reducer      identity
   :combiner-identity (constantly 0)
   :combiner          +
   :post-combiner     identity})

(def fold-count-gen
  "Generates a fold that counts elements."
  (gen/let [assoc? (gen/elements [true false])]
    (assoc fold-count
           :associative? assoc?)))

(defn fold-type
  "A fold which always ignores inputs and returns name, and checks that name is
  threaded through correctly at every turn."
  [name]
  (let [red [:r name]
        postred [:pr name]
        comb  [:combine name]]
    {:name              name
     :associative?      true
     :asap?             true
     :model             (fn model [fold _ _]
                          (if (:combiner fold)
                            name
                            postred))
     :reducer-identity (constantly red)
     :reducer           (fn reducer [acc _]
                          (assert (identical? acc red))
                          acc)
     :post-reducer      (fn post-reducer [acc]
                          (assert+ (identical? acc red))
                          postred)
     :combiner-identity (constantly comb)
     :combiner          (fn combiner [acc reduced-acc]
                          (assert+ (identical? acc comb))
                          (assert+ (identical? reduced-acc postred)
                                   (str "Expected " (pr-str postred)
                                        ", got " (pr-str reduced-acc)))
                          acc)
     :post-combiner     (fn post-combiner [acc]
                          (assert (identical? acc comb))
                          name)}))

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
   :asap?            true
   :model (fn [fold chunk-size dogs]
            (if (:combiner fold)
              (if (seq dogs)
                (/ (reduce + 0 (map :cuteness dogs))
                   (count dogs))
                :nan)
              [(count dogs) (reduce + (map :cuteness dogs))]))
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

(defn fold-call-count
  "This fold ensures that each chunk is reduced exactly one time. It keeps an
  atom :counts which is a map of :reducer-identity, :reducer, etc counts for
  each function's invocations. The post-combiner result is the output of this
  map.

  There's no :model for this, because the naive reduce implementation and our
  optimized executor do different things."
  []
  (let [counts (atom {:reducer-identity   0
                      :reducer            0
                      :post-reducer       0
                      :combiner-identity  0
                      :combiner           0
                      :post-combiner      0})]
    {:name              :call-count
     :associative?      true
     :asap?             true
     :reducer-identity  (fn reducer-identity []
                           (swap! counts update :reducer-identity inc)
                           nil)
     :reducer           (fn reducer [acc x]
                          (swap! counts update :reducer inc)
                          nil)
     :post-reducer      (fn post-reducer [acc]
                          (swap! counts update :post-reducer inc))
     :combiner-identity  (fn combiner-identity []
                           (swap! counts update :combiner-identity inc)
                           nil)
     :combiner           (fn combiner [acc x]
                          (swap! counts update :combiner inc)
                          nil)
     :post-combiner      (fn post-combiner [acc]
                          (swap! counts update :post-combiner inc)
                          @counts)}))

(def fold-call-count-gen
  "Generates a fold which counts invocations."
  (gen/let [assoc? (gen/elements [true false])]
    (assoc (fold-call-count)
           :associative? assoc?)))

(def fold-mut-legs
  "Trivial fold which returns a vector of leg counts, one for each dog. Uses
  mutable ArrayLists without synchronization, to ensure that our memory
  guarantees hold."
  (let [id (fn identity [] (ArrayList.))]
    {:name             :mut-legs
     :associative?     true
     :asap?            true
     :model            (fn model [fold chunk-size dogs]
                         (mapv    :legs dogs))
     :reducer-identity id
     :reducer          (fn reducer [^ArrayList l, dog]
                         (.add  l (:legs dog))
                         l)
     :post-reducer      identity
     :combiner-identity id
     :combiner          (fn combiner [^ArrayList acc, ^ArrayList legs]
                          (.addAll acc legs)
                          acc)
     :post-combiner     vec}))

(def fold-mut-legs-gen
  "Generates a mutable legs fold."
  (gen/let [assoc? gen/boolean]
    (assoc fold-mut-legs
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
           :model (fn model [fold chunk-size dogs]
                    [((:model old-fold) fold chunk-size dogs)
                     ((:model new-fold) fold chunk-size dogs)]))))

(defn basic-fold-gen
  "Makes a generator of a random, basic fold over dogs. If given combiner?,
  ensures folds either do or don't have a combiner."
  ([]
   (gen/let [combiner? gen/boolean]
     (basic-fold-gen combiner?)))
  ([combiner?]
   (gen/let [asap? gen/boolean
             fold (gen/one-of
                    [fold-type-gen
                     fold-count-gen
                     fold-mean-cuteness-gen
                     fold-mut-legs-gen])]
     (cond-> (assoc fold :asap? asap?)
       (not combiner?) (dissoc fold
                               :combiner :combiner-identity :post-combiner)))))

(def fold-gen
  "Makes a (possibly recursively fused) fold over dogs. Either every fold will
  have a combiner, or none will."
  (gen/let [combiner? gen/boolean]
    (gen/recursive-gen (fn [fold-gen]
                         (fold-fuse-gen fold-gen fold-gen))
                       (basic-fold-gen combiner?))))

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
  "Constructs a fresh concurrent pass over chunks. Always does a type pass; we
  don't actually run these, and it makes debugging easier."
  [chunks]
  (f/pass (fold-type (rand-nth [:a :b :c :d])) chunks))

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
                    (-> (t/finish state task)
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
  "Will a task eventually complete? Either it already ran, or its in the state."
  [state task]
  (or (t/ran? task)
      (t/has-task? state task)))

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
        deliver-task       (get-task (:deliver-task pass))

        ; Should be concurrent
        _ (when (not= :concurrent (:type pass))
            (return! (err :wrong-type :concurrent (:type pass))))

        ; Should have a final combine of *some* kind
        _ (when (not (nth combine-tasks (dec n)))
            (return! (err :no-final-combine combine-tasks)))

        ; Which must complete in the new state
        _ (when-not (will-run? state' deliver-task)
            (return! (err :won't-complete deliver-task)))

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
                              (when task
                                (->> (f/task-workers task)
                                     (remove besides)
                                     (filter (partial could-run? state'))
                                     seq))))
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
               :pass     (dissoc pass :fold :deliver))))))

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
                        ; (prn :result result)
                        (if (realized? result)
                          (passing-result)
                          (err :nontermination result))))))]
      (all-results (concat pass-results term-results)))))

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

; We try to verify that concurrent pass join plans, you know, make *sense*
; without actually executing them.
(defspec join-concurrent-passes-spec {:num-tests (* n 10)
                                      ;:seed 1666379400858
                                      }
  (prop/for-all [inputs join-concurrent-passes-inputs-gen]
                ;(prn)
                ;(prn)
                ;(println "# RUN " inputs)
                ;(prn)
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
        ]
    (if-not combiner
      post-reduced
      (let [combined (combiner (combiner-identity) post-reduced)
            ;_ (prn :combined (f/ary->vec combined))
            post-combined (post-combiner combined)
            ;_ (prn :post-combined post-combined)
            ]
        post-combined))))

(defn secs
  "Nanos to seconds"
  [nanos]
  (float (/ nanos 1000000000)))

(defn test-fold
  "Runs fold on folder, returning a test.check Result"
  [folder chunk-size dogs fold]
  (let [t0 (System/nanoTime)
        exec-res   (f/fold folder fold)
        t1 (System/nanoTime)
        model-res  ((:model fold) fold chunk-size dogs)
        t2 (System/nanoTime)
        reduce-res (apply-fold-with-reduce fold dogs)
        t3 (System/nanoTime)]
    ;(info "Tested fold" (:name fold) "over" (count dogs) "dogs in"
    ;      (long (Math/ceil (/ (count dogs) chunk-size))) "chunks:"
    ;      "exec"  (secs (- t1 t0))
    ;      "reduce" (secs (- t3 t2))
    ;      "model"  (secs (- t2 t1)))
    (reify Result
      (pass? [_]
        (= model-res reduce-res exec-res))

      (result-data [_]
        {:name   (:name fold)
         :model  model-res
         :reduce reduce-res
         :exec   exec-res}))))

; Runs folds one at a time.
(defspec fold-equiv-serial (* n 10)
  (prop/for-all [dogs       dogs-gen
                 chunk-size (gen/scale (fn [size]
                                         (inc (/ size 10)))
                                         small-pos-int)
                 fold       fold-gen]
                (let [folder   (f/folder (hc/chunked chunk-size dogs))]
                  (test-fold folder chunk-size dogs fold))))

; Submits a bunch of folds at the same time to a single folder. Stress-tests
; all the concurrency safety!
(defspec fold-equiv-parallel n
  (prop/for-all [dogs       dogs-gen
                 chunk-size (gen/scale (fn [size]
                                         (inc (/ size 10)))
                                         small-pos-int)
                 folds      (gen/vector (basic-fold-gen))]
                (let [folder (f/folder (hc/chunked chunk-size dogs))
                      results  (real-pmap
                                 (partial test-fold folder chunk-size dogs)
                                 folds)]
                  (all-results results))))

; LORGE DATA. This one still tests safety and compares results to the models.
(defspec ^:slow fold-equiv-parallel-lorge n
  (let [dog-count 100000000]
    (prop/for-all [dogs       (gen/not-empty dogs-gen)
                   folds      (gen/not-empty (gen/vector (basic-fold-gen)))]
                  (let [dogs  (vec (take dog-count (cycle dogs)))
                        chunk-size (long (/ (count dogs) 64))]
                    ; (info "parallel dogs" (count dogs) "chunk-size" chunk-size
                    ;      "folds" (mapv :name folds))
                    (let [folder (f/folder (hc/chunked chunk-size dogs))
                          results  (real-pmap (partial test-fold folder
                                                       chunk-size dogs)
                                     folds)]
                      (all-results results))))))

; Checks call counts more rigorously to make sure we aren't multiply invoking
; reducers on a chunk.
(defspec call-count-spec n
  (prop/for-all
    [dogs       dogs-gen
     chunk-size (gen/scale (fn [size]
                             (inc (/ size 10)))
                           small-pos-int)
     folds      (gen/vector fold-call-count-gen)]
    (let [folder (f/folder (hc/chunked chunk-size dogs))
          n        (count dogs)
          results  (real-pmap (partial f/fold folder) folds)]
      (all-results
        (map (fn [fold {:keys [reducer-identity reducer post-reducer
                               combiner-identity combiner post-combiner]
                        :as result}]
               (let [c (if (:combiner fold)
                         (long (Math/ceil (/ n chunk-size)))
                         1)]
                 (reify Result
                   (pass? [_]
                     ; If no elements, we don't even need to reduce chunks
                     (and (= reducer-identity (min n c))
                          (= reducer n)
                          (= post-reducer (min n c))
                          (= combiner-identity 1)
                          (= combiner (min n c))
                          (= post-combiner 1)))
                   (result-data [_]
                     (assoc result
                            :n n
                            :c c
                            :folds (mapv :name folds))))))
             folds
             results)))))

(def dogs-dir         "dogs")
(def dogs-sample-file (str dogs-dir "/sample.json"))
(def dogs-file        (str dogs-dir "/dogs.json"))

(defn on-demand-reducible
  "A reducible which invokes (make-coll) to get a collection every time it
  reduces. Simulates what you might do for parsing files on disk without
  retaining the head."
  [make-coll]
  (reify clojure.lang.IReduceInit
    (reduce [this f init]
      (reduce f init (make-coll)))))

(defn gen-dogs-file!
  "Spits out a file dogs/dogs.json with lots of random dogs, and returns a lazy
  chunked collection of `dog-count` dogs in all, backed by that file."
  [dog-count]
  (let [chunk-count 200
        dogs-per-file (Math/ceil (/ dog-count chunk-count))
        sample-size 1e4
        samples-per-file (Math/ceil (/ dogs-per-file sample-size))
        ; Generate just a few for interest
        dog-sample (gen/sample dog-gen sample-size)
        ; Write a file with the sample
        _ (io/delete-file dogs-file true)
        _ (when-not (.exists (io/file dogs-file))
            (info "Making small JSON sample")
            (io/make-parents dogs-sample-file)
            (with-open [w (io/writer dogs-sample-file)]
              (doseq [dog dog-sample]
                (json/generate-stream dog w)
                (.write w "\n")))
            (info "Making that sample... large")
            (dotimes [_ samples-per-file]
              (sh "sh" "-c" (str "cat " dogs-sample-file " >> " dogs-file)))
            (io/delete-file dogs-sample-file))
        chunks (->> (repeat dogs-file)
                    (take chunk-count)
                    (mapv (fn [file]
                            (on-demand-reducible
                              (fn load-chunk []
                                (json/parsed-seq (io/reader file) true)))))
                    hc/chunked)]
    chunks))

; This one's *just* for perf testing. Spews JSON to a file then folds over it as if it were tons of files.
(deftest ^:perf fold-parallel-perf
  ; ten million dogs might be enough -- nona, probably
  (let [dog-count   1e7
        dogs (gen-dogs-file! dog-count)
        exec (f/folder dogs)]
    (dotimes [i 1]
      ; Do a pass!
      (let [folds (->> (gen/sample (basic-fold-gen) 10)
                       (mapv #(assoc %
                                     ; ASAP slower for multiple folds
                                     :asap? false)))
            _ (println "Starting concurrent folds" (pr-str (map :name folds)))
            t0      (System/nanoTime)
            results (real-pmap (partial f/fold exec) folds)
            t1      (System/nanoTime)
            _ (println "Finished folds in " (secs (- t1 t0)) " seconds")
            _ (prn (mapv :name folds))
            _ (binding [*print-length* 8] (prn results))
            ; Now compare to serial
            _ (println)
            _ (println "Comparing to serial execution...")
            t0 (System/nanoTime)
            results1 (mapv (fn [fold]
                             (prn :fold (:name fold))
                             (apply-fold-with-reduce
                               fold
                               dogs))
                           folds)
            t1 (System/nanoTime)
            _ (println "Finished serial folds in " (secs (- t1 t0)) " seconds")
            _ (prn (mapv :name folds))
            _ (binding [*print-length* 4] (prn results1))
            ]))))

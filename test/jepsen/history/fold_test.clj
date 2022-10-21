(ns jepsen.history.fold-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]
                                [results :as results :refer [Result]]
                                [rose-tree :as rose]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap]]
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
      ; Don't need to see every trial
      :trial nil
      ; We want to know a failure occurred
      :failure (println "Trial failed, shrinking...")
      ; Don't need to know about shrinking in detail
      :shrink-step (print ".")
      ; Don't care about the original fail; show me the shrunk one
      :shrunk (do (prn)
                  (pprint (dissoc args :result :fail)))
      ; Result is just a reify; result-data has the goods
      (pprint (dissoc args :result)))))

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

(defn all-results
  "A Result from a seq of Results"
  [results]
  (reify Result
    (pass? [_]
      (every? results/pass? results))

    (result-data [_]
      (first (remove results/pass? results)))))

(defspec ^:focus fold-equiv-parallel n
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

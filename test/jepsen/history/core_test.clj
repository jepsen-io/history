(ns jepsen.history.core-test
    (:require [cheshire.core :as json]
            [clojure [pprint :refer [pprint]]
                     [set :as set]
                     [test :refer :all]]
            [clojure.core.reducers :as r]
            [clojure.java [io :as io]
                          [shell :refer [sh]]]
            [clojure.test :refer [deftest is]]
            [clojure.test.check [clojure-test :refer [defspec]]
                                [generators :as gen]
                                [properties :as prop]
                                [results :as results :refer [Result]]
                                [rose-tree :as rose]]
            [clojure.tools.logging :refer [info warn]]
            [com.gfredericks.test.chuck.clojure-test :as chuck
             :refer [checking for-all]]
            [dom-top.core :refer [assert+ letr loopr real-pmap]]
            [jepsen.history :as h]
            [jepsen.history [core :as hc]
                            [fold :as f]
                            [test-support :refer [n]]]
            [tesser.core :as t])
  (:import (io.lacuna.bifurcan IEntry
                               ISet
                               IMap
                               IntMap
                               Map)
           (java.io StringWriter)
           (java.util ArrayList)
           (jepsen.history IHistory)
           ))

(deftest chunked-test
  (is (= [[1 2] [3]]
         (hc/chunks (hc/chunked 2 [1 2 3])))))

(defn soft-chunked-vec
  "Makes a soft chunked vec for testing"
  [xs chunk-size]
  (let [chunks  (vec (map vec (partition-all chunk-size xs)))
        indices (butlast (reductions + 0 (map count chunks)))
        ;_ (prn :xs xs)
        ;_ (prn :chunks chunks)
        ;_ (prn :indices indices)
        load-nth (fn load-nth [i]
                   ;(prn :load-nth i)
                   (nth chunks i))]
    (hc/soft-chunked-vector (count xs)
                            indices
                            load-nth)))

(deftest soft-chunked-vector-test
  (checking "nats" n
            [xs         (gen/vector gen/nat)
             chunk-size (gen/choose 1 (max 1 (count xs)))]
            (let [v (soft-chunked-vec xs chunk-size)]
              (testing "count"
                (is (= (count xs) (count v))))

              (testing "hash"
                (is (= (hash xs) (hash v)))
                (is (= (.hashCode xs) (.hashCode v))))

              (testing "nth/get/fn"
                (doseq [i (range (count xs))]
                  (is (= (nth xs i) (nth v i)))
                  (is (= (get xs i) (get v i)))
                  (is (= (xs i) (v i)))
                  (is (= (nth xs i ::not-found) (nth v i ::not-found)))
                  (is (= (get xs i ::not-found) (get v i ::not-found)))))

              (testing "equality"
                (is (= xs v))
                (is (= v xs)))

              (testing "seq"
                (is (= (seq xs) (seq v))))

              (testing "string"
                (is (= (str xs) (str v))))

              (testing "pprint"
                (is (= (with-out-str (pprint xs))
                       (with-out-str (pprint v)))))

              (testing "iterator"
                (is (= (iterator-seq (.iterator xs))
                       (iterator-seq (.iterator v)))))

              (testing "reduce"
                (is (= (into [] xs) (into [] v)))
                (is (= (reduce + xs) (reduce + v))))

              (testing "empty"
                (is (= (empty xs) (empty v))))

              (testing "cons"
                (is (= (conj xs ::unique) (conj v ::unique))))

              (testing "assoc"
                (doseq [i (range (count xs))]
                  (is (= (assoc xs i ::unique)
                         (assoc v i ::unique)))))

              (testing "stack"
                (is (= (peek xs) (peek v)))
                (when-not (empty? xs)
                  (is (= (pop xs) (pop v)))))
                )))

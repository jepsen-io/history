(ns jepsen.history-test
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

;; Simple example-based tests

(deftest pprint-test
  (is (= "{:process nil,\n :type :invoke,\n :f :read,\n :value nil,\n :index nil,\n :time nil}\n"
        (with-out-str (pprint (h/op {:type :invoke, :f :read, :value nil})))))

  (testing "extra fields"
    (is (= "{:process nil,\n :type nil,\n :f nil,\n :value nil,\n :extra 3,\n :index nil,\n :time 2}\n"
           (with-out-str (pprint (h/op {:time 2 :extra 3})))))))

(deftest history-test
  (testing "no indices"
    (let [h (h/history [{:process 0, :type :invoke, :f :read}
                        {:process 0, :type :ok, :f :read, :value 5}])]
      (is (= [{:index 0, :process 0, :type :invoke, :f :read, :value nil,
               :time nil}
              {:index 1, :process 0, :type :ok, :f :read, :value 5,
               :time nil}]
             (h/as-maps h)))
      (is (h/dense-indices? h))))

  (testing "sparse indices"
    (let [h (h/history [{:index 5, :process 0, :type :invoke, :f :read}
                        {:index 7, :process 0, :type :ok, :f :read, :value 5}])]
      (is (= [{:index 5, :process 0, :type :invoke, :f :read, :value nil,
               :time nil}
              {:index 7, :process 0, :type :ok, :f :read, :value 5,
               :time nil}]
             (h/as-maps h)))
      (is (not (h/dense-indices? h))))))

;; Generative tests

(def small-pos-int
  "A nonzero positive integer."
  (gen/sized (fn [size] (gen/choose 1 (inc size)))))

(def client-process-gen
  "Generates a random client process"
  (gen/choose 0 3))

(def nemesis-process-gen
  "Generates a random nemesis process"
  (gen/return :nemesis))

(def process-gen
  "Generates a random process."
  (gen/one-of [client-process-gen nemesis-process-gen]))

(def client-f-gen
  "Generates a random :f for a client operation"
  (gen/elements [:r :w]))

(def nemesis-f-gen
  "Generates a random :f for a nemesis operation"
  (gen/elements [:kill :start]))

(def client-invoke-gen
  "Makes a random client invocation"
  (gen/let [process client-process-gen
            f       client-f-gen
            value   (case f
                      :w small-pos-int
                      :r (gen/return nil))]
    {:type    :invoke
     :process process
     :f       f
     :value   value}))

(def nemesis-invoke-gen
  "Makes a random nemesis invocation"
  (gen/let [process nemesis-process-gen
            f       nemesis-f-gen
            value   (gen/elements ["n1" "n2" "n3"])]
    {:type    :info
     :process process
     :f       f
     :value   value}))

(defn invoke-gen
  "Makes a random invocation"
  []
  (gen/one-of [client-invoke-gen nemesis-invoke-gen]))

(defn complete-gen
  "Returns a generator for the given invoke op"
  [{:keys [process f value]}]
  (if (number? process)
    ; Client
    (gen/let [type (gen/elements [:ok :info :fail])
              value (case f
                      :r (if (= type :ok)
                           small-pos-int
                           (gen/return nil))
                      :w (gen/return value))]
      {:process process
       :type    type
       :f       f
       :value   value})
    ; Nemesis
    (gen/return {:process process
                 :type    :info
                 :f       f
                 :value   value})))

(def op-pair-gen
  "Generates a pair of matched invocation and completion."
  (gen/let [invoke   (invoke-gen)
            complete (complete-gen invoke)]
    [invoke complete]))

(def ops-gen*
  "Generates a series of ops without indices"
  ; Either do a pair of ops, or complete a pending op.
  (gen/let [instructions (gen/vector
                           (gen/one-of [op-pair-gen (gen/return :complete)]))]
    ; Restructure to be concurrent
    (loopr [history (transient [])
            ; Map of process to invoke op
            ^IMap pending (.linear (Map.))]
           [instruction instructions]
           (if (= :complete instruction)
             ; Complete a random op
             (if (< 0 (.size pending))
               (let [^IEntry kv (.nth pending 0)
                     process    (.key kv)
                     complete   (.value kv)]
                 (recur (conj! history complete)
                        (.remove pending process)))
               ; Can't complete anything
               (recur history pending))
             ; Invocation
             (let [[{:keys [process] :as invoke} complete] instruction]
               (if-let [last-complete (.get pending process nil)]
                 ; Have to complete before we can start this
                 (recur (-> history (conj! last-complete) (conj! invoke))
                        (.put pending process complete))
                 ; Can begin right away
                 (recur (conj! history invoke)
                        (.put pending process complete)))))
           (persistent! history))))

(def ops-gen
  "Generates a series of ops with dense indices"
  (gen/fmap h/add-dense-indices ops-gen*))

(def Ops-gen
  "Generates a vector of Op records with dense indices."
  (gen/fmap (partial mapv h/map->Op) ops-gen))

(def sparse-Ops-gen
  "Generates a vector of ops with sparse indices"
  (gen/let [ops   ops-gen
            skips (gen/vector small-pos-int (count ops))]
    (mapv (fn [op index]
            (assoc op :index index))
          (map h/op ops)
          (reductions + skips))))

; Some basic transforms on ops
(defn read?
  "Is this a read operation?"
  [op]
  (= :r (:f op)))

(defn rewrite-op
  "Adds a testing field to each operation and wraps the value"
  [{:keys [process value] :as op}]
  (assoc op
         :process (if (integer? process)
                    (inc process)
                    process)
         :value [:rewrite value]))

(defn process-set
  "A reducing fn for finding the set of all processes in a history."
  ([] #{})
  ([ps op]
   (conj ps (:process op))))

;; Checking indexing folds

(defn model-dense-pair-index
  "A naive implementation of a dense pair index."
  [ops]
  (let [pair-index (int-array (count ops))]
    (loopr [; A map of processes to the index of the op they just
            ; invoked
            invokes (transient {})]
           [op ops]
           (let [p (:process op)]
             (case (:type op)
               :invoke
               (if-let [invoke (get invokes p)]
                 (throw (ex-info
                          (str "Process " p " is still executing "
                               (pr-str invoke) " and cannot invoke "
                               (pr-str op))
                          {:type       ::double-invoke
                           :op         op
                           :running-op invoke}))
                 (recur (assoc! invokes p op)))

               :info
               (if (h/client-op? op)
                 ; For clients, you need to have invoked something
                 (if-let [invoke (get invokes p)]
                   (do (aset-int pair-index (:index invoke) (:index op))
                       (aset-int pair-index (:index op) (:index invoke))
                       (recur (dissoc! invokes p)))
                   (throw (ex-info (str "Client " p " logged an :info without invoking anything: " (pr-str op))
                                   {:type    ::info-without-invoke
                                    :op      op})))
                 ; For non-clients, match successive pairs of infos
                 (if-let [invoke (get invokes p)]
                   ; Second
                   (do (aset-int pair-index (:index invoke) (:index op))
                       (aset-int pair-index (:index op) (:index invoke))
                       (recur (dissoc! invokes p)))
                   ; First
                   (recur (assoc! invokes p op))))

               (:ok, :fail)
               (if-let [invoke (get invokes p)]
                 (do (aset-int pair-index (:index invoke) (:index op))
                     (aset-int pair-index (:index op) (:index invoke))
                     (recur (dissoc! invokes p)))
                 (throw (ex-info (str "Process " p " can not complete "
                                     (pr-str op) "without invoking it first")
                                 {:type ::complete-without-invoke
                                  :op   op})))))
           ; Any remaining invokes, fill in -1
           (doseq [op (-> invokes persistent! vals)]
             (aset-int pair-index (:index op) -1)))
    pair-index))

(deftest pair-index-test
  (checking "pair index" n
            [ops        Ops-gen
             chunk-size (gen/choose 1 (max 1 (count ops)))]
            (is (= (vec (model-dense-pair-index ops))
                   (vec (h/dense-history-pair-index
                          (f/folder (hc/chunked chunk-size ops))))))))

(defn model-sparse-history-by-index
  "A simple model of a sparse history by-index map."
  [ops]
  (first
    (reduce (fn [[^IntMap m, i] op]
              [(.put m (:index op) i) (inc i)])
            [(IntMap.) 0]
            ops)))

(deftest sparse-history-by-index-fold-spec
  (checking "by-index" n
            [ops        sparse-Ops-gen
             chunk-size (gen/choose 1 (max 1 (count ops)))]
            (let [ops ops]
              (is (= (model-sparse-history-by-index ops)
                     (f/fold (f/folder (hc/chunked chunk-size ops))
                             h/sparse-history-by-index-fold))))))

(defn check-history-equiv
  "checks that two histories are equivalent."
  [a b]
  ;(prn :a (class a) a)
  ;(prn :b (class b) b)
  (testing "count"
    (is (= (count a) (count b))))
  (testing "hash"
    (is (= (hash a) (hash b)))
    (is (= (.hashCode a) (.hashCode b))))
  (testing "equality"
    (is (= a b))
    (is (= b a))
    (is (.equals a b))
    (is (.equals b a)))
  (testing "string"
    (is (= (str a) (str b))))
  (testing "nth/get/fn"
    (doseq [i (range (count a))]
      (is (= (nth a i) (nth b i)))
      (is (= (get a i) (get b i)))
      (is (= (a i) (b i)))))
  (testing "seq"
    (is (= (seq a) (seq b))))
  (testing "iterator"
    (is (= (iterator-seq (.iterator a))
           (iterator-seq (.iterator b)))))

  (testing "reduce"
    ; trivial reducer
    (is (= (into [] a) (into [] b)))
    ; a set of processes
    (is (= (reduce process-set #{} a) (reduce process-set #{} b))))

  (testing "coll-fold"
    (is (= (r/fold set/union process-set a)
           (r/fold set/union process-set b))))

  (when (instance? IHistory a)
    (testing "pair-index"
      (doseq [i (map :index a)]
        (is (= (h/pair-index a i) (h/pair-index b i)))))

    (testing "get-index"
      (doseq [op a]
        (let [i (:index op)]
          (is (= op (h/get-index a i)))
          (is (= op (h/get-index b i))))))

    (testing "invocation & completion"
      (doseq [op a]
        (if (or (h/invoke? op) (not (h/client-op? op)))
          (is (= (h/completion a op)
                 (h/completion b op)))
          (is (= (h/invocation a op)
                 (h/invocation b op))))))

    (testing "fold"
      (testing "reduce equivalence"
        (let [into-vec {:reducer-identity (constantly [])
                        :reducer conj}]
          (is (= (vec a)
                 (h/fold a into-vec)
                 (h/fold b into-vec)))))

      (testing "complex"
        (is (= (h/fold a h/pair-index-fold)
               (h/fold b h/pair-index-fold)))))

    (testing "tesser"
      (is (= (->> (t/map :f) (t/frequencies) (h/tesser a))
             (->> (t/map :f) (t/frequencies) (h/tesser b)))))))

(defn check-invoke-complete
  "Takes a history, an invocation, and a completion and validates that they're
  of the same process, have ascending indices, etc."
  [h invoke complete]
  (is (map? invoke))
  (is (map? complete))
  (testing "order"
    (is (< (:index invoke) (:index complete)))
    (is (= (:process invoke) (:process complete))))
  (testing "same process"
    (is (= (:f invoke) (:f complete))))
  (testing "nothing between"
    (is (->> (range (inc (:index invoke)) (:index complete))
             (map (partial h/get-index h))
             (filter (partial = (:process invoke)))
             seq
             not))))

(defn check-pairs
  "Checks pair-index related functionality"
  [ops h]
  (testing "pairs"
    (doseq [op ops]
      (testing "symmetric"
        (when (or (h/invoke? op)
                  (not (h/client-op? op)))
          (when-let [completion (h/completion h op)]
            (is (= op (h/invocation h completion))))))
      (if (h/client-op? op)
        ; For clients, we should have an obvious invoke/complete
        ; pair
        (let [invoke (if (h/invoke? op)
                       op
                       (h/invocation h op))
              complete (if (h/invoke? op)
                         (h/completion h op)
                         op)]
          (is (h/invoke? invoke))
          (is (not= invoke complete))
          (when complete
            (is (not (h/invoke? complete)))
            (check-invoke-complete h invoke complete)))
        ; For nemeses, we won't know which is invoke and which is
        ; fail
        (do (is (= :info (:type op)))
            (let [other (h/completion h op)]
              (when other
                (let [[a b] (sort-by :index [op other])]
                  (check-invoke-complete h a b)))))))))


(defn check-history
  "Checks that something works like the given vector of operations."
  [ops h]
  ; Should be equivalent to underlying ops vector
  (check-history-equiv ops h)

  ; But also support get-index
  (testing "get-index"
    (doseq [op ops]
      (is (= op (h/get-index h (:index op))))))

  ; And folds
  (testing "tesser"
    (is (= (frequencies (map :f ops))
           (->> (t/map :f)
                (t/frequencies)
                (h/tesser h)))))

  (testing "tasks"
    (testing "basics"
      (let [a (h/task! h :a (fn [_] :a))
            b (h/task! h :b [a] (fn [[a]] [:b a]))]
        (is (= [:b :a] @b))
        (is (= :a @a))))

    (testing "catch"
      (let [a (h/task! h :crash (fn [_] (/ 1 0)))
            b (h/catch-task! h :catch a (fn [err] :caught))]
        (is (= :caught @b))
        (is (thrown? ArithmeticException @a))))

    (testing "cancel"
      (let [p (promise)
            a (h/task! h :a (fn [_] @p :a))
            b (h/task! h :b [a] (fn [[a]] [a :b]))
            b (h/task! h :c [b] (fn [[b]] [b :c]))]
        (h/cancel-task! h a)
        (p true)
        (is (not (realized? b)))))))

(def dense-history-gen
  "Generator of dense histories."
  (gen/let [ops ops-gen]
    (h/dense-history ops)))

(deftest dense-history-test
  (checking "dense history" n
            [ops Ops-gen]
            (let [h (h/dense-history ops)]
              (check-history ops h)
              (check-pairs ops h)

              (testing "indices"
                (is (h/dense-indices? h))
                (is (= (range (count h))
                       (mapv :index (seq h))))))))

(deftest sparse-history-test
  (checking "clients" n
            [ops sparse-Ops-gen]
            (let [h (h/sparse-history ops)]
              (check-history ops h)
              (check-pairs ops h)
              (testing "indices"
                (is (not (h/dense-indices? h))))))
  (checking "dense history"
            [ops Ops-gen]
            (let [h0 (h/dense-history ops)
                  h1 (h/sparse-history ops)]
              (check-history h0 h1))))

(deftest map-test
  (checking "rewrite" n
            [ops Ops-gen]
            (let [h (h/map rewrite-op (h/dense-history ops))]
              (is (h/dense-indices? h))
              (check-history (mapv rewrite-op ops) h)
              (testing "equivalence to dense history"
                (let [h0 (h/dense-history (map rewrite-op ops))]
                  (check-history-equiv h0 h))))))

(deftest filter-test
  (checking "dense -> clients" n
            [ops Ops-gen]
            (let [h (h/filter h/client-op? (h/dense-history ops))]
              (is (not (h/dense-indices? h)))
              (check-history (vec (filter h/client-op? ops)) h)
              (testing "equivalence to sparse history"
                (let [h0 (h/sparse-history (filter h/client-op? ops))]
                  (check-history-equiv h0 h))))))

(defn transform-history
  "Takes a model and a history and applies a named transformation to both."
  [[model history] transform]
  (case transform
    :dense-history  [model (h/dense-history history)]
    :sparse-history [model (h/sparse-history history)]
    :rewrite-op     [(mapv rewrite-op model)
                     (h/map rewrite-op history)]
    :clients        [(vec (filter h/client-op? model))
                     (h/client-ops history)]
    :oks            [(vec (filter h/ok? model))
                     (h/oks history)]
    :reads          [(vec (filter read? model))
                     (h/filter (comp #{:r} :f) history)]))

(deftest transform-history-test
  ; Generates a series of basic ops, then a series of transformations on top of
  ; it. Applies transforms to both a model and a history, and verifies that the
  ; product checks out.
  (checking "transformed" n
            [ops Ops-gen
             init-transform (gen/elements [:dense-history :sparse-history])
             transform-count (gen/choose 1 5)
             transforms (gen/vector
                          (gen/elements [:sparse-history
                                         :rewrite-op
                                         :clients
                                         :oks
                                         :reads])
                          transform-count)]
            (let [[model history] (reduce transform-history [ops ops]
                                          (cons init-transform transforms))]
              (check-history model history))))

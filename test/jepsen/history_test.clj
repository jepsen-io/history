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

; Some basic transforms on ops
(defn read?
  "Is this a read operation?"
  [op]
  (identical? :r (:f op)))

(defn write?
  "Is this a write operation?"
  [op]
  (identical? :w (:f op)))

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


(defn check-history-equiv
  "Checks that two histories are equivalent. a can be a plain vector, in which
  case we skip history-specific tests."
  [a b]
  ;(prn :a (class a) a)
  ;(prn :b (class b) b)
  (testing "predicates"
    (is (= (associative? a) (associative? b)))
    (is (= (coll? a) (coll? b)))
    (is (= (empty? a) (empty? b)))
    (is (= (indexed? a) (indexed? b)))
    (is (= (list? a) (list? b)))
    (is (= (seqable? a) (seqable? b)))
    (is (= (seq? a) (seq? b)))
    (is (= (sequential? a) (sequential? b)))
    (is (= (vector? a) (vector? b))))

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
    (testing "out of bounds"
      (is (= (nth a -1 :default)
             (nth b -1 :default)))
      (is (= (try (nth a (count a)) (catch Throwable e (class e)))
             (try (nth b (count b)) (catch Throwable e (class e))))))
    (doseq [i (range (count a))]
      (is (= (nth a i) (nth b i)))
      (is (= (get a i) (get b i)))
      (is (= (a i) (b i)))
      (is (= (nth a i ::missing) (nth b i ::missing)))
      (is (= (get a i ::missing) (get b i ::missing)))))
  (testing "destructuring bind"
    (let [[a1 a2 & as] a
          [b1 b2 & bs] b]
      (is (= a1 b1))
      (is (= a2 b2))
      (is (= as bs)))
    (let [[first-a :as as] a
          [first-b :as bs] b]
      (is (= first-a first-b))
      (is (= as bs))))
  (testing "seq"
    (is (= (seq a) (seq b))))
  (testing "iterator"
    (is (= (iterator-seq (.iterator a))
           (iterator-seq (.iterator b)))))

  ; Reductions
  (testing "reduce"
    ; trivial reducer
    (is (= (into [] a) (into [] b)))
    ; a set of processes
    (is (= (reduce process-set #{} a) (reduce process-set #{} b))))

  (testing "coll-fold"
    (is (= (r/fold set/union process-set a)
           (r/fold set/union process-set b))))

  ; Changes
  (testing "empty"
    (is (= (empty a) (empty b))))
  (testing "conj"
    (is (= (conj a ::x) (conj b ::x))))
  (testing "assoc"
    (doseq [i (range (count a))]
      (is (= (assoc a i ::x)
             (assoc b i ::x)))))
  (testing "stack"
    (is (= (peek a) (peek b)))
    (when-not (empty? a)
      (is (= (pop a) (pop b)))))

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


;; Simple example-based tests

(deftest op-test
  (testing "no index"
    (is (thrown? IllegalArgumentException
                 (h/op {:type :invoke, :time 0, :f :read})))))

(deftest pprint-test
  (is (= "{:process nil, :type :invoke, :f :read, :value nil, :index 1, :time 2}\n"
        (with-out-str (pprint (h/op {:index 1, :time 2, :type :invoke, :f :read, :value nil})))))

  (testing "extra fields"
    (is (= "{:process nil,\n :type nil,\n :f nil,\n :value nil,\n :extra 3,\n :index 1,\n :time 2}\n"
           (with-out-str (pprint (h/op {:index 1, :time 2 :extra 3})))))))

(deftest index-op-test
  (let [a1 (h/op {:type :invoke, :index 0})
        a2 (h/op {:type :ok, :index 0})
        b  (h/op {:type :invoke, :index 1})]
    (testing "index="
      (is (h/index= a1 a2))
      (is (not (h/index= a1 b))))
    (testing "index-hash"
      (is (= 0 (h/index-hash a1)))
      (is (= 0 (h/index-hash a2)))
      (is (= 1392991556 (h/index-hash b))))))

(deftest history-test
  (testing "no indices"
    (let [h (h/history [{:process 0, :type :invoke, :f :read}
                        {:process 0, :type :ok, :f :read, :value 5}])]
      (is (= [{:index 0, :process 0, :type :invoke, :f :read, :value nil,
               :time -1}
              {:index 1, :process 0, :type :ok, :f :read, :value 5,
               :time -1}]
             (h/as-maps h)))
      (is (h/dense-indices? h))))

  (testing "sparse indices"
    (let [h (h/history [{:index 5, :process 0, :type :invoke, :f :read}
                        {:index 7, :process 0, :type :ok, :f :read, :value 5}])]
      (is (= [{:index 5, :process 0, :type :invoke, :f :read, :value nil,
               :time -1}
              {:index 7, :process 0, :type :ok, :f :read, :value 5,
               :time -1}]
             (h/as-maps h)))
      (is (not (h/dense-indices? h))))))

(deftest filter-f-test
  (let [h (h/history [{:f :x}
                      {:f [:complex]}
                      {:f :y}])
        [op0 op1 op2] h]
    (testing "empty"
      (check-history-equiv [] (h/filter (constantly false) h)))
    (testing "kw"
      (is (= [op0] (h/filter-f :x h))))
    (testing "compound"
      (is (= [op1] (h/filter-f [:complex] h))))
    (testing "set"
      (is (= [op0 op2] (h/filter-f #{:x :y} h))))))

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
     :value   value
     :time    -1}))

(def nemesis-invoke-gen
  "Makes a random nemesis invocation"
  (gen/let [process nemesis-process-gen
            f       nemesis-f-gen
            value   (gen/elements ["n1" "n2" "n3"])]
    {:type    :info
     :process process
     :f       f
     :value   value
     :time    -1}))

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
       :value   value
       :time    -1})
    ; Nemesis
    (gen/return {:process process
                 :type    :info
                 :f       f
                 :value   value
                 :time    -1})))

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
      (let [a (h/task-call h :a (fn [_] :a))
            b (h/task-call h :b [a] (fn [[a]] [:b a]))]
        (is (= [:b :a] @b))
        (is (= :a @a))))

    (testing "catch"
      (let [a (h/task-call h :crash (fn [_] (/ 1 0)))
            b (h/catch-task-call h :catch a (fn [err] :caught))]
        (is (= :caught @b))
        (is (thrown? ArithmeticException @a))))

    (testing "cancel"
      (let [p (promise)
            a (h/task-call h :a (fn [_] @p :a))
            b (h/task-call h :b [a] (fn [[a]] [a :b]))
            b (h/task-call h :c [b] (fn [[b]] [b :c]))]
        (h/cancel-task h a)
        (p true)
        (is (not (realized? b)))))

    (testing "task sugar"
      (let [a (h/task h a [] :a)
            b (h/task h b :data [x a] [:b x])]
        (is (= [:b :a] @b))))

    (testing "catch sugar"
      (let [a (h/task h a [] (/ 1 0))
            b (h/catch-task h err-handler :data [err a]
                            [:caught (.getMessage err)])]
        (is (= [:caught "Divide by zero"] @b))))))

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
                     (h/filter read? history)]
    ;:writes         [(vec (filter write? model))
    ;                 (h/filter write? history)])
    ))

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
                                         :reads
                                         ;:writes
                                         ])
                          transform-count)]
            (let [[model history] (reduce transform-history [ops ops]
                                          (cons init-transform transforms))]
              (check-history model history))))

(def failed-write-mod
  "Which intervals should be failed writes?"
  7001)

(defn gen-history-file!
  "Writes out a history file of n records, starting at index starting-index,
  containing a few failed writes."
  [filename n starting-index]
  (io/make-parents filename)
  (print ".") (flush)
  ;(info "Writing" filename)
  (with-open [w (io/writer filename)]
    (loop [i starting-index]
      (when (< i (+ starting-index n))
        (let [op (case (mod i 4)
                   ; A write invocation
                   0 {:index i, :type :invoke, :process 0, :f :write, :value i}
                   ; A read invocation
                   1 {:index i, :type :invoke, :process 1, :f :read, :value nil}
                   ; A write completion
                   2 {:index i, :type (if (= 0 (mod i failed-write-mod))
                                        :fail
                                        :ok)
                      :process 0, :f :write, :value (- i 2)}
                   ; A read completion
                   3 {:index i, :type :ok, :process 1, :f :read,
                      :value (- i 3)})]
          (json/generate-stream op w)
          (.write w "\n"))
        (recur (inc i)))))
  filename)

(defn gen-history-files!
  "Constructs a history of n writes, each containing about rough-chunk-size ops
  of a read-write history, with a very small probability of failing a write.
  Returns a soft-chunked-vector over those files."
  [n rough-chunk-size]
  ; First, clear existing files
  (->> (file-seq (io/file "test-history"))
       (filter #(.isFile %))
       (mapv io/delete-file))
  (let [n                 (long n)
        rough-chunk-size  (long rough-chunk-size)
        counts (->> (repeatedly #(-> rough-chunk-size
                                     (- (rand-int 10))
                                     (max 1)))
                     (take (/ n rough-chunk-size))
                     vec)
        rough-n (reduce + counts)
        trim    (- rough-n n)
        counts  (update counts (dec (count counts)) - trim)
        indices (vec (butlast (reductions + 0 counts)))
        ; _ (prn :n n :counts counts :indices indices)
        files   (vec (pmap (fn [n starting-index]
                             (gen-history-file!
                               (str "test-history/" starting-index ".json")
                               n
                               starting-index))
                           counts indices))
        load-chunk (fn load-chunk [i]
                     (->> (json/parsed-seq (io/reader (files i)) true)
                          (map (fn op [op]
                                 (h/op (assoc op
                                              :type (keyword (:type op))
                                              :f    (keyword (:f op))))))))
        scv (hc/soft-chunked-vector n indices load-chunk)]
    (h/history scv {:have-indices?  true
                    :already-ops?   true
                    :dense-indices? true})))

(defn print-mem-stats
  "Prints out some basic memory stats"
  []
  (let [heap-size (.maxMemory (Runtime/getRuntime))
        free-size (.freeMemory (Runtime/getRuntime))]
    (info "Free:"    (long (/ free-size 1024 1024))
          "MB, max:" (long (/ heap-size 1024 1024)) "MB")))

(defn find-g1a
  "Finds aborted reads in a history."
  [h]
  (let [failed-write-h (h/filter (fn [op]
                                   (and (h/fail? op)
                                        (= :write (:f op))))
                                 h)
        failed-writes (h/task h failed-writes []
                              (->> (t/map :value)
                                   (t/set)
                                   (h/tesser failed-write-h)))
        g1a (h/task h g1a [failed failed-writes]
                    (->> (t/filter h/ok?)
                         (t/filter (fn filt [op]
                                     (and (= :read (:f op))
                                          (failed (:value op)))))
                         (t/into [])
                         (t/post-combine (partial sort-by :index))
                         (h/tesser h)))]
    @g1a))

; This test creates a massive (bigger than RAM) on-disk history with a small
; number of aborted reads, then builds a soft-chunked-vector around those
; files, wraps that in a history, and finds those aborted reads using a couple
; folds.
(deftest ^:slow integrative-test
  (let [_ (info "Writing history to disk...")
        h (gen-history-files! 2e8 1e6)]
        ;h (gen-history-files! 1e3 1e2)]
    (info "\nWrote" (count h) "ops to disk")
    ; On my box, this burns through 100 million ops in about 145 seconds;
    ; roughly 690,000 ops/sec.
    (testing "g1a"
      (info "Finding G1a")
      (let [g1a (time (find-g1a h))]
        ; (pprint g1a)
        (is (= #{:read} (set (map :f g1a))))
        ; We know exactly which writes fail
        (is (= (->> (range 0 (count h) failed-write-mod)
                    (filter #(= 2 (mod % 4)))
                    (map #(- % 2)))
               (map :value g1a)))))

    (testing "in-memory"
      (let [reporter (future
                       (while true
                         (print-mem-stats)
                         (Thread/sleep 5000)))]
        (try
          (info "Loading into memory...")
          (time
            (is (thrown? OutOfMemoryError (prn (count (vec h))))))
          (finally
            ; no really, GC, or our remaining tests will explode for lack of
            ; memory
            (while (< 1073741824 (.freeMemory (Runtime/getRuntime)))
              (System/gc)
              (Thread/sleep 100))
            (future-cancel reporter)
            (print-mem-stats)))))))

(ns jepsen.history.task-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
                        [clojure.test.check [clojure-test :refer :all]
                                [generators :as gen]
                                [properties :as prop]
                                [results :refer [Result]]
                                [rose-tree :as rose]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen.history.task :as t]))

(def trials
  "How aggressive should we be?"
  100)

(deftest basic-test
  (let [e     (t/executor)
        log   (atom [])
        log!  (fn [name] (fn [inputs]
                           (swap! log conj [name inputs])
                           [name inputs]))
        ; Takes a promise and returns a function that calls f after p is
        ; delivered
        block-on (fn [p f] (fn [in] @p (f in)))]
    (testing "a single op"
      (let [a (t/submit! e :a (log! :a))]
        (is (= 0 (t/id a)))
        ; Should be deref-able to result of computation, and actually happened.
        (is (= [:a []] @a))
        ; Should have also logged its execution
        (is (= [[:a []]] @log))))

    (testing "name and data"
      (reset! log [])
      (let [a (t/submit! e :a-name :custom-data nil (log! :a))]
        (is (= 1 (t/id a)))
        (is (= :a-name (t/name a)))
        (is (= :custom-data (t/data a)))
        (is (= [[:a []]] @log))))

    (testing "dependent ops"
      (reset! log [])
      (let [ap (promise)
            a (t/submit! e :a nil (block-on ap (log! :a)))
            b (t/submit! e :b [a] (log! :b))
            _ (is (= 2 (t/id a)))
            _ (is (= 3 (t/id b)))
            ; At this juncture a should be (or about to be) running and b
            ; should be waiting on a
            _ (is (not (realized? a)))
            _ (is (not (realized? b)))
            ; Allow a to run
            _ (deliver ap true)
            a-res [:a []]
            _ (is (= a-res @a))
            ; b should run automatically, and receive a's output
            b-res [:b [a-res]]
            _ (is (= b-res @b))
            ; Both should have logged
            _ (is (= [a-res b-res]
                     @log))
            ; Now if we submit c which depends on both completed tasks, it
            ; should run immediately and still see their outputs
            c (t/submit! e :c [b a] (log! :c))
            c-res [:c [b-res a-res]]
            _ (is (= c-res @c))]))))

(deftest exception-test
  (let [e (t/executor)]
    (let [a (t/submit! e :a     (fn [_] (assert false)))
          _ (is (thrown? AssertionError @a))
          b (t/submit! e :b [a] (fn [a-in] :b))]
      _ (is (thrown? AssertionError @b)))))

(deftest cancel-test
  (let [e    (t/executor)
        log  (atom [])
        ; A little worker fn that logs [:start name], then waits for a promise,
        ; then logs [:end name].
        log! (fn [name promise] (fn [inputs]
                                  (swap! log conj [:start name])
                                  @promise
                                  (swap! log conj [:end name])
                                  name))
        ; Submit a task
        ap (promise)
        a (t/submit! e :a nil (log! :a ap))
        ; a runs immediately, but blocks on the ap promise
        ; Add a second task that depends on a, and a third that depends on b
        bp (promise)
        cp (promise)
        b (t/submit! e :b [a] (log! :b bp))
        c (t/submit! e :c [b] (log! :c cp))
        ; Add a fourth that doesn't depend on anything, just to make sure
        ; things still work
        dp (promise)
        d (t/submit! e :d [] (log! :d dp))
        ; Cancel a. This should also cancel b and c.
        state1 (t/txn! e (fn [state]
                           (t/cancel state a)))
        ; Allow a to continue
        _ (deliver ap true)
        ; A should have blown up during execution
        ;_ (is (thrown? InterruptedException @a))
        ; Changed my mind: we can't interrupt tasks safely or it'll break
        ; mutable reducers. A should complete.
        _ (is (= :a @a))
        ; Even if we let b and c run, b and c should not have executed at all,
        ; but a should have started. and completed. D also runs to completion,
        ; since it had no deps. Not sure how to do this without a sleep;
        ; unfortunately this is fragile.
        _ (deliver bp true)
        _ (deliver cp true)
        _ (deliver dp true)
        _ (Thread/sleep 10)
        _ (is (= [[:start :a]
                  [:start :d]
                  [:end :a]
                  [:end :d]]
                 @log))
        ]))

(deftest concurrent-test
  ; Every task should execute exactly once, and in dependency order.
  (let [n     1000
        log   (atom [])
        e     (t/executor)
        tasks (loop [i 0
                     tasks (transient [])]
                (if (= i n)
                  (persistent! tasks)
                  ; Pick some random deps
                  (let [dep-count (rand-int (min 5 (count tasks)))
                        deps (loop [i    0
                                    deps (transient [])]
                               (if (= i dep-count)
                                 (persistent! deps)
                                 ; Pick recent tasks to make it interesting
                                 (let [dep-index (-> (count tasks)
                                                     (- 1 (rand-int 10))
                                                     (max 0))]
                                   (recur (inc i)
                                          (conj! deps (nth tasks dep-index))))))
                        task (t/submit! e
                                        i
                                        deps
                                        (fn go [inputs]
                                          (swap! log conj [i inputs])
                                          i))]
                        (recur (inc i) (conj! tasks task)))))
        ; Await all tasks
        _ (mapv deref tasks)
        log @log]
    ;(pprint log)
    ; Check tasks
    (is (= n (count tasks)))
    (loopr [i 0]
           [task tasks]
           ; Task names should be 0, 1, ...
           (do (is (= i (t/name task)))
               ; Value should also be i
               (is (= i @task))))
    ; Check log
    (is (= n (count log)))
    (loopr [seen? (transient #{})]
           [[i input] log]
           ; We don't want to see a task more than once
           (do (is (not (seen? i)))
               ; Should have seen right inputs
               (let [task (nth tasks i)
                     ; Because dep-id = retval
                     expected-input (t/dep-ids task)]
                 (is (= expected-input input)))))))

(deftest mem-test
  ; We don't want to retain all the memory in the entire dep tree when we
  ; allocate a series tasks that depend on each other.
  (let [heap-size (.maxMemory (Runtime/getRuntime))
        free-size (.freeMemory (Runtime/getRuntime))
        ; We want tasks to fit comfortably in memory
        task-size (long (/ heap-size 20))
        ; Which means we need n altogether to blow the heap
        n         (long (/ (* heap-size 1.2) task-size))
        task-array-size (int (/ task-size 8))
        e         (t/executor)
        make-big-ary (fn task [_]
                       (print ".") (flush)
                       (long-array task-array-size))
        _ (info "creating" n "tasks, each using"
                (long (/ task-size 1024 1024)) "MB"
                "of"
                (long  (/ heap-size 1024 1024)) "MB heap "
                (str "(" (long (/ free-size 1024 1024)) "MB free)"))
        ; Right, now make a linear stream of tasks, each making a big array
        task (loop [i    0
                    task nil]
               (condp = i
                 n task
                 0 (recur (inc i) (t/submit! e i nil make-big-ary))
                   (recur (inc i) (t/submit! e i [task] make-big-ary))))]
    (is (= (alength @task) task-array-size))
    ))

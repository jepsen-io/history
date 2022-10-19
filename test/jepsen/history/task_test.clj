(ns jepsen.history.task-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen.history.task :as t]))

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
      (let [a (t/submit! e :a nil (log! :a))]
        (is (= 0 (t/id a)))
        ; Should be deref-able to result of computation, and actually happened.
        (is (= [:a {}] @a))
        ; Should have also logged its execution
        (is (= [[:a {}]] @log))))

    (testing "dependent ops"
      (reset! log [])
      (let [ap (promise)
            a (t/submit! e :a nil   (block-on ap (log! :a)))
            b (t/submit! e :b [a]   (log! :b))
            _ (is (= 1 (t/id a)))
            _ (is (= 2 (t/id b)))
            ; At this juncture a should be (or about to be) running and b
            ; should be waiting on a
            _ (is (not (realized? a)))
            _ (is (not (realized? b)))
            ; Allow a to run
            _ (deliver ap true)
            a-res [:a {}]
            _ (is (= a-res @a))
            ; b should run automatically, and receive a's output
            b-res [:b {1 a-res}]
            _ (is (= b-res @b))
            ; Both should have logged
            _ (is (= [a-res b-res]
                     @log))
            ; Now if we submit c which depends on both completed tasks, it
            ; should run immediately and still see their outputs
            c (t/submit! e :c [a b] (log! :c))
            c-res [:c {1 a-res, 2 b-res}]
            _ (is (= c-res @c))]))))

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

(deftest cancel-test
  (let [e    (t/executor)
        log  (atom [])
        ; A little worker fn that logs [:start name], then waits for a promise,
        ; then logs [:end name].
        log! (fn [name promise] (fn [inputs]
                                  (swap! log conj [:start name])
                                  @promise
                                  (swap! log conj [:end name])))
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
        ; Cancel a. This should also cancel b and c should be
        ; descheduled.
        state1 (t/txn! e (fn [state]
                           (t/cancel-task state a)))
        _ (is (= [[:cancel-task a] [:cancel-task b] [:cancel-task c]]
                 (:effects state1)))
        ; Allow a to continue
        _ (deliver ap true)
        ; A should have blown up during execution
        _ (is (thrown? InterruptedException @a))
        ; Even if we let b and c run, b and c should not have executed at all,
        ; but a should have started. D runs to completion, since it had no
        ; deps. Not sure how to do this without a sleep; unfortunately this is
        ; fragile.
        _ (deliver bp true)
        _ (deliver cp true)
        _ (deliver dp true)
        _ (Thread/sleep 10)
        _ (is (= [[:start :a]
                  [:start :d]
                  [:end :d]]
                 @log))
        ]))

(ns jepsen.history.core-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen.history.core :as c]))

(deftest chunked-test
  (is (= [[1 2] [3]]
         (c/chunks (c/chunked 2 [1 2 3])))))

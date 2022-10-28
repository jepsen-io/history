(ns jepsen.history.test-support
  "Common variables and utilities for other tests.")

(def n
  "How aggressive should tests be? We always do at least this many trials. QC
  scales up scaling factors to 200. Some of our tests are orders of magnitude
  more expensive than others, so we scale many tests to N x 10 or whatever."
  ;10
  ;100
  1000
  )

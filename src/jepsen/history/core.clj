(ns jepsen.history.core
  "Basic interfaces and protocols, utility functions"
  (:refer-clojure :exclude [chunk])
  (:require [potemkin :refer [definterface+
                               deftype+
                               def-abstract-type
                               ]]
            [tesser.utils :refer [chunk-vec
                                  chunk-array
                                  reducible-chunk]]))

;; Custom collections

(def-abstract-type AbstractVector
  clojure.core.protocols.CollReduce
  (coll-reduce [this f]
               (reduce f this))

  (coll-reduce [this f init]
               (reduce f init this))

  clojure.lang.Associative
  (containsKey [this k]
               (< -1 k (count this)))

  (entryAt [this k]
           (.nth this k))

  (assoc [this k v]
         (.assocN this k v))

  clojure.lang.Indexed
  (nth [this i]
       (.nth this i nil))

  clojure.lang.IPersistentCollection
  (equiv [this x]
         (and (vector? x)
              (= x (into [] this))))

  clojure.lang.IPersistentStack
  (peek [this]
        (when (pos? count)
          (nth this (dec count))))

  (pop [this]
       (pop (vec this)))

  clojure.lang.IPersistentVector
  (length [this]
          (.count this))

  clojure.lang.IReduce
  (reduce [this f]
          (.reduce this f (f)))

  clojure.lang.ILookup
  (valAt [this k]
         (.valAt this k nil))

  (valAt [this k default]
         (.nth this k default))

  clojure.lang.Sequential

  Object
  (equals [this other]
          (.equiv this other))

  potemkin.collections/PotemkinFn
  (invoke [this k]
          (.nth this k nil))

  (invoke [this k default]
          (.nth this k default)))

;; Chunks

(defprotocol Chunkable
  "Chunked collections offer chonky chunks of elements which can be reduced
  over. There should be enough chunks to offer meaningful parallelism, and they
  should be small enough that the system doesn't get bogged down *forever*
  working on a chunk--but they should also be big enough that we spend more
  time processing a chunk than doing bookkeeping and combining work across
  chunks."
  (chunks [this]
          "Returns a collection of chunks, each a reducible."))

(def default-chunk-size
  "How big is a chunk, by default?"
  16384)

(extend-protocol Chunkable
  nil
  (chunks [_] [])

  clojure.lang.IPersistentVector
  (chunks [this]
    (chunk-vec default-chunk-size this))

  clojure.lang.Seqable
  (chunks [this]
    (reducible-chunk default-chunk-size this)))

(deftype Chunked [chunks]
  Chunkable
  (chunks [this]
    chunks)

  clojure.lang.IReduceInit
  (reduce [this f init]
    (reduce (fn [acc chunk]
              (reduce f acc chunk))
            init
            chunks)))

(defn chunked
  "Takes a collection of chunks and creates a Chunked wrapper around them. Or,
  with two args, slices up a collection into chunks of the given size, and
  makes a Chunked out of them."
  ([chunks]
   (Chunked. chunks))
  ([n coll]
   (Chunked. (reducible-chunk n coll))))

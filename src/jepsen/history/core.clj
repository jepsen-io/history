(ns jepsen.history.core
  "Basic interfaces and protocols, utility functions"
  (:refer-clojure :exclude [chunk])
  (:require [clojure.core :as c]
            [dom-top.core :refer [assert+]]
            [potemkin :refer [definterface+
                               deftype+
                               def-abstract-type
                               ]]
            [tesser.utils :refer [chunk-vec
                                  chunk-array
                                  reducible-chunk]])
  (:import (clojure.lang IHashEq
                         IPersistentCollection
                         IPersistentVector)
           (java.lang.ref SoftReference)
           (java.util Arrays)))

;; Custom collections

(def-abstract-type AbstractVector
  ;clojure.core.protocols.CollReduce
  ;(coll-reduce [this f]
  ;             (reduce f this))

  ;(coll-reduce [this f init]
  ;             (reduce f init this))

  clojure.lang.Associative
  (containsKey [this k]
               (< -1 k (.count this)))

  (entryAt [this k]
           (.nth this k))

  (assoc [this k v]
         (.assocN this k v))

  IHashEq
  (hasheq [this]
          (.hasheq ^IHashEq (vec this)))

  clojure.lang.Indexed
  (nth [this i]
       ; Note that the form with a not-found argument does not perform bounds
       ; checking! Yup, this is how Clojure vectors work!
       (if (and (<= 0 i) (< i (.count this)))
         (.nth this i nil)
         (throw (IndexOutOfBoundsException.))))

  clojure.lang.IPersistentCollection
  (cons [this x]
        (.cons ^IPersistentCollection (vec this) x))

  (empty [this] [])

  (equiv [this x]
         (and (vector? x)
              (= x (into [] this))))

  clojure.lang.IPersistentStack
  (peek [this]
        (let [c (.count this)]
          (when (pos? c)
            (nth this (dec c)))))

  (pop [this]
       (pop (vec this)))

  clojure.lang.IPersistentVector
  (assocN [this, i, x]
          (.assocN ^IPersistentVector (vec this) i x))

  (length [this]
          (.count this))

  clojure.lang.IReduce
  (reduce [this f]
          (.reduce this f (f)))

  clojure.lang.ILookup
  (valAt [this k]
         (.nth this k nil))

  (valAt [this k default]
         (.nth this k default))

  clojure.lang.Reversible
  (rseq [this]
        (rseq (vec this)))

  clojure.lang.Sequential

  Object
  (equals [this other]
          (.equiv this other))

  (hashCode [this]
            (.hashCode ^Object (vec this)))

  (toString [this]
    (.toString ^Object (vec this)))

  potemkin.collections/PotemkinFn
  (invoke [this k]
          (.nth this k nil))

  (invoke [this k default]
          (.nth this k default)))

;; Chunks
;
; Look, this whole thing might seem a little incoherent. I can't exactly figure
; out all the different ways that we ought to provide chunks to various
; systems. Hopefully some experience actually USING this library will be
; informative and we can clean it up.

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

  ; I don't know if this is really a good idea. In a sense a Chunked is "like
  ; the underlying collection, but split up", so when we reduce, we reduce over
  ; the elements in each chunk in turn. I only wrote this for
  ; testing/demonstration purposes.
  clojure.lang.IReduceInit
  (reduce [this f init]
    (reduce (fn [acc chunk]
              (reduce f acc chunk))
            init
            chunks)))

(defn chunked
  "Takes a collection of chunks and creates a Chunked wrapper around them
  which, when asked for chunks, returns exactly that collection. Or, with two
  args, slices up a collection (e.g. a vector) into chunks of the given size,
  and makes a Chunked out of them."
  ([chunks]
   (Chunked. chunks))
  ([n coll]
   (Chunked. (reducible-chunk n coll))))

(deftype+ SoftVector [name load-nth ^objects refs]
  AbstractVector

  Chunkable
  (chunks [this]
    this)

  clojure.lang.Counted
  (count [this]
    (alength refs))

  clojure.lang.Indexed
  (nth [this i not-found]
    (let [^SoftReference r (aget refs i)
          element (when r (.get r))]
      (or element
          ; Avoid concurrently re-fetching the same element
          (locking r
            ; Might have been replaced by another thread
            (or (when-let [^SoftReference r' (aget refs i)]
                  (.get r'))
                (let [element (load-nth i)]
                  (aset refs i (SoftReference. element))
                  element)))
          not-found)))

  clojure.lang.Seqable
  (seq [this]
    (letfn [(seq-from [i]
              (when (< i (alength refs))
                (lazy-seq
                  (cons (nth this i) (seq-from (inc i))))))]
      (seq-from 0)))

  clojure.lang.Sequential

  Object
  (equals [this other]
    (identical? this other))

  (toString [this]
    (let [sb (StringBuilder.)]
      (.append sb "(SoftVector ")
      (.append sb (pr-str name))
      (.append sb " [")
      (doseq [^SoftReference r refs]
        (.append sb (if (.get r) "▒" " ")))
      (.append sb "])"))))

(defn soft-vector
  "Takes a number of elements n and a function `(load-nth i)` which takes an
  index in [0, n) and returns a value at that index. Returns a counted,
  indexed, seqable, sequential, chunkable collection whose elements are the
  values [(load-nth 0), (load-nth 1), ... (load-nth (dec n))]. Values are
  loaded on-demand and cached in soft references. Use this to represent chunked
  collections bigger than memory. The chunks are the vector itself.

  Takes an optional name for debugging."
  ([n load-nth]
   (soft-vector nil n load-nth))
  ([name n load-nth]
   ; Populate initial refs array so we have things to lock
   (let [refs (object-array n)]
     (loop [i 0]
       (when (< i n)
         (aset refs i (SoftReference. nil))
         (recur (inc i))))
     (SoftVector. name load-nth refs))))

(defn chunked-vector-chunk-id
  "Takes an array of starting indices for each chunk and an index in a chunked
  vector; returns the chunk ID for that index."
  ^long [^long count, ^ints indices, ^long target]
  (assert+ (< -1 target count)
           ArrayIndexOutOfBoundsException
           (str "Index " target " out of bounds for vector of " count
                " elements"))
  (let [i (Arrays/binarySearch indices (int target))]
    (if (<= 0 i)
      ; Found an exact hit
      i
      ; We found (- insertion-point) - 1, where insertion point is the index of
      ; the first element *greater* than the key. The chunk we want is the
      ; chunk *before* that, so we add 2 to the negative index here.
      (-> i (unchecked-add-int 2) unchecked-negate))))

(deftype+ SoftChunkedVector [name
                             ; Cached count
                             ^int      count
                             ; The starting index of each chunk. Initially -1
                             ; for all chunks.
                             ^ints     indices
                             ; A SoftVector actually storing our chunks
                             ^SoftVector chunks]
  AbstractVector

  Chunkable
  (chunks [this] chunks)

  clojure.lang.Counted
  (count [this] count)

  clojure.lang.Indexed
  (nth [this i not-found]
    (let [chunk-id (chunked-vector-chunk-id count indices i)
          chunk (nth chunks chunk-id)
          chunk-i (- i (aget indices chunk-id))]
      (nth chunk chunk-i not-found)))

  clojure.lang.IReduceInit
  (reduce [this f init]
    (let [; We need to wrap f to determine if it terminated early due to
          ; returning a reduced value. We *double* wrap these, then do a nested
          ; reduce over chunks.
          wrapped-f (fn wrapper [acc x]
                      (let [res (f acc x)]
                        (if (reduced? res)
                          (reduced res)
                          res)))]
      (loop [acc init
             i   0]
        (if (= i (alength indices))
          (unreduced acc)
          (let [chunk (nth chunks i)
                acc'  (reduce wrapped-f acc chunk)]
            (if (reduced? acc')
              (unreduced acc')
              (recur acc' (inc i))))))))

  clojure.lang.Seqable
  (seq [this]
    (let [s (mapcat identity chunks)]
      (when (seq s) s)))

  clojure.lang.Sequential

  java.lang.Iterable
  (forEach [this consumer]
    (.forEach ^Iterable (mapcat identity chunks) consumer))

  (iterator [this]
    (.iterator ^Iterable (mapcat identity chunks)))

  (spliterator [this]
    (.spliterator ^Iterable (mapcat identity chunks)))

  Object)

(defn soft-chunked-vector
  "Makes a vector divided into several chunks, each stored in a soft reference,
  so the collection can be much larger than RAM. Takes an overall count for the
  vector's elements, and a list of starting indices--the first being 0, the
  second being the index of the first element in the second chunk, the third
  being the index of the first element in the third chunk, and so on. Takes a
  function load-nth which loads a chunk from disk. The resulting collection is
  also Chunkable, so it works with history.fold collections."
  ([count starting-indices load-nth]
   (soft-chunked-vector nil count starting-indices load-nth))
  ([name count starting-indices load-nth]
   (SoftChunkedVector. name count (int-array starting-indices)
                       (soft-vector name (c/count starting-indices) load-nth))))

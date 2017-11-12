(ns credn.core
  (:require [clojure.set :as set])
  (:refer-clojure :exclude [compare]))

(defprotocol ICRDT
  "Op-based CRDTs implement this protocol."
  (step [this op] "step applies an operation to a CRDT, returning a new CRDT. nil is a valid op, and yields the same CRDT"))

;; ======================================================================
;; Vector Clocks

(defn ->replica-ids
  "Returns all the replica-ids present in a vector-clock"
  [clock]
  (keys (:replica->counter clock)))

(defn every-replica?
  "Given two vector clocks a and b, it compares the counters for each replica id with pred. Returns true if all comparisons return true

  (pred counter-a-for-some-replica counter-b-for-some-replica) => bool"
  [pred a b]
  {:pre [(ifn? pred)]}
  (let [all-rids (set/union (keys a) (keys b))]
    (every? (fn [rid]
              (let [counter-a (get a rid 0)
                    counter-b (get b rid 0)]
                (pred counter-a counter-b)))
            all-rids)))

(defn any-replica?
  "Like every-replica? but returns true if any of the comparisons returns true"
  [pred a b]
  {:pre [(ifn? pred)]}
  (let [all-rids (set/union (keys a) (keys b))]
    (boolean (some (fn [rid]
                     (let [counter-a (get a rid 0)
                           counter-b (get b rid 0)]
                       (pred counter-a counter-b)))
                   all-rids))))

(defn compare
  "Compares two the inner maps of two vector clocks and it is used to track causality. Returns -1, 0, 1.

  If vector-clock b is greater than a, it means that b is causally dependent on a.

  (< a b) => true, then b happened after a, after seeing a, returns -1
  (= a b) => true, then b and a happened concurrently, returns 0
  (> a b) => true, then b happened before a, and then a happened, returns 1 "
  [a b]
  (let [a-greater? (every-replica? (fn [counter-a counter-b]
                                     (or (zero? counter-b) (<= counter-b counter-a)))
                                   a b)
        b-greater? (every-replica? (fn [counter-a counter-b]
                                     (or (zero? counter-a) (<= counter-a counter-b)))
                                   a b)]
      (cond
        (and a-greater? b-greater?) 0
        b-greater?                 -1
        a-greater?                 1
        :else                       0)))

(defrecord ^{:doc "Vector clocks represent the number of operations that each replica has seen. It is a map from replica-id (rid, uuid) to a counter (int). Each replica keeps a vector clock, tracking the versions that it knows that other replicas are at. It can only increment its own vector clock and emit an operation referencing its own replica-id.

Use `deref`` to get the underlying replica-id->counter hash-map and `compare`` to check if one vector clock is casually dependent on the other"}
    VectorClock [replica->counter]
  clojure.lang.IDeref
  (deref [this] replica->counter)
  java.lang.Comparable
  (compareTo [a b]
    (compare (:replica->counter a) (:replica->counter b))))

(defmethod print-method VectorClock [clock ^java.io.Writer w]
  (.write w "#credn/vector-clock ")
  (.write w (pr-str (:replica->counter clock))))

(defn inc-at
  "Moves the vector clock forward in time in the replica"
  [clock replica-id]
  (update-in clock [:replica->counter replica-id] (fnil inc 0)))

(defn ->version
  "Returns a vector clock that tracks the version for only one replica-id"
  [clock replica-id]
  (VectorClock. {replica-id (or (get-in clock [:replica->counter replica-id]) 0)}))

(defn successor?
  "Checks if vector-clock b is one of the direct successors of vector-clock a.

  Direct successor, means that there is only one inc-at operation between them."
  [a b]
  (and (any-replica? (fn [counter-a counter-b]
                       (= 1 (- counter-b counter-a)))
                     @a @b)
       (every-replica? (fn [counter-a counter-b]
                         (let [diff (- counter-b counter-a)]
                           (or (zero? diff) (= 1 diff))))
                       @a @b)))

(defn merge-clocks
  "Given two vector clocks, keeps the latest version for each replica-id.

  Useful when all the changes from one of the replicas was merged into the other."
  [a b]
  (VectorClock. (merge-with (fn [a b]
                              (if (and a b)
                                (max a b)
                                (or a b)))
                            (:replica->counter a)
                            (:replica->counter b))))

(defn vector-clock
  "Creates a new vector-clock"
  ([] (vector-clock {}))
  ([replica->counter] (VectorClock. replica->counter)))

(ns credn.core
  (:require [clojure.set :as set])
  (:refer-clojure :exclude [compare]))

(defprotocol ICRDT
  (step [this op]))

;; ======================================================================
;; Vector Clocks

(defn every-replica? [pred a b]
  {:pre [(ifn? pred)]}
  (let [all-rids (set/union (keys a) (keys b))]
    (every? (fn [rid]
              (let [counter-a (get a rid 0)
                    counter-b (get b rid 0)]
                (pred counter-a counter-b)))
            all-rids)))

(defn any-replica? [pred a b]
  {:pre [(ifn? pred)]}
  (let [all-rids (set/union (keys a) (keys b))]
    (boolean (some (fn [rid]
                     (let [counter-a (get a rid 0)
                           counter-b (get b rid 0)]
                       (pred counter-a counter-b)))
                   all-rids))))

;; Used to track causality.

;; if one vector-clock is greater than another, it means it is causally dependent

;; (< a b) => true, then b happened after a, after seeing a
;; (= a b) => true, then b and a happened concurrently
;; (> a b) => true, then b happened before a, and then a happened

(defn compare [a b]
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

(defrecord VectorClock [replica->counter]
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
  [clock replica-id]
  (VectorClock. {replica-id (or (get-in clock [:replica->counter replica-id]) 0)}))

(defn successor?
  "Checks if b is one of the direct successors of a"
  [a b]
  (and (any-replica? (fn [counter-a counter-b]
                       (= 1 (- counter-b counter-a)))
                     @a @b)
       (every-replica? (fn [counter-a counter-b]
                         (let [diff (- counter-b counter-a)]
                           (or (zero? diff) (= 1 diff))))
                       @a @b)))

(defn merge-clocks [a b]
  (VectorClock. (merge-with (fn [a b]
                              (if (and a b)
                                (max a b)
                                (or a b)))
                            (:replica->counter a)
                            (:replica->counter b))))

(defn ->replica-ids [clock]
  (keys (:replica->counter clock)))

(defn vector-clock
  ([] (vector-clock {}))
  ([replica->counter] (VectorClock. replica->counter)))

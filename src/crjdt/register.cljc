(ns crjdt.register
  (:require [clojure.set :as set]
            [crjdt.util :as util]
            [crjdt.core :as crdt])
  (:import [crjdt.core ICRDT]))

(defprotocol IRegister
  (assign-op [this v]))

;; ======================================================================
;; Last Write Wins Register

(defrecord LWW [v latest-ts]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_] v)
  IRegister
  (assign-op [this v] [::assign {::value v ::ts (util/now)}])
  ICRDT
  (step [this [op-name op-args]]
    (case op-name
      ::assign (if (compare latest-ts (::ts op-args))
                 (assoc this :v (::value op-args))
                 this)
      this)))

;; ======================================================================
;; Vector Clocks

;; Used to track causality.

;; if one vector-clock is greater than another, it means it is causally dependent

;; (< a b) => true, then b happened after a, after seeing a
;; (= a b) => true, then b and a happened concurrently
;; (> a b) => true, then b happened before a, and then a happened

(defrecord VectorClock [replica->counter]
  java.lang.Comparable
  (compareTo [a b]
    (let [all-replica-ids (set/union (keys (:replica->counter a))
                                     (keys (:replica->counter b)))
          a-greater?      (every? (fn [replica-id]
                                    (let [counter-a (get-in a [:replica->counter replica-id])
                                          counter-b (get-in b [:replica->counter replica-id])]
                                      (or (nil? counter-b)
                                          (and (some? counter-a) (<= counter-b counter-a)))))
                                  all-replica-ids)
          b-greater?      (every? (fn [replica-id]
                                    (let [counter-a (get-in a [:replica->counter replica-id])
                                          counter-b (get-in b [:replica->counter replica-id])]
                                      (or (nil? counter-a)
                                          (and (some? counter-b) (<= counter-a counter-b)))))
                                  all-replica-ids)]
      (cond
        (and a-greater? b-greater?) 0
        a-greater?                  -1
        b-greater?                  1
        :else                       0))))

(defn inc-at
  "Moves the vector clock forward in time in the replica"
  [clock replica-id]
  (update-in clock [:replica->counter replica-id] (fnil inc 0)))

(defn merge-clocks [a b]
  (VectorClock. (merge-with (fn [a b]
                              (if (and a b)
                                (max a b)
                                (or a b)))
                            (:replica->counter a)
                            (:replica->counter b))))

;;  ======================================================================
;; Multi-Value Register

(defrecord MV [replica-id v ^VectorClock clock]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_] v)
  IRegister
  (assign-op [this v] [::assign {::value v ::version (inc-at clock replica-id)}])
  ICRDT
  (step [this [op-name op-args]]
    (case op-name
      ::assign (case (compare (::version op-args) clock)
                 ;; the local value is older than the remote version
                 -1 (assoc this :clock (::version op-args) :v #{(::value op-args)})
                 ;; the local value is conflicting with the remove value, add it to the set
                 0  (-> this
                        (update :clock #(merge-clocks % (::version op-args)))
                        (update :v #(conj % (::value op-args))))
                 ;; the local value is newer than the remote version
                 1  this)
      this)))

(defn mv
  "Always returns a set (even when there is only one value) when derefed to ensure that you handle the multiple possible values"
  ([] (mv (util/new-uuid)))
  ([replica-id] (mv replica-id #{nil}))
  ([replica-id init-val]
   {:pre [(set? init-val)]}
   (MV. replica-id init-val (VectorClock. {}))))

(ns credn.register
  (:require [clojure.set :as set]
            [credn.util :as util]
            [credn.core :as crdt])
  (:import [credn.core ICRDT]))

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
      ::assign (case (compare latest-ts (::ts op-args))
                 -1 (assoc this :v (::value op-args) :latest-ts (::ts op-args))
                 ;; arbitrary if the timestamps are exactly the same, the local copy wins
                 0  this
                 1 this)
      this)))

(defn lww
  ([] (lww (util/new-uuid)))
  ([replica-id] (lww replica-id nil))
  ([replica-id init-val] (LWW. init-val (util/now))))

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

(defn ->replica-ids [clock]
  (keys (:replica->counter clock)))

;;  ======================================================================
;; Multi-Value Register

(defrecord MV [replica-id replica->val ^VectorClock clock]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_] (def -replica->val replica->val) (set (vals replica->val)))
  IRegister
  (assign-op [this v]
    [::assign {::value v ::replica-id replica-id ::version (inc-at clock replica-id)}])
  ICRDT
  (step [this [op-name {:keys [::version ::value] :as op-args}]]
    (case op-name
      ::assign (case (compare version clock)
                 ;; the local value is older than the remote version, take it as the only one
                 -1 (assoc this :clock version :replica->val {(::replica-id op-args) value})
                 ;; some of the local value are conflicting with the remote value, replace the local value where possible
                 0  (-> this
                        (update :clock #(merge-clocks % version))
                        (assoc :replica->val (->> (distinct (concat (->replica-ids version) (keys replica->val)))
                                                  (map (fn [rid]
                                                         (let [local-version  (get-in clock [:replica->counter rid])
                                                               remote-version (get-in version [:replica->counter rid])]
                                                           [rid (if (or (nil? local-version)
                                                                        (and (some? remote-version)
                                                                             (< local-version remote-version)))
                                                                  value
                                                                  (replica->val rid))])))
                                                  (into {}))))
                 ;; the local value is newer than the remote version
                 1  this)
      this)))

(defn mv
  "Always returns a set (even when there is only one value) when derefed to ensure that you handle the multiple possible values"
  ([] (mv (util/new-uuid)))
  ([replica-id] (mv replica-id nil))
  ([replica-id init-val]
   (MV. replica-id (when (some? init-val) {replica-id init-val}) (VectorClock. {}))))

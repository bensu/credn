(ns credn.counter
  (:import credn.core.ICRDT))

(defprotocol ICRDTCounter
  (inc-op [this])
  (dec-op [this]))

;; ======================================================================
;; G Counter

(defrecord GCounter [replica-id replica-counters]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [crdt]
    (apply + (vals replica-counters)))
  ICRDTCounter
  (inc-op [this]
    [::inc {::replica-id replica-id}])
  (dec-op [this]
    [::dec {::replica-id replica-id}])
  ICRDT
  (step [this [op-name op-args]]
    (case op-name
      ::inc (update-in this [:replica-counters (::replica-id op-args)] (fnil inc 0))
      this)))

(defn g-counter
  ([] (g-counter #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))))
  ([replica-id] (g-counter replica-id 0))
  ([replica-id init-value] (GCounter. replica-id {replica-id init-value})))

;; ======================================================================
;; PN Counter

(defrecord PNCounter [replica-id p-counters n-counters]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [crdt]
    (- (apply + (vals p-counters))
       (apply + (vals n-counters))))
  ICRDTCounter
  (inc-op [this]
    [::inc {::replica-id replica-id}])
  (dec-op [this]
    [::dec {::replica-id replica-id}])
  ICRDT
  (step [this [op-name op-args]]
    (case op-name
      ::inc (update-in this [:p-counters (::replica-id op-args)] (fnil inc 0))
      ::dec (update-in this [:n-counters (::replica-id op-args)] (fnil dec 0))
      this)))

(defn pn-counter
  ([] (pn-counter #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))))
  ([replica-id] (pn-counter replica-id 0))
  ([replica-id init-value] (PNCounter. replica-id {replica-id init-value} {replica-id 0})))

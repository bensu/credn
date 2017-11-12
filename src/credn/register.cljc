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

;;  ======================================================================
;; Multi-Value Register

(defrecord MV [replica-id replica->val clock]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_] (set (vals replica->val)))
  IRegister
  (assign-op [this v]
    [::assign {::value v ::replica-id replica-id ::version (crdt/inc-at clock replica-id)}])
  ICRDT
  (step [this [op-name {:keys [::version ::value] :as op-args}]]
    (case op-name
      ;; XXX is version a VectorClock instance?
      ::assign (case (compare clock version)
                 ;; the local value is older than the remote version, take it as the only one
                 -1 (assoc this :clock version :replica->val {(::replica-id op-args) value})
                 ;; some of the local value are conflicting with the remote value, replace the local value where possible
                 0  (-> this
                        (update :clock #(crdt/merge-clocks % version))
                        (assoc :replica->val (->> (distinct (concat (crdt/->replica-ids version) (keys replica->val)))
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
   (MV. replica-id (when (some? init-val) {replica-id init-val}) (crdt/vector-clock))))

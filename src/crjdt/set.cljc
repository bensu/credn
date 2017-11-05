(ns crjdt.set
  (:require [clojure.set :as set]))

(defn now []
  #?(:clj (java.util.Date.)
     :cljs (js/Date.)))

(defn new-uuid []
  #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid)))

(defprotocol ICRDT
  (step [this op]))

(defprotocol ICRDTSet
  (add-op [this element])
  (remove-op [this element]))

;; ======================================================================
;; G Set

(defrecord GSet [replica-id s]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_] s)
  ICRDTSet
  (add-op [_ element] [::add {::element element}])
  (remove-op [_ element] nil)
  ICRDT
  (step [this [op-name op-args]]
    (case op-name
      ::add (update this :s (fn [s] (conj s (::element op-args))))
      this)))

(defn g-set
  "Creates a GSet. It only supports once operation: add-element [::add {::element x}]

  No elements can be removed once added."
  ([] (g-set (new-uuid)))
  ([replica-id] (g-set replica-id #{}))
  ([replica-id init-value]
   {:pre [(set? init-value)]}
   (GSet. replica-id init-value)))

;; ======================================================================
;; 2P-Set

(defrecord PSet [replica-id added removed]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_]
    (set/difference added removed))
  ICRDTSet
  (add-op [this element]
    [::add {::element element}])
  (remove-op [this element]
    (when (contains? @this element)
      [::remove {::element element}]))
  ICRDT
  (step [this [op-name op-args]]
    (case op-name
      ::add (update this :added (fn [s] (conj s (::element op-args))))
      ::remove (update this :removed #(conj % (::element op-args)))
      this)))

(defn p-set
  "Creates a 2P-Set. An element can only be added and removed once."
  ([] (p-set (new-uuid)))
  ([replica-id] (p-set replica-id #{}))
  ([replica-id init-value]
   {:pre [(set? init-value)]}
   (PSet. replica-id init-value #{})))

;; ======================================================================
;; LWW-Element-Set

(defrecord LWWSet [replica-id element->timestamps]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_]
    (reduce (fn [s [k {:keys [added-ts removed-ts]}]]
              (if (or (nil? removed-ts) (compare removed-ts added-ts))
                (conj s k)
                s))
            #{}
            element->timestamps))
  ICRDTSet
  (add-op [this element]
    [::remove {::element element ::added-ts (now)}])
  (remove-op [this element]
    [::remove {::element element ::added-ts (now)}])
  ICRDT
  (step [this [op-name op-args]]
    (case op-name
      ::add (assoc-in this [:elements->timestamps (::element op-args) :added-ts] (::added-ts op-args))
      ::remove (assoc-in this [:elements->timestamps (::element op-args) :remove-ts] (::remove-ts op-args))
      this)))

(defn lww-set
  ([] (lww-set (new-uuid)))
  ([replica-id] (lww-set replica-id #{}))
  ([replica-id init-value] (LWWSet. replica-id (into {} (map (fn [k] [k {:added-ts (now)}]) init-value)))))

;; ======================================================================
;; OR-Set

(defrecord ORSet [replica-id element->tags]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_]
    (reduce (fn [s [k {:keys [added-tags removed-tags]}]]
              (if (empty? (set/difference added-tags (or removed-tags #{})))
                s
                (conj s k)))
            #{}
            element->tags))
  ICRDTSet
  (add-op [this element]
    [::add {::element element ::tag (new-uuid)}])
  (remove-op [this element]
    [::remove {::element element ::tag (new-uuid)}])
  ICRDT
  (step [this [op-name op-args]]
    (case op-name
      ::add (update-in this [:element->tags (::element op-args) :add-tags] (fn [s] (conj s (::tag op-args))))
      ::remove (update-in this [:element->tags (::element op-args) :remove-tags] (fn [s] (conj s (::tag op-args))))
      this)))

(defn or-set
  ([] (or-set (new-uuid)))
  ([replica-id] (or-set replica-id #{}))
  ([replica-id init-value] (ORSet. replica-id (into {} (map (fn [k] [k {:add-tags #{(new-uuid)}}]) init-value)))))

;; ======================================================================
;; MC Set

(defrecord MCSet [replica-id element->counter]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_]
    (reduce (fn [s [k n]]
              (if (odd? n)
                (conj s k)
                s))
            #{}
            element->counter))
  ICRDTSet
  (add-op [this element]
    [::add {::element element}])
  (remove-op [this element]
    [::remove {::element element}])
  ICRDT
  (step [this [op-name op-args]]
    (case op-name
      ::add (update-in this [:element->counter (::element op-args)] (fnil (fn [n] (if (even? n) (inc n) n)) 1))
      ::remove (update-in this [:element->counter (::element op-args)] (fnil (fn [n] (if (odd? n) (inc n) n)) 0))
      this)))

(defn mc-set
  ([] (mc-set (new-uuid)))
  ([replica-id] (mc-set replica-id #{}))
  ([replica-id init-value] (MCSet. replica-id (into {} (map (fn [k] [k 1]) init-value)))))

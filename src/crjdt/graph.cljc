(ns crjdt.graph
  (:require [crjdt.core :as crdt]
            [crjdt.util :as util]
            [clojure.set :as set]
            [clojure.test :as t :refer [deftest testing is are]])
  (:import [crjdt.core ICRDT]))

(defn path?
  "Checks if there is a path between from and to"
  [{:keys [vertices edges] :as dag} from to]
  {:post [boolean?]}
  ;; XXX: assumes it is a dag, otherwise doesn't terminate
  (let [edges-from (filter (fn [[start end]] (= from start)) edges)
        edges-to   (filter (fn [[start end]] (= to end)) edges-from)]
    (cond
      (empty? edges-from)     false
      (not (empty? edges-to)) true
      :else (or (some (fn [[start end]] (path? dag end to)) edges-from)
                false))))

(defn add-between-op
  "new-vertext needs to be globally unique. Use uuids (default arity) or make sure you are passing other unique identifiers"
  ([dag from to] (add-between-op dag from to (util/new-uuid)))
  ([dag from to new-vertex]
   (when (and (not (contains? (:vertices @dag) new-vertex))
              (contains? (:vertices @dag) from)
              (contains? (:vertices @dag) to)
              (path? @dag from to))
     [::add-between {::from from ::to to ::new new-vertex}])))

(defn add-edge-op
  "Adds an edge between to vertices only if there is already a path between the two vertices"
  [dag from to]
  (when (and (contains? (:vertices dag) from)
             (contains? (:vertices dag) to)
             (path? @dag from to))
    [::add-edge {::from from ::to to}]))

(defn remove-vertex-op [dag v]
  (when (and (contains? (:vertices @dag) v)
             (not= (:start-vertex dag) v)
             (not= (:end-vertex dag) v))
    [::remove-vertex {::vertex v}]))

;; ======================================================================
;; Monotonic DAG

(defrecord MonotonicDAG [vertices edges]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_] {:vertices vertices :edges edges})
  ICRDT
  (step [this [op-name op-args]]
    (case op-name
      ::add-between (if (and (contains? vertices (::from op-args))
                             (contains? vertices (::to op-args)))
                      (-> this
                          (update :vertices #(conj % (::new op-args)))
                          (update :edges #(conj % [(::from op-args) (::new op-args)] [(::new op-args) (::to op-args)])))
                      this)
      ::add-edge (update this :edges #(conj % [(::from op-args) (::to op-args)]))
      this)))

(defn monotonic-dag
  ([] (monotonic-dag #{::start ::end} #{[::start ::end]}))
  ([init-vertex init-edges]
   ;; XXX: check that the graph is dag!
   (MonotonicDAG. init-vertex init-edges)))

;; ======================================================================
;; Add Remove Partial Order

(defrecord PartialOrder [start-vertex end-vertex va vr edges]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_]
    {:vertices (set/difference va vr)
     ;; removing edges from removed nodes is not in the spec but it seems like expected behavior to me
     ;; XXX: this might be a bad idea if it changes the behavior of path? in unexepected way
     ;; the tombostones require unique names, but you can likely try to add one again
     :edges (set (remove (fn [[start end]]
                           (or (contains? vr start)
                               (contains? vr end)))
                         edges))})
  ICRDT
  (step [this [op-name op-args]]
    (case op-name
      ::add-between (if (and (contains? va (::from op-args))
                             (contains? va (::to op-args)))
                      (-> this
                          (update :va #(conj % (::new op-args)))
                          (update :edges #(conj % [(::from op-args) (::new op-args)] [(::new op-args) (::to op-args)])))
                      this)
      ::remove-vertex (update this :vr #(conj % (::vertex op-args)))
      this)))

(defn partial-order
  ([] (partial-order ::start ::end))
  ([start-vertex end-vertex]
   (partial-order start-vertex end-vertex #{start-vertex end-vertex} #{[start-vertex end-vertex]}))
  ([start-vertex end-vertex init-vertex init-edges]
   ;; XXX: check that the graph is dag!
   (PartialOrder. start-vertex end-vertex init-vertex #{} init-edges)))

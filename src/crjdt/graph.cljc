(ns crjdt.graph
  (:require [crjdt.core :as crdt]
            [crjdt.util :as util]
            [clojure.set :as set]
            [clojure.test :as t :refer [deftest testing is are]])
  (:import [crjdt.core ICRDT]))

(defprotocol ICRDTGraph
  (add-vertex-op [graph v])
  (add-edge-op [graph from to])
  (add-between-op [graph from to] [graph from to vertex])
  (remove-vertex-op [graph v])
  (remove-edge-op [graph from to]))

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

;; ======================================================================
;; Monotonic DAG

(defrecord MonotonicDAG [vertices edges]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_] {:vertices vertices :edges edges})
  ICRDTGraph
  (add-between-op [dag from to]
    (add-between-op dag from to (util/new-uuid)))
  (add-between-op [dag from to v]
    (when (and (not (contains? (:vertices @dag) v))
               (contains? (:vertices @dag) from)
               (contains? (:vertices @dag) to)
               (path? @dag from to))
      [::add-between {::from from ::to to ::new v}]))
  (add-edge-op [dag from to]
    (when (and (contains? (:vertices dag) from)
               (contains? (:vertices dag) to)
               (path? @dag from to))
      [::add-edge {::from from ::to to}]))
  (add-vertex-op [dag v] nil)
  (remove-vertex-op [dag v] nil)
  (remove-edge-op [dag from to] nil)
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
  ICRDTGraph
  (add-between-op [dag from to]
    (add-between-op dag from to (util/new-uuid)))
  (add-between-op [dag from to v]
    (when (and (not (contains? (:vertices @dag) v))
               (contains? (:vertices @dag) from)
               (contains? (:vertices @dag) to)
               (path? @dag from to))
      [::add-between {::from from ::to to ::new v}]))
  (add-edge-op [dag from to] nil)
  (add-vertex-op [dag v] nil)
  (remove-vertex-op [dag v]
    (when (and (contains? (:vertices @dag) v)
               (not= (:start-vertex dag) v)
               (not= (:end-vertex dag) v))
      [::remove-vertex {::vertex v}]))
  (remove-edge-op [dag from to] nil)
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

;; ======================================================================
;; 2P2P Graph

(defrecord TPTPGraph [va vr ea er]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_]
    (let [vertices (set/difference va vr)]
      {:vertices vertices :edges (set (remove (fn [[start end]]
                                                (or (not (contains? vertices start))
                                                    (not (contains? vertices end))))
                                              (set/difference ea er)))}))
  ICRDTGraph
  (add-vertex-op [graph v]
    [::add-vertex {::new v}])
  (add-edge-op [graph from to]
    (when (and (contains? (:vertices @graph) from)
               (contains? (:vertices @graph) to))
      [::add-edge {::from from ::to to}]))
  (remove-vertex-op [graph v]
    (when (empty? (filter (fn [[start end]] (or (= v start) (= v end))) (:edges @graph)))
      [::remove-vertex {::vertex v}]))
  (remove-edge-op [graph from to]
    (when (contains? (:edges @graph) [from to])
      [::remove-edge {::edfge [from to]}]))
  (add-between-op [_ _ _] nil)
  (add-between-op [_ _ _ _] nil)
  ICRDT
  (step [this [op-name op-args]]
    (case op-name
      ::add-vertex    (update this :va #(conj % (::new op-args)))
      ::add-edge      (update this :ea #(conj % [(::from op-args) (::to op-args)]))
      ::remove-vertex (update this :vr #(conj % (::vertex op-args)))
      this)))

(defn tptp
  ([] (tptp #{} #{}))
  ([init-vertex init-edges] (TPTPGraph. #{} #{} #{} #{})))

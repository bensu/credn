(ns credn.graph
  (:require [credn.core :as crdt]
            [credn.util :as util]
            [clojure.set :as set]
            [clojure.test :as t :refer [deftest testing is are]])
  (:import [credn.core ICRDT]))

(defprotocol ICRDTGraph
  (add-vertex-op [graph v])
  (add-edge-op [graph from to])
  (add-between-op [graph from to] [graph from to vertex])
  (remove-vertex-op [graph v])
  (remove-edge-op [graph from to]))

(defn path?
  "Checks if there is a path between from and to.

  Assumes it is a dag, otherwise doesn't terminate"
  [{:keys [vertices edges] :as dag} from to]
  {:post [boolean?]}
  ;; XXX:
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
  "Creates a monotonic Directed Acyclic Graph. Monotonic because you can only add, never remove. Based on grow only sets.

  Supports:

  - (add-edge-op dag from to): only works if there is already a path? from to and thus maintains the 'direction' of the graph
  - (add-between-op dag from to v): inserts a vertex and two edges and only works if there is already a path? from to

  The underlying graph is represented by a set of vertices #{v} and a set of edges #{(from, to)}"
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
  "Creates a partial order graph. All elements can be found in a partial order, given by the edges between them.

  Supports:

  - (add-between-op dag from to v): adds a vertex v and two edges between (from v), (v to), removing the edge (from to)
  - (remove-vertex-op dag from to): removes a vertex between existing vertices from and to"
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
    (when-not (contains? (:vertices @graph) v)
      [::add-vertex {::new v}]))
  (add-edge-op [graph from to]
    (when (and (contains? (:vertices @graph) from)
               (contains? (:vertices @graph) to))
      [::add-edge {::from from ::to to}]))
  (remove-vertex-op [graph v]
    (let [{:keys [edges vertices]} @graph]
      (when (contains? vertices v)
        [::remove-vertex {::vertex v}])))
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
      ::remove-edge   (update this :ev #(conj % [(::from op-args) (::to op-args)]))
      this)))

(defn tptp
  "Creates a two-phase-two-phase graph. Backed by two two-phase sets, one for nodes the other for edges.

  Can't enforce any global guarantees on the graph, for example DAG or Tree. Even if a replica thinks it is adding an edge that respects a DAG constraint, a conflicting operation can break that guarantee.

  Supports all the natural operations:

  - (add-vertex-op graph v)
  - (add-edge--op graph from to)
  - (remove-vertex-op graph v)
  - (remove-edge-op graph from to)"
  ([] (tptp #{} #{}))
  ([init-vertex init-edges] (TPTPGraph. #{} #{} #{} #{})))

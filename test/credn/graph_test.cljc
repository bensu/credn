(ns credn.graph-test
  (:require [credn.graph :as crdt-graph]
            [credn.core :as crdt]
            #?(:clj [clojure.test :as t :refer [deftest testing is are]]
               :cljs [cljs.test :as t :include-macros true])))

(deftest check-paths
  (testing "can find the path between two simple nodes"
    (let [vertices #{1 2 3 4 5}
          edges #{[1 2] [2 3] [3 4] [4 5]}
          dag {:vertices vertices :edges edges}]
      (is (true? (crdt-graph/path? dag 1 5)))
      (is (true? (crdt-graph/path? dag 2 3)))
      (is (false? (crdt-graph/path? dag 5 1)))
      (is (false? (crdt-graph/path? dag 4 3))))))

(deftest build-linear-graph
  (testing "can find the path between two simple nodes"
    (let [dag (crdt-graph/monotonic-dag)]
      (is (true? (crdt-graph/path? dag ::crdt-graph/start ::crdt-graph/end)))
      (let [dag' (reduce (fn [dag [a b]]
                           (crdt/step dag (crdt-graph/add-between-op dag a ::crdt-graph/end b)))
                         dag
                         (map vector [::crdt-graph/start 1 2 3 4] [1 2 3 4 5]))]
        (is (= #{::crdt-graph/start 1 2 3 4 5 ::crdt-graph/end} (:vertices dag')))
        (is (= #{[::crdt-graph/start ::crdt-graph/end]
                 [::crdt-graph/start 1] [1 ::crdt-graph/end]
                 [1 2] [2 ::crdt-graph/end]
                 [2 3] [3 ::crdt-graph/end]
                 [3 4] [4 ::crdt-graph/end]
                 [4 5] [5 ::crdt-graph/end]}
               (:edges dag')))
        (is (true? (crdt-graph/path? dag' ::crdt-graph/start ::crdt-graph/end)))
        (is (true? (crdt-graph/path? dag' 1 5)))
        (is (true? (crdt-graph/path? dag' 2 3)))
        (is (false? (crdt-graph/path? dag' 5 1)))
        (is (false? (crdt-graph/path? dag' 4 3)))))))

(deftest build-partial-order
  (testing "can find the path between two simple nodes"
    (let [dag (crdt-graph/partial-order)]
      (is (true? (crdt-graph/path? @dag ::crdt-graph/start ::crdt-graph/end)))
      (let [dag' (reduce (fn [dag [a b]]
                           (crdt/step dag (crdt-graph/add-between-op dag a ::crdt-graph/end b)))
                         dag
                         (map vector [::crdt-graph/start 1 2 3 4] [1 2 3 4 5]))]
        (is (= #{::crdt-graph/start 1 2 3 4 5 ::crdt-graph/end} (:vertices @dag')))
        (is (= #{[::crdt-graph/start ::crdt-graph/end]
                 [::crdt-graph/start 1] [1 ::crdt-graph/end]
                 [1 2] [2 ::crdt-graph/end]
                 [2 3] [3 ::crdt-graph/end]
                 [3 4] [4 ::crdt-graph/end]
                 [4 5] [5 ::crdt-graph/end]}
               (:edges @dag')))
        (is (true? (crdt-graph/path? @dag' ::crdt-graph/start ::crdt-graph/end)))
        (is (true? (crdt-graph/path? @dag' 1 5)))
        (is (true? (crdt-graph/path? @dag' 2 3)))
        (is (false? (crdt-graph/path? @dag' 5 1)))
        (is (false? (crdt-graph/path? @dag' 4 3)))
        (let [dag'' (crdt/step dag' (crdt-graph/remove-vertex-op dag' 4))]
          (is (false? (crdt-graph/path? @dag'' 3 5))))))))

(deftest build-tptp-graph
  (testing "can find the crdt-graph/path between to simple nodes"
    (let [graph (crdt-graph/tptp)]
      (is (false? (crdt-graph/path? @graph ::crdt-graph/start ::crdt-graph/end)))
      (let [graph' (reduce (fn [graph [a b]]
                             (as-> graph $
                               (crdt/step $ (crdt-graph/add-vertex-op $ b))
                               (crdt/step $ (crdt-graph/add-edge-op $ a b))))
                           (crdt/step graph (crdt-graph/add-vertex-op graph 1))
                           (map vector [1 2 3 4] [2 3 4 5]))]
        (is (= #{1 2 3 4 5} (:vertices @graph')))
        (is (= #{[1 2] [2 3] [3 4] [4 5]} (:edges @graph')))
        (is (true? (crdt-graph/path? @graph' 1 5)))
        (is (true? (crdt-graph/path? @graph' 2 3)))
        (is (false? (crdt-graph/path? @graph' 5 1)))
        (is (false? (crdt-graph/path? @graph' 4 3)))
        (let [graph'' (crdt/step graph' (crdt-graph/remove-vertex-op graph' 4))]
          (is (false? (crdt-graph/path? @graph'' 3 5))))))))

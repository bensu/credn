(ns crjdt.graph-test
  (:require [crjdt.graph :as crdt-graph]
            [crjdt.core :as crdt]
            #?(:clj [clojure.test :as t :refer [deftest testing is are]]
               :cljs [cljs.test :as t :include-macros true])))

(deftest check-paths
  (testing "can fidn the crdt-graph/path between to simple nodes"
    (let [vertices #{1 2 3 4 5}
          edges #{[1 2] [2 3] [3 4] [4 5]}
          dag {:vertices vertices :edges edges}]
      (is (true? (crdt-graph/path? dag 1 5)))
      (is (true? (crdt-graph/path? dag 2 3)))
      (is (false? (crdt-graph/path? dag 5 1)))
      (is (false? (crdt-graph/path? dag 4 3))))))

(deftest build-linear-graph
  (testing "can fidn the path between to simple nodes"
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
  (testing "can fidn the crdt-graph/path between to simple nodes"
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

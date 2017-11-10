(ns credn.counter-test
  #?@(:clj
       [(:require
         [clojure.test :as t :refer [deftest is testing]]
         [credn.core :as crdt]
         [credn.counter :as crdt-counter])]
       :cljs
       [(:require [credn.core :as crdt] [credn.counter :as crdt-counter])]))

(deftest g-counter-convergence
  (testing "operations applied to different copies converge"
    (let [a (crdt-counter/g-counter)
          b (crdt-counter/g-counter)
          n 10
          ops (take n (map crdt-counter/inc-op (repeat (rand-nth [a b]))))]
      (is (= 0 @a @b))
      (is (= 1 @(crdt/step a (crdt-counter/inc-op a)) @(crdt/step b (crdt-counter/inc-op a))))
      (is (= n @(reduce crdt/step a ops) @(reduce crdt/step b (shuffle ops)))))))

(deftest pn-counter-convergence
  (testing "operations applied to different copies converge"
    (let [a (crdt-counter/pn-counter)
          b (crdt-counter/pn-counter)
          n 10
          ops (take n (map #(%1 %2) (repeat (rand-nth [crdt-counter/dec-op crdt-counter/inc-op])) (repeat (rand-nth [a b]))))]
      (is (= 0 @a @b))
      (is (= 1 @(crdt/step a (crdt-counter/inc-op a)) @(crdt/step b (crdt-counter/inc-op a))))
      (is (= n @(reduce crdt/step a ops) @(reduce crdt/step b (shuffle ops)))))))

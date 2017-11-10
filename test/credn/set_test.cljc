(ns credn.set-test
  #?@(:clj
       [(:require
         [clojure.test :as t :refer [deftest is testing]]
         [credn.core :as crdt]
         [credn.set :as crdt-set])]
       :cljs
       [(:require [credn.core :as crdt] [credn.set :as crdt-set])]))

(deftest g-set-convergence
  (testing "operations applied to different copies converge"
    (let [a (crdt-set/g-set)
          b (crdt-set/g-set)
          n 10
          ops (take n (map crdt-set/add-op (repeat (rand-nth [a b])) (range)))]
      (is (= #{} @a @b))
      (is (= (set (range n)) @(reduce crdt/step a ops) @(reduce crdt/step b (shuffle ops))))
      (is (= n (count @(reduce crdt/step a ops)) (count @(reduce crdt/step b (shuffle ops))))))))

(deftest tp-set-convergence
  (testing "you can't remove elements that haven't been added"
    (let [a          (crdt-set/tp-set)
          b          (crdt-set/tp-set)
          n          10
          add-ops    (take n (map #(%1 %2 %3)
                                  (repeat crdt-set/add-op)
                                  (repeatedly #(rand-nth [a b]))
                                  (range)))
          full-a     (reduce crdt/step a add-ops)
          remove-ops (take n (map #(%1 %2 %3)
                                  (repeat crdt-set/remove-op)
                                  (repeatedly #(rand-nth [full-a b]))
                                  (range)))]
      (is (= #{} @a @b))
      (is (= #{} @(reduce crdt/step a remove-ops) @(reduce crdt/step b (shuffle remove-ops))))
      (is (= (set (range n)) @(reduce crdt/step a add-ops) @(reduce crdt/step b (shuffle add-ops))))
      (is (= @(reduce crdt/step a (concat add-ops remove-ops)) @(reduce crdt/step b (shuffle (concat add-ops remove-ops)))))))
  (testing "operations applied to different copies converge"
    (let [a   (crdt-set/tp-set)
          b   (crdt-set/tp-set)
          n   10
          ops (take n (map #(%1 %2 %3)
                           (repeatedly #(rand-nth [crdt-set/add-op crdt-set/remove-op]))
                           (repeatedly #(rand-nth [a b]))
                           (range)))]
      (is (= #{} @a @b))
      (is (= @(reduce crdt/step a ops) @(reduce crdt/step b (shuffle ops)))))))

(deftest lww-convergence
  (testing "operations applied to different copies converge"
    (let [a (crdt-set/lww-set)
          b (crdt-set/lww-set)
          n 10
          ops (take n (map #(%1 %2 %3)
                           (repeatedly #(rand-nth [crdt-set/add-op crdt-set/remove-op]))
                           (repeatedly #(rand-nth [a b]))
                           (range)))]
      (is (= #{} @a @b))
      (is (= @(reduce crdt/step a ops) @(reduce crdt/step b (shuffle ops)))))))

(deftest or-set-convergence
  (testing "operations applied to different copies converge"
    (let [a (crdt-set/or-set)
          b (crdt-set/or-set)
          n 10
          ops (take n (map #(%1 %2 %3)
                           (repeatedly #(rand-nth [crdt-set/add-op crdt-set/remove-op]))
                           (repeatedly #(rand-nth [a b]))
                           (range)))]
      (is (= #{} @a @b))
      (is (= @(reduce crdt/step a ops) @(reduce crdt/step b (shuffle ops)))))))

(deftest mc-set-convergence
  (testing "operations applied to different copies converge"
    (let [a (crdt-set/mc-set)
          b (crdt-set/mc-set)
          n 10
          ops (take n (map #(%1 %2 %3)
                           (repeatedly #(rand-nth [crdt-set/add-op crdt-set/remove-op]))
                           (repeatedly #(rand-nth [a b]))
                           (range)))]
      (is (= #{} @a @b))
      (is (= @(reduce crdt/step a ops) @(reduce crdt/step b (shuffle ops)))))))

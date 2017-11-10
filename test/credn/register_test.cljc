(ns credn.register-test
  #?@(:clj
       [(:require
         [clojure.test :as t :refer [deftest is testing]]
         [credn.core :as crdt]
         [credn.register :as crdt-register])]
       :cljs
       [(:require [credn.core :as crdt] [credn.register :as crdt-register])]))

(deftest lww-convergence
  (testing "lww converges as we expect"
    (let [a   (crdt-register/lww)
          b   (crdt-register/lww)
          _   (Thread/sleep 10)
          op1 (crdt-register/assign-op a 1)
          _   (Thread/sleep 10)
          op2 (crdt-register/assign-op b 2)]
      (is (= 2
             @(-> a
                  (crdt/step op1)
                  (crdt/step op2))
             @(-> b
                  (crdt/step op2)
                  (crdt/step op1)))))))

(deftest mv-convergence
  (testing "mv converges as we expect"
    (let [a   (crdt-register/mv)
          b   (crdt-register/mv)
          op1 (crdt-register/assign-op a 1)
          op2 (crdt-register/assign-op b 2)
          op3 (crdt-register/assign-op (crdt/step a op1) 3)
          op4 (crdt-register/assign-op (crdt/step b op2) 4) ]
      (is (= #{1} @(crdt/step a op1)))
      (is (= #{2} @(crdt/step b op2)))
      (is (= #{1 2} @(reduce crdt/step a [op1 op2]) @(reduce crdt/step b [op2 op1])))
      (is (= #{3 4}
             @(reduce crdt/step a [op1 op3 op2 op4])
             @(reduce crdt/step a [op1 op3 op4 op2])
             @(reduce crdt/step b [op2 op4 op1 op3])
             @(reduce crdt/step b [op2 op4 op3 op1]))))))

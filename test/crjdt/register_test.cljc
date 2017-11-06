(ns crjdt.register-test
  #?@(:clj
       [(:require
         [clojure.test :as t :refer [deftest is testing]]
         [crjdt.core :as crdt]
         [crjdt.register :as crdt-register])]
       :cljs
       [(:require [crjdt.core :as crdt] [crjdt.register :as crdt-register])]))

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

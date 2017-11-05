(ns crjdt.counter
  (:require #?(:clj [clojure.test :as test :refer [deftest testing is are]]
               :cljs [cljs.test :as test :refer [deftest testing is are]])))

(defn inc-op [g]
  [::inc {::replica-id (:replica-id g)}])

(defn dec-op [g]
  [::dec {::replica-id (:replica-id g)}])

;; ======================================================================
;; G Counter

(defmulti step-g-counter (fn [g [op-name op-args]] op-name))

(defmethod step-g-counter ::inc [g [_  op-args]]
  (update-in g [:replica-counters (::replica-id op-args)] (fnil inc 0)))

(defrecord GCounter [replica-id replica-counters]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [crdt]
    (apply + (vals replica-counters))))

(defn g-counter
  ([] (g-counter #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))))
  ([replica-id] (g-counter replica-id 0))
  ([replica-id init-value] (GCounter. replica-id {replica-id init-value})))

(deftest simple-convergence
  (testing "operations applied to different copies converge"
    (let [a (g-counter)
          b (g-counter)
          n 10
          ops (take n (map inc-op (repeat (rand-nth [a b]))))]
      (is (= 0 @a @b))
      (is (= 1 @(step-g-counter a (inc-op a)) @(step-g-counter b (inc-op a))))
      (is (= n @(reduce step-g-counter a ops) @(reduce step-g-counter b (shuffle ops)))))))

;; ======================================================================
;; PN Counter

(defrecord PNCounter [replica-id p-counters n-counters]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [crdt]
    (- (apply + (vals p-counters))
       (apply + (vals n-counters)))))

(defn pn-counter
  ([] (pn-counter #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))))
  ([replica-id] (pn-counter replica-id 0))
  ([replica-id init-value] (PNCounter. replica-id {replica-id init-value} {replica-id 0})))

(defmulti step-pn-counter (fn [pn [op-name op-args]] op-name))

(defmethod step-pn-counter ::inc [pn [_ op-args]]
  (update-in pn [:p-counters (::replica-id op-args)] (fnil inc 0)))

(defmethod step-pn-counter ::dec [pn [_ op-args]]
  (update-in pn [:n-counters (::replica-id op-args)] (fnil dec 0)))

(deftest interleaved-convergence
  (testing "operations applied to different copies converge"
    (let [a (pn-counter)
          b (pn-counter)
          n 10
          ops (take n (map #(%1 %2) (repeat (rand-nth [dec-op inc-op])) (repeat (rand-nth [a b]))))]
      (is (= 0 @a @b))
      (is (= 1 @(step-pn-counter a (inc-op a)) @(step-pn-counter b (inc-op a))))
      (is (= n @(reduce step-pn-counter a ops) @(reduce step-pn-counter b (shuffle ops)))))))

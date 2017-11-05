(ns crjdt.counter
  (:require #?(:clj [clojure.test :as test :refer [deftest testing is are]]
               :cljs [cljs.test :as test :refer [deftest testing is are]])))

(defn inc-op [g]
  [::inc {::replica-id (:replica-id g)}])

(defmulti step (fn [g [op-name op-args]] op-name))

(defmethod step ::inc [g [_  op-args]]
  (update-in g [:replica-counters (::replica-id op-args)] (fnil inc 0)))

(defrecord GCounter [replica-id replica-counters]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [crdt]
    (apply + (vals replica-counters))))

(defn g-counter
  ([] (g-counter #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))))
  ([replica-id] (g-counter replica-id 0))
  ([replica-id init-value] (GCounter. replica-id {:replica-counters init-value})))

(deftest simple-convergence
  (testing "operations applied to different copies converge"
    (let [a (g-counter)
          b (g-counter)
          n 10
          ops (take n (map inc-op (repeat (rand-nth [a b]))))]
      (is (= 0 @a @b))
      (is (= 1 @(step a (inc-op a)) @(step b (inc-op a))))
      (is (= n @(reduce step a ops) @(reduce step b (shuffle ops)))))))

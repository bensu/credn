(ns crjdt.set
  #?(:clj  (:require [clojure.set :as set] [clojure.test :as test :refer [deftest testing is are]])
     :cljs (:require [cljs.test :as test :refer [deftest testing is are]])))

(defn now []
  #?(:clj (java.util.Date.)
     :cljs (js/Date.)))

(defn new-uuid []
  #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid)))

(defn add-op [g-set element]
  [::add {::element element ::added-ts (now) ::tag (new-uuid)}])

(defn remove-op [g-set element]
  [::remove {::element element ::remove-ts (now) ::tag (new-uuid)}])

;; ======================================================================
;; G Set

(defrecord GSet [replica-id s]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_] s))

(defn g-set
  ([] (g-set (new-uuid)))
  ([replica-id] (g-set replica-id #{}))
  ([replica-id init-value] (GSet. replica-id init-value)))

(defn step-g-set [g-set [op-name op-args]]
  (update g-set :s (fn [s] (conj s (::element op-args)))))

(deftest simple-convergence
  (testing "operations applied to different copies converge"
    (let [a (g-set)
          b (g-set)
          n 10
          ops (take n (map add-op (repeat (rand-nth [a b])) (range)))]
      (is (= #{} @a @b))
      (is (= @(reduce step-g-set a ops) @(reduce step-g-set b (shuffle ops))))
      (is (= n (count @(reduce step-g-set a ops)) (count @(reduce step-g-set b (shuffle ops))))))))

;; ======================================================================
;; 2P-Set

(defrecord PSet [replica-id added removed]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_]
    (set/difference added removed)))

(defn p-set
  ([] (p-set #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))))
  ([replica-id] (p-set replica-id #{}))
  ([replica-id init-value] (PSet. replica-id init-value #{})))

(defmulti step-p-set (fn [pset [op-name op-args]] op-name))

(defmethod step-p-set ::add [pset [_ op-args]]
  (update pset :added (fn [s] (conj s (::element op-args)))))

(defmethod step-p-set ::remove [pset [_ op-args]]
  (update pset :removed (fn [s] (conj s (::element op-args)))))

(deftest simple-convergence
  (testing "operations applied to different copies converge"
    (let [a (p-set)
          b (p-set)
          n 10
          ops (take n (map #(%1 %2 %3)
                           (repeatedly #(rand-nth [add-op remove-op]))
                           (repeatedly #(rand-nth [a b]))
                           (range)))]
      (is (= #{} @a @b))
      (is (= @(reduce step-p-set a ops) @(reduce step-p-set b (shuffle ops)))))))

;; ======================================================================
;; LWW-Element-Set

(defrecord LWWSet [replica-id element->timestamps]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_]
    (reduce (fn [s [k {:keys [added-ts removed-ts]}]]
              (if (or (nil? removed-ts) (compare removed-ts added-ts))
                (conj s k)
                s))
            #{}
            element->timestamps)))

(defn lww-set
  ([] (lww-set (new-uuid)))
  ([replica-id] (lww-set replica-id #{}))
  ([replica-id init-value] (LWWSet. replica-id (into {} (map (fn [k] [k {:added-ts (now)}]) init-value)))))

(defmulti step-lww-set (fn [lwwset [op-name op-args]] op-name))

(defmethod step-lww-set ::add [lwwset [_ op-args]]
  (assoc-in lwwset [:elements->timestamps (::element op-args) :added-ts] (::added-ts op-args)))

(defmethod step-lww-set ::remove [lwwset [_ op-args]]
  (assoc-in lwwset [:elements->timestamps (::element op-args) :remove-ts] (::remove-ts op-args)))

(deftest simple-convergence
  (testing "operations applied to different copies converge"
    (let [a (lww-set)
          b (lww-set)
          n 10
          ops (take n (map #(%1 %2 %3)
                           (repeatedly #(rand-nth [add-op remove-op]))
                           (repeatedly #(rand-nth [a b]))
                           (range)))]
      (is (= #{} @a @b))
      (is (= @(reduce step-lww-set a ops) @(reduce step-lww-set b (shuffle ops)))))))

;; ======================================================================
;; OR-Set

(defrecord ORSet [replica-id element->tags]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_]
    (reduce (fn [s [k {:keys [added-tags removed-tags]}]]
              (if (empty? (set/difference added-tags (or removed-tags #{})))
                s
                (conj s k)))
            #{}
            element->tags)))

(defn or-set
  ([] (or-set (new-uuid)))
  ([replica-id] (or-set replica-id #{}))
  ([replica-id init-value] (ORSet. replica-id (into {} (map (fn [k] [k {:add-tags #{(new-uuid)}}]) init-value)))))

(defmulti step-or-set (fn [orset [op-name op-args]] op-name))

(defmethod step-or-set ::add [orset [_ op-args]]
  (update-in orset [:element->tags (::element op-args) :add-tags] (fn [s] (conj s (::tag op-args)))))

(defmethod step-or-set ::remove [orset [_ op-args]]
  (update-in orset [:element->tags (::element op-args) :remove-tags] (fn [s] (conj s (::tag op-args)))))

(deftest simple-convergence
  (testing "operations applied to different copies converge"
    (let [a (or-set)
          b (or-set)
          n 10
          ops (take n (map #(%1 %2 %3)
                           (repeatedly #(rand-nth [add-op remove-op]))
                           (repeatedly #(rand-nth [a b]))
                           (range)))]
      (is (= #{} @a @b))
      (is (= @(reduce step-or-set a ops) @(reduce step-or-set b (shuffle ops)))))))

;; ======================================================================
;; MC Set

(defrecord MCSet [replica-id element->counter]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_]
    (reduce (fn [s [k n]]
              (if (odd? n)
                (conj s k)
                s))
            #{}
            element->counter)))

(defn mc-set
  ([] (mc-set (new-uuid)))
  ([replica-id] (mc-set replica-id #{}))
  ([replica-id init-value] (MCSet. replica-id (into {} (map (fn [k] [k 1]) init-value)))))

(defmulti step-mc-set (fn [orset [op-name op-args]] op-name))

(defmethod step-mc-set ::add [orset [_ op-args]]
  (update-in orset [:element->counter (::element op-args)] (fnil (fn [n] (if (even? n) (inc n) n)) 1)))

(defmethod step-mc-set ::remove [orset [_ op-args]]
  (update-in orset [:element->counter (::element op-args)] (fnil (fn [n] (if (odd? n) (inc n) n)) 0)))

(deftest simple-convergence
  (testing "operations applied to different copies converge"
    (let [a (mc-set)
          b (mc-set)
          n 10
          ops (take n (map #(%1 %2 %3)
                           (repeatedly #(rand-nth [add-op remove-op]))
                           (repeatedly #(rand-nth [a b]))
                           (range)))]
      (is (= #{} @a @b))
      (is (= @(reduce step-mc-set a ops) @(reduce step-mc-set b (shuffle ops)))))))

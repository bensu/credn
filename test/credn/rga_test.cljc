(ns credn.rga-test
  (:require [credn.rga :as rga]
            [credn.core :as credn]
            [credn.util :as util]
            [clojure.test :as t :refer [deftest testing is are]]))

(deftest linear-seq
  (testing "can edit linearly"
    (let [r   (rga/rga)
          r'  (reduce (fn [r [a b]]
                        (credn/step r (rga/add-right-op r a b)))
                      r
                      (map vector [::rga/start 0 1 2 3 4] [0 1 2 3 4 5]))
          r'' (reduce (fn [r x]
                        (credn/step r (rga/remove-op r x)))
                      r'
                      (range 6))]
      (is (empty? @r))
      (is (= (range 6) @r'))
      (is (empty? @r'')))))

(deftest two-seqs
  (testing "can edit linearly"
    (let [xs        (rga/rga)
          ys        (rga/rga)
          vs        (map vector (repeatedly util/new-uuid) (range 6))
          [xs' ops] (reduce (fn [[r ops] [a b]]
                              (let [op (rga/add-right-op r a b)]
                                [(credn/step r op) (conj ops op)]))
                            [xs []]
                            (map vector (cons ::rga/start (drop-last vs)) vs))
          ys'       (reduce credn/step ys (shuffle ops))]
      (is (= (range 6) (map second @xs') (map second @ys')))))
  (testing "can edit linearly with repeated elements"
    (let [xs        (rga/rga)
          ys        (rga/rga)
          vs        (map vector (repeatedly util/new-uuid) (concat (range 4) (range 4)))
          [xs' ops] (reduce (fn [[r ops] [a b]]
                              (let [op (rga/add-right-op r a b)]
                                [(credn/step r op) (conj ops op)]))
                            [xs []]
                            (map vector (cons ::rga/start (drop-last 1 vs)) vs))
          ys'       (reduce credn/step ys (shuffle ops))]
      (is (= (concat (range 4) (range 4)) (map second @xs') (map second @ys'))))))

(deftest with-remove-ops
  (testing "can edit linearly"
    (let [xs                (rga/rga)
          ys                (rga/rga)
          n                 (+ 10 (rand-int 90))
          m                 (rand-int (dec n))
          [xs' ops]         (reduce (fn [[r ops] [a b]]
                                      (let [op (rga/add-right-op r a b)]
                                        [(credn/step r op) (conj ops op)]))
                                    [xs []]
                                    (map vector (cons ::rga/start (range n)) (range (inc n))))
          [xs'' remove-ops] (reduce (fn [[r ops] x]
                                      (let [op (rga/remove-op r x)]
                                        [(credn/step r op) (conj ops op)]))
                                    [xs' []]
                                    (take m @xs'))
          ys''              (reduce credn/step ys (shuffle (concat ops remove-ops)))]
      (is (= (range m (inc n))  @xs'' @ys'')))))

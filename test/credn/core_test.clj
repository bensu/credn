(ns credn.core-test
  (:require [clojure.test :refer :all]
            [credn.core :as credn]
            [credn.util :as util]))

(deftest vector-clocks
  (testing "can-increment"
    (let [rida (util/new-uuid)
          ridb (util/new-uuid)
          a    (credn/vector-clock)
          b    (credn/vector-clock)]
      (is (= @a @b))
      (is (not= (credn/inc-at a rida) (credn/inc-at b ridb)))
      (is (zero? (compare (credn/inc-at a rida) (credn/inc-at b ridb))))
      (is (= -1 (compare a (credn/inc-at b ridb))))
      (is (= 1 (compare (credn/inc-at b ridb) a)))
      (is (true? (credn/successor? a (credn/inc-at a rida))))
      (is (true? (credn/successor? a (credn/inc-at (credn/inc-at a rida) ridb))))
      (is (false? (credn/successor? a (credn/inc-at (credn/inc-at a rida) rida))))
      (is (false? (credn/successor? (credn/inc-at a rida) (credn/inc-at b ridb))))
      (is (false? (credn/successor? (credn/inc-at a rida) (credn/inc-at a rida)))))))

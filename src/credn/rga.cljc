(ns credn.rga
  (:require [clojure.set :as set]
            [credn.util :as util]
            [credn.core :as credn]
            [clojure.test :as t :refer [deftest testing is are]])
  (:import [credn.core ICRDT]))

(defn edges->list [edges]
  (loop [xs [::start]
         edges edges]
    (if-let [next (get edges (last xs))]
      (recur (conj xs next) (dissoc edges (last xs)))
      xs)))

(defprotocol IRGA
  (add-right-op [rga a b])
  (cons-op [rga a])
  (remove-op [rga a]))

(defrecord RGA [rid va vr edges clock ops-buffer]
  #?(:clj clojure.lang.IDeref :cljs IDeref)
  (#?(:clj deref :cljs -deref) [_]
    (drop-last 1 (drop 1 (remove (partial contains? vr) (edges->list edges)))))
  IRGA
  (add-right-op [_ a b]
    (when (and (not= ::end a) ;; you can't add-right of the end
               ;; you can only add right of an element that already exists
               (contains? (set/difference va vr) a))
      [::add-right {::a       a
                    ::b       b
                    ::version (-> clock (credn/inc-at rid) (credn/->version rid))}]))
  (cons-op [this x]
    (add-right-op this ::start x))
  (remove-op [this x]
    (when (and (not= ::start x) (not= ::end x)
               (contains? (set/difference va vr) x))
      [::remove {::x       x
                 ::version (-> clock (credn/inc-at rid) (credn/->version rid))}]))
  ICRDT
  (step [this op]
    (letfn [(step-op [rga [op-name op-args :as op]]
              (case op-name
                ::add-right (let [a       (::a op-args)
                                  b       (::b op-args)
                                  after-a (get (:edges rga) a)]
                              (-> rga
                                  (assoc :clock (::version op-args))
                                  (update :va conj b)
                                  (update :edges #(assoc % a b b after-a))))
                ::remove    (-> rga
                                (assoc :clock (::version op-args))
                                (update :vr conj (::x op-args)))
                rga))
            (step-or-buffer [rga [op-name op-args :as op]]
              ;; check if the next operation is exactly one off
              (case (compare (:clock rga) (::version op-args))
                ;; the operation was already applied
                1  rga
                0  rga
                ;; the operation has not been applied
                -1 (if (credn/successor? (:clock rga) (::version op-args))
                     (step-op rga op)
                     (update rga :ops-buffer #(conj % op)))))]
      ;; apply the new op and then try to apply all the buffered ops
      (reduce step-or-buffer
              (assoc this :ops-buffer []) ;; clear the buffer
              ;; apply the buffered ops in order
              (sort-by (comp ::version second) (conj (:ops-buffer this) op))))))

(defn rga
  "Creates a Replicated Growable Array. Conceptually, it represents a linked list supporting:

  - (add-right-op rga a b): adds b to the right of a, if a exists
  - (cons-op rga x): adds x at the beginning of the list
  - (remove-op rga x): removes x from the list

  In the current implementation each element in the array needs to be unique. If you want to repeat elements this is easy to achieve by transforming your elements from e to a tulpe (uuid, e). "
  ([] (rga (util/new-uuid)))
  ([rid] (rga rid '(::start ::end)))
  ([rid init-seq]
   (let [va (conj (set init-seq) ::start ::end)
         xs (into {} (map vector (drop-last 1 init-seq) (drop 1 init-seq)))]
     (RGA. rid va #{} xs (credn/vector-clock) []))))

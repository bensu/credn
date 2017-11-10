(ns credn.util)

(defn now []
  #?(:clj (java.util.Date.)
     :cljs (js/Date.)))

(defn new-uuid []
  #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid)))

(ns crjdt.core)

(defprotocol ICRDT
  (step [this op]))

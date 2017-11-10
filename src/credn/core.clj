(ns credn.core)

(defprotocol ICRDT
  (step [this op]))

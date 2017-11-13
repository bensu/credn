# credn

## Conflict-free Replicated EDN

Implementation of common operation-based CRDTs offering an EDN value to the user. Mostly taken from [A comprehensive study of Convergent and Commutative Replicated Data Types](https://hal.inria.fr/inria-00555588/PDF/techreport.pdf).

**Warning:** The existing implementations are neither correct nor performant.

## Usage

Each CRDT is implemented as a record that implements at least three protocols:

- IDeref to obtain the value being represented. If the CRDT is implementing a set, `@crdt` returns a set.
- A custom protocol for that type of CRDT, for example ICRDTSet, ICRDTGraph, ICRDTCounter. These protocols expose the valid operations for each CRDT. They are called with an instance of the CRDT and custom arguments. They return an edn, serializable operation that can be sent to other replicas of the CRDT.
- ICRDT which implements `(step crdt op) => crdt'`. Given a `crdt` and a operation `op` generated with the CRDT's custom protocol, it applies the operation `op` to the CRDT and returns the updated version `crdt'`.

### Example: Grow only counter

```clj

;; you can create a replica of the crdt by calling its constructor
> (def a (crdt.counter/g-counter))
;; => #'a

;; @ gives you the value it is representing
> @a
;; => 0

;; ops return data structures that you can serialize and send to other replicas
> (crdt.counter/inc-op a)
;; => [::crdt.counter/inc {::crdt.counter/replica-id #uuid ""}]

;; step applies the ops to the crdt and returns an update version
> @(crdt.core/step a (crdt.counter/inc-op a))
;; => 1

;; step plays well with reduce
> @(reduce crdt.core/step a (take 5 (repeatedly #(crdt.counter/inc-op a))))
;; => 5
```

## License

Copyright © 2017 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

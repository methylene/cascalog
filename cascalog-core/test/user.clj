(ns user
  (:refer-clojure :exclude [read-string])
  (:require [clojure.set :refer (difference intersection union subset?)]
            [clojure.zip :as czip]
            [jackknife.core :as u]
            [jackknife.seq :as s]
            [cascalog.logic.def :as d]
            [cascalog.logic.algebra :as algebra]
            [cascalog.logic.vars :as v]
            [cascalog.logic.zip :as zip]
            [cascalog.logic.predicate :as p]
            [cascalog.logic.predmacro :as pm]
            [cascalog.logic.platform :as platform]
            [cascalog.logic.options :as opts]
            [cascalog.logic.parse :refer :all]
            [cascalog.logic.parse-smb :as pb]
            [cascalog.api :refer [stdout ?- ??<- cross-join]]
            [clojure.pprint :refer [pprint]]
            [clojure.stacktrace :refer [print-cause-trace]])
  (:import [cascalog.logic.predicate
            Operation FilterOperation Aggregator Generator
            GeneratorSet RawPredicate RawSubquery]
           [clojure.lang IPersistentVector]
           [java.io File PrintStream]
           [cascalog WriterOutputStream]))

(defn ppprint [x] (print \space) (pprint x))
(defn pct [] (print-cause-trace *e))

(defn init-logging []
  (System/setOut (PrintStream. (WriterOutputStream. *out*)))
  (println "Logging inited. Try:")
  (ppprint '(test-run (<- [?x] ([[1]] ?x))))
  (ppprint '(pb/<-  [?x ?y ?z]
                    ([[1 2 3]] ?x)
                    (* ?x ?x :> ?y)
                    ( * ?x ?y :> ?z))))

(defmacro test-run
  "Run form, print result to stdout. Examples:
   (test-run (<- [?x] ([[1]] ?x)))
   (<-***  [?x ?y ?z]
           ([[1 2 3]] ?x)
           (* ?x ?x :> ?y)
           ( * ?x ?y :> ?z))"
  [form] `(?- (stdout) ~form))



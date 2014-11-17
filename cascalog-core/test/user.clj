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

(defn pct [] (print-cause-trace *e))

(comment

  (*<-* [?x ?y ?z]
        ([[1] [2] [3]] :>> [?x])
        (* :<< [?x ?x] :>> [?y])
        (* :<< [?x ?y] :>> [?z]))

  (*<-* [?c ?y]
        ('[[a 1] [b 1] [a 1]] :>> [?c ?x])
        (+ :<< [?x] :>> [?y]))

  (macroexpand
   '(pb/parse-subquery [?x ?y ?z]
                       ([[1] [2] [3]] :>> [?x])
                       (* :<< [?x ?x] :>> [?y])
                       (* :<< [?x ?y] :>> [?z])))

  (macroexpand
   '(pb/parse-subquery [?c ?y]
                       ('[[a 1] [b 1] [a 1]] :>> [?c ?x])
                       (+ :<< [?x] :>> [?y])))

  
)



(ns cascalog.logic.parse-smb
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
            [cascalog.logic.options :as opts])
  (:import [cascalog.logic.predicate
            Operation FilterOperation Aggregator Generator
            GeneratorSet RawPredicate RawSubquery]
           [clojure.lang IPersistentVector IPersistentMap]))

(defn parse-variables
  "parse variables into input-output map"
  [vars]
  (let [{input :<< output :>>}
        (into {} (map vec (partition 2 vars)))]
    {:input input
     :output output}))

(defn normalize [[op & rest]]
  (let [{:keys [input output]} (parse-variables rest)]
    {:op op :input (not-empty input) :output (not-empty output)
     :type :raw-predicate}))

(defmacro build-query
  [{:keys [output-fields predicates]}]
  `( ~(hash-map
       :fields output-fields
       :predicates predicates
       :options (opts/generate-option-map [])
       :type :raw-subquery)))

(defn prepare-subquery [output-fields raw-predicates]
  {:output-fields output-fields
   :predicates (map normalize raw-predicates)})

(defmacro parse-subquery
  "Parses predicates and output fields and returns a proper subquery."
  [output-fields & raw-predicates]
  `(build-query ~(prepare-subquery output-fields raw-predicates)) ;; TODO build-rule
  )

(defmacro <-
  [outvars & predicates]
  `(v/with-logic-vars
     (parse-subquery ~outvars [~@(map vec predicates)])))

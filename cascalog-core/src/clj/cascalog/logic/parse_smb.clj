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
           [clojure.lang IPersistentVector]))

;; ## Variable Parsing

;; TODO: Note that this is the spot where we'd go ahead and add new
;; selectors to Cascalog. For example, say we wanted the ability to
;; pour the results of a query into a vector directly; :>> ?a. This is
;; the place.

;; TODO: validation on the arg-m. We shouldn't ever have the sugar arg
;; and the non-sugar arg. Move the examples out to tests.

(defn desugar-selectors
  "Accepts a map of cascalog input or output symbol (:< or :>, for
  example) to var sequence, a <sugary input or output selector> and a
  <full vector input or output selector> and either destructures the
  non-sugary input or moves the sugary input into its proper
  place. For example:

 (desugar-selectors {:>> ([\"?a\"])} :> :>>)
 ;=> {:>> [\"?a\"]}

 (desugar-selectors {:> [\"?a\"] :<< [[\"?b\"]]} :> :>> :< :<<)
 ;=>  {:>> [\"?a\"], :<< [\"?b\"]}"
  [arg-m & sugar-full-pairs]
  (letfn [(desugar [m [sugar-k full-k]]
            (if-not (some m #{sugar-k full-k})
              m
              (-> m
                  (dissoc sugar-k)
                  (assoc full-k
                    (or (first (m full-k))
                        (m sugar-k))))))]
    (reduce desugar arg-m
            (partition 2 sugar-full-pairs))))

(defn expand-positional-selector
  "Accepts a map of cascalog selector to var sequence and, if the map
  contains an entry for Cascalog's positional selector, expands out
  the proper number of logic vars and replaces each entry specified
  within the positional map. This function returns the updated map."
  [arg-m]
  (if-let [[var-count selector-map] (:#> arg-m)]
    (let [expanded-vars (reduce (fn [v [pos var]]
                                  (assoc v pos var))
                                (vec (v/gen-nullable-vars var-count))
                                selector-map)]
      (-> arg-m
          (dissoc :#>)
          (assoc :>> expanded-vars)))
    arg-m))

(defn parse-variables
  "parses variables of the form ['?a' '?b' :> '!!c'] and returns a map
   of input variables, output variables, If there is no :>, defaults
   to selector-default."
  [vars default-selector]
  {:pre [(contains? #{:> :<} default-selector)]}
  (let [vars (cond (v/selector? (first vars)) vars
                   (some v/selector? vars) (cons :< vars)
                   :else (cons default-selector vars))
        {input :<< output :>>} (-> (s/to-map v/selector? vars)
                                   (desugar-selectors :> :>>
                                                      :< :<<)
                                   (expand-positional-selector))]
    {:input  (v/sanitize input)
     :output (v/sanitize output)}))

(defn default-selector
  "Default selector (either input or output) for this
  operation. Dispatches based on type."
  [op]
  (if (or (keyword? op)
          (p/filter? op))
    :< :>))

(extend-protocol p/IRawPredicate
  IPersistentVector
  (normalize [[op & rest]]
    (let [{:keys [input output]} (parse-variables rest (default-selector op))]
      (if (pm/predmacro? op)
        (mapcat p/normalize (pm/expand op input output))
        [(p/RawPredicate. op (not-empty input) (not-empty output))]))))

;; ## Unground Var Validation

(defn unground-outvars
  "For the supplied sequence of RawPredicate instances, returns a seq
  of all ungrounding vars in the output position."
  [predicates]
  (mapcat (comp (partial filter v/unground-var?) :output)
          predicates))

(defn unground-assertions!
  "Performs various validations on the supplied set of parsed
  predicates. If all validations pass, returns the sequence
  unchanged."
  [gens ops]
  (let [gen-outvars (unground-outvars gens)
        extra-vars  (difference (set (unground-outvars ops))
                                (set gen-outvars))
        dups (s/duplicates gen-outvars)]
    (when (not-empty extra-vars)
      (u/throw-illegal (str "Ungrounding vars must originate within a generator. "
                            extra-vars
                            " violate(s) the rules.")))
    (when (not-empty dups)
      (u/throw-illegal (str "Each ungrounding var can only appear once per query."
                            "The following are duplicated: "
                            dups)))))

(defn aggregation-assertions! [buffers aggs options]
  (when (and (not-empty aggs)
             (not-empty buffers))
    (u/throw-illegal "Cannot use both aggregators and buffers in same grouping"))
  ;; TODO: Move this into the fluent builder?
  (when (and (empty? aggs) (empty? buffers) (:sort options))
    (u/throw-illegal "Cannot specify a sort when there are no aggregators"))
  (when (> (count buffers) 1)
    (u/throw-illegal "Multiple buffers aren't allowed in the same subquery.")))

(defn validate-predicates! [preds opts]
  (let [grouped (group-by (fn [x]
                            (condp #(%1 %2) (:op x)
                              platform/gen? :gens
                              d/bufferop? :buffers
                              d/aggregateop? :aggs
                              :ops))
                          preds)
        [options _] (opts/extract-options preds)]
    (unground-assertions! (:gens grouped)
                          (:ops grouped))
    (aggregation-assertions! (:buffers grouped)
                             (:aggs grouped)
                             options)))

(defn query-signature?
  "Accepts the normalized return vector of a Cascalog form and returns
  true if the return vector is from a subquery, false otherwise. (A
  predicate macro would trigger false, for example.)"
  [vars]
  (not (some v/selector? vars)))

(defn split-outvar-constants
  "Accepts a sequence of output variables and returns a 2-vector:

  [new-outputs, [seq-of-new-raw-predicates]]

  By creating a new output predicate for every constant in the output
  field."
  [output]
  (let [[_ cleaned] (v/replace-dups output)]
    (reduce (fn [[new-output pred-acc] [v clean]]
              (if (v/cascalog-var? v)
                (if (= v clean)
                  [(conj new-output v) pred-acc]
                  [(conj new-output clean)
                   (conj pred-acc
                         (p/RawPredicate. = [v clean] nil))])
                (let [newvar (v/gen-nullable-var)]
                  [(conj new-output newvar)
                   (conj pred-acc
                         (if (or (fn? v)
                                 (u/multifn? v))
                           (p/RawPredicate. v [newvar] nil)
                           (p/RawPredicate. = [v newvar] nil)))])))
            [[] []]
            (map vector output cleaned))))

(defn validate-generator-set!
  "GeneratorSets can't be unground, ever."
  [input output]
  (when (not-empty input)
    (when (> (count output) 1)
      (u/throw-illegal "Only one output variable allowed in a generator-as-set."))
    (when-let [unground (not-empty (filter v/unground-var? (concat input output)))]
      (u/throw-illegal (str "Can't use unground vars in generators-as-sets. "
                            (vec unground)
                            " violate(s) the rules.\n\n")))))

(defn expand-outvars [{:keys [op input output] :as pred}]
  (when (p/can-generate? op)
    (validate-generator-set! input output))
  (let [[cleaned new-preds] (split-outvar-constants output)]
    (concat new-preds
            (if (and (not-empty input) (p/can-generate? op))
              (expand-outvars
               (p/RawPredicate. (p/GeneratorSet. op (first cleaned))
                                []
                                input))
              [(assoc pred :output cleaned)]))))

(defn build-query
  [{:keys [output-fields predicates]}]
  (let [[options predicates] (opts/extract-options predicates)
        expanded (mapcat expand-outvars predicates)]
    (validate-predicates! expanded options)
    (p/RawSubquery. output-fields expanded options)))

(defn prepare-subquery [output-fields raw-predicates]
  (let [output-fields (v/sanitize output-fields)
        raw-predicates (mapcat p/normalize raw-predicates)]
    {:output-fields output-fields
     :predicates raw-predicates}))

(defn parse-subquery
  "Parses predicates and output fields and returns a proper subquery."
  [output-fields raw-predicates]
  (let [{output-fields :output-fields
         predicates :predicates :as m}
        (prepare-subquery output-fields raw-predicates)]
    (if (query-signature? output-fields)
      (p/to-map (build-query m) ;; TODO build-rule
                )
      (let [parsed (parse-variables output-fields :<)]
        (pm/build-predmacro (:input parsed)
                            (:output parsed)
                            predicates)))))

(defmacro <-
  [outvars & predicates]
  `(v/with-logic-vars
     (parse-subquery ~outvars [~@(map vec predicates)])))




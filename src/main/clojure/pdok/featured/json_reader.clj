(ns pdok.featured.json-reader
  (:require [pdok.featured.feature :refer [nilled as-jts]]
            [cheshire [factory :as jfac] [parse :as jparse]]
            [pdok.util :as util]
            [clojure.walk :refer [postwalk]])
  (:import (com.fasterxml.jackson.core JsonParser JsonToken)
           (pdok.featured GeometryAttribute)))

(def ^:private pdok-field-replacements
  {"_action" :action "_collection" :collection "_id" :id "_validity" :validity "_current_validity" :current-validity})

(declare geometry-from-json
         upgrade-data)

(defn map-to-feature [obj]
  (let [action (keyword (get obj "_action"))
        validity (util/parse-time (obj "_validity"))
        current-validity (util/parse-time (obj "_current_validity"))
        feature (cond-> (upgrade-data obj)
                    true (assoc :action action)
                    true (assoc :validity validity)
                    current-validity (assoc :current-validity current-validity))]
    feature))

(defn clojurify [s]
  (keyword (cond->
               (clojure.string/replace s #"_" "-")
               (clojure.string/starts-with? s "_")
               (subs 1))))

(defn- parse-object [^JsonParser jp]
  (jparse/parse* jp identity nil nil))

(defn- read-meta-data [^JsonParser jp]
  (.nextToken jp)
  (loop [state {}
         currentName (-> jp .getCurrentName .toLowerCase)]
    (if (= "features" currentName)
      state
      (let [obj (do (.nextToken jp) (parse-object jp))
            newName (-> (doto jp .nextToken) .getCurrentName .toLowerCase)]
        (recur (merge state {(clojurify currentName) obj}) newName)))))

(defn- read-features [^JsonParser jp]
  (if (and (.nextToken jp) (not= (.getCurrentToken jp) JsonToken/END_ARRAY))
    (lazy-seq (cons (assoc (parse-object jp) :src :json) (read-features jp)))
    []))

(defn- features-from-stream* [^JsonParser jp & overrides]
  (.nextToken jp)
  (when (= JsonToken/START_OBJECT (.getCurrentToken jp))
    (let [meta (read-meta-data jp)]
      ;; features should be array
      (when (= JsonToken/START_ARRAY (.nextToken jp))
        [meta (map map-to-feature (read-features jp))]))))

(defn features-from-stream [input-stream & args]
  "Parses until 'features' for state. Then returns vector [meta <lazy sequence of features>]."
  (let [reader (clojure.java.io/reader input-stream)
        factory jfac/json-factory
        parser (.createParser factory reader)
        meta-and-features (apply features-from-stream* parser args)]
    meta-and-features))

(defn file-stream [path]
  (clojure.java.io/reader path))

(defn- element-is-function? [element]
  (and (vector? element)
       (= 2 (count element))
       (string? (first element))
       (-> element first (clojure.string/starts-with? "~#"))))

(defn- get-valid-srid [geometry]
  (if-let [srid (get geometry "srid")]
    (Integer. srid)))

(defn create-geometry-attribute [geometry]
  (if-let [type (get geometry "type")]
    (GeometryAttribute. type (get geometry type) (get-valid-srid geometry))))

(defn- evaluate-f [element]
  (let [[function params] element]
    (case function
      "~#moment"  (if params (apply util/parse-time params) (nilled org.joda.time.DateTime))
      "~#date"    (if params (apply util/parse-date params) (nilled org.joda.time.LocalDate))
      "~#int"     (if params (int (first params)) (nilled java.lang.Integer))
      "~#boolean" (if params (boolean (first params)) (nilled java.lang.Boolean))
      "~#double"  (if params (double (first params)) (nilled java.lang.Double))
      "~#geometry" (if params (create-geometry-attribute (first params)) (nilled pdok.featured.GeometryAttribute))
      element))) ; never fail just return element

(defn- element-is-pdok-field? [element]
  (contains? pdok-field-replacements element))

(defn- pdok-field [element]
  (get pdok-field-replacements element))

(defn- replace-fn [element]
  (condp #(%1 %2) element
    element-is-function? (evaluate-f element)
    element-is-pdok-field? (pdok-field element)
    ;; else just return element, replace nothing
    element))

(defn- upgrade-geometry [element]
  (if (get element "_geometry")
    (update element "_geometry" create-geometry-attribute)
    element))

(defn- upgrade-data [attributes]
  "Replaces functions with their parsed values"
  (let [attributes (postwalk replace-fn attributes)]
    (postwalk upgrade-geometry attributes)))

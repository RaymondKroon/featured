(ns pdok.featured.extracts
   (:require [pdok.featured.mustache  :refer :all]))

(defn- template-file [dataset feature-type]
  (str "pdok/featured/templates/" dataset "-" feature-type ".template"))

(defn gml-features [dataset feature-type features]
  "Returns gml-representation of the collection of features for a the given feature-type,
   based on the feature-type and dataset a (gml-) template will be used"
  (let [template (template-file dataset feature-type)]
    (render-template template features)))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
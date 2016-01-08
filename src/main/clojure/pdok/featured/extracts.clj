(ns pdok.featured.extracts
  (:require [clojure.java.jdbc :as j]
            [pdok.featured.config :as config]
            [pdok.featured.json-reader :as json-reader]
            [pdok.featured.mustache  :as m]
            [pdok.featured.timeline :as timeline]
            [pdok.featured.template :as template]
            [pdok.cache :as cache]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go chan]])
  (:gen-class))


(def ^{:private true } extract-schema "extractmanagement")
(def ^{:private true } extractset-table "extractmanagement.extractset")
(def ^{:private true } extractset-area-table "extractmanagement.extractset_area")

(defn features-for-extract [dataset feature-type extract-type features]
  "Returns the rendered representation of the collection of features for a given feature-type inclusive tiles-set"
  (if (empty? features)
    [nil nil]
    (let [template-key (template/template-key dataset extract-type feature-type)]
      [nil (map #(vector feature-type (:_version %) (:_tiles %) (m/render template-key %)
                           (:_valid_from %) (:_valid_to %) (:lv-publicatiedatum %)) features)])))

(defn- jdbc-insert-extract [db table entries]
   (try (j/with-db-connection [c db]
          (apply (partial j/insert! c (str extract-schema "." table) :transaction? false
                    [:feature_type :version :valid_from :valid_to :publication :tiles :xml])
                    entries))
        (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn get-or-add-extractset [db extractset version]
  "return id"
  (let [query (str "select id from " extractset-table " where extractset_name = ? and version = ?")]
    (j/with-db-connection [c db]
      (let [result (j/query c [query extractset version])]
        (if (empty? result)
          (do
            (j/query c [(str "SELECT " extract-schema ".add_extractset(?,?)") extractset (int version)])
            (get-or-add-extractset db extractset version))

          (:id (first result)))))))

(defn add-extractset-area [db extractset-id tiles]
  (let [query (str "select * from " extractset-area-table " where extractset_id = ? and area_id = ?")]
    (j/with-db-transaction [c db]
      (doseq [area-id tiles]
        (if (empty? (j/query c [query extractset-id area-id]))
          (j/insert! db extractset-area-table {:extractset_id extractset-id :area_id area-id}))))))

(defn- tiles-from-feature [[type version tiles & more]]
   tiles)

(defn add-metadata-extract-records [db extractset-id rendered-features]
   (let [tiles (reduce clojure.set/union (map tiles-from-feature rendered-features))]
     (add-extractset-area db extractset-id tiles)))

(defn- tranform-feature-for-db [[feature-type version tiles xml-feature valid-from valid-to publication-date]]
  [feature-type version valid-from valid-to publication-date (vec tiles) xml-feature])

(defn add-extract-records [db dataset extract-type rendered-features]
  "Inserts the xml-features and tile-set in an extract schema based on dataset, extract-type, version and feature-type,
   if schema or table doesn't exists it will be created."
  (let [extractset (str dataset "_" extract-type)
        extractset-id (get-or-add-extractset db extractset 0) ]
    (do
      (jdbc-insert-extract db (str extractset "_v0") (map tranform-feature-for-db rendered-features))
      (add-metadata-extract-records db extractset-id rendered-features))
    (count rendered-features)))

 
(defn transform-and-add-extract [extracts-db dataset feature-type extract-type features]
    (let [[error features-for-extract] (features-for-extract dataset
                                                             feature-type
                                                             extract-type
                                                             features)]
    (if (nil? error)
      (if (nil? features-for-extract)
        {:status "ok" :count 0}
        {:status "ok" :count (add-extract-records extracts-db dataset extract-type features-for-extract)})
      {:status "error" :msg error :count 0})))
 
(defn- insert-extract [dataset collection extract-type]
  (let [batch-size 10000
        rc (timeline/features-insert-in-delta (config/timeline) dataset collection)
        parts (a/pipe rc (a/chan batch-size (partition-all batch-size)))]
    (loop [features (a/<!! parts)]
      (when features
        (transform-and-add-extract config/extracts-db dataset collection extract-type features)
        (recur (a/<!! parts))))))

(defn- jdbc-udpate-marked-for-deletion [db table versions]
  (let [versions (set (map #(str "'" % "'") versions))
        versions (clojure.string/join ", " versions)
        query (str "UPDATE " extract-schema "." table " SET marked_for_deletion = true WHERE version IN ( " versions " )")]
   (try (j/with-db-connection [c db]
          (j/execute! c [query]))
        (catch java.sql.SQLException e (j/print-sql-exception-chain e)))))

(defn- update-extract-marked-for-deletion [db dataset feature-type extract-type versions]
  (let [table (str dataset "_" extract-type "_v0_" feature-type)]
    (jdbc-udpate-marked-for-deletion db table versions)))
 
(defn- update-marked-for-deletion [dataset collection extract-type]
  (let [batch-size 10000
        rc (timeline/versions-deleted-in-delta (config/timeline) dataset collection)
        parts (a/pipe rc (a/chan batch-size (partition-all batch-size)))]
    (loop [versions (a/<!! parts)]
      (when versions
        (update-extract-marked-for-deletion config/extracts-db dataset collection extract-type versions)
        (recur (a/<!! parts))))))

(defn flush-delta [dataset]
  (do 
   (log/info "Flush-delta request voor dataset: " dataset)
   (timeline/delete-delta (config/timeline) dataset)))

(defn file-to-features [path dataset]
  "Helper function to read features from a file.
   Returns features read from file."
  (with-open [s (json-reader/file-stream path)]
   (doall (json-reader/features-from-stream s :dataset dataset))))

(defn fill-extract [dataset collection extract-type]
  (do 
    (insert-extract dataset collection extract-type)
    (update-marked-for-deletion dataset collection extract-type))
  {:status "ok"})


(defn -main [template-location dataset collection extract-type & args]
  (let [templates-with-metadata (template/templates-with-metadata dataset template-location)]
        (when-not (some false? (map template/add-or-update-template templates-with-metadata))
          (fill-extract dataset collection extract-type))))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))

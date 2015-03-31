(ns pdok.featured.projectors
  (:require [pdok.cache :refer :all]
            [pdok.featured.feature :refer [as-jts]]
            [pdok.postgres :as pg]
            [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [environ.core :refer [env]]))

(defprotocol Projector
  (new-feature [proj feature])
  (change-feature [proj feature])
  (close [proj]))

(defn- gs-dataset-exists? [db dataset]
  ;(println "dataset exists?")
  (pg/schema-exists? db dataset))

(defn- gs-create-dataset [db dataset]
  (pg/create-schema db dataset))

(defn- gs-collection-exists? [db dataset collection]
  ;(println "collection exists?")
  (pg/table-exists? db dataset collection))

(defn- gs-create-collection [db dataset collection]
  "Create table with default fields"
  (pg/create-table db dataset collection
                [:gid "serial" :primary :key]
                [:_id "varchar(100)"]
                [:_geometry "geometry"])
  (pg/create-index db dataset collection "_id"))

(defn- gs-collection-attributes [db dataset collection]
  ;(println "attributes")
  (let [columns (pg/table-columns db dataset collection)
        no-defaults (filter #(not (some #{(:column_name %)} ["gid" "_id" "_geometry"])) columns)
        attributes (map #(:column_name %) no-defaults)]
    attributes))

(defn- gs-add-attribute [db dataset collection attribute-name attribute-type]
  (try
    (pg/add-column db dataset collection attribute-name attribute-type)
    (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn- feature-to-sparse-record [{:keys [id geometry attributes]} all-fields-constructor]
  (let [sparse-attributes (all-fields-constructor attributes)
        record (concat [id (-> geometry as-jts)] sparse-attributes)]
    record))

(defn- conj!-when
  ([target delegate src & srcs]
    (let [nw (if (delegate src) (conj! target src) target)]
      (if (empty? srcs)
        nw
        (recur nw delegate (first srcs) (rest srcs))))))

(defn- conj!-when-not-nil [target src & srcs]
  (apply conj!-when target identity src srcs))

(defn- feature-keys [feature]
  (let [geometry? (contains? feature :geometry)]
    (-> (apply conj!-when-not-nil
               (transient [])
               (when geometry? :_geometry)
               (keys (:attributes feature)))
        (persistent!))))

(defn- feature-to-update-record [{:keys [id geometry attributes]}]
  (let [attr-vals (vals attributes)
        rec (conj!-when-not-nil (transient []) (as-jts geometry))
        rec (apply conj!-when rec (fn [_] true) attr-vals)
        rec (conj! rec id)]
    (persistent! rec)))

(defn- gs-add-feature
  ([db all-attributes-fn features]
   (try
     (let [per-dataset-collection
           (group-by #(select-keys % [:dataset :collection]) features)
           ]
       (doseq [[{:keys [dataset collection]} grouped-features] per-dataset-collection]
         (j/with-db-connection [c db]
           (let [all-attributes (all-attributes-fn dataset collection)
                 all-fields-constructor (apply juxt (map #(fn [col] (get col %)) all-attributes))
                 records (map #(feature-to-sparse-record % all-fields-constructor) grouped-features)
                 fields (concat [:_id :_geometry] (map keyword all-attributes))]
             (apply
              (partial j/insert! c (str dataset "." collection) fields) records)))))
      (catch java.sql.SQLException e (j/print-sql-exception-chain e))))
  )

(defn- gs-update-sql [schema table columns]
  (str "UPDATE " (pg/quoted schema) "." (pg/quoted table)
       " SET " (str/join "," (map #(str (pg/quoted %) " = ?") columns))
       " WHERE \"_id\" = ?;"))

(defn- gs-update-feature [db features]
  (try
    (let [per-dataset-collection
          (group-by #(select-keys % [:dataset :collection]) features)]
      (doseq [[{:keys [dataset collection]} collection-features] per-dataset-collection]
        ;; group per key collection so we can batch every group
        (let [keyed (group-by feature-keys collection-features)]
          (doseq [[columns vals] keyed]
            (let [sql (gs-update-sql dataset collection (map name columns))
                  update-vals (map feature-to-update-record vals)]
              (j/execute! db (cons sql update-vals) :multi? true :transaction? false))))
        ))
    ;; (catch java.sql.SQLException e (j/print-sql-exception-chain e))
    ))

(deftype GeoserverProjector [db cache insert-batch insert-batch-size update-batch update-batch-size]
    Projector
    (new-feature [_ feature]
      (let [{:keys [dataset collection attributes]} feature
            cached-dataset-exists? (cached cache gs-dataset-exists? db)
            cached-collection-exists? (cached cache gs-collection-exists? db)
            cached-collection-attributes (cached cache gs-collection-attributes db)
            batched-add-feature
            (with-batch insert-batch insert-batch-size (partial gs-add-feature db cached-collection-attributes))]
        (do (when (not (cached-dataset-exists? dataset))
              (gs-create-dataset db dataset)
              (cached-dataset-exists? :reload dataset))
            (when (not (cached-collection-exists? dataset collection))
              (gs-create-collection db dataset collection)
              (cached-collection-exists? :reload dataset collection))
            (let [current-attributes (cached-collection-attributes dataset collection)
                  new-attributes (filter #(not (some #{(first %)} current-attributes)) attributes)]
              (doseq [a new-attributes]
                (gs-add-attribute db dataset collection (first a) (-> a second type)))
              (when (not-empty new-attributes) (cached-collection-attributes :reload dataset collection)))
            (batched-add-feature feature))))
    (change-feature [_ feature]
      (let [{:keys [dataset collection attributes]} feature
            cached-collection-attributes (cached cache gs-collection-attributes)
            batched-update-feature (with-batch update-batch update-batch-size
                                     (partial gs-update-feature db))]
        (let [current-attributes (cached-collection-attributes dataset collection)
                  new-attributes (filter #(not (some #{(first %)} current-attributes)) attributes)]
              (doseq [a new-attributes]
                (gs-add-attribute db dataset collection (first a) (-> a second type)))
              (when (not-empty new-attributes) (cached-collection-attributes :reload dataset collection)))
        (batched-update-feature feature)))
    (close [_]
      (let [cached-collection-attributes (cached cache gs-collection-attributes db)]
        (flush-batch insert-batch (partial gs-add-feature db cached-collection-attributes))
        (flush-batch update-batch (partial gs-update-feature db)))))

(defn geoserver-projector [config]
  (let [db (:db-config config)
        cache (atom {})
        insert-batch-size (or (:insert-batch-size config) (:batch-size config) 10000)
        insert-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        update-batch-size (or (:update-batch-size config) (:batch-size config) 10000)
        update-batch (ref (clojure.lang.PersistentQueue/EMPTY))]
    (GeoserverProjector. db cache insert-batch insert-batch-size update-batch update-batch-size)))

(def data-db {:subprotocol "postgresql"
                     :subname (or (env :data-database-url) "//localhost:5432/pdok")
                     :user (or (env :data-database-user) "postgres")
                     :password (or (env :data-database-password) "postgres")})

(def projectors
  [(geoserver-projector {:db-config data-db})])
(ns pdok.featured.migrations.persistence.20160126-001-collections
  (:require [pdok.postgres :as pg]
            [pdok.featured.dynamic-config :as dc]))

(defn up [db]
  (pg/create-table db dc/*persistence-schema* dc/*persistence-collections*
                   [:id "bigserial" :primary :key]
                   [:dataset "varchar(100)"]
                   [:collection "varchar(255)"]
                   [:parent_collection "varchar(100)"])
  (pg/create-index db dc/*persistence-schema* dc/*persistence-collections*
                   :dataset :collection :child_collection))

(defn down [db])

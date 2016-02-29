(ns pdok.featured.timeline-test
  (:require [pdok.featured.timeline :as timeline]
            [clojure.test :refer :all]))

;; merge target path feature

(defn- make-feature [attributes geometry id version]
  (cond-> {:collection "only-path-should-matter"
           :id id
           :attributes attributes}
          geometry (assoc :geometry geometry)
          version (assoc :version version)))

(defn- make-root [versions & tiles]
  {:_version (first versions) :_all_versions versions :_tiles (into #{} tiles)})

(defn- mustafied
  ([attributes] (mustafied attributes nil))
  ([attributes geom] (mustafied attributes geom "not-important"))
  ( [attributes geom id]
   (reduce (fn [m [k v]] (assoc m k v))
           {:_collection "only-path-should-matter"
            :_id id}
           (cond-> attributes geom (assoc :_geometry geom)))))

(deftest single-depth-merge
  (let [path '()
        feature (make-feature {:attribute2 "value2"} nil "not-important" 1)
        merged (#'timeline/merge {:attribute1 "value1"} path feature)
        should-be (merge (make-root '(1)) (mustafied {:attribute1 "value1" :attribute2 "value2"}))]
    (is (= should-be merged))))

(deftest double-depth-merge ;; with tiles
  (let [path '(["parent-collection" "parent-id" "nested1" "child-id"])
        feature (make-feature {:attribute "value"} {:dummy :dummy :nl-tiles #{666}} "not-important" 3)
        merged (#'timeline/merge (make-root '(1) 1) path feature)
        should-be (merge (make-root '(3 1) 1 666)
                         {:nested1 [(mustafied {:attribute "value"} {:dummy :dummy :nl-tiles #{ 666}} )]})]
    (is (= should-be merged))))

(deftest append-to-empty-vector-merge
  (let [starts-with (assoc (make-root '(1) 1) :nested1 [])
        path '(["parent-collection" "parent-id" "nested1" "child-id"])
        feature (make-feature {:attribute "value"} {:dummy :dummy :nl-tiles #{666}} "not-important" 3)
        merged (#'timeline/merge starts-with path feature)
        should-be (merge (make-root '(3 1) 1 666)
                         {:nested1 [(mustafied {:attribute "value"} {:dummy :dummy :nl-tiles #{ 666}} )]})]
    (is (= should-be merged))))

(deftest merge-existing-nested
  (let [starts-with (assoc (make-root '(1) 1) :nested1 [(mustafied {:attribute "value" :keep 1} {:dummy :dummy} "child-id")])
        path '(["parent-collection" "parent-id" "nested1" "child-id"])
        feature (make-feature {:attribute "new-value"} {:dummy :dummy :nl-tiles #{666}} "not-important" 3)
        merged (#'timeline/merge starts-with path feature)
        should-be (merge (make-root '(3 1) 1 666)
                         {:nested1 [(mustafied {:attribute "new-value" :keep 1} {:dummy :dummy :nl-tiles #{ 666}} )]})]
    (is (= should-be merged))))

(deftest replace-attr-merge
  (let [starts-with (assoc (make-root '(1) 1) :nested1 "value")
        path '(["parent-collection" "parent-id" "nested1" "child-id"])
        feature (make-feature {:attribute "value"} {:dummy :dummy :nl-tiles #{666}} "not-important" 3)
        merged (#'timeline/merge starts-with path feature)
        should-be (merge (make-root '(3 1) 1 666)
                         {:nested1 [(mustafied {:attribute "value"} {:dummy :dummy :nl-tiles #{ 666}} )]})]
    (is (= should-be merged))))

(deftest replace-map-merge
  (let [starts-with (assoc (make-root '(1) 1) :nested1 {:bla :bla})
        path '(["parent-collection" "parent-id" "nested1" "child-id"])
        feature (make-feature {:attribute "value"} {:dummy :dummy :nl-tiles #{666}} "not-important" 3)
        merged (#'timeline/merge starts-with path feature)
        should-be (merge (make-root '(3 1) 1 666)
                         {:nested1 [(mustafied {:attribute "value"} {:dummy :dummy :nl-tiles #{ 666}} )]})]
    (is (= should-be merged))))

(deftest triple-depth-merge
  (let [starts-with (assoc (make-root '(1) 1) :attr1 :a)
        path '(["pc" "pid" "nested_1" "id1"] ["pc2" "pid2" "nested_2" "id2"])
        feature (make-feature {:attributeX "valueY"} nil "not-important" 5)
        merged (#'timeline/merge starts-with path feature)
        should-be (merge (make-root '(5 1) 1)
                         {:attr1 :a :nested_1
                          [{:nested_2 [(mustafied {:attributeX "valueY"})]}]})]
    (is (= should-be merged))))

(deftest triple-depth-merge-in-existing-map
  (let [starts-with (assoc (make-root '(1) 1) :nested_1 [{:_id "id1"}])
        path '(["pc" "pid" "nested_1" "id1"] ["pc2" "pid2" "nested_2" "id2"])
        feature (make-feature {:attributeX "valueY"} nil "not-important" 5)
        merged (#'timeline/merge starts-with path feature)
        should-be (merge (make-root '(5 1) 1)
                         {:nested_1
                          [{:_id "id1" :nested_2 [(mustafied {:attributeX "valueY"})]}]})]
    (is (= should-be merged))))

(deftest close-one-of-two-child-merge ;; with tiles
  (let [path '(["parent-collection" "parent-id" "nested1" "child-id"])
        feature (-> (make-feature {:attribute "value"} {:dummy :dummy} "child-id" 3)
                    (assoc :action :close))
        merged (#'timeline/merge
                (-> (make-root '(1) 1)
                    (assoc :nested1 [(mustafied {:attribute "value"} {:dummy :dummy} "child-id")
                                     (mustafied {:attribute "value"} {:dummy :dummy} "keep-id")]))
                path
                feature)
        should-be (merge (make-root '(3 1) 1)
                         {:nested1 [(mustafied {:attribute "value"} {:dummy :dummy} "keep-id")]})]
    (is (= should-be merged))))

(deftest close-single-child-keeps-vector
  (let [path '(["parent-collection" "parent-id" "nested1" "child-id"])
        feature (-> (make-feature {:attribute "value"} {:dummy :dummy} "child-id" 3)
                    (assoc :action :close))
        merged (#'timeline/merge
                (-> (make-root '(1) 1)
                    (assoc :nested1 [(mustafied {:attribute "value"} {:dummy :dummy} "child-id")]))
                path
                feature)
        should-be (merge (make-root '(3 1) 1)
                         {:nested1 []})]
    (is (= should-be merged))))

(deftest close-non-existing-child-keeps-vector
  (let [path '(["parent-collection" "parent-id" "nested1" "child-id"])
        feature (-> (make-feature {:attribute "value"} {:dummy :dummy} "child-id" 3)
                    (assoc :action :close))
        merged (#'timeline/merge
                (-> (make-root '(1) 1)
                    (assoc :nested1 []))
                path
                feature)
        should-be (merge (make-root '(3 1) 1)
                         {:nested1 []})]
    (is (= should-be merged))))

(deftest close-no-vector->no-error+no-delete
  (let [path '(["parent-collection" "parent-id" "nested1" "child-id"])
        feature (-> (make-feature {:attribute "value"} {:dummy :dummy} "child-id" 3)
                    (assoc :action :close))
        merged (#'timeline/merge
                (-> (make-root '(1) 1)
                    (assoc :nested1 "err string"))
                path
                feature)
        should-be (merge (make-root '(3 1) 1)
                         {:nested1 "err string"})]
    (is (= should-be merged))))

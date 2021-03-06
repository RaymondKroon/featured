(ns pdok.featured.processor-test
  (:require [clj-time.local :as tl]
            [pdok.featured.json-reader :as reader]
            [pdok.featured.processor :as processor :refer [consume consume* shutdown]]
            [pdok.featured.persistence :as pers]
            [pdok.featured.projectors :as proj]
            [clojure.test :refer :all]
            [clojure.java.io :as io])
  (:import (pdok.featured GeometryAttribute)))

(defrecord MockedPersistence [streams streams-n state events-n collections]
  pers/ProcessorPersistence
  (init [this for-dataset]
    (vswap! collections conj {:name :col1})
    (assoc this :initialized true))
  (prepare [this features] this)
  (flush [this] this)
  (collections [persistence] @collections)
  (create-collection [persistence collection] (vswap! collections conj {:name collection}))
  (stream-exists? [this collection id] (get @streams [collection id]))
  (create-stream [this collection id]
    (swap! streams assoc [collection id] 1)
    (swap! streams-n inc))
  (append-to-stream [this version action collection id validity attributes]
    (swap! state assoc [collection id] [action validity])
    (swap! events-n inc))
  (current-validity [_ collection id]
    (second (get @state [collection id] nil)))
  (last-action [this collection id]
    (first (get @state [collection id] nil)))
  (current-version [_ _ _]
    nil)
  (close [this] (assoc this :closed true)))

(defrecord MockedProjector [features-n changes-n collections]
  proj/Projector
  (init [this tx for-dataset current-collections]
    (vreset! collections current-collections)
    (-> this (assoc :initialized true)))
  (new-collection [this collection] (vswap! collections conj {:name collection}))
  (flush [this] this)
  (new-feature [_ feature]
    (swap! features-n inc))
  (change-feature [_ feature]
    (swap! changes-n inc))
  (close [this] (assoc this :closed true)))

(defn create-persistence [] (->MockedPersistence (atom {}) (atom 0) (atom {}) (atom 0) (volatile! #{})))

(defn create-projectors [n]
  (repeatedly n #(->MockedProjector (atom 0) (atom 0) (volatile! #{}))))

(defn create-processor
  ([] (create-processor 1))
  ([n-projectors]
   (processor/create
     {:batch-size 1}
     "testset"
     (create-persistence)
     (create-projectors n-projectors))))

(defn consume-single [processor feature]
  (->> [feature] (consume* processor) (first)))

(def default-validity (tl/local-now))

(def dummy-geometry (GeometryAttribute. "dummy" "dummy"))

(def valid-new-feature
  {:action :new
   :collection "collection-1"
   :id "valid-feature"
   :validity default-validity
   :geometry dummy-geometry
   :attributes {:field1 "test"}})

(def valid-change-feature
  {:action :change
   :collection "collection-1"
   :id "valid-feature"
   :validity default-validity
   :current-validity default-validity
   :geometry dummy-geometry
   :attributes {:field2 "test"}})

(defn- init-with-feature [persistence {:keys [collection id action validity]}]
  (pers/create-stream persistence collection id)
  (pers/append-to-stream persistence nil action collection id validity nil))

(deftest initialized-processor
  (let [processor (processor/create "know-dataset" (create-persistence) (create-projectors 2))]
    (testing "Initialized persistence?"
      (is (-> processor :persistence :initialized)))
    (testing "Initialized projectors?"
      (is (= (pers/collections (:persistence processor)) #{{:name :col1}}))
      (doseq [p (:projectors processor)]
        (is (-> p :initialized))
        (is (= @(:collections p) #{{:name :col1}}))))))

(deftest shutdown-processor
  (let [processor (shutdown (create-processor 2))]
    (testing "Closed persistence?"
      (is (-> processor :persistence :closed)))
    (comment (testing "Closed projectors?"
               (doseq [p (:projectors processor)]
                 (is (-> p :closed)))))))

(defn- new-should-be-ok [processor processed]
  (is (= false (nil? processed)))
  (is (not (:invalid? processed)))
  (is (= 1 (-> processor :persistence :streams-n deref)))
  (is (= 1 (-> processor :persistence :events-n deref)))
  (is (= 1 @(-> processor :projectors first :features-n))))

(deftest ok-new-feature
  (let [processor (create-processor)
        processed (consume* processor [valid-new-feature])]
    (new-should-be-ok processor processed)
    (testing "Emitted collections"
      (is (= (pers/collections (:persistence processor)) #{{:name :col1} {:name "collection-1"}}))
      (doseq [p (:projectors processor)]
        (is (= @(:collections p) #{{:name :col1} {:name "collection-1"}}))
        ))))

(def still-valid-new-transformations
  [["no geometry" #(dissoc % :geometry)]
   ["nil geometry" #(assoc % :geometry nil)]])

(deftest also-ok-new-feature
  (doseq [[name transform] still-valid-new-transformations]
    (let [processor (create-processor)
          also-valid-new-feature (transform valid-new-feature)
          processed (consume-single processor also-valid-new-feature)]
      (testing (str name " should be ok for " processed)
        (new-should-be-ok processor processed)))))

(def make-broken-new-transformations
  [["no collection" #(dissoc % :collection)]
   ["empty collection" #(assoc % :collection "")]
   ["no id" #(dissoc % :id)]
   ["empty id" #(assoc % :id "")]
   ["no validity" #(dissoc % :id)]])

(deftest nok-new-feature
  (doseq [[name transform] make-broken-new-transformations]
    (let [processor (create-processor)
          invalid-new-feature (transform valid-new-feature)
          processed (consume-single processor invalid-new-feature)]
      (testing (str name " should break")
        (is (= false (nil? processed)))
        (is (:invalid? processed))
        (is (= 0 (-> processor :persistence :streams-n deref)))
        (is (= 0 (-> processor :persistence :events-n deref)))
        (is (= 0 @(-> processor :projectors first :features-n)))))))

(defn- change-should-be-ok [processor processed]
  (is (= false (nil? processed)))
  (is (not (:invalid? processed)) (str (:invalid-reasons processed)))
  (is (= 1 (-> processor :persistence :streams-n deref))) ; no new stream
  (is (= 2 (-> processor :persistence :events-n deref)))
  (is (= 0 @(-> processor :projectors first :features-n))) ; no new features
  (is (= 1 @(-> processor :projectors first :changes-n))))

(deftest ok-change-feature
  (let [processor (create-processor)
        persistence (:persistence processor)
        _ (init-with-feature persistence valid-new-feature)
        processed (consume-single processor valid-change-feature)]
    (change-should-be-ok processor processed)
    ))

(deftest also-ok-change-feature
  (doseq [[name transform] still-valid-new-transformations]
    (let [processor (create-processor)
          _ (init-with-feature (:persistence processor) valid-new-feature)
          also-valid-change-feature (transform valid-change-feature)
          processed (consume-single processor also-valid-change-feature)]
      (testing (str name " should be ok")
        (change-should-be-ok processor processed)))))

(def make-broken-change-transformations
  [["no current-validity" #(dissoc % :current-validity)]
   ["wrong current-validity" #(assoc % :current-validity (tl/local-now))]])

(deftest nok-change-feature
  (doseq [[name transform] (concat make-broken-new-transformations
                                   make-broken-change-transformations)]
    (let [processor (create-processor)
          _ (init-with-feature (:persistence processor) valid-new-feature)
          invalid-change-feature (transform valid-change-feature)
          processed (consume-single processor invalid-change-feature)]
      (testing (str name " should break")
        (is (not (nil? processed)))
        (is (:invalid? processed))
        (is (= 1 (-> processor :persistence :events-n deref))) ;; only new
        (is (= 0 @(-> processor :projectors first :features-n))) ; no new features
        (is (= 0 @(-> processor :projectors first :changes-n))))
      )))

(def extreme-nested (io/resource "processor/extreme-nested.json"))

(deftest extreme-nested-should-work
  (with-open [in (io/input-stream extreme-nested)]
    (let [[meta features] (reader/features-from-stream in)
          processor (create-processor)
          processed (into '() (consume processor features))]
      (is (= 1 (count processed)))
      (is (= 0 (count (filter #(:invalid? %) processed)))))))

(def feature-with-array-file (io/resource "processor/feature-with-array.json"))

(deftest process-feature-with-array
  (with-open [in (io/input-stream feature-with-array-file)]
    (let [[meta features] (reader/features-from-stream in)
          processor (create-processor)
          processed (into '() (consume processor features))]
      (is (= 1 (count processed)))
      (is (= 0 (count (filter #(:invalid? %) processed))))
      (let [bronhouders (get (-> processed first :attributes) "bronhouder")]
        (is (= 3 (count bronhouders)))
        (is (some #{"B0001"} bronhouders))
        (is (some #{"B0002"} bronhouders))
        (is (some #{"B0003"} bronhouders))))))

(ns pdok.featured.workers
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [pdok.filestore :as fs]
            [pdok.featured
             [config :as config]
             [persistence :as pers]
             [processor :as processor]]
            [pdok.featured.json-reader :as reader]
            [clojure.string :as str])
  (:gen-class)
  (:import (com.netflix.conductor.client.worker Worker)
           (com.netflix.conductor.common.metadata.tasks TaskResult$Status TaskResult)
           (java.io File)
           (java.util.zip ZipFile ZipEntry ZipOutputStream)
           (com.microsoft.azure.storage CloudStorageAccount)
           (com.microsoft.azure.storage.blob CloudBlobClient CloudBlockBlob BlobContainerPublicAccessType BlobContainerPermissions CloudBlobContainer)))

(defn download-uri [^String uri]
  "Copy to tmp file, return handle"
  (let [file (File/createTempFile "tmpfile" nil)]
    (with-open [in (io/input-stream uri)
                out (io/output-stream file)]
      (io/copy in out))
    file))

(defn unzip [^File file]
  (let [zipfile (ZipFile. file)
        ^ZipEntry entry (first (enumeration-seq (.entries zipfile)))
        name (.getName entry)
        ^File tmp-dir (File/createTempFile "unzip" nil)
        _ (do (.delete tmp-dir) (.mkdirs tmp-dir))
        target (File. tmp-dir name)]
    (with-open [in (.getInputStream zipfile entry)
                out (io/output-stream target)]
      (io/copy in out))
    target))

(defn safe-container-name [name]
  (-> name (str/lower-case) (str/replace #"_" "-")))

(defn make-public! [^CloudBlobContainer container]
  (let [permissions (doto (BlobContainerPermissions.) (.setPublicAccess BlobContainerPublicAccessType/CONTAINER))]
    (.uploadPermissions container permissions)))

(defn upload [dataset ^File file]
  (let [storage-account (CloudStorageAccount/parse (config/env :storage-connection-string))
        client (.createCloudBlobClient storage-account)
        container (.getContainerReference client (safe-container-name
                                                   (str (config/env :storage-container-prefix "featured-out-") dataset)))
        _ (when-not (.exists container) (.create container) (make-public! container))
        blob (.getBlockBlobReference container (.getName file))]
    (with-open [in (io/input-stream file)]
      (.upload blob in (.length file)))
    blob))

(defn delete-recursively [fname]
  (let [func (fn [func f]
               (when (.isDirectory f)
                 (doseq [f2 (.listFiles f)]
                   (func func f2)))
               (clojure.java.io/delete-file f))]
    (func func (clojure.java.io/file fname))))

(deftype FeaturedWorker [^String name]
  Worker
  (getTaskDefName [this] name)
  (execute [this task]
    (let [^TaskResult result (doto (TaskResult. task) (.setStatus TaskResult$Status/FAILED))
          output (.getOutputData result)]
      (try
        (log/info "Working ...")
        (let [input (.getInputData task)
              dataset (.get input "dataset")
              file-uri (.get input "uri")
              local-zip (download-uri file-uri)
              unzipped (unzip local-zip)
              target-dir (File/createTempFile "target" nil)
              _ (do (.delete target-dir) (.mkdirs target-dir))
              persistence (config/persistence)
              filestore (config/filestore target-dir)
              projectors [(config/timeline filestore)]
              processor (processor/create dataset persistence projectors)]
          (with-open [in (io/input-stream unzipped)]
            (let [[meta features] (reader/features-from-stream in :dataset dataset)
                  processor (merge processor meta)
                  _ (dorun (processor/consume processor features))
                  processor (processor/shutdown processor)
                  statistics (:statistics processor)
                  changelogs (:changelogs statistics)
                  blobs
                  (for [changelog changelogs]
                    (let [file (fs/get-file filestore changelog)
                          ^CloudBlockBlob uploaded (upload dataset file)]
                      {"name" (.getName uploaded) "uri" (.getUri uploaded)}))]
              (.put output "files" (doall blobs))))
          (do (.delete local-zip)
              (delete-recursively (.getParentFile unzipped))
              (delete-recursively target-dir)))
        (log/info "... Finished")
        (doto result (.setStatus TaskResult$Status/COMPLETED))
        (catch Exception e (log/error e) (log/info "... Finished (with errors)") result)))))

;; Copyright 2015 StreamBright LLC and contributors

;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at

;;     http://www.apache.org/licenses/LICENSE-2.0

;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns ^{  :doc "Rotating logs for S3 buckets and converting the content to ORC or Avro"
        :author "Istvan Szukacs"  } 
  s3-logrotate.cli
  (:require
    [clojure.tools.cli      :as   cli   ]
    [clojure.tools.logging  :as   log   ]
    [clojure.edn            :as   edn   ]
  )
  (:import
    [java.io                                File BufferedReader                   ]
    [clojure.lang                           PersistentArrayMap PersistentList     ]
  )
  (:gen-class))

(defn read-file
  "Returns {:ok string } or {:error...}"
  ^PersistentArrayMap [^File file]
  (try
    (cond
      (.isFile file)
        {:ok (slurp file) }
      :else
        (throw (Exception. "Input is not a file")))
  (catch Exception e
    {:error "Exception" :fn "read-file" :exception (.getMessage e) })))

(defn parse-edn-string
  "Returns {:ok {} } or {:error...}"
  ^PersistentArrayMap [^String s]
  (try
    {:ok (edn/read-string s)}
  (catch Exception e
    {:error "Exception" :fn "parse-config" :exception (.getMessage e)})))


(defn read-config
  "Reads the configuration file (app.edn) and returns the config as a hashmap"
  ^PersistentArrayMap [^String path]
  (let
    [ file-string (read-file (File. path)) ]
    (cond
      (contains? file-string :ok)
        ;if the file read is successful the content is sent to parse-edn-string
        ;that can return either and {:ok ...} or {:error ...}
        (parse-edn-string (file-string :ok))
      :else
        ;keeping the original error and let it fall through
        file-string)))

(defn exit
  ([^Long n]
    (log/info "init :: stop")
    (System/exit n))
  ([^Long n ^String msg]
    (log/info msg)
    (log/info "init :: stop")
    (System/exit n)))

(defn process-config
  "Processing config with error handling"
  [file]
  ; Handle help and error conditions
  (let [config (read-config file)]
    (cond
      (or (empty? config) (:error config))
        (exit 1 (str "Config cannot be read or parsed..." "\n" config))
      :else
        config)))

(def cli-options
  ;; An option with a required argument
  [
  ["-c" "--config CONFIG" "Config file name"
    :default "conf/app.edn"]
  ["-f" "--file FILE" "File to process"
    :default "/dev/null"]
  ["-t" "--type TYPE" "Upload type (patents, cpcs, entities..)"
    :default "patents"]
  ["-e" "--env ENV" "Environment (dev or prod)"
    :default "dev"]
  ["-s" "--serialization SER" "JSON or Avro"
    :default "json"]
  ["-h" "--help"]
   ])

(defn process-cli
  "Processing the cli arguments and options"
  [args cli-options]
  (let [
          cli-options-parsed (cli/parse-opts args cli-options)
          {:keys [options arguments errors summary]} cli-options-parsed
        ]
    (cond
      (:help options)
        (do
          (log/info (str "Help: \n" summary))
          (exit 0))
      errors
        (do
          (log/error errors)
          (exit 1))
      :else
        cli-options-parsed)))

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
  s3-logrotate.core
  (:require
    [s3-logrotate.cli         :as     cli                   ]
    [s3-logrotate.s3api       :as     s4                    ]
    [clojure.tools.logging    :as     log                   ]
    [clojure.edn              :as     edn                   ]
    [clojure.string           :as     string                ]
    [abracad.avro             :as     avro                  ]
    [clojure.java.io          :as     io                    ]
    [cheshire.core            :as     json                  ]
    [clojure.walk             :as     walk                  ]
    [abracad.avro.util        :refer [returning mangle 
                                      unmangle coerce]      ]
  )
  (:import
    [java.io              File BufferedReader               ]
    [clojure.lang         PersistentArrayMap PersistentList ]
    [java.util            Date GregorianCalendar            ]
    [java.text            SimpleDateFormat                  ]
    [org.apache.avro      Schema$RecordSchema               ]
  )
  (:gen-class))

(defn get-credentials
  ^PersistentArrayMap [^String credentials-file]
  (let
    [ file-string (cli/read-file (File. credentials-file)) ]
    (cond
      (contains? file-string :ok)
        ;if the file read is successful the content is sent to parse-edn-string
        ;that can return either and {:ok ...} or {:error ...}
        (cli/parse-edn-string (file-string :ok))
      :else
        ;keeping the original error and let it fall through
        file-string)))

(defn days
  "Returns a list with all the days in between days-start and days-stop
  The input parameters are relativ from now()" 
  [days-start days-stop]
  (let [  ^SimpleDateFormat   format-string (SimpleDateFormat. "yyyy-MM-dd")
          ^GregorianCalendar  today         (GregorianCalendar.)
                              start-date    (.add today java.util.Calendar/DAY_OF_YEAR (* -1 days-start)) 
        ]
  (reverse 
    (take days-stop 
      (map #(.format format-string (.getTime  %))
        (repeatedly
          (fn []
            (.add today java.util.Calendar/DAY_OF_YEAR -1)
            (.clone today))))))))

(defn avro-schema
  "Returns an Avro schema"
  ^Schema$RecordSchema [^String schema-file]
  (avro/parse-schema (json/parse-stream (io/reader schema-file))))

(defn parse-line [line parts pattern]
  (let [match (drop 1 (re-find pattern line))]
    (apply hash-map (interleave parts match))))

(defn update-values
  "Updates values in map
  Source: http://blog.jayfields.com/2011/08/clojure-apply-function-to-each-value-of.html"
  [m f & args]
  (reduce (fn [r [k v]] (assoc r k (apply f v args))) {} m))

(defn clean-up-avro-entry 
  "Returns the clean version of an entry, 
  replaces - with nil, nil with 0 (only for int fields) and 
  converts int values to be actually an integer"
  [avro-entry int-fields]
  (let  [
          dash-to-nil   (update-values avro-entry #(if (= % "-") nil %))
          nil-to-zero   (into {} 
                          (for [[k v] dash-to-nil]
                            [k (if (and (contains? int-fields k) (nil? v)) 0 v)]))
          string-to-int (into {} 
                          (for [[k v] nil-to-zero] 
                            [k (if (and (contains? int-fields k) (string? v)) (read-string v) v)]))
        ]
  string-to-int
  ))  

(defn process-files
  "Iterates over all entries " 
  [s3-log-pattern schema-file days aws-s3-connection bucket prefix]
  (let [  
          s3-log-avro-schema-json           (json/parse-stream (io/reader schema-file))
          s3-log-avro-schema-fields         (map keyword (map #(get-in % ["name"]) 
                                              (get-in s3-log-avro-schema-json ["fields"])))
          s3-log-avro-schema-fields-dash    (for [k s3-log-avro-schema-fields] 
                                              (keyword (string/replace (name k) #"_" "-")))
          s3-log-avro-schema                (avro-schema schema-file)
          _                                 (log/info (type s3-log-avro-schema))
          int-fields                        #{:turn-around-time :http-status :total-time :bytes-sent :object-size}
          ]
  
    (doseq [day days]
      (let [  all-files-for-a-day   (s4/list-all-files-eager 
                                      aws-s3-connection   ;connection
                                      bucket              ;bucket-name
                                      (str prefix day)    ;prefix, path eg. "logs/2015-01-01"
                                      ""                  ;marker
                                      ""                  ;delimiter
                                      (int 1000)          ;max-keys
                                      '())                ;acc
              avro-file-name         (str day ".avro")
              avro-file              (avro/data-file-writer "deflate" s3-log-avro-schema (str "data/" avro-file-name))
            ]
        (log/info "Processing day: " day)
        (doseq [file all-files-for-a-day]
          (let [  s3-key              (:key file)
                  _                   (log/info s3-key)
                  object-content      (s4/get-object-content-safe 
                                        (s4/get-object aws-s3-connection bucket s3-key))                  
                  avro-entries      (map #(parse-line % s3-log-avro-schema-fields-dash s3-log-pattern) object-content)
                ]
            (doseq [avro-entry avro-entries]

              (let [clean-avro-entry (clean-up-avro-entry avro-entry int-fields)]

              (log/debug clean-avro-entry)
              (try
                (.append avro-file clean-avro-entry)
                (catch Exception e (log/error "caught exception: " (.getMessage e) 
                                              " key: " s3-key 
                                              " clean avro: " clean-avro-entry)))
            ))))
  
        ;file close
        (.close avro-file)
      ))))

(defn -main
  [& args]
  (let [
        ;; dealing with cli & config file
        cli-options-parsed                          (cli/process-cli args cli/cli-options)
        {:keys [options arguments errors summary]}  cli-options-parsed
        config                                      (cli/process-config (:config options))
        env                       (keyword (:env options))
        credentials-file          (:ok (get-credentials (get-in config [:ok :aws :credentials-file])))
        bucket                    (name (get-in config [:ok :aws :s3 :bucket]))
        aws-basic-cred            (s4/create-basic-aws-credentials credentials-file)
        aws-s3-connection         (s4/connect-with-basic-credentials aws-basic-cred)
        days-start                (get-in config [:ok :days :start])
        days-stop                 (get-in config [:ok :days :stop])
        days                      (days days-start days-stop)
        s3-folder                 (get-in config [:ok :aws :s3 :folder])
        schema-file               "schema/amazon-log.avsc"
        s3-log-pattern            (re-pattern (get-in config [:ok :aws :log-format]))
        ]
        
        ; main entry point for execution 
        (process-files s3-log-pattern schema-file days aws-s3-connection bucket s3-folder)

    (log/info "init :: stop")))


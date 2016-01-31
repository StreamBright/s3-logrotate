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
    [s3-logrotate.cli         :as   cli     ]
    [s3-logrotate.s3api       :as   s4      ]
    [clojure.tools.logging    :as   log     ]
    [clojure.edn              :as   edn     ]
    [clojure.string           :as   string  ]
    [abracad.avro             :as   avro    ]
    [clojure.java.io          :as   io      ]
    [cheshire.core            :as   json    ]
    [clojure.walk             :as   walk    ]
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

; (def config                   (cli/process-config "conf/app.edn"))
; (def credentials              (:ok (get-credentials (get-in config [:ok :aws :credentials]))))
; (def bucket                   (name (get-in config [:ok :aws :s3 :bucket])))
; (def aws-basic-cred           (s4/create-basic-aws-credentials credentials))
; (def aws-s3-connection        (s4/connect-with-basic-credentials aws-basic-cred)
; (def list-object-request      (s4/create-list-object-request bucket "logs/" "" "" (int 1000))
; (def aws-s3-object-listing    (s4/list-objects aws-s3-connection list-object-request)
; (def aws-s3-object-summaries  (s4/get-object-summaries aws-s3-object-listing)
;
;
; https://github.com/clojure-cookbook/clojure-cookbook/blob/master/01_primitive-data/1-32_gregorian-lazyseq.asciidoc
(defn daily-from-epoch [start-year start-month]
  (let [  ^SimpleDateFormat   format-string (SimpleDateFormat. "yyyy-MM-dd")
          ^GregorianCalendar  start-date    (GregorianCalendar. start-year start-month 0 0 0) ]
    (map #(.format format-string (.getTime  %)) (repeatedly
      (fn []
        (.add start-date java.util.Calendar/DAY_OF_YEAR 1)
        (.clone start-date)))))) 

(defn avro-schema
  "Returns an Avro schema"
  ^Schema$RecordSchema [^String schema-file]
  (avro/parse-schema (json/parse-stream (io/reader schema-file))))

(defn parse-line [line parts pattern]
  (let [match (drop 1 (re-find pattern line))]
    (apply hash-map (interleave parts match))))

(defn process-files
  "Iterates over all entries " 
  [s3-log-pattern schema-file days aws-s3-connection bucket prefix]
  (log/info days s3-log-pattern schema-file days aws-s3-connection bucket prefix)
  (let [  
          s3-log-avro-schema-json   (json/parse-stream (io/reader schema-file))
          s3-log-avro-schema-fields (map keyword (map #(get-in % ["name"]) 
                                      (get-in s3-log-avro-schema-json ["fields"])))
          s3-log-avro-schema        (avro-schema schema-file)
          _                         (log/info (type s3-log-avro-schema))

          ]

  ;(first all-files-for-a-day)
  ;{:bucket-name "www.streambrightdata.com", :e-tag "21b23f0b74d49f9ee619cf1899fc246b", 
  ; :key "logs/2015-12-25-00-23-17-8AC95FEBE0374F7B", 
  ; :last-modified #inst "2015-12-25T00:23:18.000-00:00",
  ; :owner #object[com.amazonaws.services.s3.model.Owner 0x6aeb82e1 "S3Owner [name=s3-log-service,id=3272ee65a908a7677109fedda345db8d9554ba26398b2ca10581de88777e2b61]"], 
  ; :size 395, :storage-class "STANDARD"}
  
    ;"2015-12-12" "2015-12-13" "2015-12-14"
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
                  avro-entries      (map #(parse-line % s3-log-avro-schema-fields s3-log-pattern) object-content)
                ]
            (doseq [avro-entry avro-entries]
              (log/info "adding entry")

              ;(clean up avro-entry)

              ;{:turn_around_time "22", :http_status "200", 
              ;:key "logs/2015-12-08-03-40-14-00BF9041E4921944", 
              ;:remote_ip "10.144.194.145", :total_time "49", :version_id "-", 
              ;:bytes_sent "-", :time "08/Dec/2015:03:40:14 +0000", 
              ;:operation "REST.PUT.OBJECT", :user_agent "aws-internal/3", :error_code "-", 
              ;:request_uri "PUT /www.streambrightdata.com/logs/2015-12-08-03-40-14-00BF9041E4921944 HTTP/1.1", 
              ;:referrer "-", 
              ;:requester "3272ee65a908a7677109fedda345db8d9554ba26398b2ca10581de88777e2b61",
              ;:bucket_owner "f2b98d9dd4d99c07ad532dc8a7daf9639e5362e084f9d3ac74f67ed516040f03", 
              ;:object_size "396", :bucket "www.streambrightdata.com", 
              ;:request_id "5B49BD4E3BE026CD"}

              (.append avro-file avro-entry)
            )))
  
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
        credentials               (:ok (get-credentials (get-in config [:ok :aws :credentials])))
        bucket                    (name (get-in config [:ok :aws :s3 :bucket]))
        aws-basic-cred            (s4/create-basic-aws-credentials credentials)
        aws-s3-connection         (s4/connect-with-basic-credentials aws-basic-cred)
        days                      (take 10 (daily-from-epoch 2015 11))
        schema-file               "schema/amazon-log.avsc"
        s3-log-pattern            #"(\S+) ([a-z0-9][a-z0-9-.]+) \[(.*\+.*)\] (\b(?:\d{1,3}\.){3}\d{1,3}\b) (\S+) (\S+) (\S+) (\S+) \"(\w+\ \S+ \S+)\" (\d+|\-) (\S+) (\d+|\-) (\d+|\-) (\d+|\-) (\d+|\-) \"(\S+)\" \"(\S+)\" (\S+)"
        _                         (log/info s3-log-pattern)
        ]
        (process-files s3-log-pattern schema-file days aws-s3-connection bucket "logs/")

    (log/info "init :: stop")))


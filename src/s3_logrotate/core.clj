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
    [s3-logrotate.cli         :as   cli   ]
    [s3-logrotate.s3api       :as   s4    ]
    [clojure.tools.logging    :as   log   ]
    [clojure.edn              :as   edn   ]
    [amazonica.aws.s3         :as   s3    ]
    [amazonica.aws.s3transfer :as   s3t   ]
  )
  (:import
    [java.io              File BufferedReader               ]
    [clojure.lang         PersistentArrayMap PersistentList ]
    [java.util            Date GregorianCalendar            ]
    [java.text            SimpleDateFormat                  ]
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

(defn -main
  [& args]
  (let [
        ;; dealing with cli & config file
        cli-options-parsed                          (cli/process-cli args cli/cli-options)
        {:keys [options arguments errors summary]}  cli-options-parsed
        config                                      (cli/process-config (:config options))

        ;; wrapping out variables need for further execution
        env                       (keyword (:env options))
        credentials               (:ok (get-credentials (get-in config [:ok :aws :credentials])))
        bucket                    (name (get-in config [:ok :aws :s3 :bucket]))
        aws-basic-cred            (s4/create-basic-aws-credentials credentials)
        aws-s3-connection         (s4/connect-with-basic-credentials aws-basic-cred)
        ;[ ^String bucket-name ^String prefix ^String marker ^String delimiter ^Integer max-keys]
        ;list-object-request       (s4/create-list-object-request bucket "logs/2015" "" "" (int 1000))
        ;aws-s3-object-listing     (s4/list-objects aws-s3-connection list-object-request)
        ;aws-s3-object-summaries   (s4/get-object-summaries aws-s3-object-listing)
        days                      (take 10 (daily-from-epoch 2015 11))
        ]

    (doseq 
      [day days]
      (let [ all-files-for-a-day (s4/list-all-files-eager 
                                    aws-s3-connection   ;connection
                                    bucket              ;bucket-name
                                    (str "logs/" day)   ;path eg. "logs/2015-01-01"
                                    ""                  ;marker
                                    ""                  ;delimiter
                                    (int 1000)          ;max-keys
                                    '())                ;acc
          ]
      (log/info day)
      (doseq [file all-files-for-a-day]
        (let [key             (:key file)
              object          (s4/get-object aws-s3-connection bucket key)
              object-content  (s4/get-object-content object)
              object-lines    (s4/get-lines object-content)
          ]
          (log/info key)
          (log/info object-lines)
          (s4/close-object object)))))

    (log/info "init :: stop")))


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
  s3-logrotate.s3api
  (:require
    [clojure.tools.logging    :as   log   ]
    [clojure.java.io          :as   io    ]
  )
  (:import

    [com.amazonaws.services.s3
              AmazonS3Client]

    [com.amazonaws.services.s3.model
              ListObjectsRequest
              ObjectListing
              S3ObjectSummary
              S3Object
              S3ObjectInputStream]

    [com.amazonaws
             AmazonServiceException
             ClientConfiguration]

    [com.amazonaws.auth
             AWSCredentials
             AWSCredentialsProvider
             BasicAWSCredentials
             BasicSessionCredentials
             DefaultAWSCredentialsProviderChain]

    com.amazonaws.auth.profile.ProfileCredentialsProvider
    
    [com.amazonaws.regions
             Region
             Regions]

    [java.io        File BufferedReader                   ]
    [java.util      ArrayList                             ]
    [clojure.lang   PersistentArrayMap PersistentList     ]
  ))

; 
; http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html
;

(defn create-basic-aws-credentials
  "Takes a hashmap with AWS security credentials and creates a BasicAWSCredentials" 
  ^BasicAWSCredentials [^PersistentArrayMap credentials]
  ;guard function with both keys checked if present
  (BasicAWSCredentials.
        (:aws_access_key_id credentials)
        (:aws_secret_access_key credentials)))

(defn connect-with-basic-credentials 
  "Connecting to S3 only with credentials"
  ^AmazonS3Client [^BasicAWSCredentials basic-aws-credentials]
  (AmazonS3Client. basic-aws-credentials))

;could not find disconnect method
(defn disconnect [] "ok")

(defn create-list-object-request
  "Takes a set of imputs and returns an ListObjectsRequest"
  ;no safeguards for now nil checking 
  ^ListObjectsRequest [ ^String   bucket-name ^String prefix 
                        ^String   marker      ^String delimiter 
                        ^Integer  max-keys ]
  (ListObjectsRequest. bucket-name prefix marker delimiter max-keys))

(defn list-objects
  "Returns a lazy sequence of all of the objects"
  ^ObjectListing [  ^AmazonS3Client     amazon-s3-client 
                    ^ListObjectsRequest list-object-request ]
  (.listObjects amazon-s3-client list-object-request))

(defn get-object-summaries
  "Returns object summaries for this object-listing" 
  [^ObjectListing object-listing]
  (.getObjectSummaries object-listing))

(defn get-next-marker
  "Returns the next marker for this object-listing"
  [^ObjectListing object-listing]
  (.getNextMarker object-listing))

(defn get-bucket-name
  "Returns the bucket name for this object-listing"
  [^ObjectListing object-listing]
  (.getBucketName object-listing))

(defn is-truncated?
  "Returns true or false"
  [^ObjectListing object-listing]
  (.isTruncated object-listing))

(defn get-max-keys
  "Returns the max keys used for this object-listing"
  [^ObjectListing object-listing]
  (.getMaxKeys object-listing))

(defn get-object 
  "Gets S3 object"
  [^AmazonS3Client amazon-s3-client ^String bucket-name ^String s3-key]
  (.getObject amazon-s3-client bucket-name s3-key))

(defn get-object-content-unsafe  
  "Gets the input stream containing the contents of this object.
  This function returns an InputStream, holding onto it result in resource pool
  exhaustion"
  ^S3ObjectInputStream [^S3Object object]
  (.getObjectContent object))

(defn close-object
  "Closes object"
  [^S3Object object]
  (.close object))

(defn get-object-content-safe
  ""
  [^S3Object object]
  (let [
          ^PersistentVector return  (with-open 
                                      [rdr (io/reader (get-object-content-unsafe object))] 
                                       (reduce conj () (line-seq rdr)))
                            _       (close-object object) ]
    ; returning a vector of lines
    return))

(defn get-s3-object-summary-clj
  "Returns a Clojure representation of a S3ObjectSummary"
  [^S3ObjectSummary s3-object-summary]
  { :bucket-name    (.getBucketName     s3-object-summary)
    :e-tag          (.getETag           s3-object-summary)
    :key            (.getKey            s3-object-summary) 
    :last-modified  (.getLastModified   s3-object-summary)
    :owner          (.getOwner          s3-object-summary)
    :size           (.getSize           s3-object-summary)
    :storage-class  (.getStorageClass   s3-object-summary) })

(defn list-all-files-eager
  "Returns a lazy-sequence of the items in a bucket or bucket/folder  "
  [ ^AmazonS3Client amazon-s3-client ^String bucket-name ^String prefix 
    ^String marker ^String delimiter ^Integer max-keys ^PersistentList acc]
  (log/debug bucket-name acc)
  (let [  ^ListObjectsRequest list-object-request (create-list-object-request 
                                                    bucket-name 
                                                    prefix marker 
                                                    delimiter 
                                                    max-keys)
          ^ObjectListing object-listing  (list-objects amazon-s3-client list-object-request)]
    (if-not (is-truncated? object-listing)
      ; return
      (flatten (concat acc (map get-s3-object-summary-clj 
                      (get-object-summaries object-listing))))
      ; recur with the new request                           ; 
      (recur  amazon-s3-client
              bucket-name 
              prefix 
              (get-next-marker object-listing) 
              delimiter 
              max-keys 
              (conj acc (map get-s3-object-summary-clj 
                          (get-object-summaries object-listing)))))))


;end

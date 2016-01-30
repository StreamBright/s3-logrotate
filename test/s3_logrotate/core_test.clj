(ns s3-logrotate.core-test
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [s3-logrotate.core :refer :all]
            [clojure.tools.logging  :as   log   ]
            ;[s3-logrotate.cli :as :cli]
            ))

(def sort-idempotent-prop
  (prop/for-all [v (gen/vector gen/int)]
    (= (sort v) (sort (sort v)))))

(log/info (tc/quick-check 100 sort-idempotent-prop))




(ns sqlite4clj.test-common
  (:require
   [clojure.java.io :as io]
   [sqlite4clj.core :as d]))

(def test-db-path "test-data/test.db")

(defn test-fixture [f]
  (let [test-dir (io/file "test-data")]
    (.mkdirs test-dir)
    (when (.exists test-dir)
      (doseq [file (.listFiles test-dir)]
        (.delete file))))
  (f)
  (let [test-dir (io/file "test-data")]
    (when (.exists test-dir)
      (doseq [file (.listFiles test-dir)]
        (.delete file))
      (.delete test-dir))))

(defmacro with-db
  "Test helper macro that manages database lifecycle.
   Automatically closes writer and reader pools in finally block."
  {:clj-kondo/lint-as 'clojure.core/with-open}
  [[db-sym init-expr] & body]
  `(let [~db-sym ~init-expr]
     (try
       ~@body
       (finally
         ((:close (:writer ~db-sym)))
         ((:close (:reader ~db-sym)))))))

(defn test-db []
  (d/init-db! test-db-path {:pool-size 2}))

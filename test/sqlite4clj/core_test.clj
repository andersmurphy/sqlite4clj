(ns sqlite4clj.core-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [sqlite4clj.core :as d]
   [sqlite4clj.test-common :refer [test-db test-fixture with-db]]))

(use-fixtures :once test-fixture)

(deftest pool-objects-are-references
  (testing "Ensure pool objects are references to connections."
    (with-db [db (test-db)]
      (d/with-conn [conn (:writer db)]
        (is (identical? conn (first (:connections (db :writer)))))))))

(deftest improper-access-of-connections-or-prepared-statements
  (testing "Does not cause segfault when connections are accessed from
            separate threads."

    (with-db [db (test-db)]

      (d/q (:writer db)
        ["CREATE TABLE IF NOT EXISTS foo(id INT PRIMARY KEY, data BLOB)"])

      (d/with-write-tx [tx (:writer db)]
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (future (d/q tx ["select sum(data) from foo"])
                (d/q tx ["select sum(data) from foo"])
                (d/q tx ["select sum(data) from foo"])
                (d/q tx ["select sum(data) from foo"])
                (d/q tx ["select sum(data) from foo"])
                (d/q tx ["select sum(data) from foo"]))
        (d/q tx ["select sum(data) from foo"])
        (d/q tx ["select sum(data) from foo"])
        (d/q tx ["select sum(data) from foo"])
        (d/q tx ["select sum(data) from foo"])
        (d/q tx ["select sum(data) from foo"])
        (d/q tx ["select sum(data) from foo"])))

    ;; Didn't crash
    (is (= true true))))

(deftest transactions-handle-sqlite-exceptions

  (testing "Write sransactions rollback when a sqlite error happens.
            This also makes sure the connection is returned."

    (with-db [db (test-db)]
      (d/q (:writer db)
        ["CREATE TABLE IF NOT EXISTS bar(id INT PRIMARY KEY, data BLOB)"])

      (try
        (d/with-write-tx [tx (:writer db)]
          (d/q tx ["select count(*) from bar"])
          (d/q tx ["INSERT INTO bar (id, data) VALUES (?, ?)" 1 (bigdec 10.0)])
          (d/q tx ["INSERT INTO bar (id, data) VALUES (?, ?)" 1 (bigdec 10.0)]))
        (catch Throwable _))
      (try
        (d/with-write-tx [tx (:writer db)]
          (d/q tx ["select count(*) from bar"])
          (d/q tx ["INSERT INTO bar (id, data) VALUES (?, ?)" 1 {:a 4}])
          (d/q tx ["INSERT INTO bar (id, data) VALUES (?, ?)" 1 {:a 4}]))
        (catch Throwable _))

      (is (= 0 (first (d/q (:reader db) ["select count(*) from bar"])))))))

(deftest transactions-handle-java-exceptions

  (testing "Write transactions rollback when a java excepions happens.
            This also makes sure the connection is returned."

    (with-db [db (test-db)]
      (d/q (:writer db)
        ["CREATE TABLE IF NOT EXISTS bar(id INT PRIMARY KEY, data BLOB)"])

      (try
        (d/with-write-tx [tx (:writer db)]
          (d/q tx ["select count(*) from bar"])
          (d/q tx ["INSERT INTO bar (id, data) VALUES (?, ?)" 1 (bigdec 10.0)])
          (throw (ex-info "non-sql-exception" {})))
        (catch Throwable _))
      (try
        (d/with-write-tx [tx (:writer db)]
          (d/q tx ["select count(*) from bar"])
          (d/q tx ["INSERT INTO bar (id, data) VALUES (?, ?)" 2 {:a 4}])
          (throw (ex-info "non-sql-exception" {})))
        (catch Throwable _))

      (is (= 0 (first (d/q (:reader db) ["select count(*) from bar"])))))))

(deftest encoding-edn

  (testing "Handles *print-length* being set in user land."

    (with-db [db (test-db)]
      (d/q (:writer db)
        ["CREATE TABLE IF NOT EXISTS encoding(id INT PRIMARY KEY, data BLOB)"])

      (binding [*print-length* 1]
        (d/q (:writer db)
          ["INSERT INTO encoding (id, data) VALUES (?, ?)" 1
           {:id 0, :email "bob@foobar.com", :username "bob"}]))

      (is (= [{:id 0, :email "bob@foobar.com", :username "bob"}]
             (d/q (:reader db)
               ["select data from encoding where id = 1"]))))))

(deftest encoding-raw-byte-arrays
  (testing "Raw byte arrays round-trip unchanged and keep the prefixed blob format."
    (with-db [db (test-db)]
      (d/q (:writer db)
        ["CREATE TABLE IF NOT EXISTS raw_bytes(id INT PRIMARY KEY, data BLOB)"])
      (let [payload (byte-array [1 2 3 4 5])]
        (d/q (:writer db)
          ["INSERT INTO raw_bytes (id, data) VALUES (?, ?)" 1 payload])
        (let [[stored-bytes] (d/q (:reader db)
                              ["select data from raw_bytes where id = 1"])
              [stored-length] (d/q (:reader db)
                               ["select length(data) from raw_bytes where id = 1"])]
          (is (= (seq payload) (seq stored-bytes)))
          (is (= (inc (alength payload)) stored-length)))))))

(deftest limits-can-be-set-at-db-init
  (testing "Attached limit prevents connections from attaching."
    (with-db [db (test-db {:limits {:attached 0}})]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
            #"too many attached databases - max 0"
            (d/q (:writer db)
              ["ATTACH DATABASE 'test-data/test.db'AS other"]))))))

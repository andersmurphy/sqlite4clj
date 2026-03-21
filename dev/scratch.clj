(ns scratch
  (:require [sqlite4clj.core :as d]
            [sqlite4clj.batch :as b]
            [clj-async-profiler.core :as prof]
            [fast-edn.core :as edn])
  (:import (java.util.concurrent Executors)))

;; Make futures use virtual threads
(set-agent-send-executor!
  (Executors/newVirtualThreadPerTaskExecutor))

(set-agent-send-off-executor!
  (Executors/newVirtualThreadPerTaskExecutor))

(defonce db
  (d/init-db! "database.db"
    {:pool-size 4
     :pragma    {:foreign_keys false}}))

(def reader  (db :reader))
(def writer (db :writer))

(comment

  (d/q reader ["pragma mmap_size;"])
  (d/q reader ["pragma cache_size;"])

  (->>
    (d/q reader ["PRAGMA compile_options;"])
    (filter #(re-find #"MAX_" %)))


  (d/with-read-tx [tx reader]
    (d/q tx ["pragma foreign_keys;"])
    (d/q tx ["pragma foreign_keys;"]))

  (d/q reader ["some malformed sqlite"])

  (d/q reader ["pragma wal;" "pragma wal;"])

  (d/q reader ["pragma query_only"])
  (d/q writer ["pragma query_only"])

  (count (:conn-pool reader))

  (d/optimize-db reader)
  (d/optimize-db writer))

(comment  
  (d/q writer
    ["CREATE TABLE IF NOT EXISTS session(id TEXT PRIMARY KEY, data BLOB) WITHOUT ROWID"])
  
  (def sid "someid")

  (defn test-case [db sid]
    (let [[data] (d/q db ["SELECT data FROM session WHERE id = ?" sid])]
      (if data
        (d/q db ["UPDATE session SET data = ? WHERE id = ?" data sid])
        (d/q db ["INSERT INTO session (id, data) VALUES (?, ?)" sid {:a 2}]))))

  (do
    (test-case writer sid)
    (test-case writer sid)
    (d/q writer ["SELECT data FROM session WHERE id = ?" sid]))

  (d/with-write-tx [tx writer]
    (test-case tx sid)
    (test-case tx sid)
    (d/q tx ["SELECT data FROM session WHERE id = ?" sid]))
  
  (d/q writer ["UPDATE session SET data = ? WHERE id = ?" [2] sid])

  (d/with-write-tx [tx writer]
    (d/q tx ["SAVEPOINT thunk-tx;"])
    (try
      (d/q tx ["UPDATE session SET data = ? WHERE id = ?" {:a 6} sid])
      (d/q tx ["INSERT INTO session (id, data) VALUES (?, ?);" sid 1])
      (catch Throwable _
        (d/q tx ["ROLLBACK TO thunk-tx;"])))
    (d/q tx ["RElEASE thunk-tx;"])
    ;; (d/q tx ["UPDATE session SET data = ? WHERE id = ?" {:a 4} sid])
    (d/q tx ["SELECT data FROM session WHERE id = ?" sid])))

(comment
  (d/q writer
    ["CREATE TABLE IF NOT EXISTS blobby(id TEXT PRIMARY KEY, data BLOB) WITHOUT ROWID"])

  (d/q writer
    ["INSERT INTO blobby (id, data) VALUES (?, ?)"
     "blob-test4"
     (String/.getBytes "hello there")])

  (String/.getBytes "blob-test1")

  (count (String/.getBytes "hello there" "UTF-8"))

  (type (first (d/q reader ["SELECT data FROM blobby WHERE id = ?" "blob-test2"])))
  (d/q reader ["SELECT id, data FROM blobby WHERE id = ?" "blob-test4"])

  (d/q writer
    ["INSERT INTO blobby (id, data) VALUES (?, ?)"
     (str (random-uuid))
     ["some encoded blob"]])

  (d/q reader ["SELECT id, data FROM blobby"])
  (d/q reader ["SELECT id FROM blobby"])

  (d/q reader ["SELECT id, data FROM blobby where id = 3"])

  (d/q reader ["SELECT id, data FROM blobby"]
    {:result-set-fn d/qualified-keyword-result-set-fn})

  (d/q reader ["SELECT id FROM blobby"]
    {:result-set-fn d/qualified-keyword-result-set-fn})

  (d/q reader ["SELECT id, data FROM blobby WHERE id = ?" "blob-test5"])
  )

(comment ;; encoded blob vs regular table

  (d/q writer
    ["CREATE TABLE IF NOT EXISTS test_1(id INT PRIMARY KEY, data BLOB)"])
  (run!
    (fn [id]
      (d/q writer
        ["INSERT INTO test_1 (id, data) VALUES (?, ?)"
         id
         {:id       id :email "bob@foobar.com"
          :username "escalibardarian"}]))
    (range 0 100))

  (d/q writer
    ["CREATE TABLE IF NOT EXISTS test_2(id INT PRIMARY KEY, email TEXT , username TEXT)"])
  (run!
    (fn [id]
      (d/q writer
        ["INSERT INTO test_2 (id, email, username) VALUES (?, ?, ?)"
         id "bob@foobar.com" "escalibardarian"]))
    (range 0 100))

  (d/q writer
    ["CREATE TABLE IF NOT EXISTS test_3(id INT PRIMARY KEY, data TEXT)"])
  (run!
    (fn [id]
      (d/q writer
        ["INSERT INTO test_3 (id, data) VALUES (?, ?)"
         id (str {:id       id :email "bob@foobar.com"
                  :username "escalibardarian"})]))
    (range 0 100))

  (d/q writer
    ["CREATE TABLE IF NOT EXISTS test_4(id INT PRIMARY KEY, data BLOB)"])
  (run!
    (fn [id]
      (d/q writer
        ["INSERT INTO test_4 (id, data) VALUES (?, ?)"
         id (String/.getBytes
              (str {:id       id :email "bob@foobar.com"
                    :username "escalibardarian"}))]))
    (range 0 100))

  (user/bench
    ;; 1.206098 ms
    ;; 238.893356 µs
    ;; 61.222818 µs
    ;; 58.882514 µs
    ;; 54.009261 µs
    ;; 52.006687 µs
    ;; 51.460267 µs
    (d/q reader ["select data from test_1"]))

  (user/bench
    (d/q reader ["select id, data from test_1"]))

  (user/bench
    (d/q reader ["select id, data from test_1"]
      {:result-set-fn d/qualified-keyword-result-set-fn}))

  (d/q reader ["select id as foo from test_1"]
    {:result-set-fn d/qualified-keyword-result-set-fn})

  (user/bench ;; 20.496617 µs
    (->> (d/q reader ["select * from test_2"])
      (mapv (fn [[id email username]]
              {:id id :email email :username username}))))

  ;; Raw get bytes
  (user/bench
    ;; 18.753884 µs
    (->> (d/q reader ["select * from test_4"])))

  ;; Raw get text
  (user/bench ;; 12.411071 µs
    (d/q reader ["select data from test_3"]))

  (user/bench ;; 43.204357 µs
    (->> (d/q reader ["select data from test_3"])
      (mapv edn/read-string)))

  ;; Concat madness
  (user/bench ;; 30.865782 µs
    (->> (d/q reader ["select concat('[', group_concat(data), ']') from test_3 limit 1"])
      first
      edn/read-string))

  )

(comment ;; Profiling
  (prof/start)
  (prof/stop)
  (prof/serve-ui 7777)
  ;; (clojure.java.browse/browse-url "http://localhost:7777/")
  )

(comment
  (defn double [v] (* 2 v))
  (d/create-function db "double" double {:deterministic? true})
  (d/q reader ["SELECT double(5)"])
  ;; => [10]
  (d/q writer ["SELECT double(5)"])
  ;; => [10]

  (d/create-function db "double_var" #'double {:deterministic? true})
  (d/q reader ["SELECT double_var(5)"])
  ;; => [10]
  ;; now change and re-eval double and see magic!
  ;;
  )

(comment ;; batch
  (defonce tx!
    (b/async-batcher-init! db
      {:max-batch-size 10000
       :batch-fn
       (fn batch-fn [writer batch]
         (d/with-write-tx [db writer]
           (run! (fn [thunk] (thunk db)) batch)))}))

  (->>
    (mapv (fn [n]
            (tx! (fn [tx] (d/q tx ["select * from bar limit ?" n]))))
      [1 2 3 4])
    (mapv deref))

  
  )

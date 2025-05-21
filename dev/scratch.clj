(ns scratch
  (:require [sqlite4clj.core :as d])
  (:import
   (java.util.concurrent Executors)))

;; Make futures use virtual threads
(set-agent-send-executor!
  (Executors/newVirtualThreadPerTaskExecutor))

(set-agent-send-off-executor!
  (Executors/newVirtualThreadPerTaskExecutor))

(defonce db
  (d/init-db! "database.db"
    {:read-only true
     :pool-size 4
     :pragma    {:foreign_keys false}}))

(comment
  (d/q db ["pragma foreign_keys;"])

  (d/with-read-tx [tx db]
    (d/q tx ["pragma foreign_keys;"])
    (d/q tx ["pragma foreign_keys;"]))

  (d/q db ["some malformed sqlite"])

  ;; This cause hard crash
  (d/q db ["pragma wal;" "pragma wal;"])

  (time
    (->> (d/q db ["SELECT chunk_id, state FROM cell WHERE chunk_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                  1978 3955 5932 1979 3956 5933 1980 3957 5934])
      (mapv
        (fn [[chunk-id state]] {:chunk-id chunk-id :state state}))))


  ;; This causes segfault
  (d/q db ["INSERT INTO session (id, checks) VALUES (?, ?)"
           "foo"
           1])

  (d/q db ["INSERT INTO session (id, checks) VALUES ('foo', 1)"])

  (time
    (->> (mapv
           (fn [n]
             (future
               (d/q db
                 ["SELECT chunk_id, JSON_GROUP_ARRAY(state) AS chunk_cells FROM cell WHERE chunk_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?)  GROUP BY chunk_id" 1978 3955 5932 1979 3956 5933 1980 3957 5934])))
           (range 0 2000))
      (run! (fn [x] @x))))

  (time
    (->> (mapv
           (fn [n]
             (future
               (do
                 (d/q db ["SELECT chunk_id, state FROM cell WHERE chunk_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                          1978 3955 5932 1979 3956 5933 1980 3957 5934])
                 nil)))
           (range 0 2000))
      (run! (fn [x] @x))))

  (user/bench ;; Execution time mean : 545.401696 µs
    (d/q db
      ["SELECT chunk_id, state FROM cell WHERE chunk_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?)"
       1978 3955 5932 1979 3956 5933 1980 3957 5934]))

  (user/bench ;; Execution time mean : 545.401696 µs
    (->> (d/q db
      ["SELECT chunk_id, state FROM cell WHERE chunk_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?)"
       1978 3955 5932 1979 3956 5933 1980 3957 5934])
      (mapv (fn [[chunk-id state]] {:chunk-id chunk-id :state state}))))

  (user/bench ;; Execution time mean : 153.307535 µs
    (d/q db
      ["SELECT chunk_id, JSON_GROUP_ARRAY(state) AS chunk_cells FROM cell WHERE chunk_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?)  GROUP BY chunk_id" 1978 3955 5932 1979 3956 5933 1980 3957 5934])))

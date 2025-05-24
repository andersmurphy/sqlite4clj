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

(defonce write-db
  (d/init-db! "database.db"
    {:pool-size 1
     :pragma    {:foreign_keys false}}))

(comment
  (d/q db ["pragma mmap_size;"])
  (d/q db ["pragma cache_size;"])
  (d/q db ["journal_size_limit;"])
  (d/q db ["pragma mmap_size = 2147418110;"])

  (->>
    (d/q db ["PRAGMA compile_options;"])
    (map first)
    (filter #(re-find #"MAX_" %)))
  

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

  (user/bench
    (->> (mapv
           (fn [n]
             (future
               (do
                 (d/q db ["SELECT chunk_id, state FROM cell WHERE chunk_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                          (+ n 1978)
                          (+ n 3955)
                          (+ n 5932)
                          (+ n 1979) 
                          (+ n 956)
                          (+ n 5933)
                          (+ n 1980)
                          (+ n 3957)
                          (+ n 5934)])
                 nil)))
           (range 0 4000))
      (run! (fn [x] @x))))
  
  (user/bench
    ;; Execution time mean : 455.139383 µs
    ;; Execution time mean : 345.804480 µs
    (d/q db
      ["SELECT chunk_id, state FROM cell WHERE chunk_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?)"
       1978 3955 5932 1979 3956 5933 1980 3957 5934]))

  (user/bench
    (->> (d/q db
      ["SELECT chunk_id, state FROM cell WHERE chunk_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?)"
       1978 3955 5932 1979 3956 5933 1980 3957 5934])
      (mapv (fn [[chunk-id state]] {:chunk-id chunk-id :state state}))))

  (user/bench
    (d/q db
      ["SELECT chunk_id, JSON_GROUP_ARRAY(state) AS chunk_cells FROM cell WHERE chunk_id IN (?, ?, ?, ?, ?, ?, ?, ?, ?)  GROUP BY chunk_id" 1978 3955 5932 1979 3956 5933 1980 3957 5934]))

  (d/q write-db ["vacuum"])
  (d/q write-db ["optimize"])

  (+ 3 4)

  )

(ns indexing-edn-blob
  (:require [sqlite4clj.core :as d]))

(defonce db
  (d/init-db! "index-edn.db"
    {:pool-size 4
     :pragma    {:foreign_keys false}}))

(def reader  (db :reader))
(def writer (db :writer))

(defn entity-type [blob]
  (-> blob :type))

(comment
  (d/q writer
    ["CREATE TABLE IF NOT EXISTS entity(id TEXT PRIMARY KEY, data BLOB) WITHOUT ROWID"])

  (d/create-function db "entity_type" #'entity-type {:deterministic? true})

  (d/q writer
    ["CREATE INDEX IF NOT EXISTS entity_type_idx ON entity(entity_type(data))
    WHERE entity_type(data) IS NOT NULL"])

  (def type ["foo" "bar" "bam" "baz"])

  (d/q writer
    ["INSERT INTO entity (id, data) VALUES (?, ?)"
     (str (random-uuid))
     {:type (rand-nth type) :a (rand-int 10) :b (rand-int 10)}])

  (d/q writer
    ["INSERT INTO entity (id, data) VALUES (?, ?)"
     (str (random-uuid))
     {:a (rand-int 10) :b (rand-int 10)}])

  (d/q reader
    ["select * from entity where entity_type(data) = ?" "foo"])

  (d/q reader ["select * from entity"])

  ;; Check index is being used
  (d/q reader
    ["explain query plan select * from entity where entity_type(data) = ?" "foo"])
  ;; -> [[3 0 62 "SEARCH entity USING INDEX entity_type_idx (<expr>=?)"]]

  (d/q reader
    ["explain query plan select * from entity order by entity_type(data)"])
  ;; -> [[3 0 215 "SCAN entity"] [12 0 0 "USE TEMP B-TREE FOR ORDER BY"]]

  (d/q reader
    ["explain query plan select * from entity 
where entity_type(data) is not null
order by entity_type(data)"])
  ;; -> [[4 0 215 "SCAN entity USING INDEX entity_type_idx"]]

  (d/q reader
    ["select * from entity 
      where entity_type(data) is not null
      order by entity_type(data)"])


  )









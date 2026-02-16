(ns sqlite4clj.bench
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [criterium.core :as criterium]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as rs]
   [sqlite4clj.core :as d])
  (:import
   (java.nio.charset StandardCharsets)
   (java.sql Connection DriverManager PreparedStatement ResultSet Statement)))

(def benchmark-dir "target/benchmarks")
(def sqlite4clj-db-path (str benchmark-dir "/sqlite4clj-bench.db"))
(def jdbc-db-path (str benchmark-dir "/jdbc-bench.db"))
(def seed-row-count 5000)

(defmacro run-case [label expr]
  `(do
     (println (str "\n### " ~label))
     (criterium/quick-bench ~expr)))

(defn edn-bytes [v]
  (String/.getBytes (pr-str v) StandardCharsets/UTF_8))

(defn bytes->edn [bs]
  (edn/read-string (String. ^bytes bs StandardCharsets/UTF_8)))

(defn jdbc-url [db-path]
  (str "jdbc:sqlite:" db-path))

(defn reset-benchmark-dir! []
  (let [dir (io/file benchmark-dir)]
    (when (.exists dir)
      (run! #(.delete ^java.io.File %)
        (reverse (file-seq dir))))
    (.mkdirs dir)))

(defn init-sqlite4clj-db! []
  (let [db (d/init-db! sqlite4clj-db-path {:pool-size 2})
        writer (:writer db)]
    (d/q writer
      ["CREATE TABLE IF NOT EXISTS bench_items(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"])
    (d/q writer
      ["CREATE TABLE IF NOT EXISTS bench_docs(id INTEGER PRIMARY KEY, doc BLOB NOT NULL)"])
    (d/q writer
      ["CREATE TABLE IF NOT EXISTS bench_probe(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"])

    (d/with-write-tx [tx writer]
      (run! (fn [id]
              (d/q tx
                ["INSERT INTO bench_items (id, payload) VALUES (?, ?)"
                 id
                 (str "payload-" id)]))
        (range 1 (inc seed-row-count))))

    (d/with-write-tx [tx writer]
      (run! (fn [id]
              (d/q tx
                ["INSERT INTO bench_docs (id, doc) VALUES (?, ?)"
                 id
                 {:id id :payload (str "payload-" id)}]))
        (range 1 (inc seed-row-count))))
    db))

(defn init-jdbc-db! []
  (Class/forName "org.sqlite.JDBC")
  (with-open [conn (DriverManager/getConnection (jdbc-url jdbc-db-path))
              stmt (Connection/.createStatement conn)]
    (Statement/.execute stmt "PRAGMA journal_mode=WAL")
    (Statement/.execute stmt "PRAGMA synchronous=NORMAL")
    (Statement/.execute stmt "PRAGMA foreign_keys=OFF")
    (Statement/.execute stmt
      "CREATE TABLE IF NOT EXISTS bench_items(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
    (Statement/.execute stmt
      "CREATE TABLE IF NOT EXISTS bench_docs(id INTEGER PRIMARY KEY, doc BLOB NOT NULL)")
    (Statement/.execute stmt
      "CREATE TABLE IF NOT EXISTS bench_probe(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
    (Connection/.setAutoCommit conn false)
    (try
      (with-open [item-ps (Connection/.prepareStatement conn
                            "INSERT INTO bench_items (id, payload) VALUES (?, ?)")
                  doc-ps  (Connection/.prepareStatement conn
                            "INSERT INTO bench_docs (id, doc) VALUES (?, ?)")]
        (run! (fn [id]
                (PreparedStatement/.setInt item-ps 1 id)
                (PreparedStatement/.setString item-ps 2 (str "payload-" id))
                (PreparedStatement/.addBatch item-ps)
                (PreparedStatement/.setInt doc-ps 1 id)
                (PreparedStatement/.setBytes doc-ps 2
                  (edn-bytes {:id id :payload (str "payload-" id)}))
                (PreparedStatement/.addBatch doc-ps))
          (range 1 (inc seed-row-count)))
        (PreparedStatement/.executeBatch item-ps)
        (PreparedStatement/.executeBatch doc-ps))
      (Connection/.commit conn)
      (catch Throwable t
        (Connection/.rollback conn)
        (throw t))
      (finally
        (Connection/.setAutoCommit conn true)))))

(defn init-jdbc-state! []
  (let [conn (DriverManager/getConnection (jdbc-url jdbc-db-path))]
    {:conn conn
     :read-text-ps (Connection/.prepareStatement conn
                     "SELECT payload FROM bench_items WHERE id = ?")
     :read-doc-ps  (Connection/.prepareStatement conn
                     "SELECT doc FROM bench_docs WHERE id = ?")
     :insert-ps    (Connection/.prepareStatement conn
                     "INSERT INTO bench_probe (id, payload) VALUES (?, ?)")
     :delete-ps    (Connection/.prepareStatement conn
                     "DELETE FROM bench_probe WHERE id = ?")
     :write-id     (atom seed-row-count)}))

(defn close-jdbc-state! [{:keys [conn read-text-ps read-doc-ps insert-ps delete-ps]}]
  (PreparedStatement/.close read-text-ps)
  (PreparedStatement/.close read-doc-ps)
  (PreparedStatement/.close insert-ps)
  (PreparedStatement/.close delete-ps)
  (Connection/.close conn))

(defn close-sqlite4clj-db! [db]
  ((:close (:writer db)))
  ((:close (:reader db))))

(defn sqlite4clj-read-text [reader]
  (d/q reader ["SELECT payload FROM bench_items WHERE id = ?" 2500]))

(defn sqlite4clj-read-doc [reader]
  (d/q reader ["SELECT doc FROM bench_docs WHERE id = ?" 2500]))

(defn sqlite4clj-write-probe [writer write-id]
  (d/with-write-tx [tx writer]
    (let [id (swap! write-id inc)]
      (d/q tx ["INSERT INTO bench_probe (id, payload) VALUES (?, ?)" id "probe"])
      (d/q tx ["DELETE FROM bench_probe WHERE id = ?" id]))))

(defn next-jdbc-read-text [conn]
  (jdbc/execute-one! conn
    ["SELECT payload FROM bench_items WHERE id = ?" 2500]
    {:builder-fn rs/as-unqualified-lower-maps}))

(defn next-jdbc-read-doc [conn]
  (bytes->edn
    (:doc (jdbc/execute-one! conn
            ["SELECT doc FROM bench_docs WHERE id = ?" 2500]
            {:builder-fn rs/as-unqualified-lower-maps}))))

(defn next-jdbc-write-probe [conn write-id]
  (jdbc/with-transaction [tx conn]
    (let [id (swap! write-id inc)]
      (jdbc/execute! tx ["INSERT INTO bench_probe (id, payload) VALUES (?, ?)" id "probe"])
      (jdbc/execute! tx ["DELETE FROM bench_probe WHERE id = ?" id]))))

(defn sqlite-jdbc-read-text [read-text-ps]
  (PreparedStatement/.setInt read-text-ps 1 2500)
  (with-open [rs (PreparedStatement/.executeQuery read-text-ps)]
    (when (ResultSet/.next rs)
      (ResultSet/.getString rs 1))))

(defn sqlite-jdbc-read-doc [read-doc-ps]
  (PreparedStatement/.setInt read-doc-ps 1 2500)
  (with-open [rs (PreparedStatement/.executeQuery read-doc-ps)]
    (when (ResultSet/.next rs)
      (bytes->edn (ResultSet/.getBytes rs 1)))))

(defn sqlite-jdbc-write-probe [conn insert-ps delete-ps write-id]
  (let [id (swap! write-id inc)]
    (Connection/.setAutoCommit conn false)
    (try
      (PreparedStatement/.setInt insert-ps 1 id)
      (PreparedStatement/.setString insert-ps 2 "probe")
      (PreparedStatement/.executeUpdate insert-ps)
      (PreparedStatement/.setInt delete-ps 1 id)
      (PreparedStatement/.executeUpdate delete-ps)
      (Connection/.commit conn)
      (catch Throwable t
        (Connection/.rollback conn)
        (throw t))
      (finally
        (Connection/.setAutoCommit conn true)))))

(defn run-comparison-benchmarks! [sqlite4clj-db jdbc-state]
  (let [reader (:reader sqlite4clj-db)
        writer (:writer sqlite4clj-db)
        sqlite4clj-write-id (atom seed-row-count)
        jdbc-conn (:conn jdbc-state)
        read-text-ps (:read-text-ps jdbc-state)
        read-doc-ps (:read-doc-ps jdbc-state)
        insert-ps (:insert-ps jdbc-state)
        delete-ps (:delete-ps jdbc-state)
        jdbc-write-id (:write-id jdbc-state)]

    ;; Warm query plans and statement caches once before timed runs.
    (sqlite4clj-read-text reader)
    (sqlite4clj-read-doc reader)
    (sqlite4clj-write-probe writer sqlite4clj-write-id)
    (next-jdbc-read-text jdbc-conn)
    (next-jdbc-read-doc jdbc-conn)
    (next-jdbc-write-probe jdbc-conn jdbc-write-id)
    (sqlite-jdbc-read-text read-text-ps)
    (sqlite-jdbc-read-doc read-doc-ps)
    (sqlite-jdbc-write-probe jdbc-conn insert-ps delete-ps jdbc-write-id)

    (println "\n## sqlite4clj")
    (run-case "Point read: single text column"
      (sqlite4clj-read-text reader))
    (run-case "Point read: EDN blob decoding"
      (sqlite4clj-read-doc reader))
    (run-case "Write transaction: insert + delete"
      (sqlite4clj-write-probe writer sqlite4clj-write-id))

    (println "\n## next.jdbc + sqlite-jdbc")
    (run-case "Point read: single text column"
      (next-jdbc-read-text jdbc-conn))
    (run-case "Point read: EDN blob decoding"
      (next-jdbc-read-doc jdbc-conn))
    (run-case "Write transaction: insert + delete"
      (next-jdbc-write-probe jdbc-conn jdbc-write-id))

    (println "\n## raw sqlite-jdbc (PreparedStatement)")
    (run-case "Point read: single text column"
      (sqlite-jdbc-read-text read-text-ps))
    (run-case "Point read: EDN blob decoding"
      (sqlite-jdbc-read-doc read-doc-ps))
    (run-case "Write transaction: insert + delete"
      (sqlite-jdbc-write-probe jdbc-conn insert-ps delete-ps jdbc-write-id))))

(defn print-environment! []
  (println "sqlite4clj benchmark comparison")
  (println (str "sqlite4clj db path: " sqlite4clj-db-path))
  (println (str "jdbc db path: " jdbc-db-path))
  (println (str "seed rows per table: " seed-row-count))
  (println (str "java: " (System/getProperty "java.version")))
  (println (str "os: " (System/getProperty "os.name")
                " " (System/getProperty "os.version")
                " " (System/getProperty "os.arch"))))

(defn -main [& _]
  (reset-benchmark-dir!)
  (init-jdbc-db!)
  (let [sqlite4clj-db (init-sqlite4clj-db!)
        jdbc-state (init-jdbc-state!)]
    (try
      (print-environment!)
      (run-comparison-benchmarks! sqlite4clj-db jdbc-state)
      (finally
        (close-jdbc-state! jdbc-state)
        (close-sqlite4clj-db! sqlite4clj-db)))))

(ns sqlite4clj.core
  "High level interface for using sqlite4clj including connection pool
  and prepared statement caching."
  (:require
   [sqlite4clj.api :as api]
   [clojure.string :as str]
   [clojure.core.cache.wrapped :as cache])
  (:import
   (java.util.concurrent LinkedBlockingQueue)))

(defn type->sqlite3-bind [param]
  (cond
    (integer? param) api/bind-int
    (double? param)  api/bind-double
    :else            api/bind-text))

(defn bind-params [stmt params]
  (doall
    (map-indexed
      (fn [i param]
        ;; starts at 1
        ((type->sqlite3-bind param) stmt (inc i) param)) params)))

(defn prepare-cached [{:keys [pdb stmt-cache]} [sql & params]]
  (let [stmt (cache/lookup-or-miss stmt-cache sql
               (fn [sql] (api/prepare-v2 pdb sql)))]
    (bind-params stmt params)
    stmt))

(defmacro n-cols->column-fn [stmt max-cols]
  ;; This loop unrolling makes queries 25% faster
  (mapv
    (fn [n-cols]
      `(fn [] ~(mapv (fn [n] `(api/column-text ~stmt ~n)) (range n-cols))))
    (range (inc max-cols))))

(defn column-vals-fn [stmt]
  (let [n-cols (api/column-count stmt)]
    (get (n-cols->column-fn stmt 10)
      n-cols)))

(defn- q* [stmt]
  (let [c-fn (column-vals-fn stmt)
        rs   (loop [rows (transient [])]
               (case (int (api/step stmt))
                 100 (recur (conj! rows (c-fn)))
                 101 (persistent! rows)
                 :error))]
    (api/reset stmt)
    (api/clear-bindings stmt)
    rs))

(def default-pramga
  {:cache_size   15625
   :page_size    4096
   :journal_mode "WAL"
   :synchronous  "NORMAL"
   :temp_store   "MEMORY"
   :foreign_keys true})

(defn pragma->set-pragma-query [pragma]
  (->> (merge default-pramga pragma)
    (map (fn [[k v]] (str "pragma " (name k) "=" v ";")))
    str/join))

(defn new-conn! [db-name pragma read-only]
  (let [flags           (if read-only
                          ;; SQLITE_OPEN_READONLY
                          0x00000001
                          ;; SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                          (bit-or 0x00000002 0x00000004))
        *pdb            (api/open-v2 db-name flags)
        statement-cache (cache/fifo-cache-factory {} :threshold 512)
        conn            {:pdb        *pdb
                         :stmt-cache statement-cache}]
    (q* (api/prepare-v2 *pdb (pragma->set-pragma-query pragma)))
    conn))

(defn init-db!
  [db-name & [{:keys [pool-size pragma read-only]
               :or   {pool-size 4}}]]
  (let [conns (repeatedly pool-size
                (fn [] (new-conn! db-name pragma read-only)))
        pool  (LinkedBlockingQueue/new ^int pool-size)]
    (run! #(LinkedBlockingQueue/.add pool %) conns)
    {:conn-pool pool
     :close
     (fn [] (run! (fn [conn] (api/close (:pdb conn))) conns))}))

(defn q [{:keys [conn-pool] :as tx} query]
  (if conn-pool
    (let [conn (LinkedBlockingQueue/.take conn-pool)
          stmt (prepare-cached conn query)]
      (try
        (q* stmt)
        (finally (LinkedBlockingQueue/.offer conn-pool conn))))
    ;; If we don't have a connection pool then we have a tx.
    (q* (prepare-cached tx query))))

(defmacro with-read-tx
  {:clj-kondo/lint-as 'clojure.core/with-open}
  [[tx db] & body]
  `(let [conn-pool# (:conn-pool ~db)
         ~tx        (LinkedBlockingQueue/.take conn-pool#)]
     (try
       (q ~tx ["BEGIN DEFERRED;"])
       ~@body
       (finally
         (q ~tx ["COMMIT;"])
         (LinkedBlockingQueue/.offer conn-pool# ~tx)))))

(defmacro with-write-tx
  {:clj-kondo/lint-as 'clojure.core/with-open}
  [[tx db] & body]
  `(let [conn-pool# (:conn-pool ~db)
         ~tx        (LinkedBlockingQueue/.take conn-pool#)]
     (try
       (q ~tx ["BEGIN IMMEDIATE;"])
       ~@body
       (finally
         (q ~tx ["COMMIT;"])
         (LinkedBlockingQueue/.offer conn-pool# ~tx)))))

;; TODO: errors
;; TODO: response type
;; TODO: faster response build
;; TODO: finalise prepared statements when shutting down


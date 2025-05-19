(ns sqlite4clj.core
  (:require
   [coffi.mem :as mem]
   [coffi.ffi :as ffi :refer [defcfn]]
   [clojure.core.cache.wrapped :as cache])
  (:import
   (java.util.concurrent LinkedBlockingQueue)))

(ffi/load-library "resources/sqlite3.so")

(defn sqlite-ok? [code]
  (= code 0))

(defcfn sqlite3-open-v2
  "sqlite3_open_v2" [::mem/c-string ::mem/pointer ::mem/int
                     ::mem/c-string] ::mem/int
  sqlite3-open-native
  [filename flags]
  (with-open [arena (mem/confined-arena)]
    (let [pdb    (mem/alloc-instance ::mem/pointer arena)
          result (sqlite3-open-native filename pdb flags nil)]
      (if (sqlite-ok? result)
        (mem/deserialize-from pdb ::mem/pointer)
        (throw (ex-info "Failed to open sqlite3 database"
                 {:filename filename}))))))

(defcfn sqlite3-close
  sqlite3_close
  [::mem/pointer] ::mem/int)

(defcfn sqlite3-prepare-v2
  "sqlite3_prepare_v2"
  [::mem/pointer ::mem/c-string ::mem/int
   ::mem/pointer ::mem/pointer] ::mem/int
  sqlite3-prepare-native
  [pdb sql]
  (with-open [arena (mem/confined-arena)]
    (let [ppStmt (mem/alloc-instance ::mem/pointer arena)
          code   (sqlite3-prepare-native pdb sql -1 ppStmt
                   nil)]
      (if (sqlite-ok? code)
        (mem/deserialize-from ppStmt ::mem/pointer)
        (throw (ex-info "Failed to create preparde statement"
                 {:stmt sql}))))))

(defcfn sqlite3-reset
  sqlite3_reset
  [::mem/pointer] ::mem/int)

(defcfn sqlite3-clear-bindings
  sqlite3_clear_bindings
  [::mem/pointer] ::mem/int)

(defcfn sqlite3-bind-int
  sqlite3_bind_int
  [::mem/pointer ::mem/int ::mem/int] ::mem/int)

(defcfn sqlite3-bind-double
  sqlite3_bind_double
  [::mem/pointer ::mem/int ::mem/double] ::mem/int)

(def sqlite-static (mem/as-segment 0))
(def sqlite-transient (mem/as-segment -1))

(defcfn sqlite3-bind-text
  "sqlite3_bind_text"
  [::mem/pointer ::mem/int ::mem/c-string ::mem/int
   [::ffi/fn [::mem/pointer] ::mem/void]] ::mem/int
  sqlite3-bind-text-native
  [pdb idx text]
  (let [text (str text)]
    (sqlite3-bind-text-native pdb idx text
      (count (String/.getBytes text "UTF-8"))
      sqlite-transient)))

(defn type->sqlite3-bind [param]
  (cond
    (integer? param) sqlite3-bind-int
    (double? param)  sqlite3-bind-double
    :else            sqlite3-bind-text))

(defcfn sqlite3-step
  sqlite3_step
  [::mem/pointer] ::mem/int)

(defn bind-params [stmt params]
  (doall
    (map-indexed
      (fn [i param]
        ;; starts at 1
        ((type->sqlite3-bind param) stmt (inc i) param)) params)))

(defn prepare-cached [{:keys [pdb stmt-cache]} [sql & params]]
  (let [stmt (cache/lookup-or-miss stmt-cache sql
               (fn [sql] (sqlite3-prepare-v2 pdb sql)))]
    (bind-params stmt params)
    stmt))

(defcfn sqlite3-column-count
  sqlite3_column_count
  [::mem/pointer] ::mem/int)

(defcfn sqlite3-column-text
  sqlite3_column_text
  [::mem/pointer ::mem/int] ::mem/c-string)

(defn- q* [stmt & [row-builder]]
  (let [cols (range (sqlite3-column-count stmt))
        rs   (loop [rows (transient [])]
               (let [step (sqlite3-step stmt)]
                 (cond
                   (= step 100)
                   (when row-builder
                     (recur (->> (mapv #(sqlite3-column-text stmt %)
                                   cols)
                              row-builder
                              (conj! rows))))

                   (= step 101) (persistent! rows)
                   :else        :error)))]
    (sqlite3-reset stmt)
    (sqlite3-clear-bindings stmt)
    rs))

(defn new-conn! [db-name read-only]
  (let [flags           (if read-only
                          ;; SQLITE_OPEN_READONLY
                          0x00000001
                          ;; SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                          (bit-or 0x00000002 0x00000004))
        *pdb            (sqlite3-open-v2 db-name flags)
        statement-cache (cache/fifo-cache-factory {} :threshold 512)
        conn            {:pdb        *pdb
                         :stmt-cache statement-cache}]
    (q* (sqlite3-prepare-v2 *pdb
          (str
            "pragma cache_size = 15625;"
            "pragma page_size = 4096;"
            "pragma journal_mode = WAL;"
            "pragma synchronous = NORMAL;"
            "pragma temp_store = MEMORY;"
            "pragma foreign_keys = false;")))
    conn))

(defn init-db!
  [db-name & [{:keys [pool-size read-only]
               :or   {pool-size 4}}]]
  (let [conns (repeatedly pool-size
                (fn [] (new-conn! db-name read-only)))
        pool  (LinkedBlockingQueue/new ^int pool-size)]
    (run! #(LinkedBlockingQueue/.add pool %) conns)
    {:conn-pool pool
     :close
     (fn [] (run! (fn [conn] (sqlite3-close (:pdb conn))) conns))}))

(defn q [{:keys [conn-pool]} query & [row-builder]]
  (let [conn   (LinkedBlockingQueue/.take conn-pool)        
        stmt   (prepare-cached conn query)]
    (try
      (q* stmt row-builder)
      (finally
        (LinkedBlockingQueue/.offer conn-pool conn)))))

;; TODO: errors
;; TODO: response type
;; TODO: faster response build
;; TODO: pass in pragma
;; TODO: finalise prepared statements when shutting down
;; TODO: clean up API

(ns app.core
  (:require
   [coffi.mem :as mem]
   [coffi.ffi :as ffi :refer [defcfn]])
  (:import
   (java.util.concurrent Semaphore)))

(ffi/load-library "resources/sqlite3.so")

(defn sqlite-ok? [code]
  (= code 0))

(defcfn sqlite3-open
  "sqlite3_open" [::mem/c-string ::mem/pointer] ::mem/int
  sqlite3-open-native
  [filename]
  (with-open [arena (mem/confined-arena)]
    (let [pdb    (mem/alloc-instance ::mem/pointer arena)
          result (sqlite3-open-native filename pdb)]
      (if (sqlite-ok? result)
        (mem/deserialize-from pdb ::mem/pointer)
        (throw (ex-info "Failed to open sqlite3 database"
                 {:filename filename}))))))

(defcfn sqlite3-close
  sqlite3_close
  [::mem/pointer] ::mem/int)

(defn wrap-callback [callback]
  (fn [_ c-n c-text c-name]
    (try
      (callback
        (mem/deserialize-from
          (mem/reinterpret c-name
            (mem/size-of [::mem/array ::mem/c-string c-n]))
          [::mem/array ::mem/c-string c-n])
        (mem/deserialize-from
          (mem/reinterpret c-text
            (mem/size-of [::mem/array ::mem/c-string c-n]))
          [::mem/array ::mem/c-string c-n]))
      ;; TODO: make this better
      (catch Exception _))
    0))

(defcfn sqlite3-exec
  "sqlite3_exec"
  [::mem/pointer ::mem/c-string
   [::ffi/fn [::mem/pointer
              ::mem/int
              ::mem/pointer
              ::mem/pointer]
    ::mem/int]
   ::mem/pointer ::mem/pointer] ::mem/int
  sqlite3-exec-native
  [pdb sql callback]
  (with-open [arena (mem/confined-arena)]
    (let [notused-ptr (mem/serialize 0 [::mem/pointer ::mem/int])
          errmsg-ptr  (mem/alloc-instance ::mem/c-string arena)
          code        (sqlite3-exec-native
                        pdb sql (wrap-callback callback)
                        notused-ptr errmsg-ptr)]
      (if (sqlite-ok? code)
        code
        (throw
          (ex-info "SQL error"
            {:error
             (mem/deserialize-from errmsg-ptr ::mem/c-string)}))))))

(defn new-conn! [db-name]
  (let [*pdb (sqlite3-open db-name)]
    (sqlite3-exec *pdb
      (str
        "pragma cache_size = 15625;"
        "pragma page_size = 4096;"
        "pragma journal_mode = WAL;"
        "pragma synchronous = NORMAL;"
        "pragma temp_store = MEMORY;"
        "pragma foreign_keys = false;")
      (fn [_ _]))
    *pdb))

(defn init-db! [db-name & [{:keys [pool-size] :or {pool-size 4}}]]
  (let [conns (into '() (repeatedly pool-size
                          (fn [] (new-conn! db-name))))]
    {:conns         conns
     :conn-pool     (atom conns)
     :conn-pool-sem (Semaphore/new (count conns) true)
     :close         (fn [] (run! sqlite3-close conns))}))

(defn take-conn! [conn-pool]
  (let [[[conn]] (swap-vals! conn-pool pop)]
    conn))

(defn return-conn! [conn-pool conn]
  (swap! conn-pool conj conn))

(defn zipmap-key-fn [key-fn keys vals]
  (loop [map (transient {})
         ks  (seq keys)
         vs  (seq vals)]
    (if (and ks vs)
      (recur (assoc! map (key-fn (first ks)) (first vs))
        (next ks)
        (next vs))
      (persistent! map))))

(defn q [{:keys [conn-pool conn-pool-sem]} query row-builder]
  (Semaphore/.acquire conn-pool-sem)
  (let [conn   (take-conn! conn-pool)
        result (atom (transient []))]
    (try
      (sqlite3-exec conn query
        (fn [row-keys row-vals]
          (->> (zipmap-key-fn keyword row-keys row-vals)
            (row-builder)
            (swap! result conj!))))
      (persistent! @result)
      (finally
        (return-conn! conn-pool conn)
        (Semaphore/.release conn-pool-sem)))))

(defonce db (init-db! "database.db" {:pool-size 4}))

(comment

  (q db (str
          "pragma cache_size;"
          "pragma page_size;"
          "pragma journal_mode;"
          "pragma synchronous;"
          "pragma temp_store;"
          "pragma foreign_keys;")
    (fn [row] row))

  (q db "pragma compile_options" (fn [c-text _] (prn c-text)))

  (time
    (->> (mapv
           (fn [n]
             (future
               (q db "SELECT chunk_id, JSON_GROUP_ARRAY(state) AS chunk_cells FROM cell WHERE chunk_id IN (1978, 3955, 5932, 1979, 3956, 5933, 1980, 3957, 5934) GROUP BY chunk_id"
                 (fn [row] row))))
           (range 0 2000))
      (run! (fn [x] @x))))

  (user/bench
    (q db "SELECT chunk_id, state FROM cell WHERE chunk_id IN (1978, 3955, 5932, 1979, 3956, 5933, 1980, 3957, 5934)"
      (fn [row] row)))

  (user/bench
    (q db "SELECT chunk_id, JSON_GROUP_ARRAY(state) AS chunk_cells FROM cell WHERE chunk_id IN (1978, 3955, 5932, 1979, 3956, 5933, 1980, 3957, 5934) GROUP BY chunk_id"
      (fn [row] row)))

  ;; Utility
  ;; ENABLE_MATH_FUNCTIONS
  ;; ENABLE_COLUMN_METADATA
  ;; ENABLE_FTS5
  ;; ENABLE_RTREE
  ;; ENABLE_LOAD_EXTENSION

  

  )




(ns sqlite4clj.core
  "High level interface for using sqlite4clj including connection pool
  and prepared statement caching."
  (:require
   [sqlite4clj.api :as api]
   [sqlite4clj.functions :as funcs]
   [clojure.core.cache.wrapped :as cache])
  (:import
   (java.util.concurrent BlockingQueue LinkedBlockingQueue)))

(defn type->sqlite3-bind [param]
  (cond
    (integer? param) `api/bind-int
    (double? param)  `api/bind-double
    (string? param)  `api/bind-text
    :else            `api/bind-blob))

(defmacro build-bind-fn [first-run-params]
  (let [stmt   (gensym)
        params (gensym)]
    `(fn [~stmt ~params]
       ~@(map-indexed
           (fn [i param]
             ;; starts at 1
             `(~(type->sqlite3-bind param) ~stmt ~(inc i)
               (~params ~i))) first-run-params))))

(defn col-type->col-fn [sqlite-type]
  ;; See type codes here: https://sqlite.org/c3ref/c_blob.html
  (case (int sqlite-type)
    1 `api/column-int
    2 `api/column-double
    3 `api/column-text
    4 `api/column-blob
    5 `(fn [_# _#] nil)))

(defn get-column-types [stmt]
  (let [n-cols (int
                 #_{:clj-kondo/ignore [:type-mismatch]}
                 (api/column-count stmt))]
    (mapv (fn [n] (api/column-type stmt n)) (range 0 n-cols))))

(defmacro build-column-fn [column-types]
  (let [stmt (gensym)]
    `(fn [~stmt]
       ~(if (= (count column-types) 1)
          ;; Unwrap when returning single column
          `(~(col-type->col-fn (first column-types)) ~stmt 0)
          (vec (map-indexed
                 (fn [i n] `(~(col-type->col-fn n) ~stmt ~i))
                 column-types))))))

(defn prepare
  ([pdb sql params]
   (let [stmt (api/prepare-v3 pdb sql)]
     (cond-> {:stmt stmt}
       (seq params) (assoc :bind-fn (eval `(build-bind-fn ~params)))))))

(defn prepare-cached [{:keys [pdb stmt-cache]} query]
  (let [sql    (first query)
        params (subvec query 1)
        {:keys [stmt bind-fn] :as m}
        (cache/lookup-or-miss stmt-cache sql
          (fn [_] (prepare pdb sql params)))]
    (when bind-fn (bind-fn stmt params))
    m))

(defmacro with-stmt-reset
  {:clj-kondo/lint-as 'clojure.core/with-open}
  [[stmt-binding stmt] & body]
  `(let [~stmt-binding ~stmt]
     (try
       ~@body
       (finally
         (api/reset ~stmt-binding)
         (api/clear-bindings ~stmt-binding)))))

(defn step-rows [stmt col-fn rows]
  (loop [rows (transient rows)]
    (let [code (int
                 #_{:clj-kondo/ignore [:type-mismatch]}
                 (api/step stmt))]
      (case code
        100 (recur (conj! rows (col-fn stmt)))
        101 (persistent! rows)
        code))))

(defn- q* [conn [sql :as query]]
  (let [result
        (let [{:keys [stmt col-fn]} (prepare-cached conn query)]
          (with-stmt-reset [stmt stmt]
            (if col-fn
              (step-rows stmt col-fn [])
              (let [stmt-cache (:stmt-cache conn)
                    code       (int
                                 #_{:clj-kondo/ignore [:type-mismatch]}
                                 (api/step stmt))]
                (if (= 100 code)
                  (let [col-types (get-column-types stmt)
                        col-fn    (eval `(build-column-fn ~col-types))]
                    (swap! stmt-cache assoc-in [sql :col-fn] col-fn)
                    (step-rows stmt col-fn [(col-fn stmt)]))
                  code)))))]
    (cond
      (vector? result) (when (seq result) result)
      (= result 101)   nil
      :else            (throw (api/sqlite-ex-info (:pdb conn) result
                                {:sql    (first query)
                                 :params (subvec query 1)})))))

(def default-pragma
  {:cache_size   15625
   :page_size    4096
   :journal_mode "WAL"
   :synchronous  "NORMAL"
   :temp_store   "MEMORY"
   :foreign_keys true
   ;; Because of WAL and a single writer at the application level
   ;; SQLITE_BUSY error should almost never happen, see:
   ;; https://sqlite.org/wal.html#sometimes_queries_return_sqlite_busy_in_wal_mode
   :busy_timeout 100
   ;; https://sqlite.org/pragma.html#pragma_optimize
   :optimize     0x10002})

(defn pragma->set-pragma-query [pragma read-only]
  (conj (->> (merge default-pragma pragma)
          (mapv (fn [[k v]] [(str "pragma " (name k) "=" v)])))
    ;; Needs to be added at the end after all pragma are run
    ;; as optimise require connection not to be read only
    [(str "pragma query_only=" (boolean read-only))]))

(defn new-conn! [db-name pragma read-only]
  (let [;; SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
        flags           (bit-or 0x00000002 0x00000004)
        *pdb            (api/open-v2 db-name flags)
        statement-cache (cache/fifo-cache-factory {} :threshold 512)
        conn            {:pdb        *pdb
                         :stmt-cache statement-cache}]
    (->> (pragma->set-pragma-query pragma read-only)
      (run! #(q* conn %)))
    conn))

(defn init-pool!
  [db-name & [{:keys [pool-size pragma read-only]
               :or   {pool-size 4}}]]
  (let [conns (repeatedly pool-size
                (fn [] (new-conn! db-name pragma read-only)))
        pool  (LinkedBlockingQueue/new ^int pool-size)]
    (run! #(BlockingQueue/.add pool %) conns)
    {:conn-pool pool
     :connections conns
     :close
     (fn [] (run! (fn [conn] (api/close (:pdb conn))) conns))}))

(defn init-db!
  "A db consists of a read pool of size :pool-size and a write pool of size 1.
  The same pragma are set for both pools."
  [url & [{:keys [pool-size pragma] :or {pool-size 4}}]]
  (let [;; Only one write connection
        writer
        (init-pool! url
          {:pool-size 1
           :pragma    pragma})
        ;; Pool of read connections
        reader
        (init-pool! url
          {:read-only true
           :pool-size pool-size
           :pragma    pragma})]
    {:writer writer
     :reader reader
     :internal {:app-functions (atom {})}}))

(defn q
  "Run a query against a db. Return nil when no results."
  [{:keys [conn-pool] :as tx} query]
  (if conn-pool
    (let [conn (BlockingQueue/.take conn-pool)]
      (try
        (q* conn query)
        ;; Always return the conn even on error
        (finally (BlockingQueue/.offer conn-pool conn))))
    ;; If we don't have a connection pool then we have a tx.
    (q* tx query)))

(defn optimize-db
  "Use for running optimise on long lived connections. For query_only
  connections makes the connection temporarily writable."
  [db]
  (let [n-conn (count (:conn-pool db))]
    (loop [n 0]
      (if (= (first (q db ["pragma query_only"])) 1)
        (do (q db ["pragma query_only=false"])
            (q db ["pragma optimize"])
            (q db ["pragma query_only=true"]))
        (q db ["pragma optimize"]))
      (if (> n-conn n) (recur (inc n)) n))))

(defmacro with-read-tx
  {:clj-kondo/lint-as 'clojure.core/with-open}
  [[tx db] & body]
  `(let [conn-pool# (:conn-pool ~db)
         ~tx        (BlockingQueue/.take conn-pool#)]
     (try
       (q ~tx ["BEGIN DEFERRED"])
       ~@body
       (finally
         (q ~tx ["COMMIT"])
         (BlockingQueue/.offer conn-pool# ~tx)))))

(defmacro with-write-tx
  {:clj-kondo/lint-as 'clojure.core/with-open}
  [[tx db] & body]
  `(let [conn-pool# (:conn-pool ~db)
         ~tx        (BlockingQueue/.take conn-pool#)]
     (try
       (q ~tx ["BEGIN IMMEDIATE"])
       ~@body
       (finally
         (q ~tx ["COMMIT"])
         (BlockingQueue/.offer conn-pool# ~tx)))))

;; WAL + single writer enforced at the application layer means you don't need
;; to handle SQLITE_BUSY or SQLITE_LOCKED.

;; TODO: finalise prepared statements when shutting down

(defn create-function
  "register a user-defined function with sqlite on all connections.

   parameters:
   - db: database from init-db!
   - name: string function name
   - f-or-var: either a function or a var that points to a function.
   - opts: a map of options that can include:

     the sqlite function flags (see https://www.sqlite.org/c3ref/c_deterministic.html)
     - :deterministic? (boolean)
     - :direct-only? (boolean)
     - :innocuous? (boolean)
     - :sub-type? (boolean)
     - :result-sub-type? (boolean)
     - :self-order1? (boolean)

     other options:
     - :arity (int): the number of arguments the function takes.

  by default the function/var will be analyzed and the arity will be inferred.
  if the function has multiple arities then all will be registered with sqlite.
  you can specify the arity explicitly with the `:arity` option."
  [db name f-or-var & {:as opts}]
  (funcs/create-function db name f-or-var opts))

(defn remove-function
  "unregister a user-defined function from sqlite on all connections.
  if an arity is not provided, it will unregister all arities for the function."
  ([db name]
   (funcs/remove-function db name))
  ([db name arity]
   (funcs/remove-function db name arity)))

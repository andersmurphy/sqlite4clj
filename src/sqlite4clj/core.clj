(ns sqlite4clj.core
  "High level interface for using sqlite4clj including connection pool
  and prepared statement caching."
  (:require
   [sqlite4clj.impl.api :as api]
   [sqlite4clj.impl.functions :as funcs]
   [clojure.core.cache.wrapped :as cache])
  (:import
   (java.util.concurrent BlockingQueue LinkedBlockingQueue)))

(defn bind [stmt params]
  (reduce
    (fn [i param]
      (cond
        (integer? param) (api/bind-int    stmt i param)
        (double? param)  (api/bind-double stmt i param)
        (string? param)  (api/bind-text   stmt i param)
        (nil? param)     (api/bind-null   stmt i)
        :else            (api/bind-blob   stmt i param))
      (inc i))
    1 ;; starts at 1
    params))

(defn prepare
  ([pdb sql]
   (let [stmt      (api/prepare-v3 pdb sql)
         col-count (int #_{:clj-kondo/ignore [:type-mismatch]}
                     (api/column-count stmt))]
     (cond-> {:stmt stmt}
       (> col-count 0)
       (assoc :col-metadata
         (mapv (fn [c]
                 {:database (api/column-database-name stmt c)
                  :table    (api/column-table-name stmt c)
                  :origin   (api/column-origin-name stmt c)
                  :alias    (api/column-name stmt c)})
           (range 0 col-count)))))))

(defn prepare-cached [{:keys [pdb stmt-cache]} query]
  (let [sql    (first query)
        params (subvec query 1)
        {:keys [stmt] :as m}
        (cache/lookup-or-miss stmt-cache sql
          (fn [_] (prepare pdb sql)))]
    (bind stmt params)
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

(defn get-column-val [stmt n]
  (case (int #_{:clj-kondo/ignore [:type-mismatch]}
          (api/column-type stmt n))
    ;; See type codes here: https://sqlite.org/c3ref/c_blob.html
    1 (api/column-int    stmt n)
    2 (api/column-double stmt n)
    3 (api/column-text   stmt n)
    4 (api/column-blob   stmt n)
    5 nil))

(defn column [stmt n-cols]
  (case n-cols
    0 nil
    1 [(get-column-val stmt 0)]
    2 [(get-column-val stmt 0)
       (get-column-val stmt 1)]
    3 [(get-column-val stmt 0)
       (get-column-val stmt 1)
       (get-column-val stmt 2)]
    4 [(get-column-val stmt 0)
       (get-column-val stmt 1)
       (get-column-val stmt 2)
       (get-column-val stmt 3)]
    5 [(get-column-val stmt 0)
       (get-column-val stmt 1)
       (get-column-val stmt 2)
       (get-column-val stmt 3)
       (get-column-val stmt 4)]
    ;; After 5 params it's worth iterating
    (loop [n    0
           cols (transient [])]
      (if (>= n n-cols)
        (persistent! cols)
        (recur (inc n)
          (conj! cols (get-column-val stmt n)))))))

(defn- q* [conn query result-set-fn]
  ;; sqlite4clj uses -DSQLITE_THREADSAFE=2 which means sqlite4clj is
  ;; responsible for serializing access to database connections and prepared
  ;; prepared statements. SQLite will be safe to use in a multi-threaded
  ;; environment as long as no two threads attempt to use the same database
  ;; connection at the same time.

  ;; The reason for this is letting SQLite manage these locks is messy and
  ;; can lead to high tail latency (SQLITE_BUSY). So it's better for the
  ;; driver/application layer to handle it.

  ;; sqlite4clj manages connections through the pool. So most of the time
  ;; connections will only be handled by a single thread at a time.
  ;; The exception is when write/read transactions are being used in
  ;; creative async contexts. So this lock is here to prevent problems when
  ;; that happens. Outside of this usage it will not come into play/cause
  ;; contention.
  (locking conn
    (let [{:keys [stmt col-metadata]} (prepare-cached conn query)]
      (with-stmt-reset [stmt stmt]
        (let [n-cols        (int
                              #_{:clj-kondo/ignore [:type-mismatch]}
                       (api/column-count stmt))
              result-set-fn (or result-set-fn (:default-result-set-fn conn))]
          (result-set-fn col-metadata
            (reify
              clojure.lang.IReduceInit
              (reduce [_ f init]
                (loop [ret init]
                  (let [code (int
                               #_{:clj-kondo/ignore [:type-mismatch]}
                               (api/step stmt))]
                    (case code
                      100 (let [result (f ret (column stmt n-cols))]
                            (if (reduced? result)
                              @result
                              (recur result)))
                      101 ret
                      (throw (api/sqlite-ex-info (:pdb conn) code
                               {:sql    (first query)
                                :params (subvec query 1)})))))))))))))

(def default-pragma
  {:cache_size         15625
   :page_size          4096
   :journal_mode       "WAL"
   :synchronous        "NORMAL"
   :temp_store         "MEMORY"
   :foreign_keys       false
   ;; Because of WAL and a single writer at the application level
   ;; SQLITE_BUSY error should almost never happen, see:
   ;; https://sqlite.org/wal.html#sometimes_queries_return_sqlite_busy_in_wal_mode
   ;; However, sometime when using litestream for backups it can happen.
   ;; So we set it to the recommended value see:
   ;;  https://litestream.io/tips/#busy-timeout
   :busy_timeout       5000
   ;; Litestream recommends disabling autocheckpointing under high write loads
   ;; https://litestream.io/tips/#disable-autocheckpoints-for-high-write-load-servers
   :wal_autocheckpoint 0
   ;; :optimize cannot be run on connection open when using application
   ;; function in indexes. As you will get a unknown function error.
   ;; https://sqlite.org/pragma.html#pragma_optimize
   ;; :optimize     0x10002
   })

(defn pragma->set-pragma-query [pragma read-only]
  (conj (->> (merge default-pragma pragma)
          (mapv (fn [[k v]] [(str "pragma " (name k) "=" v)])))
    ;; Needs to be added at the end after all pragma are run
    ;; as optimise require connection not to be read only
    [(str "pragma query_only=" (boolean read-only))]))

(defn no-unwrap-result-set-fn
  [_col-metadata result-set]
  (let [result (into [] result-set)]
    (when (seq result) result)))

(defn qualified-keyword-result-set-fn [col-metadata result-set]
  (let [col-keys
        (mapv (fn [{:keys [table alias]}]
                (keyword table alias)) col-metadata)
        result (into []
                 (map (fn [col-vals]
                        (zipmap col-keys col-vals)))
                 result-set)]
    (when (seq result) result)))

(defn new-conn! [db-name pragma read-only vfs default-result-set-fn]
  (let [;; SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
        flags           (bit-or 0x00000002 0x00000004)
        *pdb            (api/open-v2 db-name flags vfs)
        statement-cache (cache/fifo-cache-factory {} :threshold 512)
        conn            {:pdb                   *pdb
                         :stmt-cache            statement-cache
                         :default-result-set-fn default-result-set-fn}]
    (->> (pragma->set-pragma-query pragma read-only)
      (run! #(q* conn % default-result-set-fn)))
    conn))

(defn init-pool!
  [db-name & [{:keys [pool-size pragma read-only vfs default-result-set-fn]
               :or   {pool-size
                      (Runtime/.availableProcessors (Runtime/getRuntime))}}]]
  (let [conns (repeatedly pool-size
                (fn [] (new-conn! db-name pragma read-only vfs
                         default-result-set-fn)))
        pool  (LinkedBlockingQueue/new ^int pool-size)]
    (run! #(BlockingQueue/.add pool %) conns)
    {:conn-pool   pool
     :connections conns
     :close
     (fn [] (run! (fn [conn] (api/close (:pdb conn))) conns))}))

(defn unwrap-result-set-fn
  [col-metadata result-set]
  (let [result (if (= (count col-metadata) 1)
                 (into [] cat result-set)
                 (into [] result-set))]
    (when (seq result) result)))

(defn init-db!
  "A db consists of a read pool of size :pool-size and a write pool of size 1.
  The same pragma are set for both pools."
  [url & [{:keys [pool-size pragma writer-pragma vfs
                  default-result-set-fn]
           :or   {default-result-set-fn unwrap-result-set-fn
                  pool-size (Runtime/.availableProcessors
                              (Runtime/getRuntime))}}]]
  (assert (< 0 pool-size))
  (let [;; Only one write connection
        writer
        (init-pool! url
          {:pool-size 1
           :pragma    (merge pragma writer-pragma)
           :vfs       vfs
           :default-result-set-fn default-result-set-fn})
        ;; Pool of read connections
        reader
        (init-pool! url
          {:read-only true
           :pool-size pool-size
           :pragma    pragma
           :vfs       vfs
           :default-result-set-fn default-result-set-fn})]
    {:writer   writer
     :reader   reader
     ;; Prevents application function callback pointers from getting
     ;; garbage collected.
     :internal {:app-functions (atom {})}}))

(defn q
  "Run a query against a db. Return nil when no results."
  [{:keys [conn-pool] :as tx} query &
   [{:keys [result-set-fn]}] ]
  (if conn-pool
    (binding [*print-length* nil]
      (let [conn (BlockingQueue/.take conn-pool)]
        (try
          (q* conn query result-set-fn)
          ;; Always return the conn even on error
          (finally (BlockingQueue/.offer conn-pool conn)))))
    ;; If we don't have a connection pool then we have a tx.
    (q* tx query result-set-fn)))

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
  "Wrap series of queries in a read transaction."
  {:clj-kondo/lint-as 'clojure.core/with-open}
  [[tx db] & body]
  `(let [conn-pool# (:conn-pool ~db)
         ~tx        (BlockingQueue/.take conn-pool#)]
     (binding [*print-length* nil]
       (try
         (q ~tx ["BEGIN DEFERRED"])
         ~@body
         (q ~tx ["COMMIT"])
         (catch Throwable t#
           ;; Handles non SQLITE errors crashing a transaction
           (q ~tx ["ROLLBACK"])
           (throw t#))
         (finally
           (BlockingQueue/.offer conn-pool# ~tx))))))

(defmacro with-write-tx
  "Wrap series of queries in a write transaction."
  {:clj-kondo/lint-as 'clojure.core/with-open}
  [[tx db] & body]
  `(let [conn-pool# (:conn-pool ~db)
         ~tx        (BlockingQueue/.take conn-pool#)]
     (binding [*print-length* nil]
       (try
         (q ~tx ["BEGIN IMMEDIATE"])
         ~@body
         (q ~tx ["COMMIT"])
         (catch Throwable t#
           ;; Handles non SQLITE errors crashing a transaction
           (q ~tx ["ROLLBACK"])
           (throw t#))
         (finally
           (BlockingQueue/.offer conn-pool# ~tx))))))

(defmacro with-conn
  "Use the same connection for a series of queries (not a transaction) without
  returning it to the pool until the end."
  {:clj-kondo/lint-as 'clojure.core/with-open}
  [[tx db] & body]
  `(let [conn-pool# (:conn-pool ~db)
         ~tx        (BlockingQueue/.take conn-pool#)]
     (binding [*print-length* nil]
       (try
         ~@body
         (finally
           (BlockingQueue/.offer conn-pool# ~tx))))))

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

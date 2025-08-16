(ns sqlite4clj.functions
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [sqlite4clj.api :as api])
  (:import
   [java.lang.reflect Method]))

(defn infer-arity
  "Returns the arities (a vector of ints) of:
    - anonymous functions like `#()` and `(fn [])`.
    - defined functions like `map` or `+`.
    - macros, by passing a var like `#'->`.

  Returns `[:variadic]` if the function/macro is variadic.
  Otherwise returns nil"
  [f]
  (let [func      (if (var? f) @f f)
        methods   (->> func
                       class
                       .getDeclaredMethods
                       (map (fn [^Method m]
                              (vector (.getName m)
                                      (count (.getParameterTypes m))))))
        var-args? (some #(-> % first #{"getRequiredArity"})
                        methods)
        arities   (->> methods
                       (filter (comp #{"invoke"} first))
                       (map second)
                       (sort))]
    (cond
      (keyword? f)     nil
      var-args?        [:variadic]
      (empty? arities) nil
      :else            (if (and (var? f) (-> f meta :macro))
                         (mapv #(- % 2) arities) ;; substract implicit &form and &env arguments
                         (into [] arities)))))

(def ^:private flag-map {:deterministic?   api/SQLITE_DETERMINISTIC
                         :innocuous?       api/SQLITE_INNOCUOUS
                         :direct-only?     api/SQLITE_DIRECTONLY
                         :sub-type?        api/SQLITE_SUBTYPE
                         :result-sub-type? api/SQLITE_RESULT_SUBTYPE
                         :self-order1?     api/SQLITE_SELFORDER1})

(defn function-flags->bitmask
  [opts]
  (let [flags (select-keys opts (keys flag-map))]
    (reduce (fn [mask [flag enabled?]]
              (printf "Processing flag %s with value %s\n" flag enabled?)
              (let [result (bit-or mask (if enabled?
                                          (get flag-map flag)
                                          0))]
                result))
            api/SQLITE_UTF8
            flags)))

(defn result->result-fn [v]
  (cond
    (string? v)  api/result-text
    (integer? v) api/result-int
    (double? v)  api/result-double
    (nil? v)     api/result-null
    :else        api/result-blob))

(defn deserialize-argv
  "Extract sqlite3_value pointers from argv array"
  [argv argc]
  (if (or (mem/null? argv) (zero? argc))
    []
    (let [;; Reinterpret argv as an array of pointers with the correct size
          argc-int     (int argc)
          ptr-size     8
          argv-segment (mem/reinterpret argv (* argc-int ptr-size))]
      (mapv (fn [i]
              ;; Read each pointer from the array
              (mem/read-address argv-segment (* i ptr-size)))
            (range argc-int)))))

(defn value->clj
  "Convert a sqlite3_value to a Clojure value based on its type"
  [sqlite-value]
  (let [type-code (api/value-type sqlite-value)]
    (case type-code
      1 (api/value-int sqlite-value)    ;; SQLITE_INTEGER
      2 (api/value-double sqlite-value) ;; SQLITE_FLOAT
      3 (api/value-text sqlite-value)   ;; SQLITE_TEXT
      4 (api/value-blob sqlite-value)   ;; SQLITE_BLOB
      5 nil)))                          ;; SQLITE_NULL

(defn wrap-scalar-function
  "Wrap a Clojure function to be used as a SQLite scalar function callback.
   Catches all exceptions to prevent JVM crashes."
  [f]
  (fn [context argc argv]
    (try
      (let [args      (deserialize-argv argv argc)
            clj-args  (mapv value->clj args)
            result    (apply f clj-args)
            result-fn (result->result-fn result)]
        (result-fn context result))
      (catch Throwable e
        ;; catch everything to prevent JVM crashes
        (api/result-error context
                          (or (.getMessage e)
                              (str "Unexpected " (.getSimpleName (class e)))))))))

(defn doto-connections [db f]
  (doseq [pool [(:reader db) (:writer db)]]
    (doseq [conn (:connections pool)]
      (f conn))))

(defn unregister-function-callback [db name arity flags]
  (doto-connections db
                    (fn [conn]
                      (let [pdb  (:pdb conn)
                            ;; "To delete an existing SQL function or aggregate, pass NULL pointers for all three function callbacks."
                            code (api/create-function-v2 pdb name arity flags mem/null
                                                         mem/null mem/null mem/null mem/null)]
                        (when-not (api/sqlite-ok? code)
                          (throw (api/sqlite-ex-info pdb code {:function name})))))))

(defn app-functions [db]
  (when-let [fns (get-in db [:internal :app-functions])]
    @fns))

(defn get-function
  ([db name]
   (get-in (app-functions db) [name]))
  ([db name arity]
   (get-in (app-functions db) [name arity])))

(defn- build-removal-update
  [fn-data arity]
  (let [arities-to-remove (if arity
                            (when (get fn-data arity) #{arity})
                            (set (keys (dissoc fn-data :meta))))
        remaining-arities (if arity
                            (disj (set (keys (dissoc fn-data :meta))) arity)
                            #{})]
    {:remove-arities arities-to-remove
     :remove-all?    (empty? remaining-arities)}))

(defn- clear-function-arities
  "Internal function to clear all arities while preserving var watch. "
  [db name]
  (when-let [fn-data (get-function db name)]
    (let [{:keys [remove-arities]} (build-removal-update fn-data nil)]

      ;; Unregister from SQLite first (can throw)
      (doseq [a    remove-arities
              :let [{:keys [flags]} (get fn-data a)]]
        (unregister-function-callback db name a flags))

      ;; Single atomic swap - remove arities but keep :meta
      (swap! (get-in db [:internal :app-functions])
             #(update % name (fn [fn-entry]
                               (reduce dissoc fn-entry remove-arities)))))))

(defn- do-register-function
  "Core registration that handles all arities atomically

     - name          - the name of the function
     - f             - the function to register
     - arities       - a vector of integers representing the arities of the function + :variadic  for variadic functions
     - flags-bitmask - a bitmask representing the function flags
     - var           - optional var that is holding the fn
     - watch-key     - optional watch-key for the var"
  [db name f arities flags-bitmask var watch-key]
  (let [registrations (vec
                       (for [n arities]
                         (let [arity        (if (= n :variadic) -1 n)
                               callback     (wrap-scalar-function f)
                               callback-ptr (mem/serialize callback
                                                           [::ffi/fn
                                                            [::mem/pointer ::mem/int ::mem/pointer]
                                                            ::mem/void
                                                            :raw-fn? true]
                                                           (mem/global-arena))]
                           (doto-connections db
                                             (fn [conn]
                                               (let [pdb  (:pdb conn)
                                                     code (api/create-function-v2 pdb name arity flags-bitmask mem/null
                                                                                  callback-ptr mem/null mem/null mem/null)]
                                                 (when-not (api/sqlite-ok? code)
                                                   (throw (api/sqlite-ex-info pdb code {:function name}))))))
                           {:arity arity
                            :data  {:flags        flags-bitmask
                                    :callback     callback
                                    :callback-ptr callback-ptr}})))
        metadata      (when var
                        {:var var :watch-key watch-key})]
    (swap! (get-in db [:internal :app-functions])
           update name
           (fn [existing]
             (let [new-arities (into {}
                                     (for [{:keys [arity data]} registrations]
                                       [arity data]))]
               (cond-> (merge existing new-arities)
                 metadata (assoc :meta metadata)))))))

(defn register-function
  [db name f & {:keys [arity] :as opts}]
  (let [arities       (if arity [arity] (infer-arity f))
        flags-bitmask (function-flags->bitmask opts)]
    (do-register-function db name f arities flags-bitmask nil nil)))

(defn- do-register-function-var [db name var f {:keys [arity] :as opts}]
  (let [arities       (if arity [arity] (infer-arity f))
        flags-bitmask (function-flags->bitmask opts)
        watch-key     (keyword (str "sqlite4clj-" name "-var"))]
    (do-register-function db name f arities flags-bitmask var watch-key)
    watch-key))

(defn- fn-var-updated
  [db name opts _watch-key var _old-val new-val]
  (clear-function-arities db name)
  (when (and new-val (fn? new-val))
    (do-register-function-var db name var new-val opts)))

(defn register-function-var
  [db name var & {:as opts}]
  (let [watch-key (do-register-function-var db name var (var-get var) opts)]
    (add-watch var watch-key (partial fn-var-updated db name opts))))

;; -----------------------------
;; Public API

(defn create-function
  [db name f-or-var & {:as opts}]
  (if (var? f-or-var)
    (register-function-var db name f-or-var opts)
    (register-function db name f-or-var opts)))

(defn remove-function
  ([db name]
   (remove-function db name nil))

  ([db name arity]
   (when-let [fn-data (get-function db name)]
     (let [{:keys [remove-arities remove-all?]} (build-removal-update fn-data arity)]

       ;; unregister from sqlite (can throw!)
       (doseq [a    remove-arities
               :let [{:keys [flags]} (get fn-data a)]]
         (unregister-function-callback db name a flags))

       ;; clean up watch if needed
       (when (and remove-all? (:meta fn-data))
         (let [{:keys [var watch-key]} (:meta fn-data)]
           (remove-watch var watch-key)))

       (swap! (get-in db [:internal :app-functions])
              (if remove-all?
                #(dissoc % name)
                #(update % name (fn [fn-entry]
                                  (reduce dissoc fn-entry remove-arities)))))))))

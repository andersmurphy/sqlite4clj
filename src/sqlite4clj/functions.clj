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
                         :innocuous?        api/SQLITE_INNOCUOUS
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
    (string? v) api/result-text
    (integer? v) api/result-int
    (double? v) api/result-double
    (nil? v) api/result-null
    :else api/result-blob))

(defn deserialize-argv
  "Extract sqlite3_value pointers from argv array"
  [argv argc]
  (if (or (mem/null? argv) (zero? argc))
    []
    (let [;; Reinterpret argv as an array of pointers with the correct size
          argc-int (int argc)
          ptr-size 8
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
      1 (api/value-int sqlite-value) ;; SQLITE_INTEGER
      2 (api/value-double sqlite-value) ;; SQLITE_FLOAT
      3 (api/value-text sqlite-value) ;; SQLITE_TEXT
      4 (api/value-blob sqlite-value) ;; SQLITE_BLOB
      5 nil))) ;; SQLITE_NULL

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

(defn remove-function-handle [db name arity]
  (swap! (get-in db [:internal :app-functions])
         update-in [name] dissoc arity))

(defn store-function-handle [db name arity flags callback callback-ptr]
  (swap! (get-in db [:internal :app-functions])
         assoc-in [name arity] {:callback callback
                                :flags flags
                                :callback-ptr callback-ptr}))

(defn doto-connections [db f]
  (doseq [pool [(:reader db) (:writer db)]]
    (doseq [conn (:connections pool)]
      (f conn))))

(defn unregister-function-callback [db name arity flags]
  (assert name)
  (assert arity)
  (assert flags)
  (println "Unregistering function callback for" name "with arity" arity "and flags" flags)
  (doto-connections db
                    (fn [conn]
                      (let [pdb (:pdb conn)
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

(defn remove-function
  ([db name]
   (when-let [fn-arities (get-function db name)]
     (doseq [a (keys fn-arities)]
       (println "Removing function" name "with arity" a "thing is" (get-in fn-arities [a]))
       (unregister-function-callback db name a (get-in fn-arities [a :flags]))
       (remove-function-handle db name a))))
  ([db name arity]
   (when-let [{:keys [flags]} (get-in (app-functions db) [name arity])]
     (unregister-function-callback db name arity flags)
     (remove-function-handle db name arity))))

(defn register-function-callback
  "Low-level register function"
  [db name callback arity flags-bitmask]
  (let [callback-ptr (mem/serialize callback
                                    [::ffi/fn
                                     [::mem/pointer ::mem/int ::mem/pointer]
                                     ::mem/void
                                     :raw-fn? true]
                                    (mem/global-arena))]
    (store-function-handle db name arity flags-bitmask callback callback-ptr)
    (doto-connections db
                      (fn [conn]
                        (let [pdb (:pdb conn)
                              code (api/create-function-v2 pdb name arity flags-bitmask mem/null
                                                           callback-ptr mem/null mem/null mem/null)]
                          (when-not (api/sqlite-ok? code)
                            (throw (api/sqlite-ex-info pdb code {:function name}))))))

    nil))

(defn register-function
  [db name f & {:keys [arity] :as opts}]
  (let [arities (if arity [arity]
                    (infer-arity f))
        flags-bitmask (function-flags->bitmask opts)]
    (doseq [n arities]
      (let [arity (if (= n :variadic) -1 n)
            callback (wrap-scalar-function f)]
        (register-function-callback db name callback arity flags-bitmask)))))

(defn register-function-var
  [db name var & {:as opts}])

(defn create-function
  [db name f-or-var & {:as opts}]
  ;; if it is a var we want to add-watch
  ;; our
  (if (var? f-or-var)
    (register-function-var db name f-or-var opts)
    (register-function db name f-or-var opts)))

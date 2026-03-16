(ns sqlite4clj.impl.functions-aggregates
  (:require
   [coffi.ffi :as ffi]
   [coffi.mem :as mem]
   [sqlite4clj.impl.api :as api]
   [sqlite4clj.impl.functions :as funcs])
  (:import
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicReference]))

(defn app-aggregates [db]
  (when-let [aggs (get-in db [:internal :app-aggregates])]
    @aggs))

(defn get-aggregate
  ([db name]
   (get-in (app-aggregates db) [name]))
  ([db name arity]
   (get-in (app-aggregates db) [name arity])))

(defn unregister-aggregate-callback [db name arity flags]
  (funcs/doto-connections db
    (fn [conn]
      (let [pdb  (:pdb conn)
            ;; "To delete an existing SQL function or aggregate, pass NULL pointers for all three function callbacks."
            code (api/create-function-v2 pdb name arity flags mem/null
                   mem/null mem/null mem/null mem/null)]
        (when-not (api/sqlite-ok? code)
          (throw (api/sqlite-ex-info pdb code {:aggregate name})))))))

(defn- build-removal-update
  [agg-data arity]
  (let [arities-to-remove (if arity
                            (when (get agg-data arity) #{arity})
                            (set (keys (dissoc agg-data :meta))))
        remaining-arities (if arity
                            (disj (set (keys (dissoc agg-data :meta))) arity)
                            #{})]
    {:remove-arities arities-to-remove
     :remove-all?    (empty? remaining-arities)}))

(defn- clear-aggregate-arities
  [db name]
  (when-let [agg-data (get-aggregate db name)]
    (let [{:keys [remove-arities]} (build-removal-update agg-data nil)]
      (doseq [a    remove-arities
              :let [{:keys [flags]} (get agg-data a)]]
        (unregister-aggregate-callback db name a flags))
      (swap! (get-in db [:internal :app-aggregates])
        #(update % name (fn [agg-entry]
                          (reduce dissoc agg-entry remove-arities)))))))

(defn- aggregate-error-message [^Throwable e]
  (or (.getMessage e)
    (str "Unexpected " (.getSimpleName (class e)))))

(defn wrap-aggregate-step
  [step-f initial-state state-by-context]
  (fn [context argc argv]
    (try
      (let [ctx-ptr (api/aggregate-context context 8)]
        (if (mem/null? ctx-ptr)
          (api/result-error context "Failed to allocate aggregate context")
          (let [ctx-key     (Long/valueOf (mem/address-of ctx-ptr))
                args        (funcs/deserialize-argv argv argc)
                clj-args    (mapv funcs/value->clj args)
                new-state   (AtomicReference. initial-state)
                state-ref   (or (.putIfAbsent state-by-context ctx-key new-state)
                              new-state)
                next-state  (apply step-f (.get state-ref) clj-args)]
            (.set state-ref next-state))))
      (catch Throwable e
        (when-let [ctx-ptr (let [ptr (api/aggregate-context context 0)]
                             (when-not (mem/null? ptr) ptr))]
          (.remove state-by-context (Long/valueOf (mem/address-of ctx-ptr))))
        (api/result-error context (aggregate-error-message e))))))

(defn wrap-aggregate-final
  [final-f initial-state initial-state? state-by-context]
  (fn [context]
    (try
      (let [ctx-ptr    (api/aggregate-context context 0)
            state-ref  (when-not (mem/null? ctx-ptr)
                         (.remove state-by-context
                           (Long/valueOf (mem/address-of ctx-ptr))))
            has-state? (or (some? state-ref) initial-state?)]
        (if has-state?
          (let [state     (if state-ref (.get ^AtomicReference state-ref) initial-state)
                result    (final-f state)
                result-fn (funcs/result->result-fn result)]
            (result-fn context result))
          (api/result-null context)))
      (catch Throwable e
        (api/result-error context (aggregate-error-message e))))))

(defn aggregate-arities
  [step-f {:keys [arity] :as opts}]
  (if (contains? opts :arity)
    (cond
      (= arity -1) [:variadic]
      (and (int? arity) (>= arity 0)) [arity]
      :else
      (throw (ex-info "Aggregate :arity must be an integer >= 0 or -1 for variadic"
               {:arity arity})))
    (let [step-arities (funcs/infer-arity step-f)]
      (when-not step-arities
        (throw (ex-info "Could not infer aggregate arity from step function"
                 {:step-fn step-f})))
      (mapv (fn [n]
              (if (= n :variadic)
                :variadic
                (let [sql-arity (dec n)]
                  (when (neg? sql-arity)
                    (throw (ex-info "Aggregate step function must accept at least a state argument"
                             {:step-arity n})))
                  sql-arity)))
        step-arities))))

(defn- do-register-aggregate
  [db name step-f final-f arities flags-bitmask
   {:keys [initial-state] :as opts}
   {:keys [step-source final-source watch-keys]}]
  (let [registrations          (vec
                                 (for [n arities]
                                   (let [arity              (if (= n :variadic) -1 n)
                                         state-by-context   (ConcurrentHashMap.)
                                         step-callback      (wrap-aggregate-step step-f initial-state state-by-context)
                                         step-callback-ptr  (mem/serialize step-callback
                                                              [::ffi/fn
                                                               [::mem/pointer ::mem/int ::mem/pointer]
                                                               ::mem/void
                                                               :raw-fn? true]
                                                              (mem/global-arena))
                                         final-callback     (wrap-aggregate-final final-f initial-state
                                                              (contains? opts :initial-state)
                                                              state-by-context)
                                         final-callback-ptr (mem/serialize final-callback
                                                              [::ffi/fn
                                                               [::mem/pointer]
                                                               ::mem/void
                                                               :raw-fn? true]
                                                              (mem/global-arena))]
                                     (funcs/doto-connections db
                                       (fn [conn]
                                         (let [pdb  (:pdb conn)
                                               code (api/create-function-v2 pdb name arity flags-bitmask
                                                      mem/null
                                                      mem/null
                                                      step-callback-ptr
                                                      final-callback-ptr
                                                      mem/null)]
                                           (when-not (api/sqlite-ok? code)
                                             (throw (api/sqlite-ex-info pdb code {:aggregate name}))))))
                                     {:arity arity
                                      :data  {:flags              flags-bitmask
                                              :step-callback      step-callback
                                              :step-callback-ptr  step-callback-ptr
                                              :final-callback     final-callback
                                              :final-callback-ptr final-callback-ptr
                                              :state-by-context   state-by-context}})))
        metadata               (when (or (var? step-source) (var? final-source))
                                 {:step-source step-source
                                  :final-source final-source
                                  :watch-keys watch-keys
                                  :opts opts})]
    (swap! (get-in db [:internal :app-aggregates])
      update name
      (fn [existing]
        (let [new-arities (into {}
                            (for [{:keys [arity data]} registrations]
                              [arity data]))]
          (cond-> (merge existing new-arities)
            metadata (assoc :meta metadata)))))))

(defn- aggregate-var-updated
  [db name opts step-source final-source watch-keys _watch-key _var _old-val _new-val]
  (clear-aggregate-arities db name)
  (let [step-f  (if (var? step-source) (var-get step-source) step-source)
        final-f (if (var? final-source) (var-get final-source) final-source)]
    (when (and (fn? step-f) (fn? final-f))
      (do-register-aggregate db name step-f final-f
        (aggregate-arities step-f opts)
        (funcs/function-flags->bitmask opts)
        opts
        {:step-source step-source
         :final-source final-source
         :watch-keys watch-keys}))))

(defn register-aggregate
  [db name step-f final-f & {:as opts}]
  (when-not (fn? step-f)
    (throw (ex-info "step-f must be a function" {:step-f step-f})))
  (when-not (fn? final-f)
    (throw (ex-info "final-f must be a function" {:final-f final-f})))
  (do-register-aggregate db name step-f final-f
    (aggregate-arities step-f opts)
    (funcs/function-flags->bitmask opts)
    opts
    {:step-source step-f
     :final-source final-f
     :watch-keys {}}))

(defn register-aggregate-vars
  [db name step-source final-source & {:as opts}]
  (let [step-f      (if (var? step-source) (var-get step-source) step-source)
        final-f     (if (var? final-source) (var-get final-source) final-source)
        watch-keys  {:step  (when (var? step-source)
                              (keyword (str "sqlite4clj-" name "-aggregate-step-var")))
                     :final (when (var? final-source)
                              (keyword (str "sqlite4clj-" name "-aggregate-final-var")))}]
    (when-not (fn? step-f)
      (throw (ex-info "step-f source must resolve to a function"
               {:step-source step-source})))
    (when-not (fn? final-f)
      (throw (ex-info "final-f source must resolve to a function"
               {:final-source final-source})))
    (do-register-aggregate db name step-f final-f
      (aggregate-arities step-f opts)
      (funcs/function-flags->bitmask opts)
      opts
      {:step-source step-source
       :final-source final-source
       :watch-keys watch-keys})
    (when-let [watch-key (:step watch-keys)]
      (add-watch step-source watch-key
        (partial aggregate-var-updated db name opts step-source final-source watch-keys)))
    (when-let [watch-key (:final watch-keys)]
      (add-watch final-source watch-key
        (partial aggregate-var-updated db name opts step-source final-source watch-keys)))))

(defn create-aggregate
  [db name step-f-or-var final-f-or-var & {:as opts}]
  (if (or (var? step-f-or-var) (var? final-f-or-var))
    (register-aggregate-vars db name step-f-or-var final-f-or-var opts)
    (register-aggregate db name step-f-or-var final-f-or-var opts)))

(defn remove-aggregate
  ([db name]
   (remove-aggregate db name nil))
  ([db name arity]
   (when-let [agg-data (get-aggregate db name)]
     (let [{:keys [remove-arities remove-all?]} (build-removal-update agg-data arity)]
       (doseq [a    remove-arities
               :let [{:keys [flags]} (get agg-data a)]]
         (unregister-aggregate-callback db name a flags))
       (when (and remove-all? (:meta agg-data))
         (let [{:keys [step-source final-source watch-keys]} (:meta agg-data)]
           (when-let [watch-key (:step watch-keys)]
             (remove-watch step-source watch-key))
           (when-let [watch-key (:final watch-keys)]
             (remove-watch final-source watch-key))))
       (swap! (get-in db [:internal :app-aggregates])
         (if remove-all?
           #(dissoc % name)
           #(update % name (fn [agg-entry]
                             (reduce dissoc agg-entry remove-arities)))))))))

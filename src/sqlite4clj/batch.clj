(ns sqlite4clj.batch)

(defn post-transaction-deliver [results_ p thunk-result]
  (swap! results_ conj [p thunk-result]))

(defn async-batcher-init!
  "Creates an async batching process that dynamically batches writes.

   A batch-fn operates on a batch of thunks (functions) that can be passed
   the db plus any other context (like a cache) and generally describes an
   atomic sequence of reads/writes.

   Example batch-fn function:

   (fn batch-fn [writer batch]
     (with-write-tx [db writer]
       (run! (fn [thunk] (thunk db)) batch)))

  This can be used to increase write throughput by batching writes in
  a single transaction. Use SQLite's SAVEPOINTS and ROLLBACK for logically
  nested transactions. As well as an entry point for coarse or fine grained
  subscriptions to changes.

  Returns a function tx! that lets you dispatch async batch writes. By default
  tx! returns a promise that will deliver the result after the whole batch
  has run."
  [db & {:keys [batch-fn return-promise? max-batch-size]
         :or   {max-batch-size  10000
                ;; If true tx! returns a promise
                return-promise? true}}]
  ;; Using an atom and subvec is similar to performance to a
  ;; ConcurrentLinkedQueue. However, it allows for a cleaner batch
  ;; function interface as you can provide more context (like a cache)
  ;; and subsequent iterations (e.g: reduce over changes).
  (assert (not (nil? batch-fn)))
  (let [batch-max-size max-batch-size
        batch_         (atom [])
        batch-results_ (atom [])]
    (Thread/startVirtualThread
      (bound-fn* ;; binding conveyance
        (fn batch-thread []
          (while (not (Thread/interrupted))
            (if (= (count @batch_) 0)
              (Thread/sleep 1) ;; Allows other vthreads to run
              (let [[old-b new-b] (swap-vals! batch_
                                    (fn [batch]
                                      (subvec batch
                                        (min batch-max-size (count batch)))))]
                ;; Runs batch on virtual thread, but because there is only
                ;; one writer, this at most CPU pins a single core.
                ;; The benefit of this being a virtual thread is there's no
                ;; overhead for idle batchers so you can have multiple
                ;; sqlite db batcher without incurring overhead.
                ;; A batch-fn can always execute work on it's own CPU thread
                ;; if that's desirable.
                (batch-fn (db :writer)
                  (subvec old-b 0 (- (count old-b) (count new-b))))
                ;; TODO: how to signal batch-fn has failed? Or rolled back?
                (when-not (= (count @batch-results_) 0)
                  (let [[results _] (reset-vals! batch-results_ [])]
                    (run! (fn [[p result]] (deliver p result)) results)))))))))
    (if return-promise?
      ;; Returns promise after the whole batch has completed.
      (fn tx! [thunk]
        (let [p (promise)]
          (swap! batch_ conj
            (comp
              (partial post-transaction-deliver batch-results_ p)
              thunk))
          p))
      (fn tx! [thunk] (swap! batch_ conj thunk) nil))))



(ns sqlite4clj.impl.functions-aggregates-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [sqlite4clj.core :as d]
   [sqlite4clj.impl.api :as api]
   [sqlite4clj.impl.functions-aggregates :as aggs]
   [sqlite4clj.test-common :refer [test-db test-fixture with-db]]))

(use-fixtures :once test-fixture)

(deftest basic-aggregate-registration
  (testing "Can register and call an aggregate on writer and reader pools"
    (with-db [db (test-db)]
      (d/q (:writer db) ["CREATE TABLE agg_basic_nums (n INTEGER)"])
      (d/q (:writer db) ["INSERT INTO agg_basic_nums VALUES (1), (2), (3), (4)"])
      (d/create-aggregate db "sum_n"
                          (fn [state n]
                            (+ (or state 0) n))
                          (fn [state] state)
                          {:deterministic? true})

      (is (= [10] (d/q (:writer db) ["SELECT sum_n(n) FROM agg_basic_nums"])))
      (is (= [10] (d/q (:reader db) ["SELECT sum_n(n) FROM agg_basic_nums"]))))))

(deftest aggregate-type-handling
  (testing "Aggregate step receives decoded SQLite values"
    (with-db [db (test-db)]
      (d/q (:writer db) ["CREATE TABLE agg_type_data (v BLOB)"])
      (d/q (:writer db) ["INSERT INTO agg_type_data VALUES (42), (3.14), ('hello'), (NULL), (?)"
                         (byte-array [1 2 3])])

      (d/create-aggregate db "collect_types"
                          (fn [state v]
                            (conj (or state [])
                                  (cond
                                    (nil? v) "NULL"
                                    (integer? v) "INTEGER"
                                    (double? v) "REAL"
                                    (string? v) "TEXT"
                                    (bytes? v) "BLOB"
                                    :else "UNKNOWN")))
                          (fn [state] state))

      (is (= [["INTEGER" "REAL" "TEXT" "NULL" "BLOB"]]
             (d/q (:writer db) ["SELECT collect_types(v) FROM (SELECT v FROM agg_type_data ORDER BY rowid)"]))))))

(deftest aggregate-arity-inference
  (testing "Arity is inferred from step signature and supports variadic callbacks"
    (with-db [db (test-db)]
      (d/q (:writer db) ["CREATE TABLE agg_arity_nums (n INTEGER)"])
      (d/q (:writer db) ["INSERT INTO agg_arity_nums VALUES (1), (2), (3)"])

      (d/create-aggregate db "count_rows"
                          (fn [state]
                            (inc (or state 0)))
                          (fn [state] state))
      (is (= [3] (d/q (:writer db) ["SELECT count_rows() FROM agg_arity_nums"])))

      (d/create-aggregate db "sum_pairs"
                          (fn [state a b]
                            (+ (or state 0) a b))
                          (fn [state] state))
      (is (= [12] (d/q (:writer db) ["SELECT sum_pairs(n, n) FROM agg_arity_nums"])))

      (d/create-aggregate db "count_variadic"
                          (fn [state & args]
                            (+ (or state 0) (count args)))
                          (fn [state] state))
      (is (= [6] (d/q (:writer db) ["SELECT count_variadic(n, n) FROM agg_arity_nums"])))

      (d/create-aggregate db "count_variadic_arity"
                          (fn [state & args]
                            (+ (or state 0) (count args)))
                          (fn [state] state)
                          {:arity 1})
      (is (= [3] (d/q (:writer db) ["SELECT count_variadic_arity(n) FROM agg_arity_nums"]))))))

(deftest aggregate-initial-state-and-empty-input
  (testing "Empty input returns NULL by default and finalized initial state when provided"
    (with-db [db (test-db)]
      (d/create-aggregate db "sum_empty_default"
                          (fn [state n]
                            (+ (or state 0) n))
                          (fn [state] state))

      (d/create-aggregate db "sum_empty_init"
                          (fn [state n]
                            (+ (or state 0) n))
                          (fn [state] state)
                          {:initial-state 10})

      (is (= [nil]
             (d/q (:writer db) ["SELECT sum_empty_default(n) FROM (SELECT 1 AS n WHERE 0)"])))
      (is (= [10]
             (d/q (:writer db) ["SELECT sum_empty_init(n) FROM (SELECT 1 AS n WHERE 0)"]))))))

(deftest aggregate-exception-handling
  (testing "Step and final callback exceptions are surfaced as SQLite errors"
    (with-db [db (test-db)]
      (d/q (:writer db) ["CREATE TABLE agg_exception_nums (n INTEGER)"])
      (d/q (:writer db) ["INSERT INTO agg_exception_nums VALUES (1), (2), (3)"])

      (d/create-aggregate db "explode_step"
                          (fn [state n]
                            (if (= n 2)
                              (throw (Exception. "explode step"))
                              (+ (or state 0) n)))
                          (fn [state] state))
      (is (thrown-with-msg? Exception #"explode step"
                            (d/q (:writer db) ["SELECT explode_step(n) FROM agg_exception_nums"])))

      (d/create-aggregate db "explode_final"
                          (fn [state n]
                            (+ (or state 0) n))
                          (fn [_]
                            (throw (Exception. "explode final"))))
      (is (thrown-with-msg? Exception #"explode final"
                            (d/q (:writer db) ["SELECT explode_final(n) FROM agg_exception_nums"]))))))

(defn get-flags [db name arity]
  (:flags (aggs/get-aggregate db name arity)))

(defn has-flag? [db name arity flag]
  (let [flags (get-flags db name arity)]
    (and flags (bit-test flags flag))))

(deftest aggregate-flags
  (testing "Aggregate flags are captured in registration metadata"
    (with-db [db (test-db)]
      (d/create-aggregate db "all_agg_flags"
                          (fn [state x]
                            (+ (or state 0) x))
                          (fn [state] state)
                          {:deterministic? true
                           :innocuous? true
                           :direct-only? true
                           :sub-type? true
                           :result-sub-type? true
                           :self-order1? true})
      (doseq [flag [api/SQLITE_DETERMINISTIC
                    api/SQLITE_INNOCUOUS
                    api/SQLITE_DIRECTONLY
                    api/SQLITE_SUBTYPE
                    api/SQLITE_RESULT_SUBTYPE
                    api/SQLITE_SELFORDER1]]
        (is (has-flag? db "all_agg_flags" 1 flag))))))

(deftest removing-aggregates
  (testing "Aggregates can be removed by arity or by name"
    (with-db [db (test-db)]
      (d/q (:writer db) ["CREATE TABLE agg_remove_nums (n INTEGER)"])
      (d/q (:writer db) ["INSERT INTO agg_remove_nums VALUES (1), (2), (3)"])

      (d/create-aggregate db "sum_any"
                          (fn [state x]
                            (+ (or state 0) x))
                          (fn [state] state))
      (d/create-aggregate db "sum_any"
                          (fn [state x y]
                            (+ (or state 0) x y))
                          (fn [state] state))

      (is (= [6] (d/q (:writer db) ["SELECT sum_any(n) FROM agg_remove_nums"])))
      (is (= [12] (d/q (:writer db) ["SELECT sum_any(n, n) FROM agg_remove_nums"])))
      (is (some? (aggs/get-aggregate db "sum_any" 1)))
      (is (some? (aggs/get-aggregate db "sum_any" 2)))

      (d/remove-aggregate db "sum_any" 1)
      (is (nil? (aggs/get-aggregate db "sum_any" 1)))
      (is (some? (aggs/get-aggregate db "sum_any" 2)))
      (is (= [12] (d/q (:writer db) ["SELECT sum_any(n, n) FROM agg_remove_nums"])))
      (is (thrown-with-msg? Exception #"wrong number of arguments to function sum_any\(\)"
                            (d/q (:writer db) ["SELECT sum_any(n) FROM agg_remove_nums"])))

      (d/remove-aggregate db "sum_any")
      (is (nil? (aggs/get-aggregate db "sum_any" 2)))
      (is (thrown-with-msg? Exception #"no such function: sum_any"
                            (d/q (:writer db) ["SELECT sum_any(n, n) FROM agg_remove_nums"]))))))

(deftest registering-aggregate-vars
  (testing "Aggregate vars can be redefined and stop updating after removal"
    #_{:clj-kondo/ignore [:inline-def]}
    (defn watched-agg-step [state x]
      (+ (or state 0) x))
    #_{:clj-kondo/ignore [:inline-def]}
    (defn watched-agg-final [state]
      state)
    (with-db [db (test-db)]
      (d/q (:writer db) ["CREATE TABLE agg_watch_nums (n INTEGER)"])
      (d/q (:writer db) ["INSERT INTO agg_watch_nums VALUES (1), (2), (3)"])

      (d/create-aggregate db "watched_sum" #'watched-agg-step #'watched-agg-final)
      (is (= [6] (d/q (:writer db) ["SELECT watched_sum(n) FROM agg_watch_nums"])))

      (alter-var-root #'watched-agg-step
                      (fn [_]
                        (fn [state x]
                          (+ (or state 0) (* 2 x)))))
      (is (= [12] (d/q (:writer db) ["SELECT watched_sum(n) FROM agg_watch_nums"])))

      (alter-var-root #'watched-agg-final
                      (fn [_]
                        (fn [state]
                          (inc state))))
      (is (= [13] (d/q (:writer db) ["SELECT watched_sum(n) FROM agg_watch_nums"])))

      (d/remove-aggregate db "watched_sum")
      (alter-var-root #'watched-agg-step
                      (fn [_]
                        (fn [state x]
                          (+ (or state 0) (* 3 x)))))
      (is (thrown-with-msg? Exception #"no such function: watched_sum"
                            (d/q (:writer db) ["SELECT watched_sum(n) FROM agg_watch_nums"]))))))

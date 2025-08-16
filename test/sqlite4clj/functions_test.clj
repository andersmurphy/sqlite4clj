(ns sqlite4clj.functions-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [sqlite4clj.api :as api]
   [sqlite4clj.core :as d]
   [sqlite4clj.functions :as funcs]
   [sqlite4clj.test-common :refer [test-db test-fixture with-db]]))

(use-fixtures :once test-fixture)

(deftest basic-function-registration
  (testing "Can register and call a simple deterministic function"
    (with-db [db (test-db)]
      (d/create-function db "double" (fn [v] (* 2 v)) {:deterministic? true})

      (is (= [10] (d/q (:writer db) ["SELECT double(5)"])))
      (is (= [10] (d/q (:reader db) ["SELECT double(5)"]))))))

(deftest type-handling

  (testing "Functions handle all SQLite types correctly"
    (with-db [db (test-db)]
      (d/create-function db "type_test"
                         (fn [v]
                           (cond
                             (nil? v) "NULL"
                             (integer? v) "INTEGER"
                             (float? v) "REAL"
                             (string? v) "TEXT"
                             (bytes? v) "BLOB"
                             :else
                             (do
                               (println "Unknown type:" v)
                               "UNKNOWN")))
                         {:deterministic? true})
      (is (= ["INTEGER"] (d/q (:writer db) ["SELECT type_test(42)"])))
      (is (= ["REAL"] (d/q (:writer db) ["SELECT type_test(3.14)"])))
      (is (= ["TEXT"] (d/q (:writer db) ["SELECT type_test('hello')"])))
      (is (= ["NULL"] (d/q (:writer db) ["SELECT type_test(NULL)"])))
      (d/q (:writer db) ["CREATE TABLE blob_test (data BLOB)"])
      (d/q (:writer db) ["INSERT INTO blob_test VALUES (?)" (byte-array [1 2 3])])
      (is (= ["BLOB"] (d/q (:writer db) ["SELECT type_test(data) FROM blob_test"]))))))

(deftest variadic-functions
  (testing "Variadic functions accept any number of arguments"
    (with-db [db (test-db)]
      (d/create-function db "count_args"
                         (fn [& args]
                           (count args)))

      (is (= [0] (d/q (:writer db) ["SELECT count_args()"])))
      (is (= [3] (d/q (:writer db) ["SELECT count_args(1, \"foo\", 3)"])))
      (is (= [5] (d/q (:writer db) ["SELECT count_args(1, 2, 3, 4, 5)"]))))))

(deftest multi-arity-functions
  (testing "Functions can have multiple arities"
    (with-db [db (test-db)]
      (d/create-function db "test_add"
                         (fn
                           ([] 0)
                           ([x] x)
                           ([x y] (+ x y))
                           ([x y & more] (reduce + (+ x y) more))))
      (is (= [0] (d/q (:writer db) ["SELECT test_add()"])))
      (is (= [5] (d/q (:writer db) ["SELECT test_add(5)"])))
      (is (= [8] (d/q (:writer db) ["SELECT test_add(3, 5)"])))
      (is (= [15] (d/q (:writer db) ["SELECT test_add(1, 2, 3, 4, 5)"]))))))

(deftest clojure-data-in-blobs
  (testing "Can process Clojure data structures stored as BLOBs"
    (with-db [db (test-db)]
      (d/q (:writer db) ["CREATE TABLE data (value BLOB)"])
      (d/create-function db "twiddle_map"
                         (fn [data]
                           (assoc data :twiddle true)))

      (is (= nil (d/q (:reader db) ["SELECT twiddle_map(value) FROM data"])))
      (d/q (:writer db) ["INSERT INTO data VALUES (?)" {:name "test" :count 5}])
      (is (= [{:name "test" :count 5 :twiddle true}] (d/q (:reader db) ["SELECT twiddle_map(value) FROM data"]))))))

(deftest exception-handling
  (testing "Functions that throw exceptions report errors properly"
    (with-db [db (test-db)]
      (d/create-function db "will_fail"
                         (fn []
                           (throw (Exception. "This function always fails"))))

      (is (thrown-with-msg? Exception #"This function always fails"
                            (d/q (:writer db) ["SELECT will_fail()"]))))))

(defn get-flags [db name arity]
  (:flags (funcs/get-function db name arity)))

(defn has-flag? [db name arity flag]
  (let [flags (get-flags db name arity)]
    (and flags (bit-test flags flag))))

(deftest function-flags
  (testing "Different SQLite function flags work correctly"
    (with-db [db (test-db)]
      (d/create-function db "deterministic_fn" (fn [] nil) {:deterministic? true})
      (is (has-flag? db "deterministic_fn" 0 api/SQLITE_DETERMINISTIC))
      (is (not (has-flag? db "innocuous_fn" 0 api/SQLITE_INNOCUOUS)))

      (d/create-function db "innocuous_fn" (fn [] nil) {:innocuous? true})
      (is (has-flag? db "innocuous_fn" 0 api/SQLITE_INNOCUOUS))

      (d/create-function db "direct-only_fn" (fn [] nil) {:direct-only? true})
      (is (has-flag? db "direct-only_fn" 0 api/SQLITE_DIRECTONLY))

      (d/create-function db "sub-type_fn" (fn [] nil) {:sub-type? true})
      (is (has-flag? db "sub-type_fn" 0 api/SQLITE_SUBTYPE))

      (d/create-function db "result-sub-type_fn" (fn [] nil) {:result-sub-type? true})
      (is (has-flag? db "result-sub-type_fn" 0 api/SQLITE_RESULT_SUBTYPE))

      (d/create-function db "self-order1_fn" (fn [] nil) {:self-order1? true})
      (is (has-flag? db "self-order1_fn" 0 api/SQLITE_SELFORDER1))

      (d/create-function db "all_flags_fn" (fn [] nil)
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
        (is (has-flag? db "all_flags_fn" 0 flag))))))

(deftest function-persistence-across-pools
  (testing "Functions are registered on all connections in all pools"
    (with-db [db (test-db)]
      (d/create-function db "pool_test" (fn [v] (* 3 v)))

      (is (= [15] (d/q (:writer db) ["SELECT pool_test(5)"])))

      (is (= [15] (d/q (:reader db) ["SELECT pool_test(5)"])))

      (doall (pmap (fn [_]
                     (Thread/sleep 100)
                     (is (= [15] (d/q (:reader db) ["SELECT pool_test(5)"])))) (range 5))))))

(deftest function-returning-different-types
  (testing "All return types are supported correctly"
    (with-db [db (test-db)]
      ;; Return NULL
      (d/create-function db "return_null" (fn [] nil))
      (is (= [nil] (d/q (:writer db) ["SELECT return_null()"])))

      ;; Return INTEGER
      (d/create-function db "return_int" (fn [] 42))

      (is (= [42] (d/q (:writer db) ["SELECT return_int()"])))

      ;; Return DOUBLE
      (d/create-function db "return_real" (fn [] 3.14159))
      (is (= [3.14159] (d/q (:writer db) ["SELECT return_real()"])))

      ;; Return TEXT
      (d/create-function db "return_text" (fn [] "hello world"))
      (is (= ["hello world"] (d/q (:writer db) ["SELECT return_text()"])))

      ;; Return BLOB (raw bytes)
      (d/create-function db "return_blob" (fn [] (byte-array [1 2 3 4 5])))

      (d/q (:writer db) ["CREATE TABLE blob_result (data BLOB)"])
      (d/q (:writer db) ["INSERT INTO blob_result VALUES (return_blob())"])
      (let [result (d/q (:reader db) ["SELECT data FROM blob_result"])]
        (is (bytes? (first result)))
        (is (= [1 2 3 4 5] (vec (first result)))))

      ;; Return BLOB (Clojure data)
      (d/create-function db "return_clj_blob" (fn [] {:type "clojure" :value 123}))
      (d/q (:writer db) ["CREATE TABLE clj_blob_result (data BLOB)"])
      (d/q (:writer db) ["INSERT INTO clj_blob_result VALUES (return_clj_blob())"])
      (is (= [{:type "clojure" :value 123}]
             (d/q (:reader db) ["SELECT data FROM clj_blob_result"]))))))

(deftest function-in-queries
  (testing "Custom functions work in WHERE clauses and complex queries"
    (with-db [db (test-db)]
      (d/q (:writer db) ["CREATE TABLE items (id INTEGER, price REAL)"])
      (d/q (:writer db) ["INSERT INTO items VALUES (1, 10.0), (2, 20.0), (3, 30.0)"])

      (d/create-function db "with_tax" (fn [price] (* price 1.1)))

      ;; Use in SELECT
      (is (= [11.0 22.0 33.0]
             (d/q (:reader db) ["SELECT with_tax(price) FROM items ORDER BY id"])))

      ;; Use in WHERE
      (is (= [[2 20.0]]
             (d/q (:reader db) ["SELECT * FROM items WHERE with_tax(price) = 22.0"]))))))

(deftest removing-functions
  (testing "Functions can be removed by single arity"
    (with-db [db (test-db)]
      (d/create-function db "temp_fn" (fn
                                        ([] 42)
                                        ([_] :woo))
                         {:innocuous? true})
      (is (some? (funcs/get-function db "temp_fn" 0)))
      (is (some? (funcs/get-function db "temp_fn" 1)))
      (is (= [42] (d/q (:writer db) ["SELECT temp_fn()"])))
      (is (= [:woo] (d/q (:writer db) ["SELECT temp_fn(0)"])))

      (d/remove-function db "temp_fn" 0)
      (is (nil? (funcs/get-function db "temp_fn" 0)))
      (is (some? (funcs/get-function db "temp_fn" 1)))
      (is (= [:woo] (d/q (:writer db) ["SELECT temp_fn(0)"])))
      (is (thrown-with-msg? Exception #"wrong number of arguments to function temp_fn()"
                            (d/q (:writer db) ["SELECT temp_fn()"])))))

  (testing "Functions can be removed by name removing all arities"
    (with-db [db (test-db)]
      (d/create-function db "temp_fn" (fn
                                        ([] 42)
                                        ([_] :woo))
                         {:innocuous? true})

      (d/create-function db "temp_fn" (fn [] 42))
      (is (some? (funcs/get-function db "temp_fn" 0)))
      (is (= [42] (d/q (:writer db) ["SELECT temp_fn()"])))

      (d/remove-function db "temp_fn")
      (is (nil? (funcs/get-function db "temp_fn" 0)))
      (is (nil? (funcs/get-function db "temp_fn" 1)))
      (is (thrown-with-msg? Exception #"no such function: temp_fn"
                            (d/q (:writer db) ["SELECT temp_fn()"])))
      (is (thrown-with-msg? Exception #"no such function: temp_fn"
                            (d/q (:writer db) ["SELECT temp_fn(0)"]))))))

(deftest registering-function-vars
  (testing "Can register a var, redef it twice, and remove the function"
    #_{:clj-kondo/ignore [:inline-def]}
    (defn my-double [v] (* 2 v))
    (with-db [db (test-db)]
      (d/create-function db "double" #'my-double {:deterministic? true})
      (is (= [10] (d/q (:writer db) ["SELECT double(5)"])))
      (alter-var-root #'my-double (fn [_]
                                    (fn [v] (* 3 v))))

      (is (= [15] (d/q (:writer db) ["SELECT double(5)"])))

      ;; this one will fail if the watch was removed!
      (alter-var-root #'my-double (fn [_] (fn [v] (* 4 v))))
      (is (= [20] (d/q (:writer db) ["SELECT double(5)"])))

      (d/remove-function db "double")
      (is (thrown-with-msg? Exception #"no such function: double"
                            (d/q (:writer db) ["SELECT double(5)"]))))))

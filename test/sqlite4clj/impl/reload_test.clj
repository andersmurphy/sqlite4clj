(ns sqlite4clj.impl.reload-test
  (:require
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.test :refer [deftest is]]
   [sqlite4clj.core :as d]
   [sqlite4clj.impl.api :as api]
   [sqlite4clj.session :as session]
   [sqlite4clj.test-common :refer [with-db]]))

(defn -main [& _]
  (with-db [before (d/init-db! ":memory:" {:pool-size 1})]
    (d/q (:writer before) ["CREATE TABLE retained (id INTEGER PRIMARY KEY)"])
    (d/q (:writer before) ["INSERT INTO retained VALUES (42)"])
    (let [library api/sqlite-library
          active-session (session/new-session (:writer before))]
      (try
        (doseq [namespaces [['sqlite4clj.impl.api]
                            ['sqlite4clj.impl.api 'sqlite4clj.session]]]
          (doseq [namespace namespaces]
            (require namespace :reload))
          ;; Fail before using native handles if a reload selects another copy.
          (assert (identical? library api/sqlite-library)
            "Namespace reload replaced the native library")
          (assert (= [42] (d/q (:writer before) ["SELECT id FROM retained"])))
          (with-db [after (d/init-db! ":memory:" {:pool-size 1})]
            (assert (= [43] (d/q (:writer after) ["SELECT 43"])))))
        (finally
          (session/cancel-session active-session)))))
  (println :reload-ok)
  (shutdown-agents))

(deftest namespace-reloads-preserve-native-library-and-handles
  ;; Isolate native crashes from the test runner if this behavior regresses.
  (let [{:keys [exit out err]}
        (shell/sh (str (io/file (System/getProperty "java.home") "bin" "java"))
          "--enable-native-access=ALL-UNNAMED"
          "-Dsqlite4clj.native-lib=bundled"
          "-cp" (System/getProperty "java.class.path")
          "clojure.main" "-m" "sqlite4clj.impl.reload-test")]
    (is (= {:exit 0 :out ":reload-ok\n"}
           {:exit exit :out out})
      err)))

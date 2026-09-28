(ns sqlite4clj.session-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [sqlite4clj.core :as d]
   [sqlite4clj.session :as session]
   [sqlite4clj.test-common :refer [test-db test-fixture with-db]]))

(use-fixtures :once test-fixture)

(deftest changesets-can-be-inspected-and-undone
  (with-db [db (test-db)]
    (d/q (:writer db) ["CREATE TABLE session_items (id INTEGER PRIMARY KEY)"])
    (d/with-conn [conn (:writer db)]
      (let [s (session/new-session conn)]
        (try
          (d/q conn ["INSERT INTO session_items VALUES (1)"])
          (is (= [["session_items" 1 "INSERT" false]]
                 (session/view-session-changeset conn s)))
          (session/undo-session conn s)
          (is (= [0] (d/q conn ["SELECT count(*) FROM session_items"])))
          (is (nil? @s))
          (finally
            (session/cancel-session s)))))))

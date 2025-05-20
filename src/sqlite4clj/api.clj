(ns sqlite4clj.api
  "These function map directly to SQLite's C API."
  (:require
   [coffi.mem :as mem]
   [coffi.ffi :as ffi :refer [defcfn]]))

(ffi/load-library "resources/sqlite3.so")

(defn sqlite-ok? [code]
  (= code 0))

(defcfn open-v2
  "sqlite3_open_v2" [::mem/c-string ::mem/pointer ::mem/int
                     ::mem/c-string] ::mem/int
  sqlite3-open-native
  [filename flags]
  (with-open [arena (mem/confined-arena)]
    (let [pdb    (mem/alloc-instance ::mem/pointer arena)
          result (sqlite3-open-native filename pdb flags nil)]
      (if (sqlite-ok? result)
        (mem/deserialize-from pdb ::mem/pointer)
        (throw (ex-info "Failed to open sqlite3 database"
                 {:filename filename}))))))

(defcfn close
  sqlite3_close
  [::mem/pointer] ::mem/int)

(defcfn prepare-v2
  "sqlite3_prepare_v2"
  [::mem/pointer ::mem/c-string ::mem/int
   ::mem/pointer ::mem/pointer] ::mem/int
  sqlite3-prepare-native
  [pdb sql]
  (with-open [arena (mem/confined-arena)]
    (let [ppStmt (mem/alloc-instance ::mem/pointer arena)
          code   (sqlite3-prepare-native pdb sql -1 ppStmt
                   nil)]
      (if (sqlite-ok? code)
        (mem/deserialize-from ppStmt ::mem/pointer)
        (throw (ex-info "Failed to create preparde statement"
                 {:stmt sql}))))))

(defcfn reset
  sqlite3_reset
  [::mem/pointer] ::mem/int)

(defcfn clear-bindings
  sqlite3_clear_bindings
  [::mem/pointer] ::mem/int)

(defcfn bind-int
  sqlite3_bind_int
  [::mem/pointer ::mem/int ::mem/int] ::mem/int)

(defcfn bind-double
  sqlite3_bind_double
  [::mem/pointer ::mem/int ::mem/double] ::mem/int)

(def sqlite-static (mem/as-segment 0))
(def sqlite-transient (mem/as-segment -1))

(defcfn bind-text
  "sqlite3_bind_text"
  [::mem/pointer ::mem/int ::mem/c-string ::mem/int
   [::ffi/fn [::mem/pointer] ::mem/void]] ::mem/int
  sqlite3-bind-text-native
  [pdb idx text]
  (let [text (str text)]
    (sqlite3-bind-text-native pdb idx text
      (count (String/.getBytes text "UTF-8"))
      sqlite-transient)))

(defcfn step
  sqlite3_step
  [::mem/pointer] ::mem/int)

(defcfn column-count
  sqlite3_column_count
  [::mem/pointer] ::mem/int)

(defcfn column-text
  sqlite3_column_text
  [::mem/pointer ::mem/int] ::mem/c-string)



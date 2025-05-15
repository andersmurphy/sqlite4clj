(ns app.core
  (:require
   [coffi.mem :as mem]
   [coffi.ffi :as ffi :refer [defcfn]]))

(ffi/load-library "resources/sqlite3.so")

(comment
  (defcfn sqlite3-open
    "sqlite3_open" [::mem/c-string ::mem/pointer] ::mem/int
    sqlite3-open-native
    [filename]
    (with-open [arena (mem/confined-arena)]
      (let [pdb    (mem/alloc-instance ::mem/pointer arena)
            result (sqlite3-open-native filename pdb)]
        result))))

(comment
  (defcfn sqlite3-open
    "sqlite3_open" [::mem/c-string ::mem/pointer] ::mem/int
    sqlite3-open-native
    [filename db-ptr]
    (let [pdb (mem/serialize db-ptr [::mem/pointer ::mem/pointer])]
      (sqlite3-open-native filename pdb)
      (mem/deserialize pdb [::mem/pointer ::mem/pointer]))))

(def pdb-arena (mem/auto-arena))

(defcfn sqlite3-open
  "sqlite3_open" [::mem/c-string ::mem/pointer] ::mem/int
  sqlite3-open-native
  [filename pdb-arena]
  (let [pdb    (mem/alloc-instance ::mem/pointer pdb-arena)
        result (sqlite3-open-native filename pdb)]
    result))

(comment
  (sqlite3-open "database.db" pdb-arena)
  )



(comment
  )




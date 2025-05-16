(ns app.core
  (:require
   [coffi.mem :as mem]
   [coffi.ffi :as ffi :refer [defcfn]]))

(ffi/load-library "resources/sqlite3.so")

(defn sqlite-ok? [code]
  (= code 0))

(defcfn sqlite3-open
  "sqlite3_open" [::mem/c-string ::mem/pointer] ::mem/int
  sqlite3-open-native
  [filename]
  (with-open [arena (mem/confined-arena)]
    (let [pdb    (mem/alloc-instance ::mem/pointer arena)
          result (sqlite3-open-native filename pdb)]
      (if (sqlite-ok? result)
        (mem/deserialize-from pdb ::mem/pointer)
        (throw (ex-info "Failed to open sqlite3 database"
                 {:filename filename}))))))

(defcfn sqlite3-close
  sqlite3_close
  [::mem/pointer] ::mem/int)

(defn wrap-callback [callback]
  (fn [_ c-n c-name c-text]
    (callback
      (mem/deserialize-from
        (mem/reinterpret c-name
          (mem/size-of [::mem/array ::mem/c-string c-n]))
        [::mem/array ::mem/c-string c-n])
      (mem/deserialize-from
        (mem/reinterpret c-text
          (mem/size-of [::mem/array ::mem/c-string c-n]))
        [::mem/array ::mem/c-string c-n]))
    0))

(defcfn sqlite3-exec
  "sqlite3_exec"
  [::mem/pointer ::mem/c-string
   [::ffi/fn [::mem/pointer
              ::mem/int
              ::mem/pointer
              ::mem/pointer]
    ::mem/int]
   ::mem/pointer ::mem/pointer] ::mem/int
  sqlite3-exec-native
  [pdb sql callback]
  (with-open [arena (mem/confined-arena)]
    (let [notused-ptr (mem/serialize 0 [::mem/pointer ::mem/int])
          errmsg-ptr  (mem/alloc-instance ::mem/c-string arena)
          code        (sqlite3-exec-native
                        pdb sql (wrap-callback callback)
                        notused-ptr errmsg-ptr)]
      (if (sqlite-ok? code)
        code
        (throw
          (ex-info "SQL error"
            {:error
             (mem/deserialize-from errmsg-ptr ::mem/c-string)}))))))

(comment

  (def *pdb (sqlite3-open "database.db"))
  (sqlite3-exec *pdb
    (str      
      "pragma cache_size = 15625;"
      "pragma page_size = 4096;"
      "pragma journal_mode = WAL;"
      "pragma synchronous = NORMAL;"
      "pragma temp_store = MEMORY;")
    (fn [_ _]))

  (sqlite3-exec *pdb
    (str      
      "pragma cache_size;"
      "pragma page_size;"
      "pragma journal_mode;"
      "pragma synchronous;"
      "pragma temp_store;"
      "pragma mmap_size;")
    (fn [c-name c-text]
      (prn [c-text c-name])))
  
  ;; jdbc no pool:  191.458653 µs
  ;; raw no pool:    26.324366 µs
  ;; jdbc with pool: 13.057478 µs
  
  (user/bench
    (sqlite3-exec *pdb "PRAGMA table_list;"
      (fn [_ _] )))

  )




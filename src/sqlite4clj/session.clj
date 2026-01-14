(ns sqlite4clj.session
  (:require
   [coffi.ffi :as ffi :refer [defcfn]]
   [coffi.mem :as mem]
   [sqlite4clj.impl.api :as api]
   [sqlite4clj.core :as d]))

;; -----------------------------
;; SESSION extension
;; https://sqlite.org/sessionintro.html

(defcfn session-create
  "sqlite3session_create"
  [::mem/pointer ::mem/c-string ::mem/pointer] ::mem/int
  sqlite3session-create-native
  [pdb]
  (with-open [arena (mem/confined-arena)]
    (let [ppSession (mem/alloc-instance ::mem/pointer arena)
          code      (sqlite3session-create-native pdb "main" ppSession)]
      (if (api/sqlite-ok? code)
        (mem/deserialize-from ppSession ::mem/pointer)
        (throw (api/sqlite-ex-info pdb code {}))))))

(defcfn session-attach
  sqlite3session_attach
  [::mem/pointer ::mem/c-string] ::mem/int)

(defcfn session-delete
  sqlite3session_delete
  [::mem/pointer] ::mem/int)

(defcfn session-changeset
  "sqlite3session_changeset"
  [::mem/pointer ::mem/pointer ::mem/pointer] ::mem/int
  sqlite3session-patchset-native
  [pdb pSession]
  (with-open [arena (mem/confined-arena)]
    (let [pnPatchset (mem/alloc-instance ::mem/int arena)
          ppPatchset (mem/alloc-instance ::mem/pointer arena)
          code       (sqlite3session-patchset-native pSession
                       pnPatchset
                       ppPatchset)]
      (if (api/sqlite-ok? code)
        [(mem/deserialize-from pnPatchset ::mem/int)
         (mem/deserialize-from ppPatchset ::mem/pointer)]
        (throw (api/sqlite-ex-info pdb code {}))))))

(defcfn changeset-invert
  "sqlite3changeset_invert"
  [::mem/int ::mem/pointer
   ::mem/pointer ::mem/pointer] ::mem/int
  sqlite3changeset-invert-native
  [pdb nInSet pInSet]
  (with-open [arena (mem/confined-arena)]
    (let [pnOutSet (mem/alloc-instance ::mem/int arena)
          ppOutSet (mem/alloc-instance ::mem/pointer arena)
          code     (sqlite3changeset-invert-native
                     nInSet pInSet
                     pnOutSet ppOutSet)]
      (if (api/sqlite-ok? code)
        [(mem/deserialize-from pnOutSet ::mem/int)
         (mem/deserialize-from ppOutSet ::mem/pointer)]
        (throw (api/sqlite-ex-info pdb code {}))))))

(defcfn changeset-apply
  sqlite3changeset_apply
  [::mem/pointer ;; db
   ::mem/int     ;; size of changeset
   ::mem/pointer ;; changeset
   ::mem/pointer ;; xFilter
   ::mem/pointer ;; xConflict
   ::mem/pointer ;; First arg to conflict
   ] ::mem/int)

(defcfn session-create
  "sqlite3session_create"
  [::mem/pointer ::mem/c-string ::mem/pointer] ::mem/int
  sqlite3session-create-native
  [pdb]
  (with-open [arena (mem/confined-arena)]
    (let [ppSession (mem/alloc-instance ::mem/pointer arena)
          code      (sqlite3session-create-native pdb "main" ppSession)]
      (if (api/sqlite-ok? code)
        (mem/deserialize-from ppSession ::mem/pointer)
        (throw (api/sqlite-ex-info pdb code {}))))))

(defn new-session
  "Creates a session and attaches it to the database."
  [conn]
  (d/with-conn [conn conn]
    (let [pdb      (:pdb conn)
          pSession (session-create pdb)]
      (session-attach pSession nil)
      (atom pSession))))

(defn cancel-session
  "Cancels session without undoing changes."
  [session]
  (when-let [session @session]
    (session-delete session)))

(defn undo-session
  "Undoes the current session and deletes it."
  [conn session]
  (d/with-conn [conn conn]
    (when-let [pSession @session]
      (let [pdb                     (:pdb conn)
            [nSet pSet]             (session-changeset pdb pSession)
            _                       (session-delete pSession)
            [nInvertSet pInvertSet] (changeset-invert pdb nSet pSet)]
        (with-open [arena (mem/confined-arena)]
          (let [x-conflict
                ;; Fails if there's a conflict (there should never be a conflict)
                ;; when using undo-session correctly.
                (mem/serialize (fn [_ _ _] (int 0))
                  [::ffi/fn
                   [::mem/pointer ::mem/int ::mem/pointer]
                   ::mem/int
                   :raw-fn? true]
                  arena)]
            (changeset-apply pdb nInvertSet pInvertSet nil x-conflict nil)))
        (api/free pSet)
        (api/free pInvertSet)
        (reset! session nil)))))

(defcfn changeset-start
  "sqlite3changeset_start"
  [::mem/pointer ;; changeset iterator
   ::mem/int     ;; size of changeset
   ::mem/pointer ;; changeset
   ] ::mem/int
  sqlite3session-changeset-start-native
  [pdb nSet pSet]
  (with-open [arena (mem/confined-arena)]
    (let [ppChangesetIter (mem/alloc-instance ::mem/pointer arena)
          code            (sqlite3session-changeset-start-native
                            ppChangesetIter nSet pSet)]
      (if (api/sqlite-ok? code)
        (mem/deserialize-from ppChangesetIter ::mem/pointer)
        (throw (api/sqlite-ex-info pdb code {}))))))

(defcfn changeset-next
  sqlite3changeset_next [::mem/pointer] ::mem/int)

(def op->statemen {18 "INSERT" 9  "DELETE" 23 "UPDATE"})

(defcfn changeset-op
  "sqlite3changeset_op"
  [::mem/pointer ;; IN: changeset iterator
   ::mem/pointer ;; OUT: table name
   ::mem/pointer ;; OUT: number of columns in table
   ::mem/pointer ;; OUT: statement
   ::mem/pointer ;; OUT: indirect change
   ] ::mem/int
  sqlite3changeset-op-native
  [pdb pChangesetIter]
  (with-open [arena (mem/confined-arena)]
    (let [pzTab      (mem/alloc-instance ::mem/pointer arena)
          pnCol      (mem/alloc-instance ::mem/pointer arena)
          pOp        (mem/alloc-instance ::mem/pointer arena)
          pbIndirect (mem/alloc-instance ::mem/pointer arena)

          code (sqlite3changeset-op-native
                 pChangesetIter pzTab pnCol pOp pbIndirect)]
      (if (api/sqlite-ok? code)
        [(mem/deserialize-from pzTab      ::mem/c-string)
         (mem/deserialize-from pnCol      ::mem/int)
         (-> (mem/deserialize-from pOp        ::mem/int)
             op->statemen)
         (if (= (mem/deserialize-from pbIndirect ::mem/int) 0)
           false true)]
        (throw (api/sqlite-ex-info pdb code {}))))))

(defcfn changeset-finalize
  sqlite3changeset_finalize [::mem/pointer] ::mem/int)

(defn view-session-changeset
  "Returns changeset data from session as edn."
  [conn session]
  (d/with-conn [conn conn]
    (when-let [pSession @session]
      (let [pdb         (:pdb conn)
            [nSet pSet] (session-changeset pdb pSession)
            pIter       (changeset-start pdb nSet pSet)
            ret         (loop [ret (transient [])]
                          (let [code (int
                                       #_{:clj-kondo/ignore [:type-mismatch]}
                                       (changeset-next pIter))]
                            (case code
                              ;; TODO:
                              100 (recur (conj! ret
                                           (changeset-op pdb pIter)))
                              101 (persistent! ret)
                              (throw (api/sqlite-ex-info
                                       conn code {})))))]
        (changeset-finalize pIter)
        (api/free pSet)
        ret))))

(comment

  (defonce db
    (d/init-db! "database.db"
      {:read-only true
       :pool-size 4
       :pragma    {:foreign_keys false}}))

  (d/q (db :writer)
    ["CREATE TABLE IF NOT EXISTS bar(id INT PRIMARY KEY, data BLOB)"])

  (let [session (d/with-conn [conn (:writer db)]
                  (new-session conn))]
    (println (d/q (:reader db) ["select count(*) from bar"]))
    (d/q (:writer db)
      ["insert into bar (id, data) VALUES (?, ?)"
       (str (random-uuid)) 34])
    (println (d/q (:reader db) ["select count(*) from bar"]))
    (d/with-conn [conn (:writer db)]      
    (clojure.pprint/pprint (view-session-changeset conn session))
      (undo-session conn session))
    (println (d/q (:reader db) ["select count(*) from bar"])))
  
  (let [old-sesion (new-session (:writer db))]
    (d/with-write-tx [conn (:writer db)]
      (undo-session conn old-sesion)
      (new-session conn)))
  )

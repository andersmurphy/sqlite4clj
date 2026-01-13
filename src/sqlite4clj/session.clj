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

(comment

  (defonce db
    (d/init-db! "database.db"
      {:read-only true
       :pool-size 4
       :pragma    {:foreign_keys false}}))

  (def reader  (db :reader))
  (def writer (db :writer))

  (d/q writer
    ["CREATE TABLE IF NOT EXISTS bar(id INT PRIMARY KEY, data BLOB)"])

  (let [session (d/with-conn [conn (:writer db)]
                  (new-session conn))]
    (println (d/q (:reader db) ["select count(*) from bar"]))
    (d/q (:writer db)
      ["insert into bar (id, data) VALUES (?, ?)"
       (str (random-uuid)) 34])
    (println (d/q (:reader db) ["select count(*) from bar"]))
    (d/with-conn [conn (:writer db)]
      (undo-session conn session))
    (println (d/q (:reader db) ["select count(*) from bar"])))

  
  (let [old-sesion (new-session (:writer db))]
    (d/with-write-tx [conn (:writer db)]
      (undo-session conn old-sesion)
      (new-session conn)))

  (+ 3 4)
  )

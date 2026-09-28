(ns sqlite4clj.session
  (:require
   [babashka.ffi :as ffi]
   [sqlite4clj.core :as d]
   [sqlite4clj.impl.api :as api]))

;; -----------------------------
;; SESSION extension
;; https://sqlite.org/sessionintro.html

(ffi/defcfn session-create {:library api/sqlite-library}
  "sqlite3session_create"
  [:pointer :string :pointer] :int
  sqlite3session-create-native
  [pdb]
  (with-open [arena (ffi/confined-arena)]
    (let [ppSession (ffi/alloc arena :pointer)
          code      (sqlite3session-create-native pdb "main" ppSession)]
      (if (api/sqlite-ok? code)
        (ffi/read ppSession :pointer)
        (throw (api/sqlite-ex-info pdb code {}))))))

(ffi/defcfn session-attach {:library api/sqlite-library}
  "sqlite3session_attach"
  [:pointer :string] :int)

(ffi/defcfn session-delete {:library api/sqlite-library}
  "sqlite3session_delete"
  [:pointer] :void)

(ffi/defcfn session-changeset {:library api/sqlite-library}
  "sqlite3session_changeset"
  [:pointer :pointer :pointer] :int
  sqlite3session-patchset-native
  [pdb pSession]
  (with-open [arena (ffi/confined-arena)]
    (let [pnPatchset (ffi/alloc arena :int)
          ppPatchset (ffi/alloc arena :pointer)
          code       (sqlite3session-patchset-native pSession
                       pnPatchset
                       ppPatchset)]
      (if (api/sqlite-ok? code)
        [(ffi/read pnPatchset :int)
         (ffi/read ppPatchset :pointer)]
        (throw (api/sqlite-ex-info pdb code {}))))))

(ffi/defcfn changeset-invert {:library api/sqlite-library}
  "sqlite3changeset_invert"
  [:int :pointer
   :pointer :pointer] :int
  sqlite3changeset-invert-native
  [pdb nInSet pInSet]
  (with-open [arena (ffi/confined-arena)]
    (let [pnOutSet (ffi/alloc arena :int)
          ppOutSet (ffi/alloc arena :pointer)
          code     (sqlite3changeset-invert-native
                     nInSet pInSet
                     pnOutSet ppOutSet)]
      (if (api/sqlite-ok? code)
        [(ffi/read pnOutSet :int)
         (ffi/read ppOutSet :pointer)]
        (throw (api/sqlite-ex-info pdb code {}))))))

(ffi/defcfn changeset-apply {:library api/sqlite-library}
  "sqlite3changeset_apply"
  [:pointer ;; db
   :int     ;; size of changeset
   :pointer ;; changeset
   :pointer ;; xFilter
   :pointer ;; xConflict
   :pointer ;; First arg to conflict
   ] :int)

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
        (with-open [arena (ffi/confined-arena)]
          (let [x-conflict
                ;; Fails if there's a conflict (there should never be a conflict)
                ;; when using undo-session correctly.
                (ffi/callback arena (fn [_ _ _] (int 0))
                  [:pointer :int :pointer] :int)]
            (changeset-apply pdb nInvertSet pInvertSet nil x-conflict nil)))
        (api/free pSet)
        (api/free pInvertSet)
        (reset! session nil)))))

(ffi/defcfn changeset-start {:library api/sqlite-library}
  "sqlite3changeset_start"
  [:pointer ;; changeset iterator
   :int     ;; size of changeset
   :pointer ;; changeset
   ] :int
  sqlite3session-changeset-start-native
  [pdb nSet pSet]
  (with-open [arena (ffi/confined-arena)]
    (let [ppChangesetIter (ffi/alloc arena :pointer)
          code            (sqlite3session-changeset-start-native
                            ppChangesetIter nSet pSet)]
      (if (api/sqlite-ok? code)
        (ffi/read ppChangesetIter :pointer)
        (throw (api/sqlite-ex-info pdb code {}))))))

(ffi/defcfn changeset-next {:library api/sqlite-library}
  "sqlite3changeset_next" [:pointer] :int)

(def op->statemen {18 "INSERT" 9  "DELETE" 23 "UPDATE"})

(ffi/defcfn changeset-op {:library api/sqlite-library}
  "sqlite3changeset_op"
  [:pointer ;; IN: changeset iterator
   :pointer ;; OUT: table name
   :pointer ;; OUT: number of columns in table
   :pointer ;; OUT: statement
   :pointer ;; OUT: indirect change
   ] :int
  sqlite3changeset-op-native
  [pdb pChangesetIter]
  (with-open [arena (ffi/confined-arena)]
    (let [pzTab      (ffi/alloc arena :pointer)
          pnCol      (ffi/alloc arena :int)
          pOp        (ffi/alloc arena :int)
          pbIndirect (ffi/alloc arena :int)

          code (sqlite3changeset-op-native
                 pChangesetIter pzTab pnCol pOp pbIndirect)]
      (if (api/sqlite-ok? code)
        [(ffi/read pzTab :string)
         (ffi/read pnCol :int)
         (-> (ffi/read pOp :int)
             op->statemen)
         (if (= (ffi/read pbIndirect :int) 0)
           false true)]
        (throw (api/sqlite-ex-info pdb code {}))))))

(ffi/defcfn changeset-finalize {:library api/sqlite-library}
  "sqlite3changeset_finalize" [:pointer] :int)

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

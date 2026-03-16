## What is sqlite4clj?

>⚠️ **WARNING:**  This project is highly experimental and not production ready.

Conceptually sqlite4clj is inspired by sqlite4java a sqlite library that doesn't use the JDBC interface. The goal of sqlite4clj is to have a minimalist FFI binding to SQLite's C API using Java 22 FFI (project panama). Tighter integration with SQLite can in theory offer better performance and features not available through JDBC interfaces.

By using [coffi](https://github.com/IGJoshua/coffi) to interface with SQLite's C API directly with FFI we bypass the need for: [sqlite-jdbc](https://github.com/xerial/sqlite-jdbc), [hikariCP](https://github.com/brettwooldridge/HikariCP) and [next.jdbc](https://github.com/seancorfield/next-jdbc). This massively reduces the amount of code that needs to be maintained (and a much smaller jar), allows us to use Clojure to interface with SQLite directly. It also makes it easier to add SQLite specific features. In my case I was looking to cache prepared statement for each connection (which is not possible with HikariCP) but can lead to considerable performance gains on complex queries.

This also frees up scope for common things like binary encoding and decoding to leverage SQLite's blob type.

Currently, this project is very much a proof of concept. But, I'm hoping to ultimately make it production ready.

## Usage

Currently this library is not on maven so you have to add it via git deps (note: coffi requires at least Java 22):

```clojure
andersmurphy/sqlite4clj
{:git/url "https://github.com/andersmurphy/sqlite4clj"
 :git/sha "ecf9876626985d61bd27605ea6748aed25eaad87"}
```

Initialise a db:

```clojure
(ns scratch
  (:require [sqlite4clj.core :as d]))

(defonce db
  (d/init-db! "database.db"
    {:pool-size 4
     :pragma    {:foreign_keys false}}))
```

This creates a `:reader` connection pool with a number of connections equal to `:pool-size` and a single `:writer` connection. Single writer at the application level allows you to get the most out of SQLite's performance in addition to preventing `SQLITE_BUSY` and `SQLITE_LOCKED` messages. Finally, it makes it trivial to do transaction batching at the application layer for increased write throughput.

Running a read query:

```clojure
(d/q (:reader db)
["SELECT chunk_id, state FROM cell WHERE chunk_id = ?" 1978])

=>
[[1978 0]
 [1978 0]
 [1978 0]
 [1978 0]
 [1978 0]
 [1978 0]
 ...]
```

Unwrapped results when querying a single column (this behaviour can be changed with `d/no-unwrap-result-set-fn`):

```clojure
(d/q (:reader db)
  ["SELECT chunk_id FROM cell WHERE chunk_id = ?" 1978])

=>
[1978
 1978
 1978
 1978
 ...]

```

Inserting and updating:

```clojure
(d/q (:writer db)
  ["INSERT INTO session (id, checks) VALUES (?, ?)" "user1" 1])

=>
[]

(d/q (:writer db)
  ["UPDATE session SET id = ?, checks = ? where id = ?"
   "user1" 2 "user1"])
=>
[]
```

Write transactions:

```clojure
(d/with-write-tx [tx writer]
  (let [sid "user1"
        [checks] (d/q db ["SELECT checks FROM session WHERE id = ?" sid])]
    (if checks
      (d/q tx ["UPDATE session SET checks = ? WHERE id = ?" checks sid])
      (d/q tx ["INSERT INTO session (id, checks) VALUES (?, ?)" sid 1]))))
```

Read transactions:

```clojure
(d/with-read-tx [tx writer]
  (= (d/q tx ["SELECT checks FROM session WHERE id = ?" "user1"])
     (d/q tx ["SELECT checks FROM session WHERE id = ?" "user2"])))
```

## Result set functions

By default sqlite4clj returns vectors. However, you can pass in a `result-set-fn` to change this behaviour.

sqlite4clj provides a `d/qualified-keyword-result-set-fn`:

```clojure
(d/q (:reader db)
  ["SELECT chunk_id, state FROM cell WHERE chunk_id = ?" 1978]
  {:result-set-fn d/qualified-keyword-result-set-fn})

=>
[{:cell/chunk_id 1978 :cell/state 0}
 {:cell/chunk_id 1978 :cell/state 0}
 {:cell/chunk_id 1978 :cell/state 0}
 {:cell/chunk_id 1978 :cell/state 0}
 {:cell/chunk_id 1978 :cell/state 0}
 ...]
```

Both `d/default-result-set-fn` and `d/qualified-keyword-result-set-fn` return `nil` if the query results in no results.

If you want to make your own result set function:

```clojure
(defn my-qualified-keyword-result-set-fn [col-metadata result-set]
  (let [col-keys
        (mapv (fn [{:keys [table alias]}]
                (keyword table alias)) col-metadata)]
    (into []
      (map (fn [col-vals]
             (zipmap col-keys col-vals)))
      result-set)))
```

 This result set function returns `[]` instead of `nil`:

```clojure
(d/q (:reader db)
  ["SELECT chunk_id, state FROM cell WHERE chunk_id = ?" 0]
  {:result-set-fn d/qualified-keyword-result-set-fn})
  
=>
[]
```

`col-metadata` is a vector of maps (one for each column) of the form:

```clojure
[{:database (api/column-database-name stmt c)
  :table    (api/column-table-name stmt c)
  :origin   (api/column-origin-name stmt c)
  :alias    (api/column-name stmt c)}
  ...]
```

You can also set the `result-set-fn` for all connections on a database with `default-result-set-fn`:

```clojure
(defonce db
  (d/init-db! "database.db"
    {:read-only true
     :pool-size 4
     :pragma    {:foreign_keys false}
     :default-result-set-fn d/qualified-keyword-result-set-fn}))
```

## ⚠️ A note on prepared statements

Part of why sqlite4clj is fast is because caches prepared statements. This is recommended practice for getting the most performance out of SQLite. However, it does lead to two important considerations.

1. You can only execute a statement at a time. The statement bellow will only execute `SELECT 1;` not `SELECT 2;`

```clojure 
(d/q (:reader db) ["SELECT 1; SELECT 2"])
```

This is because each statement needs to be compiled and cached separately.

2. Be careful when using `IN`, the statements below will each have their own separate entry in the prepared statement cache:

```clojure
(d/q (:reader db) ["SELECT * FROM foo WHERE a IN (?, ?, ?);" 1 2 3])
(d/q (:reader db) ["SELECT * FROM foo WHERE a IN (?, ?);" 1 2])
```

Consider using `json_each` with `IN` instead, the statements below will be a single entry in the prepared statement cache:

```clojure
(d/q (:reader db) ["SELECT * FROM foo WHERE a IN SELECT value FROM json_each(?)" (edn->json [1 2 3])])
(d/q (:reader db) ["SELECT * FROM foo WHERE a IN SELECT value FROM json_each(?)" (edn->json [1 2])])
```

I might consider adding a `edn_each` function in future.


## Connection pools not thread pools

The connection pools are not thread pools, they use a `LinkedBlockingQueue` to limit/queue access. Unlike thread pool this allows for having as many databases as you want without taking up large amount of memory particularly if you run your queries from virtual threads. With SQLite it's common to have many databases for isolation and convenience. It's not uncommon to have a database per tenant or user, or even simply as a persistent cache. So making this cheap is important as it's one of SQLite's super powers.

## Automatic EDN encoding/decoding

sqlite4clj automatically encodes any EDN object you pass it:

```clojure
(d/q writer
["CREATE TABLE IF NOT EXISTS entity(id TEXT PRIMARY KEY, data BLOB) WITHOUT ROWID"])

(d/q writer
    ["INSERT INTO entity (id, data) VALUES (?, ?)"
     (str (random-uuid))
     ;; this map will be encoded automatically
     {:type "foo" :a (rand-int 10) :b (rand-int 10)}])

(d/q reader ["select * from entity"])
;; =>
;; [["46536a4a-0b1e-4749-9c01-f44f73de3b91" {:type "foo", :a 3, :b 3}]]
```

This effectively lets you use SQLite as an EDN document store.
When decoding BLOB values, sqlite4clj reads the exact SQLite BLOB size before parsing EDN.

Encoding is done with [fast-edn](https://github.com/tonsky/fast-edn) as text and then converted into bytes. From my testing this was faster than both [deed](https://github.com/igrishaev/deed) and [nippy](https://github.com/taoensso/nippy) despite being a text format. Being a text format it is stable and can be swapped out for faster EDN text serialises without breaking changes. Of course, this also means only EDN data is supported and not arbitrary Java classes.

## Application functions

SQLite supports [Application-Defined SQL Functions](https://www.sqlite.org/appfunc.html). This lets you extend SQL with clojure defined functions. sqlite4clj streamlines these for interactive use at the repl.

Declaring and using an application function:

```clojure
(defn entity-type [blob]
  (-> blob :type))

(d/create-function db "entity_type" #'entity-type {:deterministic? true})

(d/q reader ["select * from entity where entity_type(data) = ?" "foo"])
;; =>
;; [["46536a4a-0b1e-4749-9c01-f44f73de3b91" {:type "foo", :a 3, :b 3}]]
```

When dealing with columns that are encoded EDN blobs they will automatically be decoded.

You can unregister scalar functions with:

```clojure
(d/remove-function db "entity_type")
;; optional arity-specific removal:
(d/remove-function db "entity_type" 1)
```

## Application aggregates

SQLite also supports [Application-Defined Aggregate Functions](https://www.sqlite.org/appfunc.html). sqlite4clj exposes these via `d/create-aggregate`.

The aggregate step callback receives `[state & sql-args]`, and arity is inferred as `step-arity - 1` by default:

```clojure
(d/create-aggregate db "sum_n"
  (fn [state n]
    (+ (or state 0) n))
  (fn [state] state)
  {:deterministic? true})

(d/q (:reader db) ["SELECT sum_n(checks) FROM session"])
;; =>
;; [42]
```

You can also:

- pass vars (for repl-driven redefinition), e.g. `#'sum-step` and `#'sum-final`
- set `:arity` explicitly (`-1` for variadic aggregates)
- set `:initial-state` to control empty-input results
- remove aggregates with `d/remove-aggregate` (optionally by arity)

```clojure
(d/create-aggregate db "sum_empty_init"
  (fn [state n] (+ (or state 0) n))
  (fn [state] state)
  {:initial-state 10})

(d/q (:writer db) ["SELECT sum_empty_init(n) FROM (SELECT 1 AS n WHERE 0)"])
;; =>
;; [10]

(d/remove-aggregate db "sum_n")
;; optional arity-specific removal:
(d/remove-aggregate db "sum_n" 1)
```

## Indexing on encoded edn blobs

Because SQLite supports [Indexes On Expressions](https://www.sqlite.org/expridx.html) and [Partial Indexes](https://www.sqlite.org/partialindex.html) we can easily index on any arbitrary encoded EDN data.

Using an expression index:

```clojure
(d/q writer
["CREATE INDEX IF NOT EXISTS entity_type_idx ON entity(entity_type(data))
WHERE entity_type(data) IS NOT NULL"])

(d/q reader
    ["select * from entity where entity_type(data) = ?" "foo"])
;; =>
;; [["46536a4a-0b1e-4749-9c01-f44f73de3b91" {:type "foo", :a 3, :b 3}]]

;; Check index is being used
(d/q reader
  ["explain query plan select * from entity where entity_type(data) = ?" "foo"])
;; =>
;; [[3 0 62 "SEARCH entity USING INDEX entity_type_idx (<expr>=?)"]]
```

## Using partial indexes to sort on encoded EDN blobs

A partial index is an index over a subset of the rows of a table. This leads to smaller and faster indexes (see  [Existence Based Processing](https://www.dataorienteddesign.com/dodmain/node4.html)) when only a subset of the rows have a value you care about.

Using a partial expression index:

```clojure
(d/q writer
    ["CREATE INDEX IF NOT EXISTS entity_type_idx ON entity(entity_type(data))
    WHERE entity_type(data) IS NOT NULL"])

(d/q reader
    ["select * from entity
      where entity_type(data) is not null
      order by entity_type(data)"])
;; =>
;; [["0378005a-3e28-40b0-9795-a2b85a174181" {:type "bam", :a 4, :b 3}]
;;  ["9a470400-1ebd-4bd6-8d36-b9ae06ba826a" {:type "bam", :a 0, :b 9}]
;;  ["97c18868-83e2-4eda-88da-bf89741c2242" {:type "bar", :a 9, :b 8}]
;;  ["1f272d21-7538-4397-a53d-c2a7277eee96" {:type "baz", :a 6, :b 2}]]
```

However, some care needs to be taken when partial indexes and expression indexes:

>The SQLite query planner will consider using an index on an expression when the expression that is indexed appears in the WHERE clause or in the ORDER BY clause of a query, exactly as it is written in the CREATE INDEX statement.

Consider the following index:

```clojure
(d/q writer
    ["CREATE INDEX IF NOT EXISTS entity_type_idx ON entity(entity_type(data))
    WHERE entity_type(data) IS NOT NULL"])
```

The following query will not use the index (as it omits the `where` clause):

```clojure
(d/q reader
    ["explain query plan select * from entity order by entity_type(data)"])
;; =>
;; [[3 0 215 "SCAN entity"] [12 0 0 "USE TEMP B-TREE FOR ORDER BY"]]
```

The index will be used if you add the `where entity_type(data) is not null` clause:

```clojure
(d/q reader
    ["explain query plan select * from entity
      where entity_type(data) is not null
      order by entity_type(data)"])
;; =>
;; [[4 0 215 "SCAN entity USING INDEX entity_type_idx"]]
```

## Replication and backups with litestream

[Litestream](https://litestream.io/) is an amazing open source SQLite replication tool that lets you to stream backups to S3 compatible object storage.

If litestream is installed on your system ([see installation instructions for details](https://litestream.io/install/ )) you can start replication/ restoration on application start with `sqlite4clj.litestream/restore-then-replicate!`.

```clojure
(litestream/restore-then-replicate! db-name
  {:s3-access-key-id     "XXXXXXXXXXXXXXX"
   :s3-access-secret-key "XXXXXXXXXXXXXXX"
   :bucket               "BUCKET NAME"
   :endpoint             "S3 URL"
   :region               "REGION"})
```

By default this will throw an error if backups/replication is not working correctly (to crash your application).

It will automatically attempt to restore db from replica if db does not already exist. The process is started as a JVM sub process and will be cleaned up when the application terminates.

Returns the java.lang.Process that you can monitor, in the unlikely event that the litestream process crashes you can restart it by running `restore-then-replicate!`.

sqlite4clj tries to keep its dependencies to a minimum so doesn't support complex yaml generation (which would require adding something like [clj-yaml](https://github.com/clj-commons/clj-yaml) as a dependency). If the built in config generation doesn't support your needs you can supply your own litestream config string using the `config-yml` option. Worth remembering JSON is valid YAML.

So something like this should work:

```clojure
(litestream/restore-then-replicate! db-name
  {:s3-access-key-id     (env :s3-access-key-id)
   :s3-access-secret-key (env :s3-access-secret-key)
   :config-yml
   (edn->json
     {:dbs
      [{:path db-name
        :replicas
        [{:type          "s3"
          :bucket        "hyperlith"
          :endpoint      "https://nbg1.your-objectstorage.com"
          :region        "nbg1"
          :sync-interval "1s"}]}]}
     ;; important not to escape slashes for this to work
     :escape-slash false)})
```

## Loading the Native Library

Bundled in the classpath is pre-built libsqlite3 shared library for:

- macos:   aarch64
- linux:   aarch64
- macos:   x86_64
- linux:   x86_64
- windows: x86_64

If you want to provide your own native library then specify the `sqlite4clj.native-lib` system property:

- `-Dsqlite4clj.native-lib=bundled`, uses the pre-built library (default if property is omitted)
- `-Dsqlite4clj.native-lib=system`, loads the sqlite3 library from the `java.library.path` (which includes `LD_LIBRARY_PATH`)
- `-Dsqlite4clj.native-lib=/path/to/libsqlite3.so`, the value is interpreted as a path to a file that is loaded directly

### Building SQLite from source

The compile flags used here are optional optimizations based on the [SQLite compile guide](https://sqlite.org/compile.html).
Most flags remove unused features to reduce binary size and improve performance.

The key flag is `SQLITE_THREADSAFE=2`, which sets SQLite to multi-threaded mode.
This allows safe concurrent use across multiple threads as long as each database
connection (and its prepared statements) stays within a single thread.

Since sqlite4clj manages connections through a thread pool that guarantees this
constraint, we can safely use this more performant threading mode instead of the
default Serialized mode.

```
gcc -shared -Os -I. -fPIC -DSQLITE_DQS=0 \
   -DSQLITE_THREADSAFE=2 \
   -DSQLITE_DEFAULT_MEMSTATUS=0 \
   -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 \
   -DSQLITE_LIKE_DOESNT_MATCH_BLOBS \
   -DSQLITE_MAX_EXPR_DEPTH=0 \
   -DSQLITE_OMIT_DEPRECATED \
   -DSQLITE_OMIT_PROGRESS_CALLBACK \
   -DSQLITE_OMIT_SHARED_CACHE \
   -DSQLITE_USE_ALLOCA \
   -DSQLITE_STRICT_SUBTYPE=1 \
   -DSQLITE_OMIT_AUTOINIT \
   -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS \
   -DSQLITE_ENABLE_STAT4 \
   -DSQLITE_ENABLE_RTREE \
   -DSQLITE_ENABLE_FTS5 \
   -DSQLITE_MAX_MMAP_SIZE=1099511627776 \
   -DSQLITE_ENABLE_COLUMN_METADATA \
   -DSQLITE_ENABLE_SESSION \
   -DSQLITE_ENABLE_PREUPDATE_HOOK \
   sqlite3.c -lpthread -ldl -lm -o sqlite3.so
```

## Projects using sqlite4clj

- [datahike-sqlite](https://github.com/outskirtslabs/datahike-sqlite)
- [hyperlith](https://github.com/andersmurphy/hyperlith)
- [One Billion Checkboxes](https://checkboxes.andersmurphy.com/)

## Development & Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md)

## What is sqlite4clj?

>⚠️ **WARNING:**  This project is highly experimental and not production ready.

>⚠️ **WARNING:**  API can change at any time! Use at your own risk.

Conceptually sqlite4clj is inspired by sqlite4java a sqlite library that doesn't use the JDBC interface. The goal of sqlite4clj is to have a minimalist FFI binding to SQLite's C API using Java 22 FFI (project panama). Tighter integration with SQLite can in theory offer better performance and features not available through JDBC interfaces.

By using [coffi](https://github.com/IGJoshua/coffi) to interface with SQLite's C API directly with FFI we bypass the need for: [sqlite-jdbc](https://github.com/xerial/sqlite-jdbc), [hikariCP](https://github.com/brettwooldridge/HikariCP) and [next.jdbc](https://github.com/seancorfield/next-jdbc). This massively reduces the amount of code that needs to be maintained (and a much smaller jar), allows us to use Clojure to interface with SQLite directly. It also makes it easier to add SQLite specific features. In my case I was looking to cache prepared statement for each connection (which is not possible with HikariCP) but can lead to considerable performance gains on complex queries.

This also frees up scope for common things like binary encoding and decoding as well as compression decompression to leverage SQLite's blob type.

Currently, this project is very much a proof of concept. But, I'm hoping to ultimately make it production ready.

## Usage

Currently this library is not on maven so you have to add it via git deps (note: coffi requires at least Java 22):

```clojure
andersmurphy/sqlite4clj 
{:git/url "https://github.com/andersmurphy/sqlite4clj"
 :git/sha "1a82b2b425b22539f9cc17b4cbdd892676c2a9f9"}
```

Initialise a db:

```clojure
(ns scratch
  (:require [sqlite4clj.core :as d]))
   
(defonce db
  (d/init-db! "database.db"
    {:read-only true
     :pool-size 4
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

Unwrapped results when querying a single column:

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

## Connection pools not thread pools

The connection pools are not thread pools, they use a `LinkedBlockingQueue` to limit/queue access. Unlike thread pool this allows for having as many databases as you want without taking up large amount of memory particularly if you run your queries from virtual threads. With SQLite it's common to have many databases for isolation and convenience. It's not uncommon to have a database per tenant or user, or even simply as a persistent cache. So making this cheap is important as it's one of SQLite's super powers.

## Why is this fast?

The two main speedups are from caching query statements at a connection level and using inline caching of column reading functions.

## BLOB type

SQLite's blob types are incredibly flexible. But, require establishing some conventions. For sqlite4clj the conventions are as follows:

- When inserting/updating a non `byte/1` (byte array) it will attempt to serialize the Clojure/Java data using [deed](https://github.com/igrishaev/deed). It will then compress the data using ZSTD. The first byte of the blob will be `ZSTD_ENCODED_BLOB` (`1`).
- When inserting/updating a `byte/1` (byte array) it will insert a leading byte that will be `RAW_BLOB` (`0`).

- When reading a `ZSTD_ENCODED_BLOB` the value will be decompressed and decoded automatically.
- When reading a `RAW_BLOB` the leading byte (`RAW_BLOB`) will be stripped before being returned.

## Further reading

[Clojure: SQLite C API with project Panama and Coffi](https://andersmurphy.com/2025/05/20/clojure-sqlite-c-api-with-project-panama-and-coffi.html)

## Loading the Native Library

Bundled in the classpath is pre-built libsqlite3 shared library for:

- macos: aarch64
- linux: x86_64

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
   -DSQLITE_OMIT_DECLTYPE \
   -DSQLITE_OMIT_DEPRECATED \
   -DSQLITE_OMIT_PROGRESS_CALLBACK \
   -DSQLITE_OMIT_SHARED_CACHE \
   -DSQLITE_USE_ALLOCA \
   -DSQLITE_STRICT_SUBTYPE=1 \
   -DSQLITE_OMIT_AUTOINIT \
   -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS \
   -DSQLITE_ENABLE_STAT4 \
   sqlite3.c -lpthread -ldl -lm -o sqlite3.so
```


## Development & Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md)

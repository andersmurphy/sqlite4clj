## What is sqlite4clj?

>⚠️ **WARNING:**  This project is highly experimental and not production ready.

>⚠️ **WARNING:**  API can change at any time! Use at your own risk. 

Conceptually sqlite4clj is inspired by sqlite4java a sqlite libaray that doesn't use the JDBC interface. The goal of sqlite4clj is to have a minimalist FFI binding to SQLite's C API using Java 22 FFI (project panama). Tighter integration with SQLite can in theory offer better performance and features not available through JDBC interfaces.

I've always felt using JDBC for SQLite is overkill as it's an embedded database.

By using [coffi](https://github.com/IGJoshua/coffi) to interface with SQLite's C API directly with FFI we bypass the need for: [sqlite-jdbc](https://github.com/xerial/sqlite-jdbc), [hikariCP](https://github.com/brettwooldridge/HikariCP) and [next.jdbc](https://github.com/seancorfield/next-jdbc). This massively reduces the amount of code that needs to be maintained, allows us to use Clojure to interface with SQLite directly. It also makes it easier to add SQLite specific features. In my case I was looking to cache prepared statement for each connection (which is no possible with HikariCP) but can lead to considerable performance gains on complex queries.

Currently, this project is very much a proof of concept. But, I'm hoping to ultimately make it production ready.

## Further reading

[Clojure: SQLite C API with project Panama and Coffi](https://andersmurphy.com/2025/05/20/clojure-sqlite-c-api-with-project-panama-and-coffi.html)

## Building SQLITE from source

See this [blog post](https://andersmurphy.com/2023/07/09/sqlite-building-from-source-on-macos.html) .

```
gcc -Os -I. -DSQLITE_DQS=0 \
   -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 \
   -DSQLITE_DEFAULT_MEMSTATUS=0 \
   -DSQLITE_LIKE_DOESNT_MATCH_BLOBS \
   -DSQLITE_OMIT_DEPRECATED \
   -DSQLITE_OMIT_PROGRESS_CALLBACK \
   -DSQLITE_OMIT_SHARED_CACHE \
   -DSQLITE_DISABLE_PAGECACHE_OVERFLOW_STATS \
   -DSQLITE_ENABLE_STAT4 \
   -DSQLITE_USE_ALLOCA \
   -DSQLITE_STRICT_SUBTYPE=1 \
   -DSQLITE_MAX_EXPR_DEPTH=0 \
   shell.c sqlite3.c -lpthread -ldl -lm -o sqlite3
```



# sqlite4clj Benchmarks

Tracking issue: [#15](https://github.com/andersmurphy/sqlite4clj/issues/15)

## Run

```bash
clojure -M:bench
# or
bb bench
```

## Scope

The benchmark runner compares:

- `sqlite4clj` (FFI API)
- `next.jdbc` `1.3.1093` on `sqlite-jdbc` `3.51.2.0`
- raw `sqlite-jdbc` (`PreparedStatement` API)

The suite seeds fresh DBs under `target/benchmarks/` with:

- `bench_items` (5000 text rows)
- `bench_docs` (5000 EDN blob rows)
- `bench_probe` (scratch table for write probes)

Measured scenarios:

- point read: single text column
- point read: EDN blob decode
- write transaction: insert + delete

## Baseline Results

Run date: `2026-02-16 17:45:17 UTC`

Environment:

- OS: `Linux 6.17.9-76061709-generic x86_64`
- JVM: `OpenJDK 25.0.2 LTS (Temurin)`
- Clojure CLI: `1.11.1.1403`

| Implementation | Scenario | Mean | Std Dev | Lower Q (2.5%) | Upper Q (97.5%) |
| --- | --- | --- | --- | --- | --- |
| sqlite4clj | point read: single text column | `3.475618 µs` | `372.110097 ns` | `2.822753 µs` | `3.777728 µs` |
| sqlite4clj | point read: EDN blob decoding | `3.879108 µs` | `357.514089 ns` | `3.566318 µs` | `4.284442 µs` |
| sqlite4clj | write tx: insert + delete | `17.060971 µs` | `537.982953 ns` | `16.422890 µs` | `17.658053 µs` |
| next.jdbc + sqlite-jdbc | point read: single text column | `7.854171 µs` | `848.297195 ns` | `6.881131 µs` | `8.681351 µs` |
| next.jdbc + sqlite-jdbc | point read: EDN blob decoding | `11.416293 µs` | `1.381525 µs` | `9.566414 µs` | `12.357243 µs` |
| next.jdbc + sqlite-jdbc | write tx: insert + delete | `4.068365 ms` | `27.191875 µs` | `4.038433 ms` | `4.099795 ms` |
| raw sqlite-jdbc | point read: single text column | `3.035696 µs` | `31.070321 ns` | `3.007018 µs` | `3.071417 µs` |
| raw sqlite-jdbc | point read: EDN blob decoding | `5.092551 µs` | `491.491657 ns` | `4.400165 µs` | `5.535956 µs` |
| raw sqlite-jdbc | write tx: insert + delete | `4.016703 ms` | `19.800916 µs` | `3.997144 ms` | `4.045508 ms` |

Notes:

- Results are machine-dependent; compare on the same host/JVM for trend analysis.
- These numbers use Criterium `quick-bench` for fast iteration.
- `next.jdbc` and raw JDBC benchmarks run on a persistent connection (no per-call connection-open cost).

(ns sqlite4clj.api
  "These function map directly to SQLite's C API."
  (:require
   [clojure.java.io :as io]
   [coffi.ffi :as ffi :refer [defcfn]]
   [deed.core :as deed]
   [coffi.mem :as mem])
  (:import
   [java.nio.file Files]
   [java.util Arrays]
   [io.airlift.compress.v3.zstd ZstdCompressor ZstdDecompressor
    ZstdNativeCompressor ZstdNativeDecompressor]))

(def SQLITE_UTF8 1)
(def SQLITE_DETERMINISTIC 0x000000800)
(def SQLITE_DIRECTONLY 0x000080000)
(def SQLITE_INNOCUOUS 0x000200000)
(def SQLITE_SUBTYPE 0x000100000)
(def SQLITE_RESULT_SUBTYPE 0x001000000)
(def SQLITE_SELFORDER1 0x002000000)

(def RAW_BLOB (byte 0))
(def ZSTD_ENCODED_BLOB (byte 1))

(defn add-leading-byte [blob leading-byte]
  (let [out (byte-array (inc (count blob)) [leading-byte])]
    (System/arraycopy blob 0 out 1 (count blob))
    out))

(defn remove-leading-byte [blob]
  (Arrays/copyOfRange blob 1 (count blob)))

(defn encode
  "Encode Clojure data and compress it with zstd. Compression quality
  ranges between -7 and 22 and has negligible impact on decompression speed."
  [blob quality]
  (let [compressor (ZstdNativeCompressor/new (or quality 3))
        blob       (deed/encode-to-bytes blob)
        compressed (byte-array
                     (inc (ZstdCompressor/.maxCompressedLength compressor
                            (count blob)))
                     [ZSTD_ENCODED_BLOB])
        compressed-size
        (ZstdCompressor/.compress compressor blob 0 (count blob) compressed 1
          (dec (count compressed)))]
    (Arrays/copyOfRange compressed 0 (inc compressed-size))))

(defn decode
  "Decode Clojure data and compress it with zstd."
  [blob]
  (let [decompressor (ZstdNativeDecompressor/new)
        uncompressed (byte-array
                       (ZstdDecompressor/.getDecompressedSize decompressor
                         blob 1 (dec (count blob))))]
    (ZstdDecompressor/.decompress decompressor blob 1 (dec (count blob))
      uncompressed 0 (count uncompressed))
    (deed/decode-from uncompressed)))

(defn copy-resource [resource-path output-path]
  (with-open [in  (io/input-stream (io/resource resource-path))
              out (io/output-stream (io/file output-path))]
    (io/copy in out)))

(defn load-bundled-library []
  (let [arch              (System/getProperty "os.arch")
        res-file          ({"aarch64" "sqlite3_aarch64.so"
                            "amd64"   "sqlite3_amd64.so"
                            "x86_64"  "sqlite3_amd64.so"}
                           arch)
        temp-lib-filename (str "sqlite4clj_temp_" res-file)]
    (copy-resource res-file temp-lib-filename)
    (ffi/load-library temp-lib-filename)
    ;; We delete once loaded
    (Files/deleteIfExists (.toPath (io/file temp-lib-filename)))))

(defn load-system-library []
  (ffi/load-system-library "sqlite3"))

;; Load appropriate SQLite library
(let [src (System/getProperty "sqlite4clj.native-lib")]
  (cond
    ;; default to bundled
    (or (nil? src)
        (= src "bundled")) (load-bundled-library)
    (= src "system")       (load-system-library)
    :else
    (ffi/load-library src)))

(defcfn initialize
  sqlite3_initialize
  [::mem/pointer] ::mem/int)

(defonce init-lib (initialize nil))

(defcfn errmsg
  sqlite3_errmsg
  [::mem/pointer] ::mem/c-string)

(defcfn errstr
  sqlite3_errstr
  [::mem/int] ::mem/c-string)

(defn sqlite-ex-info [pdb code data]
  (let [code-name (errstr code)
        message   (errmsg pdb)]
    (ex-info (str "SQLite error: " code-name "\n" message)
      (assoc data
        :code code-name
        :message message))))

(defn sqlite-ok? [code]
  (= code 0))

(defcfn open-v2
  "sqlite3_open_v2" [::mem/c-string ::mem/pointer ::mem/int
                     ::mem/c-string] ::mem/int
  sqlite3-open-native
  [filename flags]
  (with-open [arena (mem/confined-arena)]
    (let [pdb           (mem/alloc-instance ::mem/pointer arena)
          filename-utf8 (String/new (String/.getBytes filename "UTF-8") "UTF-8")
          code          (sqlite3-open-native filename-utf8
                 pdb flags nil)]
      (if (sqlite-ok? code)
        (mem/deserialize-from pdb ::mem/pointer)
        (throw (sqlite-ex-info pdb code {:filename filename}))))))

(defcfn close
  sqlite3_close
  [::mem/pointer] ::mem/int)

(defcfn prepare-v3
  "sqlite3_prepare_v3"
  [::mem/pointer ::mem/c-string ::mem/int
   ::mem/int
   ::mem/pointer ::mem/pointer] ::mem/int
  sqlite3-prepare-native
  [pdb sql]
  (with-open [arena (mem/confined-arena)]
    (let [ppStmt (mem/alloc-instance ::mem/pointer arena)
          sql    (String/new (String/.getBytes sql "UTF-8") "UTF-8")
          code   (sqlite3-prepare-native pdb sql -1
                   0x01 ;; SQLITE_PREPARE_PERSISTENT
                   ppStmt
                   nil)]
      (if (sqlite-ok? code)
        (mem/deserialize-from ppStmt ::mem/pointer)
        (throw (sqlite-ex-info pdb code {:sql sql}))))))

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
   ::mem/pointer] ::mem/int
  sqlite3-bind-text-native
  [pdb idx text]
  (let [text       (str text)
        text-bytes (String/.getBytes text "UTF-8")]
    (sqlite3-bind-text-native pdb idx
      (String/new text-bytes "UTF-8")
      (count text-bytes)
      sqlite-transient)))

(defcfn bind-blob
  "sqlite3_bind_blob"
  [::mem/pointer ::mem/int ::mem/pointer ::mem/int
   ::mem/pointer] ::mem/int
  sqlite3-bind-blob-native
  [pdb idx blob]
  (let [blob   (if (bytes? blob)
                 (add-leading-byte blob RAW_BLOB)
                 (encode blob 3))
        blob-l (count blob)]
    (sqlite3-bind-blob-native pdb idx
      (mem/serialize blob [::mem/array ::mem/byte blob-l])
      blob-l
      sqlite-transient)))

(defcfn step
  sqlite3_step
  [::mem/pointer] ::mem/int)

(defcfn column-count
  sqlite3_column_count
  [::mem/pointer] ::mem/int)

(defcfn column-double 
  sqlite3_column_double
  [::mem/pointer ::mem/int] ::mem/double)

(defcfn column-int 
  sqlite3_column_int
  [::mem/pointer ::mem/int] ::mem/int)

(defcfn column-text
  sqlite3_column_text
  [::mem/pointer ::mem/int] ::mem/c-string)

(defcfn column-bytes
  sqlite3_column_bytes
  [::mem/pointer ::mem/int] ::mem/int)

(defcfn column-blob
  "sqlite3_column_blob"
  [::mem/pointer ::mem/int] ::mem/pointer
  sqlite3_column_blob-native
  [stmt idx]
  (let [result (sqlite3_column_blob-native stmt idx)
        size   (column-bytes stmt idx)
        blob   (mem/deserialize
                 (mem/reinterpret result
                   (mem/size-of [::mem/array ::mem/byte size]))
                 [::mem/array ::mem/byte size :raw? true])]
    (if (= (first blob) ZSTD_ENCODED_BLOB)
      (decode blob)
      (remove-leading-byte blob))))

(defcfn column-type
  sqlite3_column_type
  [::mem/pointer ::mem/int] ::mem/int)

(defcfn finalize
  sqlite3_finalize
  [::mem/pointer] ::mem/int)

(defcfn create-function-v2
  sqlite3_create_function_v2
  [::mem/pointer  ;; db
   ::mem/c-string ;; zFunctionName
   ::mem/int      ;; nArg
   ::mem/int      ;; eTextRep (includes flags)
   ::mem/pointer  ;; pApp (user data)
   ::mem/pointer  ;; xFunc (function pointer, not inline definition)
   ::mem/pointer  ;; xStep (for aggregates)
   ::mem/pointer  ;; xFinal (for aggregates)
   ::mem/pointer] ;; xDestroy (destructor)
  ::mem/int)

(defcfn value-text
  sqlite3_value_text
  [::mem/pointer] ::mem/c-string)

(defcfn value-int
  sqlite3_value_int
  [::mem/pointer] ::mem/int)

(defcfn value-double
  sqlite3_value_double
  [::mem/pointer] ::mem/double)

(defcfn value-type
  sqlite3_value_type
  [::mem/pointer] ::mem/int)

(defcfn value-bytes
  sqlite3_value_bytes
  [::mem/pointer] ::mem/int)


(defcfn value-blob
  "sqlite3_value_blob"
  [::mem/pointer] ::mem/pointer
  sqlite3-value-blob-native
  [sqlite-value]
  (let [result (sqlite3-value-blob-native sqlite-value)]
    (if (mem/null? result)
      nil
      (let [size (value-bytes sqlite-value)
            blob (mem/deserialize
                  (mem/reinterpret result size)
                  [::mem/array ::mem/byte size :raw? true])]
        (if (and (pos? size) (= (first blob) ZSTD_ENCODED_BLOB))
          (decode blob)
          (if (pos? size)
            (remove-leading-byte blob)
            (byte-array 0)))))))

(defcfn result-text
  "sqlite3_result_text"
  [::mem/pointer ::mem/c-string ::mem/int ::mem/pointer] ::mem/void
  sqlite3-result-text-native
  [context text]
  (let [text-bytes (String/.getBytes (str text) "UTF-8")]
    (sqlite3-result-text-native context
                                (String/new text-bytes "UTF-8")
                                (count text-bytes)
                                sqlite-transient)))

(defcfn result-int
  sqlite3_result_int
  [::mem/pointer ::mem/int] ::mem/void)

(defcfn result-double
  sqlite3_result_double
  [::mem/pointer ::mem/double] ::mem/void)

(defcfn result-null
  "sqlite3_result_null"
  [::mem/pointer] ::mem/void
  sqlite3-result-null-native
  ([context]
   (sqlite3-result-null-native context))
  ([context _]
   (sqlite3-result-null-native context)))

(defcfn result-blob
  "sqlite3_result_blob"
  [::mem/pointer ::mem/pointer ::mem/int ::mem/pointer] ::mem/void
  sqlite3-result-blob-native
  [context blob]
  (let [blob (if (bytes? blob)
               (add-leading-byte blob RAW_BLOB)
               (encode blob 3))
        blob-l (count blob)]
    (sqlite3-result-blob-native context
                                (mem/serialize blob [::mem/array ::mem/byte blob-l])
                                blob-l
                                sqlite-transient)))

(defcfn result-error
  "sqlite3_result_error"
  [::mem/pointer ::mem/c-string ::mem/int] ::mem/void
  sqlite3-result-error-native
  [context msg]
  (let [msg (str msg)
        msg-bytes (String/.getBytes msg "UTF-8")]
    (sqlite3-result-error-native context
                                 (String/new msg-bytes "UTF-8")
                                 (count msg-bytes))))

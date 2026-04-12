(ns sqlite4clj.impl.encoding
  (:require [coffi.mem :as mem]
            [fast-edn.core :as edn])
  (:import [java.lang.foreign MemorySegment SegmentAllocator]))

(def RAW_BLOB (byte 0))
(def ENCODED_BLOB (byte 2))

(defn- decode-edn
  "Decode Clojure data."
  [^MemorySegment blob size]
  ;; Read exactly `size` bytes from the BLOB payload. Relying on C-string
  ;; null terminators can leak random trailing bytes into EDN decoding.
  (edn/read-string
    (String. (.toArray (mem/slice blob 0 size)
               java.lang.foreign.ValueLayout/JAVA_BYTE)
      "UTF-8")))

;; -----------------------------
;; Public API

(defn encode ^MemorySegment [arena blob]
  (let [b            (if (bytes? blob) blob
                         (binding [*print-length* nil]
                           (String/.getBytes (pr-str blob))))
        leading-byte (if (bytes? blob) RAW_BLOB ENCODED_BLOB)
        b-l          (alength ^bytes b)
        total        (unchecked-inc-int b-l)
        segment      (SegmentAllocator/.allocate arena (long total))]
    (mem/write-byte segment leading-byte)
    (mem/write-bytes segment b-l 1 ^bytes b)
    segment))

(defn decode [blob size]
  (if (pos? size)
    ;; case does not work with bytes!
    (let [f-byte (mem/read-byte blob)
          blob   (mem/slice blob 1)]
      (if (= f-byte ENCODED_BLOB)
        (decode-edn blob (dec size))
        ;; Otherwise
        (.toArray blob java.lang.foreign.ValueLayout/JAVA_BYTE)))
    (byte-array 0)))

(ns sqlite4clj.impl.encoding
  (:require [coffi.mem :as mem]
            [fast-edn.core :as edn])
  (:import [java.lang.foreign MemorySegment]))

(def RAW_BLOB (byte 0))
(def ENCODED_BLOB (byte 2))

(defn- add-leading-byte ^byte/1 [blob leading-byte]
  (let [out (byte-array (inc (count blob)) [leading-byte])]
    (System/arraycopy blob 0 out 1 (count blob))
    out))

(defn- encode-edn
  "Encode Clojure data."
  [blob]
  (let [blob (binding [*print-length* nil]
               (String/.getBytes (pr-str blob)))]
    (add-leading-byte blob ENCODED_BLOB)))

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

(defn encode [blob]
  (if (bytes? blob)
    (add-leading-byte blob RAW_BLOB)
    (encode-edn blob)))

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

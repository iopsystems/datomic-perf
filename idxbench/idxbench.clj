(ns bench.idxbench
  "DaCapo-style Datomic indexing benchmark over GH Archive data.

   Iteration model (borrowed from DaCapo's luindex):
     for i in 1..N:
       create a fresh database
       load a fixed batch of events   <- measured
       force the index to catch up    <- measured
       collect metrics
       DELETE the database
   The first N-1 iterations are warmup; the last is the reported result.
   Everything runs in ONE peer JVM so JIT state carries across iterations --
   which is the whole point of warming up.

   What is actually being measured lives in the TRANSACTOR, not here, so the
   metrics are gathered over JMX from the transactor pid. See bench.jvmstats.

   Sizes are fixed event counts, not fixed files, so a size means the same
   amount of work regardless of which archive hours are on disk."
  (:require [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [bench.jvmstats :as jvm]
            [bench.gharchive :as gh])
  (:import [java.util.zip GZIPInputStream]))

(def sizes
  "Event counts chosen so each size corresponds to a target volume of raw
   (uncompressed) GH Archive source data. Measured on 2024-01-01: 563 MB raw
   and 180,686 events per hour, i.e. ~3.117 MB of raw JSON per 1000 events.

     small   ~100 MB raw   (~0.2 hours of archive)
     medium  ~1 GB raw     (~1.8 hours)
     large   ~10 GB raw    (~18.2 hours)

   Sizing by source bytes rather than event count makes the working set
   comparable to other indexing benchmarks, where the input corpus is what is
   quoted. The datom counts these produce are roughly 7x the event count."
  {:small   32000      ;; ~100 MB raw
   :medium  329000     ;; ~1 GB raw
   :large  3285000})   ;; ~10 GB raw

(def size-raw-mb
  "Nominal raw source MB per size, for reporting."
  {:small 100 :medium 1024 :large 10240})

(defn ms [n] (/ (double n) 1e6))

;; ---------------------------------------------------------------------------
;; Data: read N events from a list of .json.gz files
;; ---------------------------------------------------------------------------

(defn events
  "Parse at most n events from `files`, in order.

   Reads each file fully inside with-open using an explicit FileInputStream and
   a sized GZIP buffer. An earlier version used a transducer with `take` over
   line-seq, which short-circuits and left the GZIP stream in a state where the
   next available() call threw IOException: Invalid argument."
  [files n]
  (loop [fs (remove str/blank? files), acc [], left n]
    (if (or (<= left 0) (empty? fs))
      acc
      (let [taken (with-open [is (java.io.FileInputStream. ^String (first fs))
                              gz (GZIPInputStream. is 65536)
                              r  (io/reader gz)]
                    (doall (map #(json/read-str %) (take left (line-seq r)))))]
        (recur (rest fs) (into acc taken) (- left (count taken)))))))

(defn preload!
  "Parse and convert events to tx-data ONCE, up front, so the measured phase
   contains no JSON parsing. Without this we would be timing Jackson, not
   Datomic -- parsing is ~40% of a naive loop."
  [files n batch]
  (->> (events files n)
       (map gh/event->tx)
       (map first)
       (partition-all batch)
       (mapv vec)))

;; ---------------------------------------------------------------------------
;; Transactor index-job log parsing (the authoritative throughput source)
;; ---------------------------------------------------------------------------

(defn index-jobs
  "Parse :index/create-index events from the transactor log, oldest first.

   Each job reports the datoms it merged and how long that took. This is the
   authoritative throughput source: d/sync-index only measures the trailing
   merge, because background indexing has already run most of the work by the
   time the load finishes."
  [log-dir]
  (let [files (->> (file-seq (io/file log-dir))
                   (filter #(str/ends-with? (.getName ^java.io.File %) ".log"))
                   (sort-by #(.lastModified ^java.io.File %)))]
    (vec
     (for [f files
           line (with-open [r (io/reader f)] (doall (filter #(str/includes? % ":index/create-index")
                                                            (line-seq r))))
           :let [dat (some-> (re-find #":datoms (\d+)" line) second parse-long)
                 mse (some-> (re-find #":msec ([0-9.E+]+)" line) second parse-double)
                 ast (some-> (re-find #":as-of-t (\d+)" line) second parse-long)
                 wri (some-> (re-find #":written (\d+)" line) second parse-long)]
           :when (and dat mse)]
       {:datoms dat :msec mse :as-of-t ast :segments-written wri}))))

(defn jobs-since [log-dir n-before]
  (drop n-before (index-jobs log-dir)))

;; ---------------------------------------------------------------------------
;; One iteration
;; ---------------------------------------------------------------------------

(defn run-iteration!
  [{:keys [uri-base uri-suffix tx-batches jmx log-dir txor-pid ncpu iteration
            peers inflight-per-peer]
     :or {peers 1 inflight-per-peer 50 uri-suffix ""}}]
  (let [dbname (str "idxbench-" (System/currentTimeMillis) "-" iteration)
        uri    (str uri-base dbname uri-suffix)
        _      (d/create-database uri)
        conn   (d/connect uri)]
    @(d/transact conn gh/schema)

    (let [jobs0  (count (index-jobs log-dir))
          jvm0   (jvm/snapshot jmx)
          host0  (jvm/host-snapshot txor-pid)
          t0     (System/nanoTime)

          ;; ---- LOAD ----
          ;; Split the batches across `peers` independent connections, each
          ;; driven by its own thread with its own in-flight window.
          ;;
          ;; A single peer cannot saturate the transactor: one connection
          ;; pipelines transactions but they are still applied serially, and the
          ;; transactor spends much of its time waiting on that one stream.
          ;; Multiple peers is the only way to find out whether the transactor's
          ;; CPU ceiling is a property of the transactor or an artefact of
          ;; offering it work through a single pipe.
          n-ev   (atom 0)
          _      (let [chunks (if (<= peers 1)
                                [tx-batches]
                                ;; deal round-robin so every peer gets a mix of
                                ;; batch sizes and upsert hit-rates
                                (->> tx-batches
                                     (map-indexed vector)
                                     (group-by #(mod (first %) peers))
                                     (sort-by key)
                                     (mapv (fn [[_ v]] (mapv second v)))))
                     futs (mapv
                           (fn [my-batches]
                             (future
                               (let [c (d/connect uri)]
                                 (loop [bs my-batches inflight []]
                                   (if-let [bb (first bs)]
                                     (let [f (d/transact-async c bb)
                                           _ (swap! n-ev + (count bb))
                                           inf (conj inflight f)]
                                       (if (>= (count inf) inflight-per-peer)
                                         (do (doseq [x inf] @x) (recur (rest bs) []))
                                         (recur (rest bs) inf)))
                                     (doseq [x inflight] @x))))))
                           chunks)]
                 (doseq [f futs] @f))
          t-load (System/nanoTime)

          ;; ---- INDEX: force the tail merge and wait ----
          bt     (d/basis-t (d/db conn))
          _      (d/request-index conn)
          idb    (deref (d/sync-index conn bt) 1800000 :timeout)
          t-idx  (System/nanoTime)

          jvm1   (jvm/snapshot jmx)
          host1  (jvm/host-snapshot txor-pid)

          load-ms  (ms (- t-load t0))
          index-ms (ms (- t-idx t-load))
          total-ms (ms (- t-idx t0))

          jobs   (vec (jobs-since log-dir jobs0))
          j-dat  (reduce + 0 (map :datoms jobs))
          j-ms   (reduce + 0.0 (map :msec jobs))
          j-seg  (reduce + 0 (keep :segments-written jobs))
          datoms (:datoms (d/db-stats (d/db conn)))
          dl     (jvm/delta jvm0 jvm1 host0 host1 total-ms ncpu)]

      (d/delete-database uri)

      {:iteration iteration
       :db dbname
       :peers peers
       :inflight-per-peer inflight-per-peer
       :events @n-ev
       :datoms datoms
       :timed-out (= idb :timeout)

       ;; --- primary metrics ---
       :load-ms load-ms
       :index-tail-ms index-ms
       :total-ms total-ms
       :events-per-sec (/ @n-ev (/ total-ms 1000.0))
       :datoms-per-sec (/ (double datoms) (/ total-ms 1000.0))

       ;; --- indexing, from the transactor's own accounting ---
       :index-jobs (count jobs)
       :index-datoms j-dat
       :index-job-ms j-ms
       :index-throughput (when (pos? j-ms) (/ j-dat (/ j-ms 1000.0)))
       :index-segments j-seg
       :index-job-detail jobs

       ;; --- JVM + system, sampled on the TRANSACTOR ---
       :jvm dl})))

;; ---------------------------------------------------------------------------
;; main
;; ---------------------------------------------------------------------------

(defn fmt [x] (if (number? x) (format "%.2f" (double x)) (str x)))

(defn report-line [r]
  (let [j (:jvm r)]
    (printf (str "iter %d  peers=%d  events=%d datoms=%d  total=%.2fs  "
                 "%.0f ev/s  %.0f datoms/s  | index: %d jobs %.0f datoms/s  "
                 "| gc=%.1f%% jit=%.1f%% cpu=%.2f cores io_w=%.1f MB/s%n")
            (:iteration r) (:peers r) (:events r) (:datoms r) (/ (:total-ms r) 1000.0)
            (:events-per-sec r) (:datoms-per-sec r)
            (:index-jobs r) (double (or (:index-throughput r) 0.0))
            (double (or (:gc-pct j) 0.0)) (double (or (:jit-pct j) 0.0))
            (double (or (:proc-cpu-cores j) 0.0)) (double (or (:io-write-MBps j) 0.0)))
    (flush)))

(defn -main [& args]
  (let [{:strs [size iterations files jmx log-dir pid out batch peers inflight protocol]}
        (into {} (map (fn [[k v]] [(str/replace k #"^--" "") v])
                      (partition 2 args)))
        size-k    (keyword (or size "medium"))
        n-events  (or (get sizes size-k) (parse-long (or size "250000")))
        iters     (parse-long (or iterations "3"))
        fileseq   (str/split (or files "") #",")
        jmx-hp    (or jmx "localhost:7091")
        logd      (or log-dir "/home/xyang/dtm/log")
        txpid     (parse-long (or pid "0"))
        batch-sz  (parse-long (or batch "200"))
        n-peers   (parse-long (or peers "1"))
        inflight-n (parse-long (or inflight "50"))
        ;; Storage protocol the transactor is running. The peer URI must match
        ;; it: a dev:// peer against a sql:// transactor fails with an H2
        ;; connection refusal, because the peer tries to reach H2 directly.
        proto     (or protocol "dev")
        ;; A sql:// peer URI must carry the JDBC connection string itself --
        ;; unlike dev://, the peer talks to storage directly and gets no
        ;; connection details from the transactor. Without it Datomic raises
        ;; :db.error/invalid-sql-connection.
        sql-url   (or (get (into {} (map (fn [[k v]] [(str/replace k #"^--" "") v])
                                         (partition 2 args)))
                           "sql-url")
                      "jdbc:postgresql://localhost:5432/datomic?user=datomic&password=datomic")
        uri-base  (if (= proto "sql")
                    (str "datomic:sql://")
                    (str "datomic:" proto "://localhost:4334/"))
        uri-suffix (if (= proto "sql") (str "?" sql-url) "")
        ncpu      (.availableProcessors (Runtime/getRuntime))
        outf      (or out "idxbench-result.json")]

    (printf "storage=%s  uri=%s<db>%s%n" proto uri-base (if (= proto "sql") "?<jdbc>" ""))
    (printf "size=%s (%d events, ~%s MB raw)  iterations=%d  batch=%d  peers=%d  inflight/peer=%d  jmx=%s  txor-pid=%d%n"
            (name size-k) n-events (str (get size-raw-mb size-k "?"))
            iters batch-sz n-peers inflight-n jmx-hp txpid)
    (printf "files parsed: %s%n" (pr-str fileseq))
    (println "preloading + converting events (excluded from measurement)...")
    (let [t0 (System/nanoTime)
          batches (preload! fileseq n-events batch-sz)
          nb (count batches)]
      (printf "preloaded %d batches in %.1f s%n" nb (/ (ms (- (System/nanoTime) t0)) 1000.0))

      (let [jmx-conn (jvm/connect jmx-hp)
            results (vec (for [i (range 1 (inc iters))]
                           (let [r (run-iteration!
                                    {:uri-base uri-base
                                     :uri-suffix uri-suffix
                                     :tx-batches batches
                                     :jmx jmx-conn
                                     :log-dir logd
                                     :txor-pid txpid
                                     :ncpu ncpu
                                     :iteration i
                                     :peers n-peers
                                     :inflight-per-peer inflight-n})]
                             (report-line r)
                             r)))
            final (last results)]

        (println)
        (println "=== FINAL (last iteration; earlier iterations are warmup) ===")
        (let [j (:jvm final)]
          (printf "  size:                %s (%d events, ~%s MB raw source)%n"
                  (name size-k) n-events (str (get size-raw-mb size-k "?")))
          (printf "  peers:               %d (x%d in-flight)%n" n-peers inflight-n)
          (printf "  datoms:              %d%n" (:datoms final))
          (printf "  load wall:           %.3f s%n" (/ (:load-ms final) 1000.0))
          (printf "  index tail wall:     %.3f s%n" (/ (:index-tail-ms final) 1000.0))
          (printf "  total wall:          %.3f s%n" (/ (:total-ms final) 1000.0))
          (printf "  ingest throughput:   %.0f events/s  %.0f datoms/s%n"
                  (:events-per-sec final) (:datoms-per-sec final))
          (printf "  INDEX THROUGHPUT:    %.0f datoms/s  (%d jobs, %.0f datoms, %.1f s in-job)%n"
                  (double (or (:index-throughput final) 0.0)) (:index-jobs final)
                  (double (:index-datoms final)) (/ (:index-job-ms final) 1000.0))
          (printf "  index segments:      %d%n" (long (:index-segments final)))
          (println "  --- transactor JVM ---")
          (printf "  GC:                  %d collections, %.0f ms (%.2f%% of wall)%n"
                  (long (or (:gc-count j) 0)) (double (or (:gc-ms j) 0.0)) (double (or (:gc-pct j) 0.0)))
          (printf "  JIT:                 %.0f ms (%.2f%% of wall)%n"
                  (double (or (:jit-ms j) 0.0)) (double (or (:jit-pct j) 0.0)))
          (printf "  CPU:                 %.2f cores (%.1f%% of %d-cpu host)%n"
                  (double (or (:proc-cpu-cores j) 0.0)) (double (or (:proc-cpu-pct-host j) 0.0)) ncpu)
          (printf "  heap after:          %.0f MB / %.0f MB%n"
                  (/ (or (:heap-used-after j) 0) 1048576.0)
                  (/ (or (:heap-max j) 0) 1048576.0))
          (printf "  threads:             %d%n" (long (or (:threads-after j) 0)))
          (println "  --- host ---")
          (printf "  host CPU busy:       %.1f%%   iowait: %.1f%%%n"
                  (double (or (:host-cpu-pct j) 0.0)) (double (or (:host-iowait-pct j) 0.0)))
          (printf "  transactor IO:       read %.1f MB  write %.1f MB  (%.1f MB/s write)%n"
                  (/ (or (:io-read-bytes j) 0) 1048576.0)
                  (/ (or (:io-write-bytes j) 0) 1048576.0)
                  (double (or (:io-write-MBps j) 0.0))))

        (spit outf (json/write-str
                    {:size (name size-k) :events n-events :iterations iters
                     :raw-mb (get size-raw-mb size-k)
                     :batch batch-sz :peers n-peers :inflight-per-peer inflight-n
                     :protocol proto
                     :ncpu ncpu
                     :warmup (vec (butlast results))
                     :final final}))
        (println "\nwrote" outf)
        (shutdown-agents)
        (System/exit 0)))))

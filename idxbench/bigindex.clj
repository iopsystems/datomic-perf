(ns bench.bigindex
  "Index to a target datom count, then back the database up and time it.

   Why this is separate from bench.idxbench: that harness calls preload!, which
   materialises every transaction in the peer heap before timing starts (~0.0157
   MB/event measured). At the ~137M events needed for a billion datoms that is
   ~2 TB of heap. This namespace streams instead: a producer thread parses and
   converts events onto a bounded queue while the consumer transacts, so memory
   is bounded by the queue depth rather than the dataset size.

   Parsing stays off the consumer's critical path (the reason preload! existed),
   because it happens on the producer thread ahead of the transactor.

   Progress and the final numbers come from the transactor's own :index/create-index
   accounting, same as the rest of this benchmark suite -- d/sync-index only times
   the trailing merge."
  (:require [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.java.shell :as sh]
            [bench.jvmstats :as jvm]
            [bench.gharchive :as gh])
  (:import [java.util.zip GZIPInputStream]
           [java.util.concurrent ArrayBlockingQueue TimeUnit]))

(def ^:private END ::end)

(defn ms [n] (/ (double n) 1e6))

;; ---------------------------------------------------------------------------
;; Streaming producer
;; ---------------------------------------------------------------------------

(defn- offset-event
  "Shift a converted tx map onto a fresh identity space.

   Only used once the real files are exhausted. Without this, replaying a file
   would upsert onto the entities it created the first time and the database
   would stop growing. Offsetting keeps the datom shape and the within-pass
   upsert distribution while adding new entities."
  [m cycle-n]
  (let [off (* cycle-n 1000000000)]
    (cond-> m
      true                (update :event/id str "-c" cycle-n)
      (:event/actor m)    (update-in [:event/actor :user/id] + off)
      (:event/repo m)     (update-in [:event/repo :repo/id] + off)
      (get-in m [:event/repo :repo/org])
                          (update-in [:event/repo :repo/org :org/id] + off)
      (:event/org m)      (update-in [:event/org :org/id] + off))))

(def ^:private ref-id
  "Identity attribute for each nested entity a converted event can carry."
  {:event/actor :user/id, :event/repo :repo/id, :event/org :org/id})

(defn- thin
  "Reduce a nested entity map to its identity alone if we have already emitted
   its full form in this transaction."
  [m idk seen]
  (let [v (get m idk)]
    (if (nil? v)
      m
      (if (contains? @seen [idk v])
        {idk v}
        (do (vswap! seen conj [idk v]) m)))))

(defn- dedupe-tx
  "Make one transaction internally consistent.

   GitHub reuses a numeric id across login spellings -- user 17592186449658
   appears as both \"kevinr99089\" and \"Kevinr99089\" within a single archive
   hour -- so two events batched together can assert different :user/login values
   for the same :user/id. Datomic rejects that with :db.error/datoms-conflict,
   which is what caps batch size on this dataset. Keeping the first full map per
   identity and reducing later references to the identity alone removes the
   conflict without dropping any entity or reference."
  [tx]
  (let [seen (volatile! #{})]
    (mapv (fn [m]
            (reduce (fn [acc [k idk]]
                      (if-let [nested (get acc k)]
                        (let [t (thin nested idk seen)
                              t (if-let [org (:repo/org t)]
                                  (assoc t :repo/org (thin org :org/id seen))
                                  t)]
                          (assoc acc k t))
                        acc))
                    m ref-id))
          tx)))

(defn- file-events
  "Lazy seq of parsed events from one .json.gz. Caller must consume fully or
   accept the reader staying open until GC; we always consume fully."
  [path]
  (let [is (java.io.FileInputStream. ^String path)
        gz (GZIPInputStream. is 262144)
        r  (io/reader gz)]
    (map #(json/read-str %) (line-seq r))))

(defn start-producer!
  "Feed batches of tx-data onto `q` until `stop?` is set. Cycles the file list
   with an identity offset once exhausted. Returns a future."
  [q files batch stop? cycles-used]
  (future
    (try
      (loop [cycle-n 0]
        (when-not @stop?
          (doseq [f files :while (not @stop?)]
            (let [evs (file-events f)]
              (doseq [chunk (partition-all batch evs) :while (not @stop?)]
                (let [tx (dedupe-tx
                          (into []
                                (comp (mapcat gh/event->tx)
                                      (map #(if (zero? cycle-n) % (offset-event % cycle-n))))
                                chunk))]
                  ;; block rather than buffer without bound: the queue is the
                  ;; memory ceiling for the whole run
                  (loop []
                    (when-not @stop?
                      (when-not (.offer q tx 250 TimeUnit/MILLISECONDS)
                        (recur))))))))
          (reset! cycles-used (inc cycle-n))
          (recur (inc cycle-n))))
      (catch Throwable t
        (binding [*out* *err*] (println "producer error:" (.getMessage t))))
      (finally
        ;; unblock every consumer
        (dotimes [_ 64] (.offer q END 1 TimeUnit/SECONDS))))))

;; ---------------------------------------------------------------------------
;; Backup
;; ---------------------------------------------------------------------------

(defn backup-db!
  "Run bin/datomic backup-db and time it. Returns timing plus the resulting
   directory size, or :error with the tail of the output."
  [dist uri backup-uri]
  (let [script (str dist "/bin/datomic")
        t0 (System/nanoTime)
        {:keys [exit out err]} (sh/sh script "-Xmx4g" "backup-db" uri backup-uri
                                       :dir dist)
        el (ms (- (System/nanoTime) t0))]
    {:exit exit
     :backup-ms el
     :tail (str/join "\n" (take-last 6 (str/split-lines (str out "\n" err))))}))

(defn dir-size [path]
  (let [f (io/file path)]
    (when (.exists f)
      (reduce + 0 (map #(.length ^java.io.File %)
                       (filter #(.isFile ^java.io.File %) (file-seq f)))))))

;; ---------------------------------------------------------------------------
;; Index job accounting (same source as the rest of the suite)
;; ---------------------------------------------------------------------------

(defn index-jobs [log-dir]
  (let [files (->> (file-seq (io/file log-dir))
                   (filter #(str/ends-with? (.getName ^java.io.File %) ".log"))
                   (sort-by #(.lastModified ^java.io.File %)))]
    (vec (for [f files
               line (with-open [r (io/reader f)]
                      (doall (filter #(str/includes? % ":index/create-index") (line-seq r))))
               :let [dat (some-> (re-find #":datoms (\d+)" line) second parse-long)
                     mse (some-> (re-find #":msec ([0-9.E+]+)" line) second parse-double)]
               :when (and dat mse)]
           {:datoms dat :msec mse}))))

;; ---------------------------------------------------------------------------
;; main
;; ---------------------------------------------------------------------------

(defn -main [& args]
  (let [a (into {} (map (fn [[k v]] [(str/replace k #"^--" "") v]) (partition 2 args)))
        target   (parse-long (or (get a "target-datoms") "1000000000"))
        files    (vec (sort (remove str/blank? (str/split (or (get a "files") "") #","))))
        batch    (parse-long (or (get a "batch") "1000"))
        inflight (parse-long (or (get a "inflight") "500"))
        qdepth   (parse-long (or (get a "queue") "2000"))
        jmx-hp   (or (get a "jmx") "localhost:7091")
        logd     (or (get a "log-dir") "log")
        txpid    (parse-long (or (get a "pid") "0"))
        uri      (or (get a "uri") "datomic:dev://localhost:4334/big")
        dist     (get a "dist")
        bdir     (get a "backup-dir")
        hints?   (= "true" (get a "hints" "false"))
        outf     (or (get a "out") "bigindex-result.json")
        progf    (or (get a "progress") "bigindex-progress.jsonl")
        ncpu     (.availableProcessors (Runtime/getRuntime))]

    (printf "target=%,d datoms  files=%d  batch=%d  inflight=%d  queue=%d  hints=%s%n"
            target (count files) batch inflight qdepth hints?)
    (printf "uri=%s%n" uri)
    (flush)

    (when (empty? files) (println "FATAL: no --files given") (System/exit 1))

    (d/create-database uri)
    (let [conn (d/connect uri)]
      @(d/transact conn gh/schema)

      (let [q     (ArrayBlockingQueue. qdepth)
            stop? (atom false)
            cycles (atom 0)
            n-ev  (java.util.concurrent.atomic.AtomicLong. 0)
            n-tx  (java.util.concurrent.atomic.AtomicLong. 0)
            jobs0 (count (index-jobs logd))
            jvm0  (jvm/snapshot (jvm/connect jmx-hp))
            host0 (jvm/host-snapshot txpid)
            jmxc  (jvm/connect jmx-hp)
            t0    (System/nanoTime)
            prod  (start-producer! q files batch stop? cycles)

            ;; progress sampler: datom count is the stop condition, and this is
            ;; also the growth curve we report
            last-datoms (atom 0)
            sampler (future
                      (with-open [w (io/writer progf)]
                        (loop []
                          (Thread/sleep 15000)
                          (let [db (d/db conn)
                                n  (:datoms (d/db-stats db))
                                el (ms (- (System/nanoTime) t0))]
                            (reset! last-datoms n)
                            (.write w (json/write-str
                                       {:t (System/currentTimeMillis)
                                        :elapsed-s (/ el 1000.0)
                                        :datoms n
                                        :events (.get n-ev)
                                        :txs (.get n-tx)
                                        :cycles @cycles
                                        :datoms-per-sec (/ (double n) (max 0.001 (/ el 1000.0)))}))
                            (.write w "\n") (.flush w)
                            (printf "  [%6.0fs] datoms=%,d  events=%,d  %,.0f datoms/s  cycles=%d%n"
                                    (/ el 1000.0) n (.get n-ev)
                                    (/ (double n) (max 0.001 (/ el 1000.0))) @cycles)
                            (flush)
                            (when (>= n target) (reset! stop? true))
                            (when-not @stop? (recur))))))]

        ;; consumer
        (loop [inf []]
          (let [item (.poll q 500 TimeUnit/MILLISECONDS)]
            (cond
              (or @stop? (= item END))
              (doseq [f inf] @f)

              (nil? item)
              (recur inf)

              :else
              (let [f (if hints?
                        (let [h (:hints (d/with (d/db conn) item :return-hints true))]
                          (d/transact-async conn item :hints h))
                        (d/transact-async conn item))
                    _ (.addAndGet n-ev (long (count item)))
                    _ (.incrementAndGet n-tx)
                    inf (conj inf f)]
                (if (>= (count inf) inflight)
                  (do (doseq [x inf] @x) (recur []))
                  (recur inf))))))

        (reset! stop? true)
        (try @sampler (catch Exception _ nil))
        (future-cancel prod)

        (let [t-load  (System/nanoTime)
              db      (d/db conn)
              datoms  (:datoms (d/db-stats db))
              load-ms (ms (- t-load t0))
              jobs    (drop jobs0 (index-jobs logd))
              j-dat   (reduce + 0 (map :datoms jobs))
              j-ms    (reduce + 0.0 (map :msec jobs))
              jvm1    (jvm/snapshot jmxc)
              host1   (jvm/host-snapshot txpid)
              dl      (jvm/delta jvm0 jvm1 host0 host1 load-ms ncpu)]

          (printf "%n=== LOAD COMPLETE ===%n")
          (printf "  datoms             %,d%n" datoms)
          (printf "  events             %,d  (txs %,d, cycles %d)%n"
                  (.get n-ev) (.get n-tx) @cycles)
          (printf "  wall               %.1f s (%.2f h)%n" (/ load-ms 1000.0) (/ load-ms 3600000.0))
          (printf "  ingest             %,.0f datoms/s%n" (/ (double datoms) (/ load-ms 1000.0)))
          (printf "  INDEX THROUGHPUT   %,.0f datoms/s  (%d jobs, %,d datoms, %.1f s in-job)%n"
                  (if (pos? j-ms) (/ j-dat (/ j-ms 1000.0)) 0.0)
                  (count jobs) j-dat (/ j-ms 1000.0))
          (printf "  transactor CPU     %.2f cores%n" (double (or (:proc-cpu-cores dl) 0.0)))
          (flush)

          ;; ---- backup ----
          (let [bk (when (and dist bdir)
                     (printf "%n=== BACKUP -> %s ===%n" bdir) (flush)
                     (let [r (backup-db! dist uri (str "file://" bdir))
                           sz (dir-size bdir)]
                       (printf "  exit               %s%n" (:exit r))
                       (printf "  backup latency     %.1f s (%.2f min)%n"
                               (/ (:backup-ms r) 1000.0) (/ (:backup-ms r) 60000.0))
                       (when sz
                         (printf "  backup size        %.2f GiB%n" (/ sz (double (* 1024 1024 1024))))
                         (printf "  backup rate        %.1f MiB/s%n"
                                 (/ (/ sz (double (* 1024 1024))) (/ (:backup-ms r) 1000.0))))
                       (println "  tail:" (:tail r))
                       (flush)
                       (assoc r :bytes sz)))]

            (spit outf (json/write-str
                        {:target target :datoms datoms
                         :events (.get n-ev) :txs (.get n-tx) :cycles @cycles
                         :batch batch :inflight inflight :hints hints?
                         :load-ms load-ms
                         :ingest-datoms-per-sec (/ (double datoms) (/ load-ms 1000.0))
                         :index-jobs (count jobs) :index-datoms j-dat :index-job-ms j-ms
                         :index-throughput (when (pos? j-ms) (/ j-dat (/ j-ms 1000.0)))
                         :jvm dl
                         :backup bk}))
            (println "\nwrote" outf)))
        (shutdown-agents)
        (System/exit 0)))))

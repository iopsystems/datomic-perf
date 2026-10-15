(ns bench.bigindex
  "Restore, index to a target datom count, then back up -- timing each phase.

   Phases, each optional:
     1. restore  --restore-from DIR: bin/datomic restore-db into --uri, then a
                 cold connect + db-stats. Without it a fresh database is created.
     2. index    --target-datoms N > 0: stream GH Archive until the database
                 holds N datoms more than it started with (0 on a fresh db).
                 --final-index true then forces and times one last index job.
     3. backup   --backup-dir DIR: bin/datomic backup-db.

   After a restore, pass --files that are not in the backup. Replaying files
   the backup already holds upserts onto existing entities and adds almost no
   datoms.

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
          ;; count only passes that ran to the end; stopping mid-pass used to
          ;; bump this too, so older results over-report cycles by one
          (when-not @stop?
            (reset! cycles-used (inc cycle-n))
            (recur (inc cycle-n)))))
      (catch Throwable t
        (binding [*out* *err*] (println "producer error:" (.getMessage t))))
      (finally
        ;; unblock every consumer
        (dotimes [_ 64] (.offer q END 1 TimeUnit/SECONDS))))))

;; ---------------------------------------------------------------------------
;; bin/datomic restore-db / backup-db
;; ---------------------------------------------------------------------------

(defn- storage-uri
  "Accept either a URI or a bare local path for a backup location."
  [s]
  (if (re-find #"^[a-z0-9]+://" s) s (str "file://" s)))

(defn- local-path
  "The local directory behind a backup location, or nil if it is remote."
  [s]
  (cond (str/starts-with? s "file://")  (subs s 7)
        (re-find #"^[a-z0-9]+://" s)    nil
        :else                           s))

(defn- run-datomic!
  "Run one bin/datomic subcommand and time it. Returns the exit code, the
   wall time and the tail of the output."
  [dist & args]
  (let [t0 (System/nanoTime)
        {:keys [exit out err]} (apply sh/sh (str dist "/bin/datomic") "-Xmx4g"
                                      (concat args [:dir dist]))]
    {:exit exit
     :ms   (ms (- (System/nanoTime) t0))
     :tail (str/join "\n" (take-last 6 (str/split-lines (str out "\n" err))))}))

(defn dir-size [path]
  (let [f (io/file path)]
    (when (.exists f)
      (reduce + 0 (map #(.length ^java.io.File %)
                       (filter #(.isFile ^java.io.File %) (file-seq f)))))))

(defn- gib [b] (/ b (double (* 1024 1024 1024))))
(defn- mib-per-sec [b el-ms] (/ (/ b (double (* 1024 1024))) (/ el-ms 1000.0)))

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

(defn- txor-probe
  "Start measuring the transactor. Call the returned fn to get the delta."
  [jmxc txpid ncpu]
  (let [j0 (jvm/snapshot jmxc)
        h0 (jvm/host-snapshot txpid)
        t0 (System/nanoTime)]
    (fn []
      (jvm/delta j0 (jvm/snapshot jmxc) h0 (jvm/host-snapshot txpid)
                 (ms (- (System/nanoTime) t0)) ncpu))))

;; ---------------------------------------------------------------------------
;; Phase 1: restore
;; ---------------------------------------------------------------------------

(defn restore-phase!
  "Restore a backup into `uri`, then connect and count what came back.
   Dev storage lives inside the transactor, so it must already be running.
   Exits on failure: without the restored database there is nothing to load
   onto."
  [dist from uri jmxc txpid ncpu]
  (printf "%n=== RESTORE <- %s ===%n" from) (flush)
  (let [bytes (some-> (local-path from) dir-size)
        probe (txor-probe jmxc txpid ncpu)
        r     (run-datomic! dist "restore-db" (storage-uri from) uri)
        dl    (probe)]
    (printf "  exit               %s%n" (:exit r))
    (printf "  restore latency    %.1f s (%.2f min)%n" (/ (:ms r) 1000.0) (/ (:ms r) 60000.0))
    (when-not (zero? (:exit r))
      (println "  tail:" (:tail r))
      (println "FATAL: restore-db failed")
      (flush)
      (System/exit 1))
    ;; the first connect and db-stats after a restore are cold: the peer has
    ;; nothing cached and has to read the index roots from storage
    (let [tc   (System/nanoTime)
          conn (d/connect uri)
          c-ms (ms (- (System/nanoTime) tc))
          ts   (System/nanoTime)
          n    (:datoms (d/db-stats (d/db conn)))
          s-ms (ms (- (System/nanoTime) ts))]
      (when bytes
        (printf "  backup size        %.2f GiB%n" (gib bytes))
        (printf "  restore rate       %.1f MiB/s%n" (mib-per-sec bytes (:ms r))))
      (printf "  datoms restored    %,d  (%,.0f datoms/s)%n" n (/ (double n) (/ (:ms r) 1000.0)))
      (printf "  first connect      %.1f s, first db-stats %.1f s%n" (/ c-ms 1000.0) (/ s-ms 1000.0))
      (printf "  transactor CPU     %.2f cores%n" (double (or (:proc-cpu-cores dl) 0.0)))
      (println "  tail:" (:tail r))
      (flush)
      {:from from :exit (:exit r) :restore-ms (:ms r)
       :bytes bytes :mib-per-sec (when bytes (mib-per-sec bytes (:ms r)))
       :datoms n :datoms-per-sec (/ (double n) (/ (:ms r) 1000.0))
       :connect-ms c-ms :first-stats-ms s-ms
       :jvm dl :tail (:tail r)})))

;; ---------------------------------------------------------------------------
;; Phase 2: index
;; ---------------------------------------------------------------------------

(defn load-phase!
  "Stream GH Archive into `conn` until it holds `base` + `target` datoms."
  [conn {:keys [files batch inflight qdepth hints? target base sample-ms logd progf]}
   jmxc txpid ncpu]
  (printf "%n=== INDEX +%,d datoms (from %,d) ===%n" target base) (flush)
  (let [goal  (+ base target)
        q     (ArrayBlockingQueue. qdepth)
        stop? (atom false)
        cycles (atom 0)
        n-ev  (java.util.concurrent.atomic.AtomicLong. 0)
        n-tx  (java.util.concurrent.atomic.AtomicLong. 0)
        jobs0 (count (index-jobs logd))
        probe (txor-probe jmxc txpid ncpu)
        t0    (System/nanoTime)
        prod  (start-producer! q files batch stop? cycles)

        ;; progress sampler: datom count is the stop condition, and this is
        ;; also the growth curve we report
        sampler (future
                  (with-open [w (io/writer progf)]
                    (loop []
                      (Thread/sleep (long sample-ms))
                      (let [db (d/db conn)
                            n  (:datoms (d/db-stats db))
                            el (ms (- (System/nanoTime) t0))
                            rate (/ (double (- n base)) (max 0.001 (/ el 1000.0)))]
                        (.write w (json/write-str
                                   {:t (System/currentTimeMillis)
                                    :elapsed-s (/ el 1000.0)
                                    :datoms n
                                    :added (- n base)
                                    :events (.get n-ev)
                                    :txs (.get n-tx)
                                    :cycles @cycles
                                    :datoms-per-sec rate}))
                        (.write w "\n") (.flush w)
                        (printf "  [%6.0fs] datoms=%,d  added=%,d  events=%,d  %,.0f datoms/s  cycles=%d%n"
                                (/ el 1000.0) n (- n base) (.get n-ev) rate @cycles)
                        (flush)
                        (when (>= n goal) (reset! stop? true))
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

    (let [load-ms (ms (- (System/nanoTime) t0))
          dl      (probe)
          datoms  (:datoms (d/db-stats (d/db conn)))
          added   (- datoms base)
          jobs    (drop jobs0 (index-jobs logd))
          j-dat   (reduce + 0 (map :datoms jobs))
          j-ms    (reduce + 0.0 (map :msec jobs))]

      (printf "%n=== LOAD COMPLETE ===%n")
      (printf "  datoms             %,d%n" datoms)
      (when (pos? base)
        (printf "  added              %,d  (on top of %,d)%n" added base))
      (printf "  events             %,d  (txs %,d, cycles %d)%n"
              (.get n-ev) (.get n-tx) @cycles)
      (printf "  wall               %.1f s (%.2f h)%n" (/ load-ms 1000.0) (/ load-ms 3600000.0))
      (printf "  ingest             %,.0f datoms/s%n" (/ (double added) (/ load-ms 1000.0)))
      (printf "  INDEX THROUGHPUT   %,.0f datoms/s  (%d jobs, %,d datoms, %.1f s in-job)%n"
              (if (pos? j-ms) (/ j-dat (/ j-ms 1000.0)) 0.0)
              (count jobs) j-dat (/ j-ms 1000.0))
      (printf "  transactor CPU     %.2f cores%n" (double (or (:proc-cpu-cores dl) 0.0)))
      (flush)

      {:added added
       :events (.get n-ev) :txs (.get n-tx) :cycles @cycles
       :load-ms load-ms
       :ingest-datoms-per-sec (/ (double added) (/ load-ms 1000.0))
       :index-jobs (count jobs) :index-datoms j-dat :index-job-ms j-ms
       :index-throughput (when (pos? j-ms) (/ j-dat (/ j-ms 1000.0)))
       :jvm dl})))

(defn final-index!
  "Index whatever is still in the memory index and time it. A short load on
   top of a large restored database may never reach memory-index-threshold,
   so without this the phase would measure ingest only, not the merge of new
   datoms into the large durable index."
  [conn logd jmxc txpid ncpu]
  (printf "%n=== FINAL INDEX ===%n") (flush)
  (let [jobs0 (count (index-jobs logd))
        t     (d/basis-t (d/db conn))
        probe (txor-probe jmxc txpid ncpu)
        t0    (System/nanoTime)]
    (d/request-index conn)
    @(d/sync-index conn t)
    (let [el (ms (- (System/nanoTime) t0))
          dl (probe)]
      ;; the job's log line can land just after sync-index returns
      (Thread/sleep 5000)
      (let [jobs  (drop jobs0 (index-jobs logd))
            j-dat (reduce + 0 (map :datoms jobs))
            j-ms  (reduce + 0.0 (map :msec jobs))]
        (printf "  sync-index         %.1f s%n" (/ el 1000.0))
        (printf "  index jobs         %d  (%,d datoms, %.1f s in-job)%n"
                (count jobs) j-dat (/ j-ms 1000.0))
        (printf "  transactor CPU     %.2f cores%n" (double (or (:proc-cpu-cores dl) 0.0)))
        (flush)
        {:ms el :index-jobs (count jobs) :index-datoms j-dat :index-job-ms j-ms
         :jvm dl}))))

;; ---------------------------------------------------------------------------
;; Phase 3: backup
;; ---------------------------------------------------------------------------

(defn backup-phase!
  "Back `uri` up to `bdir` with bin/datomic backup-db and time it."
  [dist uri bdir jmxc txpid ncpu]
  (printf "%n=== BACKUP -> %s ===%n" bdir) (flush)
  (let [probe (txor-probe jmxc txpid ncpu)
        r     (run-datomic! dist "backup-db" uri (storage-uri bdir))
        dl    (probe)
        sz    (some-> (local-path bdir) dir-size)]
    (printf "  exit               %s%n" (:exit r))
    (printf "  backup latency     %.1f s (%.2f min)%n" (/ (:ms r) 1000.0) (/ (:ms r) 60000.0))
    (when sz
      (printf "  backup size        %.2f GiB%n" (gib sz))
      (printf "  backup rate        %.1f MiB/s%n" (mib-per-sec sz (:ms r))))
    (printf "  transactor CPU     %.2f cores%n" (double (or (:proc-cpu-cores dl) 0.0)))
    (println "  tail:" (:tail r))
    (flush)
    {:exit (:exit r) :backup-ms (:ms r) :tail (:tail r) :bytes sz :jvm dl}))

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
        restore  (get a "restore-from")
        bdir     (get a "backup-dir")
        hints?   (= "true" (get a "hints" "false"))
        final?   (= "true" (get a "final-index" "false"))
        sample   (parse-long (or (get a "sample-ms") "15000"))
        outf     (or (get a "out") "bigindex-result.json")
        progf    (or (get a "progress") "bigindex-progress.jsonl")
        ncpu     (.availableProcessors (Runtime/getRuntime))]

    (printf "target=+%,d datoms  files=%d  batch=%d  inflight=%d  queue=%d  hints=%s%n"
            target (count files) batch inflight qdepth hints?)
    (printf "uri=%s  restore-from=%s  backup-dir=%s  final-index=%s%n"
            uri (or restore "-") (or bdir "-") final?)
    (flush)

    (when (and (pos? target) (empty? files))
      (println "FATAL: --target-datoms > 0 needs --files") (System/exit 1))
    (when (and (or restore bdir) (not dist))
      (println "FATAL: --restore-from and --backup-dir need --dist") (System/exit 1))

    (let [jmxc (jvm/connect jmx-hp)
          rs   (when restore (restore-phase! dist restore uri jmxc txpid ncpu))
          _    (when-not restore (d/create-database uri))
          conn (d/connect uri)
          ;; a restored database already carries the schema; leave it untouched
          _    (when-not restore @(d/transact conn gh/schema))
          ;; target counts datoms added in this run, so a fresh run measures
          ;; from 0 exactly as before restore existed
          base (if rs (:datoms rs) 0)
          ld   (when (pos? target)
                 (load-phase! conn {:files files :batch batch :inflight inflight
                                    :qdepth qdepth :hints? hints? :target target
                                    :base base :sample-ms sample :logd logd :progf progf}
                              jmxc txpid ncpu))
          fi   (when final? (final-index! conn logd jmxc txpid ncpu))
          bk   (when bdir (backup-phase! dist uri bdir jmxc txpid ncpu))
          datoms (:datoms (d/db-stats (d/db conn)))]

      (spit outf (json/write-str
                  (merge {:target target :datoms datoms :base-datoms base
                          :batch batch :inflight inflight :hints hints?}
                         ld
                         {:restore rs :final-index fi :backup bk})))
      (println "\nwrote" outf)
      (shutdown-agents)
      (System/exit 0))))

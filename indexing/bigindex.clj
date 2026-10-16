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
   datoms. Alternatively pass the same --files and --order the backup was built
   with plus --resume true: every backup records how far into the event stream
   it got (bench-cursor.json in the backup directory), and a resumed load skips
   that many events before transacting.

   --order asc|desc sets the file order by archive hour (chronological, not
   by name: hours are not zero-padded); desc loads newest first.

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

(def cursor-file "bench-cursor.json")

(defn archive-key
  "Chronological sort key for a GH Archive path. Hours are not zero-padded
   (2024-01-01-9.json.gz), so plain name order puts hour 10 before hour 9."
  [path]
  (let [n (.getName (io/file ^String path))]
    (if-let [[_ day hr] (re-find #"^(\d{4}-\d{2}-\d{2})-(\d{1,2})\." n)]
      [day (parse-long hr) n]
      [n 0 n])))

(defn- file-events
  "Lazy seq of parsed events from one .json.gz. The first lines are dropped
   unparsed while `skip` (a volatile count, shared across files) is positive.
   Caller must consume fully or accept the reader staying open until GC; we
   always consume fully."
  [path skip]
  (let [is (java.io.FileInputStream. ^String path)
        gz (GZIPInputStream. is 262144)
        r  (io/reader gz)]
    (map #(json/read-str %)
         (loop [ls (line-seq r)]
           (if (and (pos? @skip) (seq ls))
             (do (vswap! skip dec) (recur (rest ls)))
             ls)))))

(defn start-producer!
  "Feed batches of tx-data onto `q` until `stop?` is set, after skipping the
   first `skip-events` events of the stream. Cycles the file list with an
   identity offset once exhausted. Each batch carries the number of source
   events it came from as {:events n} metadata. Returns a future."
  [q files batch stop? cycles-used skip-events]
  (future
    (try
      (loop [cycle-n 0 skip (volatile! skip-events)]
        (when-not @stop?
          (doseq [f files :while (not @stop?)]
            (let [evs (file-events f skip)]
              (doseq [chunk (partition-all batch evs) :while (not @stop?)]
                (let [tx (with-meta
                           (dedupe-tx
                            (into []
                                  (comp (mapcat gh/event->tx)
                                        (map #(if (zero? cycle-n) % (offset-event % cycle-n))))
                                  chunk))
                           {:events (count chunk)})]
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
            (recur (inc cycle-n) skip))))
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
  [conn {:keys [files batch inflight qdepth hints? target base sample-ms logd progf skip-events]}
   jmxc txpid ncpu]
  (printf "%n=== INDEX +%,d datoms (from %,d) ===%n" target base) (flush)
  (let [goal  (+ base target)
        q     (ArrayBlockingQueue. qdepth)
        stop? (atom false)
        cycles (atom 0)
        n-ev  (java.util.concurrent.atomic.AtomicLong. 0)
        ;; source events behind every submitted tx; skip-events + this is the
        ;; stream position a later --resume continues from
        n-src (java.util.concurrent.atomic.AtomicLong. 0)
        n-tx  (java.util.concurrent.atomic.AtomicLong. 0)
        n-bad (java.util.concurrent.atomic.AtomicLong. 0)
        ;; one malformed event must not end a many-hour run: log the failed
        ;; transaction, count it, and keep going
        await! (fn [f]
                 (try @f
                      (catch Exception ex
                        (when (<= (.incrementAndGet n-bad) 10)
                          (binding [*out* *err*]
                            (println "tx failed:" (.getMessage (or (.getCause ex) ex)))
                            (flush))))))
        jobs0 (count (index-jobs logd))
        probe (txor-probe jmxc txpid ncpu)
        t0    (System/nanoTime)
        prod  (start-producer! q files batch stop? cycles skip-events)

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
          (run! await! inf)

          (nil? item)
          (recur inf)

          :else
          (let [f (if hints?
                    (let [h (:hints (d/with (d/db conn) item :return-hints true))]
                      (d/transact-async conn item :hints h))
                    (d/transact-async conn item))
                _ (.addAndGet n-ev (long (count item)))
                _ (.addAndGet n-src (long (:events (meta item) 0)))
                _ (.incrementAndGet n-tx)
                inf (conj inf f)]
            (if (>= (count inf) inflight)
              (do (run! await! inf) (recur []))
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
      (printf "  events             %,d  (txs %,d, failed %d, cycles %d)%n"
              (.get n-ev) (.get n-tx) (.get n-bad) @cycles)
      (printf "  wall               %.1f s (%.2f h)%n" (/ load-ms 1000.0) (/ load-ms 3600000.0))
      (printf "  ingest             %,.0f datoms/s%n" (/ (double added) (/ load-ms 1000.0)))
      (printf "  INDEX THROUGHPUT   %,.0f datoms/s  (%d jobs, %,d datoms, %.1f s in-job)%n"
              (if (pos? j-ms) (/ j-dat (/ j-ms 1000.0)) 0.0)
              (count jobs) j-dat (/ j-ms 1000.0))
      (printf "  transactor CPU     %.2f cores%n" (double (or (:proc-cpu-cores dl) 0.0)))
      (flush)

      {:added added
       :events (.get n-ev) :source-events (.get n-src) :txs (.get n-tx) :failed-txs (.get n-bad) :cycles @cycles
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
;; Phase markers
;; ---------------------------------------------------------------------------

(defn- now-ms [] (System/currentTimeMillis))
(defn- iso [ms] (str (java.time.Instant/ofEpochMilli ms)))

(defn phase!
  "Run `f` as phase `nm`, bracketing it with wall-clock (UTC epoch ms) start
   and end markers so host metrics recorded alongside (Rezolus) can be cut by
   phase. Each marker is printed and appended to `phasef` as it happens, so a
   run that dies mid-phase still leaves its start. The completed record is
   also conj'd onto `phases`."
  [phases phasef nm f]
  (let [mark (fn [m]
               (spit phasef (str (json/write-str m) "\n") :append true)
               (printf "PHASE %-11s %-5s %s  (%d)%n" nm (name (:event m))
                       (iso (:t m)) (:t m))
               (flush))
        t0   (now-ms)
        _    (mark {:phase nm :event :start :t t0})
        r    (f)
        t1   (now-ms)]
    (mark {:phase nm :event :end :t t1})
    (swap! phases conj {:phase nm :start-ms t0 :end-ms t1
                        :start (iso t0) :end (iso t1) :duration-ms (- t1 t0)})
    r))

;; ---------------------------------------------------------------------------
;; main
;; ---------------------------------------------------------------------------

(defn -main [& args]
  (let [a (into {} (map (fn [[k v]] [(str/replace k #"^--" "") v]) (partition 2 args)))
        target   (parse-long (or (get a "target-datoms") "1000000000"))
        order    (or (get a "order") "asc")
        files    (cond-> (vec (sort-by archive-key (remove str/blank? (str/split (or (get a "files") "") #","))))
                   (= order "desc") (-> rseq vec))
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
        resume?  (= "true" (get a "resume" "false"))
        final?   (= "true" (get a "final-index" "false"))
        sample   (parse-long (or (get a "sample-ms") "15000"))
        outf     (or (get a "out") "bigindex-result.json")
        progf    (or (get a "progress") "bigindex-progress.jsonl")
        phasef   (or (get a "phases") "bigindex-phases.jsonl")
        phases   (atom [])
        ncpu     (.availableProcessors (Runtime/getRuntime))]

    (printf "target=+%,d datoms  files=%d  batch=%d  inflight=%d  queue=%d  hints=%s%n"
            target (count files) batch inflight qdepth hints?)
    (printf "uri=%s  restore-from=%s  backup-dir=%s  final-index=%s  order=%s  resume=%s%n"
            uri (or restore "-") (or bdir "-") final? order resume?)
    (flush)

    (when-not (#{"asc" "desc"} order)
      (println "FATAL: --order must be asc or desc") (System/exit 1))
    (when (and (pos? target) (empty? files))
      (println "FATAL: --target-datoms > 0 needs --files") (System/exit 1))
    (when (and (or restore bdir) (not dist))
      (println "FATAL: --restore-from and --backup-dir need --dist") (System/exit 1))
    (when (and resume? (not restore))
      (println "FATAL: --resume true needs --restore-from") (System/exit 1))

    (let [names (mapv #(.getName (io/file ^String %)) files)
          _     (when (seq names)
                  (printf "files: %s .. %s%n" (first names) (peek names)) (flush))
          ;; Where in the event stream this load starts. --resume takes it from
          ;; the cursor the backup was written with; the skip is only
          ;; meaningful against the same files in the same order, so refuse
          ;; anything else rather than silently replaying or skipping new data.
          skip  (if resume?
                  (let [cf (some-> (local-path restore) (io/file cursor-file))
                        c  (when (and cf (.exists ^java.io.File cf))
                             (json/read-str (slurp cf) :key-fn keyword))]
                    (cond
                      (nil? c)
                      (do (println "FATAL: --resume: no" cursor-file "in" restore) (System/exit 1))
                      (not= (:order c) order)
                      (do (println "FATAL: --resume: backup was built with --order" (:order c)) (System/exit 1))
                      (not= (:files c) names)
                      (do (println "FATAL: --resume: --files differ from the backup's"
                                   (count (:files c)) "files") (System/exit 1))
                      :else
                      (do (printf "resume: skipping %,d events already in the backup%n" (:events c))
                          (flush)
                          (:events c))))
                  (parse-long (or (get a "skip-events") "0")))]
    (let [jmxc (jvm/connect jmx-hp)
          ph   (partial phase! phases phasef)
          _    (spit phasef "")
          rs   (when restore (ph "restore" #(restore-phase! dist restore uri jmxc txpid ncpu)))
          _    (when-not restore (d/create-database uri))
          conn (d/connect uri)
          ;; a restored database already carries the schema; leave it untouched
          _    (when-not restore @(d/transact conn gh/schema))
          ;; target counts datoms added in this run, so a fresh run measures
          ;; from 0 exactly as before restore existed
          base (if rs (:datoms rs) 0)
          ld   (when (pos? target)
                 (ph "index"
                     #(load-phase! conn {:files files :batch batch :inflight inflight
                                         :qdepth qdepth :hints? hints? :target target
                                         :base base :sample-ms sample :logd logd :progf progf
                                         :skip-events skip}
                                   jmxc txpid ncpu)))
          fi   (when final? (ph "final-index" #(final-index! conn logd jmxc txpid ncpu)))
          bk   (when bdir (ph "backup" #(backup-phase! dist uri bdir jmxc txpid ncpu)))
          cursor {:order order :files names
                  :events (+ skip (or (:source-events ld) 0))}
          ;; the cursor travels with the backup so a later --resume knows
          ;; where this database's events end
          _    (when (and bk (zero? (:exit bk)))
                 (when-let [p (local-path bdir)]
                   (spit (io/file p cursor-file) (json/write-str cursor))))
          datoms (:datoms (d/db-stats (d/db conn)))]

      (spit outf (json/write-str
                  (merge {:target target :datoms datoms :base-datoms base
                          :batch batch :inflight inflight :hints hints?
                          :order order :skip-events skip
                          :cursor-events (:events cursor)}
                         ld
                         {:restore rs :final-index fi :backup bk
                          :phases @phases})))
      (println "\nwrote" outf)
      (shutdown-agents)
      (System/exit 0)))))

(ns bench.jvmstats
  "Sample JVM + process metrics for a *remote* JVM (the transactor) via JMX,
   and host-level CPU/IO from /proc.

   Datomic does its indexing in the transactor process, not in the peer that
   calls d/transact. So a DaCapo-style in-JVM measurement would instrument the
   wrong process entirely: it would report the *loader's* GC and JIT, while the
   index merges we care about happen elsewhere. Everything here therefore
   targets a PID we are given, not our own runtime."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.java.shell])
  (:import [javax.management.remote JMXConnectorFactory JMXServiceURL]
           [javax.management ObjectName]
           [java.lang.management ManagementFactory]))

;; ---------------------------------------------------------------------------
;; JMX attach to the transactor
;; ---------------------------------------------------------------------------

(defn connect
  "Connect to a JMX endpoint, e.g. \"localhost:7091\". Returns an MBeanServerConnection."
  [host-port]
  (let [url (JMXServiceURL. (str "service:jmx:rmi:///jndi/rmi://" host-port "/jmxrmi"))]
    (.getMBeanServerConnection (JMXConnectorFactory/connect url))))

(defn- attr [conn on a]
  (try (.getAttribute conn (ObjectName. on) a) (catch Exception _ nil)))

(defn- composite [conn on a k]
  (try (some-> (.getAttribute conn (ObjectName. on) a) (.get k))
       (catch Exception _ nil)))

(defn gc-stats
  "Cumulative GC collection count + time (ms), summed over all collectors,
   plus a per-collector breakdown."
  [conn]
  (let [names (->> (.queryNames conn (ObjectName. "java.lang:type=GarbageCollector,*") nil)
                   (map #(.getCanonicalName ^ObjectName %)))
        per   (for [n names]
                {:name  (attr conn n "Name")
                 :count (attr conn n "CollectionCount")
                 :ms    (attr conn n "CollectionTime")})]
    {:collectors (vec per)
     :gc-count   (reduce + 0 (keep :count per))
     :gc-ms      (reduce + 0 (keep :ms per))}))

(defn pause-collector?
  "True for collector beans whose CollectionTime is stop-the-world pause time:
   G1's Young/Old Generation and ZGC's Minor/Major Pauses. ZGC's Cycles beans
   time concurrent work, and G1 Concurrent GC is left out to match, so summing
   these is comparable across the two collectors where gc-ms is not."
  [^String n]
  (boolean (and n (re-find #"Pauses$|Young Generation$|Old Generation$" n))))

(defn jit-stats
  "JIT compilation time (ms). Cumulative since JVM start."
  [conn]
  {:jit-ms (attr conn "java.lang:type=Compilation" "TotalCompilationTime")})

(defn mem-stats [conn]
  {:heap-used     (composite conn "java.lang:type=Memory" "HeapMemoryUsage" "used")
   :heap-max      (composite conn "java.lang:type=Memory" "HeapMemoryUsage" "max")
   :heap-committed (composite conn "java.lang:type=Memory" "HeapMemoryUsage" "committed")
   :nonheap-used  (composite conn "java.lang:type=Memory" "NonHeapMemoryUsage" "used")})

(defn proc-stats
  "Process CPU time (ns) and uptime (ms) from the OS/Runtime MXBeans."
  [conn]
  {:proc-cpu-ns (attr conn "java.lang:type=OperatingSystem" "ProcessCpuTime")
   :uptime-ms   (attr conn "java.lang:type=Runtime" "Uptime")
   :threads     (attr conn "java.lang:type=Threading" "ThreadCount")})

(defn safepoint-stats
  "Safepoint count/time if the HotSpot diagnostic bean exposes it."
  [conn]
  {:safepoints    (attr conn "java.lang:type=Runtime" "Uptime")})

(defn snapshot
  "One full sample of the remote JVM."
  [conn]
  (merge (gc-stats conn) (jit-stats conn) (mem-stats conn) (proc-stats conn)))

;; ---------------------------------------------------------------------------
;; Per-thread CPU
;; ---------------------------------------------------------------------------

(defn thread-bean
  "Proxy for the remote JVM's ThreadMXBean. The com.sun.management extension
   reads CPU time for many threads in one call."
  ^com.sun.management.ThreadMXBean [conn]
  (ManagementFactory/newPlatformMXBeanProxy
   conn ManagementFactory/THREAD_MXBEAN_NAME com.sun.management.ThreadMXBean))

(defn thread-sample
  "{java-thread-id {:name :state :cpu-ns :user-ns}} for every live thread.
   CPU times are cumulative since each thread started. Reading them does not
   stop the JVM."
  [^com.sun.management.ThreadMXBean tb]
  (let [ids  (.getAllThreadIds tb)
        cpu  (.getThreadCpuTime tb ids)
        usr  (.getThreadUserTime tb ids)
        info (.getThreadInfo tb ids 0)]
    (into {} (for [i (range (alength ids))
                   :let [^java.lang.management.ThreadInfo ti (aget info i)]
                   :when (and ti (pos? (aget cpu i)))]
               [(aget ids i) {:name   (.getThreadName ti)
                              :state  (str (.getThreadState ti))
                              :cpu-ns (aget cpu i)
                              :user-ns (aget usr i)}]))))

(defn os-thread-ids
  "Java thread id -> Linux thread id, parsed from the `jcmd <pid> Thread.print`
   headers, e.g. \"name\" #108 [37650] daemon prio=5 ... (JDK 19+). jcmd stops
   the JVM at a safepoint while it walks every stack, so call it sparingly."
  [jcmd pid]
  (let [{:keys [out exit]} (clojure.java.shell/sh jcmd (str pid) "Thread.print")]
    (when (zero? exit)
      (into {} (for [[_ jid nid] (re-seq #"(?m)^\".*\" #(\d+) \[(\d+)\]" out)]
                 [(parse-long jid) (parse-long nid)])))))

;; ---------------------------------------------------------------------------
;; Host-level: /proc CPU + per-process IO
;; ---------------------------------------------------------------------------

(defn read-proc
  "Read a procfs file to a String.

   Neither slurp nor io/reader works here: procfs reports st_size 0, and the
   BufferedReader path calls FileInputStream.available(), which throws
   IOException: Invalid argument on /proc. Files/readString reads to EOF
   without consulting the reported size."
  [path]
  (java.nio.file.Files/readString
   (java.nio.file.Path/of ^String path (into-array String []))))

(defn cpu-jiffies
  "Aggregate CPU line from /proc/stat -> {:total .. :idle .. :iowait ..} in jiffies.

   See read-proc for why this cannot use slurp or io/reader."
  []
  (let [line (first (str/split-lines (read-proc "/proc/stat")))
        v    (->> (str/split (str/trim line) #"\s+") rest (keep parse-long) vec)
        idle (get v 3 0)
        iow  (get v 4 0)]
    {:total  (reduce + 0 v)
     :idle   (+ idle iow)
     :iowait iow}))

(defn proc-io
  "Cumulative bytes this PID actually pushed to/from storage (/proc/<pid>/io).
   read_bytes/write_bytes are block-layer counts, so page-cache hits do not
   inflate them -- which is what makes them meaningful for index segment writes."
  [pid]
  (let [f (io/file (str "/proc/" pid "/io"))]
    (when (.exists f)
      (into {} (for [l (str/split-lines (read-proc (str f)))
                     :let [[k v] (str/split l #":\s*")]
                     :when (and k v)]
                 [(keyword k) (parse-long v)])))))

(defn diskstats
  "Sum sectors read/written across real block devices (skip loop/ram/dm)."
  []
  (reduce (fn [acc l]
            (let [f (str/split (str/trim l) #"\s+")
                  dev (nth f 2 "")]
              (if (or (str/starts-with? dev "loop") (str/starts-with? dev "ram"))
                acc
                (-> acc
                    (update :sectors-read + (or (parse-long (nth f 5 "0")) 0))
                    (update :sectors-written + (or (parse-long (nth f 9 "0")) 0))))))
          {:sectors-read 0 :sectors-written 0}
          (str/split-lines (read-proc "/proc/diskstats"))))

(defn host-snapshot [pid]
  {:cpu  (cpu-jiffies)
   :io   (proc-io pid)
   :disk (diskstats)})

;; ---------------------------------------------------------------------------
;; Deltas
;; ---------------------------------------------------------------------------

(defn- d [a b k] (let [x (get a k) y (get b k)] (when (and x y) (- y x))))

(defn delta
  "Difference two snapshots taken around a measured phase.
   `wall-ms` is the wall-clock of the phase, used to derive utilisation."
  [before after host-before host-after wall-ms ncpu]
  (let [gc-ms   (d before after :gc-ms)
        gc-n    (d before after :gc-count)
        jit-ms  (d before after :jit-ms)
        cpu-ns  (d before after :proc-cpu-ns)
        cpu-ms  (when cpu-ns (/ cpu-ns 1e6))
        ;; host cpu
        ct      (- (get-in host-after [:cpu :total]) (get-in host-before [:cpu :total]))
        ci      (- (get-in host-after [:cpu :idle])  (get-in host-before [:cpu :idle]))
        ciow    (- (get-in host-after [:cpu :iowait]) (get-in host-before [:cpu :iowait]))
        rb      (d (:io host-before) (:io host-after) :read_bytes)
        wb      (d (:io host-before) (:io host-after) :write_bytes)
        ;; per collector; :collectors keeps bean order between snapshots
        colls   (into {} (for [[a b] (map vector (:collectors before) (:collectors after))
                               :when (and (= (:name a) (:name b)) (:ms a) (:ms b))]
                           [(:name b) {:count (- (:count b) (:count a))
                                       :ms    (- (:ms b) (:ms a))}]))
        pause-ms (reduce + 0 (for [[n c] colls :when (pause-collector? n)] (:ms c)))]
    {:wall-ms          wall-ms
     :gc-count         gc-n
     ;; every collector bean, so under ZGC this includes concurrent cycle time;
     ;; :gc-pause-ms is the figure to compare across collectors
     :gc-ms            gc-ms
     :gc-pct           (when (and gc-ms (pos? wall-ms)) (* 100.0 (/ gc-ms wall-ms)))
     :gc-pause-ms      pause-ms
     :gc-pause-pct     (when (pos? wall-ms) (* 100.0 (/ pause-ms wall-ms)))
     :gc-collectors    colls
     :jit-ms           jit-ms
     :jit-pct          (when (and jit-ms (pos? wall-ms)) (* 100.0 (/ jit-ms wall-ms)))
     ;; transactor CPU as a fraction of ONE core, and of the whole box
     :proc-cpu-ms      cpu-ms
     :proc-cpu-cores   (when (and cpu-ms (pos? wall-ms)) (/ cpu-ms wall-ms))
     :proc-cpu-pct-host (when (and cpu-ms (pos? wall-ms) (pos? ncpu))
                          (* 100.0 (/ cpu-ms (* wall-ms ncpu))))
     :host-cpu-pct     (when (pos? ct) (* 100.0 (/ (- ct ci) (double ct))))
     :host-iowait-pct  (when (pos? ct) (* 100.0 (/ ciow (double ct))))
     :io-read-bytes    rb
     :io-write-bytes   wb
     :io-write-MBps    (when (and wb (pos? wall-ms)) (/ wb 1048.576 wall-ms))
     :heap-used-after  (:heap-used after)
     :heap-max         (:heap-max after)
     :threads-after    (:threads after)}))

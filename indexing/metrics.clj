(ns bench.metrics
  "Datomic metrics callback for the transactor and the peer.

   Datomic calls a registered callback roughly once a minute with a map of
   metric keyword -> value, where a value is either a number or a map of
   {:lo :hi :sum :count}. Registration:

     transactor properties:  metrics-callback=bench.metrics/transactor
     peer JVM argument:      -Ddatomic.metricsCallback=bench.metrics/peer

   The callback must be on the *callee's* classpath, so the transactor needs
   this namespace (AOT-compiled or as source) in its lib/ directory -- it is not
   enough for the benchmark peer to have it.

   Why this exists alongside the log parser: index throughput is currently
   derived by regexing :index/create-index lines out of the transactor log,
   which depends on an unstable log format. IndexDatoms / IndexWrites /
   CreateEntireIndexMsec are the supported equivalents, and MemoryIndexMB and
   TransactionMsec have no log-scrape equivalent at all.

   Datomic's docs mark the callback argument format as alpha and subject to
   change, so this writes whatever arrives rather than assuming a schema."
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json])
  (:import [java.util.concurrent.atomic AtomicLong]))

(def ^:private seq-no (AtomicLong. 0))

(defn- out-path
  "Where to append samples. Defaults per role, overridable so a sweep can point
   each run at its own file."
  [role]
  (or (System/getProperty (str "bench.metrics." (name role) ".out"))
      (str "/tmp/datomic-metrics-" (name role) ".jsonl")))

(defn- sample!
  "Append one callback payload as a JSON line. Never throws: a monitoring
   callback that dies takes metrics reporting down with it, and in the
   transactor's case it runs on a Datomic-internal thread."
  [role m]
  (try
    (let [rec {:role (name role)
               :n    (.incrementAndGet seq-no)
               :t    (System/currentTimeMillis)
               :metrics (into {} (map (fn [[k v]]
                                        [(name k)
                                         (if (map? v)
                                           (into {} (map (fn [[kk vv]] [(name kk) vv]) v))
                                           v)]))
                              m)}]
      (locking seq-no
        (with-open [w (io/writer (out-path role) :append true)]
          (.write w (json/write-str rec))
          (.write w "\n"))))
    (catch Throwable t
      (try (binding [*out* *err*]
             (println "bench.metrics callback error:" (.getMessage t)))
           (catch Throwable _ nil)))))

(defn transactor [m] (sample! :transactor m))
(defn peer [m] (sample! :peer m))

;; Java-style entry points, in case a deployment prefers the static-method form.
(gen-class :name bench.metrics.Callback
           :methods [^:static [transactor [Object] void]
                     ^:static [peer [Object] void]])
(defn -transactor [m] (transactor m))
(defn -peer [m] (peer m))

;;;; ---------------------------------------------------------------------
;;;; Reading samples back
;;;; ---------------------------------------------------------------------

(defn read-samples [path]
  (when (.exists (io/file path))
    (with-open [r (io/reader path)]
      (vec (keep #(try (json/read-str % :key-fn keyword) (catch Exception _ nil))
                 (line-seq r))))))

(defn delta
  "Difference the first and last sample of a cumulative counter metric."
  [samples metric]
  (let [vs (keep #(get-in % [:metrics (keyword metric)]) samples)
        nums (filter number? vs)]
    (when (seq nums) (- (last nums) (first nums)))))

(defn agg
  "Aggregate a {:lo :hi :sum :count} metric across samples."
  [samples metric]
  (let [ms (keep #(get-in % [:metrics (keyword metric)]) samples)
        ms (filter map? ms)]
    (when (seq ms)
      {:lo    (apply min (keep :lo ms))
       :hi    (apply max (keep :hi ms))
       :sum   (reduce + 0 (keep :sum ms))
       :count (reduce + 0 (keep :count ms))
       :mean  (let [s (reduce + 0 (keep :sum ms))
                    c (reduce + 0 (keep :count ms))]
                (when (pos? c) (/ (double s) c)))})))

(defn summarise
  "Pull the metrics that matter for an indexing benchmark out of a sample file."
  [path]
  (let [s (read-samples path)]
    {:samples (count s)
     ;; cumulative counters -> delta over the run
     :IndexDatoms          (delta s "IndexDatoms")
     :IndexSegments        (delta s "IndexSegments")
     :IndexWrites          (delta s "IndexWrites")
     ;; timing distributions
     :CreateEntireIndexMsec (agg s "CreateEntireIndexMsec")
     :IndexWriteMsec        (agg s "IndexWriteMsec")
     :TransactionMsec       (agg s "TransactionMsec")
     :TransactionBytes      (agg s "TransactionBytes")
     :TransactionDatoms     (agg s "TransactionDatoms")
     :LogWriteMsec          (agg s "LogWriteMsec")
     :GarbageSegments       (delta s "GarbageSegments")
     ;; gauges -- last observed
     :MemoryIndexMB (last (keep #(get-in % [:metrics :MemoryIndexMB]) s))
     :AvailableMB   (last (keep #(get-in % [:metrics :AvailableMB]) s))
     :Alarm         (some #(get-in % [:metrics :Alarm]) s)}))

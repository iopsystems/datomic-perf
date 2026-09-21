(ns bench.gharchive
  "Load GH Archive hourly event dumps into Datomic and benchmark.

  Data: https://data.gharchive.org/YYYY-MM-DD-H.json.gz  (~180k events/hour)

  Models the natural entity graph:
    event --actor--> user
    event --repo-->  repo --owner-org--> org
  Users/repos/orgs are upserted by :db.unique/identity, so repeated
  references across events collapse onto the same entity -- this exercises
  Datomic's upsert + AVET path, which a flat row-load never touches."
  (:require [datomic.api :as d]
            [clojure.java.io :as io]
            [clojure.data.json :as json])
  (:import [java.util.zip GZIPInputStream]
           [java.text SimpleDateFormat]
           [java.util TimeZone]))

(def schema
  [;; --- user ---
   {:db/ident :user/id       :db/valueType :db.type/long   :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :user/login    :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/index true}

   ;; --- org ---
   {:db/ident :org/id        :db/valueType :db.type/long   :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :org/login     :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/index true}

   ;; --- repo ---
   {:db/ident :repo/id       :db/valueType :db.type/long   :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :repo/name     :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :repo/org      :db/valueType :db.type/ref    :db/cardinality :db.cardinality/one}

   ;; --- event ---
   {:db/ident :event/id      :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :event/type    :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :event/actor   :db/valueType :db.type/ref    :db/cardinality :db.cardinality/one}
   {:db/ident :event/repo    :db/valueType :db.type/ref    :db/cardinality :db.cardinality/one}
   {:db/ident :event/org     :db/valueType :db.type/ref    :db/cardinality :db.cardinality/one}
   {:db/ident :event/at      :db/valueType :db.type/instant :db/cardinality :db.cardinality/one :db/index true}
   {:db/ident :event/action  :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
   ;; PushEvent detail -- lets us aggregate real numeric work
   {:db/ident :event/commits :db/valueType :db.type/long   :db/cardinality :db.cardinality/one}
   ;; Issue/PR detail
   {:db/ident :event/number  :db/valueType :db.type/long   :db/cardinality :db.cardinality/one}
   {:db/ident :event/title   :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/fulltext true}])

(defn parse-inst ^java.util.Date [s]
  (let [f (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss'Z'")]
    (.setTimeZone f (TimeZone/getTimeZone "UTC"))
    (.parse f s)))

(defn event->tx
  "One GH Archive event -> Datomic tx-data (a vector of maps).
   Nested maps with :db/unique attrs upsert automatically."
  [e]
  (let [actor  (get e "actor")
        repo   (get e "repo")
        org    (get e "org")
        pl     (get e "payload")
        etype  (get e "type")
        ev (cond-> {:event/id   (get e "id")
                    :event/type (keyword etype)
                    :event/at   (parse-inst (get e "created_at"))}

             actor (assoc :event/actor
                          {:user/id    (get actor "id")
                           :user/login (get actor "login")})

             repo  (assoc :event/repo
                          (cond-> {:repo/id   (get repo "id")
                                   :repo/name (get repo "name")}
                            org (assoc :repo/org {:org/id    (get org "id")
                                                  :org/login (get org "login")})))

             org   (assoc :event/org {:org/id    (get org "id")
                                      :org/login (get org "login")})

             (get pl "action")
             (assoc :event/action (keyword (get pl "action")))

             (and (= etype "PushEvent") (get pl "size"))
             (assoc :event/commits (long (get pl "size")))

             (and (get pl "issue") (get-in pl ["issue" "number"]))
             (assoc :event/number (long (get-in pl ["issue" "number"]))
                    :event/title  (str (get-in pl ["issue" "title"])))

             (and (get pl "pull_request") (get-in pl ["pull_request" "number"]))
             (assoc :event/number (long (get-in pl ["pull_request" "number"]))
                    :event/title  (str (get-in pl ["pull_request" "title"]))))]
    [ev]))

(defn events-seq
  "Lazy seq of parsed events from a gzipped GH Archive file."
  [path]
  (let [rdr (io/reader (GZIPInputStream. (io/input-stream path)))]
    (map #(json/read-str %) (line-seq rdr))))

(defn ms [n] (/ (double n) 1e6))

(defn load-file!
  "Load one hourly dump, batching `batch` events per transaction.
   Returns timing stats."
  [conn path batch]
  (let [start   (System/nanoTime)
        n       (atom 0)
        pending (atom [])
        futs    (atom [])]
    (doseq [chunk (partition-all batch (events-seq path))]
      (let [tx (into [] (mapcat event->tx) chunk)]
        (swap! n + (count chunk))
        ;; pipeline: keep at most ~50 tx in flight
        (swap! futs conj (d/transact-async conn tx))
        (when (>= (count @futs) 50)
          (doseq [f @futs] @f)
          (reset! futs []))))
    (doseq [f @futs] @f)
    (let [el (ms (- (System/nanoTime) start))]
      {:events @n :ms el :events-per-sec (/ @n (/ el 1000.0))})))

;;;; ---------------------------------------------------------------------
;;;; Rules -- composable, in the spirit of the mbrainz sample
;;;; ---------------------------------------------------------------------

(def rules
  '[;; actor performed event on repo
    [(acted-on ?u ?repo ?e)
     [?e :event/actor ?u]
     [?e :event/repo ?repo]]

    ;; two users are collaborators if they both touched the same repo
    [(collab ?u1 ?u2)
     [?e1 :event/actor ?u1]
     [?e1 :event/repo ?r]
     [?e2 :event/repo ?r]
     [?e2 :event/actor ?u2]
     [(!= ?u1 ?u2)]]

    ;; transitive collaboration, depth 2
    [(collab-2 ?u1 ?u2)
     (collab ?u1 ?x)
     (collab ?x ?u2)
     [(!= ?u1 ?u2)]]

    ;; events of a given type on a repo
    [(repo-events-of-type ?repo ?type ?e)
     [?e :event/repo ?repo]
     [?e :event/type ?type]]

    ;; repos belonging to an org
    [(org-repo ?org ?repo)
     [?repo :repo/org ?org]]

    ;; push activity: repo -> commit count
    [(push-commits ?repo ?e ?n)
     [?e :event/repo ?repo]
     [?e :event/type :PushEvent]
     [?e :event/commits ?n]]])

;;;; ---------------------------------------------------------------------
;;;; Benchmark queries
;;;; ---------------------------------------------------------------------

(defn pct [v p]
  (nth v (min (dec (count v)) (int (* (/ p 100.0) (count v))))))

(defn bench [label warmup iters f]
  (dotimes [_ warmup] (f))
  (let [ts (vec (sort (for [_ (range iters)]
                        (let [s (System/nanoTime)
                              r (f)
                              _ (when (instance? java.util.Collection r) (count r))]
                          (- (System/nanoTime) s)))))
        res (f)]
    {:label label
     :rows (cond (float? res) (format "%.2f" res)
                 (number? res) res
                 (instance? java.util.Collection res) (count res)
                 (nil? res) 0
                 :else 1)
     :p50 (ms (pct ts 50)) :p90 (ms (pct ts 90)) :p99 (ms (pct ts 99))
     :mean (ms (/ (reduce + ts) (count ts)))}))

(defn queries [db top-user top-repo top-org top-login]
  [;; --- point lookups ---
   ["G1  pull repo by unique id" 50 500
    (fn [] (d/pull db '[*] [:repo/id top-repo]))]

   ["G2  user by login (AVET)" 50 500
    (fn [] (let [u (d/q '[:find ?u . :in $ ?l :where [?u :user/login ?l]] db top-login)]
             (if u 1 0)))]

   ;; --- joins ---
   ["G3  all events by one user" 20 200
    (fn [] (d/q '[:find ?e ?type :in $ ?uid :where
                  [?u :user/id ?uid]
                  [?e :event/actor ?u]
                  [?e :event/type ?type]]
                db top-user))]

   ["G4  user -> repos touched (2-join)" 20 200
    (fn [] (d/q '[:find ?rname :in $ ?uid :where
                  [?u :user/id ?uid]
                  [?e :event/actor ?u]
                  [?e :event/repo ?r]
                  [?r :repo/name ?rname]]
                db top-user))]

   ["G5  repo -> distinct contributors (3-join)" 20 200
    (fn [] (d/q '[:find ?login :in $ ?rid :where
                  [?r :repo/id ?rid]
                  [?e :event/repo ?r]
                  [?e :event/actor ?u]
                  [?u :user/login ?login]]
                db top-repo))]

   ["G6  org -> repos -> event count (4-join)" 10 100
    (fn [] (d/q '[:find ?rname (count ?e) :in $ ?oid :where
                  [?o :org/id ?oid]
                  [?r :repo/org ?o]
                  [?r :repo/name ?rname]
                  [?e :event/repo ?r]]
                db top-org))]

   ;; --- rules ---
   ["G7  rule: acted-on" 20 200
    (fn [] (d/q '[:find ?rname :in $ % ?uid :where
                  [?u :user/id ?uid]
                  (acted-on ?u ?r ?e)
                  [?r :repo/name ?rname]]
                db rules top-user))]

   ["G8  rule: collab (shared-repo graph)" 5 50
    (fn [] (d/q '[:find ?l2 :in $ % ?uid :where
                  [?u1 :user/id ?uid]
                  (collab ?u1 ?u2)
                  [?u2 :user/login ?l2]]
                db rules top-user))]

   ;; --- aggregation ---
   ["G9  count events by type (group-by)" 5 30
    (fn [] (d/q '[:find ?type (count ?e) :where
                  [?e :event/type ?type]]
                db))]

   ["G10 sum commits per repo, top activity" 3 20
    (fn [] (d/q '[:find ?rname (sum ?n) :with ?e :where
                  [?e :event/type :PushEvent]
                  [?e :event/commits ?n]
                  [?e :event/repo ?r]
                  [?r :repo/name ?rname]]
                db))]

   ["G11 avg commits per push" 3 20
    (fn [] (d/q '[:find (avg ?n) . :with ?e :where
                  [?e :event/commits ?n]] db))]

   ;; --- time range (indexed instant) ---
   ["G12 events in 10-min window (AVET range)" 10 100
    (let [t0 (d/q '[:find (min ?t) . :where [?e :event/at ?t]] db)
          lo ^java.util.Date t0
          hi (java.util.Date. (+ (.getTime ^java.util.Date t0) 600000))]
      (fn [] (d/q '[:find (count ?e) . :in $ ?lo ?hi :where
                    [?e :event/at ?t]
                    [(>= ?t ?lo)] [(< ?t ?hi)]]
                  db lo hi)))]

   ;; --- fulltext ---
   ["G13 fulltext search issue/PR titles" 10 100
    (fn [] (d/q '[:find ?title :in $ ?q :where
                  [(fulltext $ :event/title ?q) [[?e ?title]]]]
                db "fix"))]

   ;; --- raw index scan (engine-overhead comparison) ---
   ["G14 raw AEVT scan :event/type" 3 20
    (fn [] (reduce (fn [n _] (inc n)) 0 (d/datoms db :aevt :event/type)))]

   ["G15 count all events (via query)" 3 20
    (fn [] (d/q '[:find (count ?e) . :where [?e :event/type]] db))]])

;;;; ---------------------------------------------------------------------
;;;; main
;;;; ---------------------------------------------------------------------

(defn hot-entities
  "Pick a high-degree user / repo / org so joins have real fan-out
   (querying a random singleton would understate join cost)."
  [db]
  (let [top (fn [attr id-attr]
              (->> (d/q '[:find ?id (count ?e) :in $ ?ref ?ida :where
                          [?e ?ref ?x]
                          [?x ?ida ?id]]
                        db attr id-attr)
                   (sort-by second >)
                   ffirst))]
    (let [u (top :event/actor :user/id)]
      {:user  u
       :login (d/q '[:find ?l . :in $ ?id :where
                     [?x :user/id ?id] [?x :user/login ?l]] db u)
       :repo  (top :event/repo :repo/id)
       :org   (top :event/org  :org/id)})))

(defn -main [& args]
  (let [files (if (seq args) (vec args) ["/home/xyang/dtm/gh/2024-01-01-15.json.gz"])
        uri   (str "datomic:dev://localhost:4334/gh-" (System/currentTimeMillis))]
    (println "db:" uri)
    (println "files:" files)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      (let [s (System/nanoTime)]
        @(d/transact conn schema)
        (printf "schema: %.1f ms%n" (ms (- (System/nanoTime) s))))

      ;; ---------- LOAD ----------
      (println "\n=== LOAD (upsert-heavy: users/repos/orgs dedupe by identity) ===")
      (printf "%-10s %10s %12s %12s%n" "batch" "events" "seconds" "events/sec")
      (println (apply str (repeat 48 "-")))
      (let [totals (atom {:events 0 :ms 0.0})]
        (doseq [f files]
          (let [r (load-file! conn f 200)]
            (swap! totals #(-> % (update :events + (:events r)) (update :ms + (:ms r))))
            (printf "%-10s %10d %12.2f %12.0f%n" "200"
                    (:events r) (/ (:ms r) 1000.0) (:events-per-sec r))
            (flush)))
        (let [t @totals
              db (d/db conn)
              n  (:datoms (d/db-stats db))]
          (printf "\ntotal: %d events -> %d datoms in %.2f s  (%.0f events/s, %.0f datoms/s)%n"
                  (:events t) n (/ (:ms t) 1000.0)
                  (/ (:events t) (/ (:ms t) 1000.0))
                  (/ (double n) (/ (:ms t) 1000.0)))))

      ;; ---------- INDEX ----------
      (println "\n=== INDEX ===")
      (let [db (d/db conn) bt (d/basis-t db) s (System/nanoTime)]
        (d/request-index conn)
        (let [idb (deref (d/sync-index conn bt) 1800000 :timeout)
              el  (ms (- (System/nanoTime) s))]
          (if (= idb :timeout)
            (println "sync-index TIMED OUT")
            (printf "index caught up to t=%s in %.2f s%n" (d/basis-t idb) (/ el 1000.0)))))

      ;; ---------- ENTITY COUNTS ----------
      (let [db (d/db conn)
            st (:attrs (d/db-stats db))]
        (println "\n=== ENTITY COUNTS ===")
        (doseq [k [:event/id :user/id :repo/id :org/id :event/commits :event/title]]
          (printf "  %-18s %10d%n" (str k) (or (:count (get st k)) 0))))

      ;; ---------- QUERIES ----------
      (let [db  (d/db conn)
            _   (println "\nselecting hot entities for join fan-out...")
            hot (hot-entities db)
            _   (println "  hot:" (pr-str hot))]
        (println "\n=== QUERY LATENCY (ms) ===")
        (printf "%-44s %8s %9s %9s %9s %9s%n" "query" "rows" "p50" "p90" "p99" "mean")
        (println (apply str (repeat 92 "-")))
        (doseq [[label w i f] (queries db (:user hot) (:repo hot) (:org hot) (:login hot))]
          (let [r (try (bench label w i f)
                       (catch Exception e {:label label :rows -1 :p50 -1.0 :p90 -1.0 :p99 -1.0 :mean -1.0
                                           :err (.getMessage e)}))]
            (printf "%-44s %8s %9.3f %9.3f %9.3f %9.3f%s%n"
                    (:label r) (str (:rows r)) (:p50 r) (:p90 r) (:p99 r) (:mean r)
                    (if (:err r) (str "  ERR: " (subs (:err r) 0 (min 60 (count (:err r))))) ""))
            (flush))))

      (println "\ndone. db =" uri)
      (shutdown-agents)
      (System/exit 0))))

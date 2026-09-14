local systemslab = import 'systemslab.libsonnet';

// Datomic indexing benchmark over GH Archive data.
//
// Datomic does its index merges in the TRANSACTOR process, not in the peer that
// calls d/transact. Two consequences shape this spec:
//
//   1. The JVM metrics that matter (GC, JIT, CPU) belong to the transactor, so
//      it is started with JMX exposed and the harness attaches to it. A
//      DaCapo-style in-JVM measurement would instrument the loader instead and
//      report the wrong process entirely.
//
//   2. Indexing is asynchronous and continuous: by the time a load finishes,
//      the transactor has already merged most of the novelty in the background.
//      So d/sync-index measures only the trailing merge (~2s regardless of
//      data size — verified). The real throughput comes from the transactor's
//      own per-job accounting in its log, which reports datoms and msec for
//      every :index/create-index event. That is what `index_throughput`
//      reports; the sync-index wall time is kept separately as index_tail_s.
//
// Iteration model follows DaCapo's luindex: N iterations in one peer JVM, each
// creating a fresh database, loading a fixed event count, forcing the index,
// then DELETING the database. The first N-1 are warmup, the last is reported.
// JIT settles measurably across iterations (observed 24% -> 12% of wall on the
// small size), which is exactly what the warmup is for.
//
// Sweepable parameters and why each is interesting:
//   index_parallelism  Datomic ships this at 1, so index jobs are effectively
//                      serial. On a 24-thread host that leaves the machine
//                      idle during merges. Max is 8.
//   memory_index_threshold  How often index jobs fire. Larger = fewer, bigger
//                      merges (better amortisation, worse latency spikes).
//   memory_index_max   The backpressure point where the transactor throttles
//                      writes if indexing cannot keep up.
//   heap               memory-index-max lives in this heap; too small and the
//                      two fight.
//   jdk                JIT and GC behaviour differ enough across LTS releases
//                      to matter for a merge-heavy workload.
//   gc                 G1 (Datomic's default) vs ZGC vs Parallel.

function(host='amd',
         size='medium',
         iterations='3',
         index_parallelism='1',
         memory_index_threshold='32m',
         memory_index_max='512m',
         heap='4g',
         jdk='21',
         gc='g1',
         write_concurrency='4',
         batch='200',
         hours='15,16',
         dtm_root='/opt/datomic-bench')

  local num(v) = if std.isNumber(v) then v else std.parseInt(v);

  local args = {
    host: host,
    size: size,
    iterations: num(iterations),
    index_parallelism: num(index_parallelism),
    memory_index_threshold: memory_index_threshold,
    memory_index_max: memory_index_max,
    heap: heap,
    jdk: jdk,
    gc: gc,
    write_concurrency: num(write_concurrency),
    batch: num(batch),
    hours: hours,
    dtm: dtm_root,
  };

  local gc_opts =
    if args.gc == 'zgc' then '-XX:+UseZGC -XX:+ZGenerational'
    else if args.gc == 'parallel' then '-XX:+UseParallelGC'
    else '-XX:+UseG1GC -XX:MaxGCPauseMillis=50';

  local java_home = '/usr/lib/jvm/java-%s-openjdk-amd64' % args.jdk;

  // Writable scratch for H2 storage and transactor logs. The shared asset
  // tree under dtm_root is read-only to the agent, so storage cannot live there.
  local scratch = '/home/systemslab-agent/datomic-scratch';

  // Fixed event counts per size, so a size means the same work regardless of
  // which archive hours happen to be on disk.
  local event_counts = { small: 50000, medium: 250000, large: 1000000 };

  local files = std.join(',', [
    '%s/gh/2024-01-01-%s.json.gz' % [args.dtm, h]
    for h in std.split(args.hours, ',')
  ]);

  local tag = '%s-p%d-t%s-m%s-%s-jdk%s-%s' % [
    args.size, args.index_parallelism, args.memory_index_threshold,
    args.memory_index_max, args.heap, args.jdk, args.gc,
  ];

  // Transactor properties for this variant.
  local txor_props = std.join('\n', [
    'protocol=dev',
    'host=localhost',
    'port=4334',
    'data-dir=%s/sweep-data' % scratch,
    'log-dir=%s/sweep-log' % scratch,
    'memory-index-threshold=%s' % args.memory_index_threshold,
    'memory-index-max=%s' % args.memory_index_max,
    'object-cache-max=1g',
    'index-parallelism=%d' % args.index_parallelism,
    'write-concurrency=%d' % args.write_concurrency,
    '',
  ]);

  {
    name: 'datomic-index-%s' % tag,
    metadata: {
      description: 'Datomic %s indexing benchmark over GH Archive (%d events, %d iterations, index-parallelism=%d, mem-threshold=%s, heap=%s, jdk%s, %s)' % [
        args.size, event_counts[args.size], args.iterations,
        args.index_parallelism, args.memory_index_threshold, args.heap,
        args.jdk, args.gc,
      ],
      reference: 'transactor :index/create-index job accounting',
    },

    jobs: {
      bench: {
        host: { tags: [args.host] },

        steps: [

          // Preflight. Fail loudly and early rather than producing a run whose
          // numbers are quietly wrong (wrong JDK, missing data, stale JVM).
          systemslab.bash(|||
            set -eu
            echo "=== host: $(hostname -s) ==="
            DTM=%(dtm)s
            SCRATCH=%(scratch)s
            JH=%(java_home)s

            test -d "$DTM/datomic-pro-1.0.7705" || { echo "FATAL: no datomic install"; exit 1; }
            test -d "$JH" || { echo "FATAL: JDK not present: $JH"; echo "available:"; ls /usr/lib/jvm/; exit 1; }
            "$JH/bin/java" -version 2>&1

            for h in $(echo %(hours)s | tr ',' ' '); do
              f="$DTM/gh/2024-01-01-$h.json.gz"
              test -s "$f" || { echo "FATAL: missing data $f"; exit 1; }
              echo "data ok: $f ($(stat -c%%s "$f") bytes)"
            done

            # Any surviving JVM would hold the H2 write lock and contend for CPU.
            pkill -x java 2>/dev/null || true
            sleep 3
            if pgrep -x java >/dev/null; then echo "FATAL: java still running"; exit 1; fi

            # Fresh storage per variant: a carried-over H2 file would let an
            # earlier variant's segments change this one's merge costs.
            mkdir -p "$SCRATCH"
            rm -rf "$SCRATCH/sweep-data" "$SCRATCH/sweep-log"
            mkdir -p "$SCRATCH/sweep-data" "$SCRATCH/sweep-log"
            echo "preflight ok"
          ||| % { dtm: args.dtm, scratch: scratch, java_home: java_home, hours: args.hours }),

          // Start the transactor under test, with JMX open for the harness.
          systemslab.bash(|||
            set -eu
            DTM=%(dtm)s
            SCRATCH=%(scratch)s
            export JAVA_HOME=%(java_home)s
            export PATH="$JAVA_HOME/bin:$PATH"

            # Written here, not carried from the write_file step: each step has
            # its own cwd, and the shared asset tree is read-only to the agent.
            {
              echo "protocol=dev"
              echo "host=localhost"
              echo "port=4334"
              echo "data-dir=$SCRATCH/sweep-data"
              echo "log-dir=$SCRATCH/sweep-log"
              echo "memory-index-threshold=%(mit)s"
              echo "memory-index-max=%(mim)s"
              echo "object-cache-max=1g"
              echo "index-parallelism=%(par)d"
              echo "write-concurrency=%(wc)d"
            } > "$SCRATCH/sweep.properties"
            cat "$SCRATCH/sweep.properties"
            cd "$DTM/datomic-pro-1.0.7705"

            nohup bin/transactor \
              -Xmx%(heap)s -Xms%(heap)s \
              %(gc_opts)s \
              -Dcom.sun.management.jmxremote \
              -Dcom.sun.management.jmxremote.port=7091 \
              -Dcom.sun.management.jmxremote.rmi.port=7091 \
              -Dcom.sun.management.jmxremote.local.only=false \
              -Dcom.sun.management.jmxremote.authenticate=false \
              -Dcom.sun.management.jmxremote.ssl=false \
              -Djava.rmi.server.hostname=127.0.0.1 \
              "$SCRATCH/sweep.properties" > "$SCRATCH/sweep-transactor.log" 2>&1 &

            for i in $(seq 1 60); do
              grep -q "System started" "$SCRATCH/sweep-transactor.log" && break
              sleep 1
            done
            grep -q "System started" "$SCRATCH/sweep-transactor.log" || {
              echo "FATAL: transactor did not start"; cat "$SCRATCH/sweep-transactor.log"; exit 1; }

            PID=$(pgrep -f datomic.launcher | head -1)
            echo "$PID" > "$SCRATCH/txor.pid"
            echo "transactor pid=$PID"
            # Confirm it really is the JDK we asked for, not a stale default.
            tr '\0' '\n' < /proc/$PID/cmdline | head -1
            ls -l /proc/$PID/exe
          ||| % { dtm: args.dtm, scratch: scratch, java_home: java_home, heap: args.heap, gc_opts: gc_opts, mit: args.memory_index_threshold, mim: args.memory_index_max, par: args.index_parallelism, wc: args.write_concurrency }),

          // The benchmark itself.
          systemslab.bash(|||
            set -eu
            DTM=%(dtm)s
            SCRATCH=%(scratch)s
            export JAVA_HOME=%(java_home)s
            export PATH="$JAVA_HOME/bin:$PATH"
            PID=$(cat "$SCRATCH/txor.pid")

            cd "$DTM/bench"
            clojure -J-Xmx12g -J-server -M -m bench.idxbench \
              --size %(size)s \
              --iterations %(iterations)d \
              --batch %(batch)d \
              --files %(files)s \
              --jmx localhost:7091 \
              --pid "$PID" \
              --log-dir "$SCRATCH/sweep-log" \
              --out "$SCRATCH/idxbench.json" 2>&1 | tee "$SCRATCH/idxbench.log"

            test -s "$SCRATCH/idxbench.json" || { echo "FATAL: no result json"; exit 1; }
          ||| % {
            dtm: args.dtm, scratch: scratch, java_home: java_home, size: args.size,
            iterations: args.iterations, batch: args.batch, files: files,
          }),

          // Collect every artifact into ONE step's cwd. upload_artifact resolves
          // relative to the cwd of the step it runs in, and steps do not share
          // one, so gathering and uploading must happen together.
          systemslab.write_file('summarise.py', |||
            import json, sys
            d = json.load(open(sys.argv[1]))
            f = d['final']; j = f.get('jvm', {})
            out = {
              'size': d['size'], 'events': d['events'], 'datoms': f['datoms'],
              'iterations': d['iterations'],
              'index_throughput_datoms_per_sec': f.get('index-throughput'),
              'index_jobs': f.get('index-jobs'),
              'index_datoms': f.get('index-datoms'),
              'index_job_seconds': (f.get('index-job-ms') or 0) / 1000.0,
              'index_segments': f.get('index-segments'),
              'index_tail_s': (f.get('index-tail-ms') or 0) / 1000.0,
              'ingest_events_per_sec': f.get('events-per-sec'),
              'ingest_datoms_per_sec': f.get('datoms-per-sec'),
              'load_s': (f.get('load-ms') or 0) / 1000.0,
              'total_s': (f.get('total-ms') or 0) / 1000.0,
              'gc_count': j.get('gc-count'), 'gc_ms': j.get('gc-ms'),
              'gc_pct': j.get('gc-pct'),
              'jit_ms': j.get('jit-ms'), 'jit_pct': j.get('jit-pct'),
              'txor_cpu_cores': j.get('proc-cpu-cores'),
              'txor_cpu_pct_host': j.get('proc-cpu-pct-host'),
              'host_cpu_pct': j.get('host-cpu-pct'),
              'host_iowait_pct': j.get('host-iowait-pct'),
              'io_write_bytes': j.get('io-write-bytes'),
              'io_write_MBps': j.get('io-write-MBps'),
              'heap_used_after': j.get('heap-used-after'),
              'heap_max': j.get('heap-max'),
              'threads': j.get('threads-after'),
            }
            json.dump(out, sys.stdout, indent=2)
          |||),

          systemslab.bash(|||
            set -eu
            SCRATCH=%(scratch)s
            cp "$SCRATCH/idxbench.json" idxbench.json
            cp "$SCRATCH/idxbench.log"  idxbench.log
            cp "$SCRATCH/sweep-transactor.log" transactor.log || true
            grep -h "index/create-index" "$SCRATCH"/sweep-log/*.log > index-jobs.log 2>/dev/null || true
            python3 summarise.py idxbench.json > summary.json
            echo "=== summary ==="
            cat summary.json
            echo
            wc -l index-jobs.log || true
          ||| % { scratch: scratch }),

          systemslab.upload_artifact('idxbench.json'),
          systemslab.upload_artifact('summary.json'),
          systemslab.upload_artifact('idxbench.log'),
          systemslab.upload_artifact('transactor.log'),
          systemslab.upload_artifact('index-jobs.log'),

          // Always leave the host clean, whatever happened above.
          systemslab.bash(|||
            pkill -x java 2>/dev/null || true
            sleep 2
            rm -rf %(scratch)s/sweep-data
            echo "cleaned up"
          ||| % { scratch: scratch }),
        ],
      },
    },
  }

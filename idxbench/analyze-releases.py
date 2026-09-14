#!/usr/bin/env python3
"""Average Datomic indexing results across repeated experiment invocations.

Each experiment runs ITERATIONS passes inside one JVM (the first N-1 are warmup)
and reports only the last. This script averages that final-iteration figure
across the REPEAT invocations of the experiment, which are separate JVMs with
separate storage -- so the spread here is genuine run-to-run variance, not the
warmup convergence the iterations already absorbed.

Usage:
  systemslab --systemslab-url URL artifact download-all -o out --context CTX
  python3 analyze-releases.py out
"""
import json, glob, os, re, statistics, sys
from collections import defaultdict

METRICS = [
    ("index_throughput_datoms_per_sec", "index datoms/s", "{:>14,.0f}"),
    ("ingest_datoms_per_sec",           "ingest datoms/s", "{:>15,.0f}"),
    ("total_s",                         "total s",         "{:>9.2f}"),
    ("gc_pct",                          "gc%",             "{:>6.2f}"),
    ("jit_pct",                         "jit%",            "{:>6.2f}"),
    ("txor_cpu_cores",                  "txor cores",      "{:>10.2f}"),
]

def version_key(v):
    return tuple(int(x) for x in v.split("."))

def main(root):
    # group summaries by release
    by_release = defaultdict(list)
    for f in glob.glob(os.path.join(root, "*", "bench", "summary.json")):
        m = re.search(r"datomic-index-(\d+\.\d+\.\d+)-r(\d+)-", f)
        if not m:
            print(f"  skip (unparseable name): {f}", file=sys.stderr)
            continue
        rel, rep = m.group(1), int(m.group(2))
        try:
            d = json.load(open(f))
        except Exception as e:
            print(f"  skip (bad json): {f}: {e}", file=sys.stderr)
            continue
        d["_repeat"] = rep
        by_release[rel].append(d)

    if not by_release:
        print("no summary.json files found under", root, file=sys.stderr)
        return 1

    releases = sorted(by_release, key=version_key)

    hdr = f"{'release':<10} {'n':>2}"
    for _, label, _ in METRICS:
        hdr += f" {label:>15}"
    hdr += f" {'idx stdev%':>11}"
    print(hdr)
    print("-" * len(hdr))

    baseline = None
    rows = []
    for rel in releases:
        runs = by_release[rel]
        line = f"{rel:<10} {len(runs):>2}"
        vals = {}
        for key, label, fmt in METRICS:
            xs = [r[key] for r in runs if r.get(key) is not None]
            mean = statistics.mean(xs) if xs else float("nan")
            vals[key] = mean
            line += f" {mean:>15,.2f}" if "cores" in key or "pct" in key or key.endswith("_s") \
                    else f" {mean:>15,.0f}"
        idx = [r["index_throughput_datoms_per_sec"] for r in runs
               if r.get("index_throughput_datoms_per_sec") is not None]
        cv = (statistics.stdev(idx) / statistics.mean(idx) * 100) if len(idx) > 1 else 0.0
        line += f" {cv:>10.1f}%"
        print(line)
        rows.append((rel, vals, cv, len(runs)))
        if baseline is None:
            baseline = vals["index_throughput_datoms_per_sec"]

    # relative view against the oldest tested release
    print()
    print(f"{'release':<10} {'index datoms/s':>15} {'vs oldest':>11}")
    print("-" * 40)
    for rel, vals, cv, n in rows:
        it = vals["index_throughput_datoms_per_sec"]
        print(f"{rel:<10} {it:>15,.0f} {it/baseline:>10.3f}x")

    json.dump(
        [{"release": r, "runs": n, "index_cv_pct": cv, **{k: v for k, v in vals.items()}}
         for r, vals, cv, n in rows],
        open(os.path.join(root, "release-averages.json"), "w"), indent=2)
    print(f"\nwrote {os.path.join(root, 'release-averages.json')}")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "."))

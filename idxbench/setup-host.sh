#!/bin/bash
# Stage a host for the Datomic indexing benchmark.
#
# The Systemslab spec assumes this layout already exists; it does not create it.
# Run this once on the benchmark host (as a user with sudo) before submitting
# any experiment.
#
#   ./setup-host.sh                 # everything
#   ./setup-host.sh releases        # just the Datomic distributions
#   ./setup-host.sh data 24         # just GH Archive, 24 hours
#
# Layout produced:
#   /opt/datomic-bench/releases/datomic-pro-<version>/   19 distributions (~7.8 GB)
#   /opt/datomic-bench/gh/*.json.gz                      GH Archive (~1.2 GB for 24h)
#   /opt/datomic-bench/bench/src/bench/*.clj             harness source
#
# Everything is owned by the Systemslab agent user: the agent runs as its own
# uid and cannot traverse a normal user's home directory (mode drwxr-x---), so
# the assets must live somewhere it owns.
set -euo pipefail

AGENT_USER=${AGENT_USER:-systemslab-agent}
ROOT=${ROOT:-/opt/datomic-bench}
HOURS=${2:-24}
HERE="$(cd "$(dirname "$0")" && pwd)"

# 1.0.7393 is absent deliberately: Maven Central lists it but the S3
# distribution returns HTTP 403 (withdrawn; 1.0.7394 shipped three days later).
RELEASES="1.0.6726 1.0.6733 1.0.6735 1.0.7010 1.0.7021 1.0.7075 1.0.7180
          1.0.7187 1.0.7260 1.0.7277 1.0.7364 1.0.7387 1.0.7394 1.0.7469
          1.0.7482 1.0.7491 1.0.7556 1.0.7622 1.0.7705"

step_prereqs() {
  echo "==> prerequisites"
  sudo apt-get update -qq
  # Datomic 1.0.7705 requires Java 17+; both JDKs are needed for the jdk sweep.
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    openjdk-21-jdk-headless openjdk-17-jdk-headless unzip curl
  if ! command -v clojure >/dev/null; then
    curl -sSL -o /tmp/clj-install.sh https://download.clojure.org/install/linux-install.sh
    chmod +x /tmp/clj-install.sh && sudo /tmp/clj-install.sh
  fi
  java -version 2>&1 | head -1
  clojure --version
}

step_releases() {
  echo "==> staging $(echo $RELEASES | wc -w) Datomic releases into $ROOT/releases"
  sudo mkdir -p "$ROOT/releases"
  sudo chown "$AGENT_USER" "$ROOT/releases"
  for v in $RELEASES; do
    if [ -d "$ROOT/releases/datomic-pro-$v" ]; then echo "  have $v"; continue; fi
    echo "  fetching $v"
    sudo -u "$AGENT_USER" curl -sSL -o "$ROOT/releases/$v.zip" \
      "https://datomic-pro-downloads.s3.amazonaws.com/$v/datomic-pro-$v.zip" \
      || { echo "  FAILED to download $v"; continue; }
    sudo -u "$AGENT_USER" unzip -q -o "$ROOT/releases/$v.zip" -d "$ROOT/releases"
    sudo rm -f "$ROOT/releases/$v.zip"
  done
  echo "  staged: $(ls -d $ROOT/releases/datomic-pro-* 2>/dev/null | wc -l)"
}

step_data() {
  echo "==> staging $HOURS hours of GH Archive into $ROOT/gh"
  sudo mkdir -p "$ROOT/gh"; sudo chown "$AGENT_USER" "$ROOT/gh"
  local n=0
  for d in 2024-01-01 2024-01-02; do
    for h in $(seq 0 23); do
      [ "$n" -ge "$HOURS" ] && break 2
      f="$d-$h.json.gz"
      n=$((n+1))
      [ -s "$ROOT/gh/$f" ] && continue
      sudo -u "$AGENT_USER" curl -sS -o "$ROOT/gh/$f" \
        "https://data.gharchive.org/$f" || echo "  FAILED $f"
    done
  done
  echo "  staged: $(ls $ROOT/gh/*.json.gz 2>/dev/null | wc -l) files, $(du -sh $ROOT/gh | cut -f1)"
}

step_harness() {
  echo "==> installing harness source into $ROOT/bench"
  sudo mkdir -p "$ROOT/bench/src/bench"
  sudo cp "$HERE"/{gharchive,idxbench,jvmstats,metrics}.clj "$ROOT/bench/src/bench/"
  # The metrics callback must be on the TRANSACTOR's classpath, not just the
  # peer's. bin/classpath includes resources/, so drop it there per release.
  for d in "$ROOT"/releases/datomic-pro-*; do
    [ -d "$d" ] || continue
    sudo mkdir -p "$d/resources/bench"
    sudo cp "$HERE/metrics.clj" "$d/resources/bench/metrics.clj"
  done
  # Seed the agent's maven cache so the first run does not fail offline.
  if [ -d "$HOME/.m2/repository" ]; then
    sudo mkdir -p "/home/$AGENT_USER/.m2"
    sudo rsync -a "$HOME/.m2/repository" "/home/$AGENT_USER/.m2/"
  fi
  sudo chown -R "$AGENT_USER" "$ROOT" "/home/$AGENT_USER/.m2" 2>/dev/null || true
  sudo chmod -R a+rX "$ROOT"
}

step_verify() {
  echo "==> verify (as $AGENT_USER)"
  sudo -u "$AGENT_USER" test -r "$ROOT/gh/2024-01-01-15.json.gz" \
    && echo "  data readable: OK" || echo "  data readable: FAILED"
  sudo -u "$AGENT_USER" test -x "$ROOT/releases/datomic-pro-1.0.7705/bin/transactor" \
    && echo "  transactor executable: OK" || echo "  transactor executable: FAILED"
  for j in 17 21; do
    test -d "/usr/lib/jvm/java-$j-openjdk-amd64" \
      && echo "  jdk$j: OK" || echo "  jdk$j: MISSING"
  done
  echo "  disk: $(df -h "$ROOT" | tail -1 | awk '{print $4}') free"
}

case "${1:-all}" in
  prereqs)  step_prereqs ;;
  releases) step_releases ;;
  data)     step_data ;;
  harness)  step_harness ;;
  verify)   step_verify ;;
  all)      step_prereqs; step_releases; step_data; step_harness; step_verify ;;
  *) echo "usage: $0 [all|prereqs|releases|data|harness|verify] [hours]"; exit 1 ;;
esac
echo "done."

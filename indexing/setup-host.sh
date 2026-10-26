#!/bin/bash
# Stage a host for the Datomic indexing benchmark (indexing.toml).
#
# The Systemslab spec assumes this layout already exists; it does not create it.
# Run this on the benchmark host (as a user with sudo) before submitting the
# experiment. On EC2, run it again after any instance stop/start: /mnt/datomic
# is the i3en instance-store NVMe and is wiped when the instance stops. On a
# host where /mnt/datomic is a plain directory, skip the mount step.
#
#   ./setup-host.sh                 # everything
#   ./setup-host.sh mount           # just mount the instance store at /mnt/datomic
#   ./setup-host.sh releases        # just the Datomic distribution(s)
#   ./setup-host.sh data 720        # just GH Archive, 720 hours
#   ./setup-host.sh verify          # re-run indexing.toml's preflight checks
#
# Layout produced (ROOT must match R in indexing.toml):
#   /mnt/datomic/datomic-bench/releases/datomic-pro-1.0.7705/   Datomic distribution
#   /mnt/datomic/datomic-bench/gh/*.json.gz                     GH Archive (720 hours)
#   /mnt/datomic/datomic-bench/bench/src/bench/*.clj            harness source
#
# Everything is owned by the Systemslab agent user: the agent runs as its own
# uid and cannot traverse a normal user's home directory (mode drwxr-x---), so
# the assets must live somewhere it owns.
set -euo pipefail

AGENT_USER=${AGENT_USER:-systemslab-agent}
MNT=${MNT:-/mnt/datomic}
ROOT=${ROOT:-$MNT/datomic-bench}
# indexing.toml runs a single release; override to stage more.
RELEASES=${RELEASES:-1.0.7705}
# A billion datoms of real events takes ~700 hourly files (~137M events). 720
# hours is 30 days starting at START, which covers that with a little
# headroom; once the real files are exhausted bigindex replays them, so more is
# not required. Small runs need far fewer: 1M datoms is about one hour.
HOURS=${2:-${HOURS:-720}}
START=${START:-2024-01-01}
# Parallel downloads from data.gharchive.org.
JOBS=${JOBS:-8}
# Thresholds verify checks, matching indexing.toml's defaults. Override them
# the same way the spec's parameters are overridden for a small host, e.g.
#   REQUIRE_MOUNT=false MIN_FREE_GIB=20 ./setup-host.sh verify
MIN_FILES=${MIN_FILES:-24}
MIN_FREE_GIB=${MIN_FREE_GIB:-200}
REQUIRE_MOUNT=${REQUIRE_MOUNT:-true}
HERE="$(cd "$(dirname "$0")" && pwd)"
AGENT_HOME=$(getent passwd "$AGENT_USER" | cut -d: -f6 || true)
AGENT_HOME=${AGENT_HOME:-/home/$AGENT_USER}

step_prereqs() {
  echo "==> prerequisites"
  # JDK 21 is the only JVM indexing.toml uses. python3 prints the headline in
  # the collect step.
  if command -v dnf >/dev/null; then
    sudo dnf install -y -q java-21-openjdk-headless unzip curl rsync python3 util-linux e2fsprogs
  else
    sudo apt-get update -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
      openjdk-21-jdk-headless unzip curl rsync python3 util-linux e2fsprogs
  fi
  if ! command -v clojure >/dev/null; then
    curl -sSL -o /tmp/clj-install.sh https://download.clojure.org/install/linux-install.sh
    chmod +x /tmp/clj-install.sh && sudo /tmp/clj-install.sh
  fi
  java -version 2>&1 | head -1
  clojure --version
}

step_mount() {
  echo "==> instance store at $MNT"
  if mountpoint -q "$MNT"; then
    echo "  already mounted: $(df -h "$MNT" | tail -1 | awk '{print $1, $2}')"
  else
    # Pick the first instance-store NVMe that is not mounted and has no
    # partitions. EBS volumes report model "Amazon Elastic Block Store" and are
    # never selected.
    local dev=""
    for d in $(lsblk -dn -o NAME,MODEL | awk '/Instance Storage/ {print "/dev/"$1}'); do
      if [ -z "$(lsblk -n -o MOUNTPOINT "$d" | tr -d '[:space:]')" ] \
         && [ "$(lsblk -n "$d" | wc -l)" -eq 1 ]; then
        dev=$d; break
      fi
    done
    [ -n "$dev" ] || { echo "FATAL: no free instance-store NVMe found"; lsblk -o NAME,MODEL,SIZE,MOUNTPOINT; exit 1; }
    # After a stop/start the device comes back blank. Only format a device that
    # has no filesystem; an existing one is mounted as-is.
    if [ -z "$(sudo blkid -o value -s TYPE "$dev" || true)" ]; then
      echo "  formatting $dev (ext4)"
      sudo mkfs.ext4 -q -E nodiscard "$dev"
    fi
    sudo mkdir -p "$MNT"
    sudo mount -o noatime "$dev" "$MNT"
    echo "  mounted $dev at $MNT ($(df -h "$MNT" | tail -1 | awk '{print $2}'))"
  fi
  sudo mkdir -p "$ROOT"
  sudo chown "$AGENT_USER" "$ROOT"
}

step_releases() {
  echo "==> staging $(echo $RELEASES | wc -w) Datomic release(s) into $ROOT/releases"
  sudo mkdir -p "$ROOT/releases"
  sudo chown "$AGENT_USER" "$ROOT/releases"
  for v in $RELEASES; do
    if [ -x "$ROOT/releases/datomic-pro-$v/bin/transactor" ]; then echo "  have $v"; continue; fi
    echo "  fetching $v"
    sudo -u "$AGENT_USER" curl -sSL --fail --retry 5 -o "$ROOT/releases/$v.zip" \
      "https://datomic-pro-downloads.s3.amazonaws.com/$v/datomic-pro-$v.zip" \
      || { echo "  FAILED to download $v"; continue; }
    sudo -u "$AGENT_USER" unzip -q -o "$ROOT/releases/$v.zip" -d "$ROOT/releases"
    sudo rm -f "$ROOT/releases/$v.zip"
  done
  echo "  staged: $(ls -d $ROOT/releases/datomic-pro-* 2>/dev/null | wc -l)"
}

step_data() {
  echo "==> staging $HOURS hours of GH Archive from $START into $ROOT/gh ($JOBS parallel)"
  sudo mkdir -p "$ROOT/gh"; sudo chown "$AGENT_USER" "$ROOT/gh"
  # Each file is downloaded to a .part, checked with gzip -t, then renamed, so
  # an interrupted run never leaves a truncated archive that bigindex would
  # choke on mid-load. Existing complete files are skipped, so re-running
  # resumes.
  local base
  base=$(date -u -d "$START" +%s)
  for i in $(seq 0 $((HOURS - 1))); do
    date -u -d "@$((base + i * 3600))" +%Y-%m-%d-%-H
  done | xargs -P "$JOBS" -I{} sudo -u "$AGENT_USER" sh -c '
    f="$1/$2.json.gz"
    [ -s "$f" ] && exit 0
    if curl -sS --fail --retry 5 --retry-delay 2 -o "$f.part" "https://data.gharchive.org/$2.json.gz" \
       && gzip -t "$f.part" 2>/dev/null; then
      mv "$f.part" "$f"
    else
      rm -f "$f.part"; echo "  FAILED $2"
    fi' _ "$ROOT/gh" {} || true
  echo "  staged: $(ls $ROOT/gh/*.json.gz 2>/dev/null | wc -l) files, $(du -sh $ROOT/gh | cut -f1)"
}

step_harness() {
  echo "==> installing harness source into $ROOT/bench"
  sudo mkdir -p "$ROOT/bench/src/bench"
  sudo cp "$HERE"/{bigindex,gharchive,jvmstats,metrics}.clj "$ROOT/bench/src/bench/"
  # The metrics callback must be on the TRANSACTOR's classpath, not just the
  # peer's. bin/classpath includes resources/, so drop it there per release.
  for d in "$ROOT"/releases/datomic-pro-*; do
    [ -d "$d" ] || continue
    sudo mkdir -p "$d/resources/bench"
    sudo cp "$HERE/metrics.clj" "$d/resources/bench/metrics.clj"
  done
  sudo chown -R "$AGENT_USER" "$ROOT"
  sudo chmod -R a+rX "$ROOT"
}

step_deps() {
  # The peer step builds a deps.edn pinned to RELEASE and runs `clojure`. Fetch
  # those artifacts into the agent's maven cache now so the first run does not
  # spend its time (or fail) downloading.
  echo "==> prefetching peer dependencies into $AGENT_HOME/.m2"
  local tmp
  tmp=$(sudo -u "$AGENT_USER" mktemp -d)
  for v in $RELEASES; do
    sudo -u "$AGENT_USER" tee "$tmp/deps.edn" >/dev/null <<DEPS
{:paths ["src"]
 :deps {com.datomic/peer {:mvn/version "$v"}
        org.clojure/clojure {:mvn/version "1.12.0"}
        org.clojure/data.json {:mvn/version "2.5.1"}}}
DEPS
    # sudo resets PATH to secure_path, which on Rocky/RHEL omits /usr/local/bin
    # where the Clojure installer puts `clojure`
    sudo -u "$AGENT_USER" HOME="$AGENT_HOME" PATH="/usr/local/bin:/usr/bin:/bin" \
      sh -c "cd '$tmp' && clojure -P" \
      && echo "  peer $v: OK" || { echo "  peer $v: FAILED"; exit 1; }
  done
  sudo rm -rf "$tmp"
}

step_verify() {
  # Mirrors the preflight step in indexing.toml, run as the agent user.
  echo "==> verify (as $AGENT_USER)"
  local fail=0 v
  ok()  { echo "  $1: OK${2:+ ($2)}"; }
  bad() { echo "  $1: FAILED${2:+ ($2)}"; fail=1; }

  if [ "$REQUIRE_MOUNT" = true ]; then
    mountpoint -q "$MNT" && ok "$MNT mounted" || bad "$MNT mounted"
  fi
  for v in $RELEASES; do
    sudo -u "$AGENT_USER" test -x "$ROOT/releases/datomic-pro-$v/bin/transactor" \
      && ok "transactor $v" || bad "transactor $v"
    sudo -u "$AGENT_USER" test -r "$ROOT/releases/datomic-pro-$v/resources/bench/metrics.clj" \
      && ok "transactor metrics callback $v" || bad "transactor metrics callback $v"
  done
  sudo -u "$AGENT_USER" test -r "$ROOT/bench/src/bench/bigindex.clj" \
    && ok "bigindex.clj staged" || bad "bigindex.clj staged"
  local nf
  nf=$(sudo -u "$AGENT_USER" sh -c "ls $ROOT/gh/*.json.gz 2>/dev/null | wc -l")
  [ "$nf" -ge "$MIN_FILES" ] && ok "archive files" "$nf" || bad "archive files" "$nf, need >=$MIN_FILES"
  local d
  # one of the two paths never exists, so ls fails; don't let pipefail end verify
  d=$(ls -d /usr/lib/jvm/java-21-openjdk-amd64 /usr/lib/jvm/java-21-openjdk 2>/dev/null | head -1 || true)
  [ -n "$d" ] && ok "jdk21" "$d" || bad "jdk21" "not at the paths indexing.toml probes"
  command -v clojure >/dev/null && ok "clojure" || bad "clojure"
  command -v python3 >/dev/null && ok "python3" || bad "python3"
  local avail
  avail=$(df -BG --output=avail "$MNT" | tail -1 | tr -dc '0-9')
  [ "$avail" -ge "$MIN_FREE_GIB" ] && ok "free on $MNT" "${avail} GiB" \
    || bad "free on $MNT" "${avail} GiB, need >=${MIN_FREE_GIB}"
  [ "$fail" -eq 0 ] || { echo "verify FAILED"; exit 1; }
}

case "${1:-all}" in
  prereqs)  step_prereqs ;;
  mount)    step_mount ;;
  releases) step_releases ;;
  data)     step_data ;;
  harness)  step_harness ;;
  deps)     step_deps ;;
  verify)   step_verify ;;
  all)      step_prereqs; step_mount; step_releases; step_data; step_harness; step_deps; step_verify ;;
  *) echo "usage: $0 [all|prereqs|mount|releases|data|harness|deps|verify] [hours]"; exit 1 ;;
esac
echo "done."

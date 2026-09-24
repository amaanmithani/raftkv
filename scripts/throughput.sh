#!/usr/bin/env bash
# Throughput and latency vs cluster size (3/5/7), real processes on one
# machine, fsync on. Writes results/throughput.json.
set -euo pipefail
cd "$(dirname "$0")/.."
DUR=${DUR:-20s} CLIENTS=${CLIENTS:-"32 128"} SIZES=${SIZES:-"3 5 7"}
BIN=$(mktemp -d)
go build -o "$BIN/raftkv" ./cmd/raftkv
go build -o "$BIN/loadgen" ./cmd/loadgen
OUT=$(mktemp)
cleanup() { pkill -f "$BIN/raftkv" 2>/dev/null || true; rm -rf "$BIN"; }
trap cleanup EXIT
for n in $SIZES; do
  for fsync in on off; do
    DATA=$(mktemp -d)
    peers=""; urls=""
    for i in $(seq 1 "$n"); do
      peers+="$i=http://127.0.0.1:$((17000+i)),"; urls+="http://127.0.0.1:$((17000+i)),"
    done
    peers=${peers%,}; urls=${urls%,}
    flag=""; [ "$fsync" = off ] && flag="-nosync"
    for i in $(seq 1 "$n"); do
      "$BIN/raftkv" -id "$i" -peers "$peers" -data "$DATA/$i" $flag 2>/dev/null &
    done
    sleep 2
    for c in $CLIENTS; do
      echo "n=$n fsync=$fsync clients=$c" >&2
      res=$("$BIN/loadgen" -peers "$urls" -clients "$c" -duration "$DUR")
      echo "{\"nodes\": $n, \"fsync\": \"$fsync\", \"result\": $res}" >> "$OUT"
    done
    pkill -f "$BIN/raftkv" || true
    sleep 1
    rm -rf "$DATA"
  done
done
python3 - "$OUT" "$DUR" "${CLIENTS// / and }" > results/throughput.json <<'PY'
import json, sys, platform, subprocess, os, datetime
rows = [json.loads(l) for l in open(sys.argv[1])]
try:
    cpu = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
except Exception:
    cpu = platform.processor()
print(json.dumps({"what": "write throughput and latency vs cluster size",
  "method": f"all nodes and the load generator on one machine (localhost HTTP); {sys.argv[3]} concurrent clients (one run each), "
            f"sequential PUTs of 64-byte values over 1,000 keys for {sys.argv[2]} after a 3s warm-up; each request "
            "commits through the log; fsync on = the WAL record for each batch is fsynced before messages are sent (group commit). "
            "On macOS Go's File.Sync is F_FULLFSYNC, a full flush of the drive cache, much slower than Linux fdatasync",
  "machine": {"cpu": cpu, "cores": os.cpu_count(), "os": platform.platform()},
  "date": datetime.date.today().isoformat(), "runs": rows}, indent=2))
PY
cat results/throughput.json

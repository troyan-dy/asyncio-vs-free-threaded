#!/usr/bin/env bash
# PROTOTYPE. HTTP service matrix: runtimes x builds under mixed load with background state updates.
set -uo pipefail
cd "$(dirname "$0")"
OUT=${OUT:-results/host_http_results.jsonl}
CONC=${CONC:-64}
DUR=${DUR:-10}
mkdir -p results; : > "$OUT"
: > "$OUT.rss"

.venv-gil/bin/python backends.py & BE=$!
sleep 1.5

wait_ready() {
  for _ in $(seq 1 50); do
    curl -s -o /dev/null "http://127.0.0.1:8080/health" && return 0
    sleep 0.2
  done
  echo "server did not become ready" >&2; return 1
}

bench() { # label cmd...
  local label=$1; shift
  "$@" > "server.log" 2>&1 & local SP=$!
  wait_ready || { kill $SP; return; }
  sleep 1.5   # let background tasks populate state
  ( sleep $((DUR / 2)); echo "{\"label\": \"$label\", \"server_rss_mb\": $(( $(ps -o rss= -p $SP) / 1024 ))}" >> "$OUT.rss" ) &
  .venv-gil/bin/python loadgen.py --port 8080 --conc "$CONC" --duration "$DUR" --label "$label" | tee -a "$OUT"
  kill $SP; wait $SP 2>/dev/null
  grep -i "traceback\|error" server.log | head -3 || true
}

for build in gil ft; do
  for w in 1 4 8; do
    bench "$build greenlet $w" ".venv-$build/bin/python" serve.py --rt greenlet --workers $w
  done
  bench "$build threads" ".venv-$build/bin/python" serve.py --rt threads
  bench "$build flask" ".venv-$build/bin/python" flask_serve.py
done

kill $BE; wait $BE 2>/dev/null
echo "done -> $OUT"

#!/usr/bin/env bash
# PROTOTYPE. Runs the whole matrix and appends JSON lines to results.jsonl.
set -euo pipefail
cd "$(dirname "$0")"
OUT=${OUT:-results/host_greenlet_microbench.jsonl}
mkdir -p results; : > "$OUT"

run() { # build mode workers scenario conc reqs [extra...]
  local build=$1 mode=$2 workers=$3 scenario=$4 conc=$5 reqs=$6; shift 6
  ".venv-$build/bin/python" bench.py --mode "$mode" --workers "$workers" --scenario "$scenario" \
      --conc "$conc" --reqs "$reqs" "$@" | tee -a "$OUT"
}

echo "== CPU: 80 tasks x 200000 iters, no I/O =="
for build in gil ft; do
  for w in 1 2 4 8; do
    run $build greenlet $w cpu 80 1 --cpu-iters 200000
    run $build threads  $w cpu 80 1 --cpu-iters 200000
  done
done

echo "== IO fast echo: conc=200, 200 req each =="
".venv-gil/bin/python" echo_server.py 9000 4 0 & EP=$!; sleep 1
for build in gil ft; do
  for w in 1 2 4 8; do
    run $build greenlet $w io 200 200
  done
  run $build threads 0 io 200 200
done
kill $EP; wait $EP 2>/dev/null || true

echo "== IO slow echo (1ms 'db'): conc=1000, 20 req each =="
".venv-gil/bin/python" echo_server.py 9000 4 0.001 & EP=$!; sleep 1
for build in gil ft; do
  for w in 1 4 8; do
    run $build greenlet $w io 1000 20
  done
  run $build threads 0 io 1000 20
done
kill $EP; wait $EP 2>/dev/null || true

echo "== MIXED: fast echo + 20000 iters CPU per request, conc=200, 50 req each =="
".venv-gil/bin/python" echo_server.py 9000 4 0 & EP=$!; sleep 1
for build in gil ft; do
  for w in 1 2 4 8; do
    run $build greenlet $w mixed 200 50
  done
  run $build threads 0 mixed 200 50
done
kill $EP; wait $EP 2>/dev/null || true
echo "done -> $OUT"

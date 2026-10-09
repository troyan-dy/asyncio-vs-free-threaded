#!/usr/bin/env bash
# PROTOTYPE. Same matrix as run_async_matrix.sh, but every server runs in a container with --cpus 4.
# backends: --cpus 2, loadgen (free-threaded build, real parallel threads): --cpus 3.
set -uo pipefail
cd "$(dirname "$0")"
IMG=${IMG:-ft-bench}
OUT=${OUT:-results/docker_results.jsonl}
CONC=${CONC:-64}
DUR=${DUR:-10}
SERVER_CPUS=${SERVER_CPUS:-4}
ECHO_DELAY=${ECHO_DELAY:-0.001}
CPU_ITERS=${CPU_ITERS:-20000}
NET=ftnet
SHARED="$PWD/shared"
mkdir -p "$SHARED"
: > "$OUT"
: > "$OUT.rss"

docker build -q -t "$IMG" . >/dev/null || { echo "build failed" >&2; exit 1; }
docker network inspect $NET >/dev/null 2>&1 || docker network create $NET >/dev/null
docker rm -f backends server >/dev/null 2>&1 || true

docker run -d --rm --name backends --network $NET --cpus 2 \
  -v "$SHARED:/shared" -e CONFIG_PATH=/shared/config.json -e BIND_HOST=0.0.0.0 -e ECHO_DELAY="$ECHO_DELAY" -e CPU_ITERS="$CPU_ITERS" \
  "$IMG" .venv-gil/bin/python backends.py >/dev/null
sleep 2

wait_ready() {
  for _ in $(seq 1 100); do
    docker exec server curl -s -o /dev/null http://127.0.0.1:8080/health 2>/dev/null && return 0
    sleep 0.2
  done
  echo "server did not become ready" >&2; docker logs server 2>&1 | tail -5; return 1
}

bench() { # label cmd...
  local label=$1; shift
  docker run -d --rm --name server --network $NET --cpus "$SERVER_CPUS" \
    -v "$SHARED:/shared" -e CONFIG_PATH=/shared/config.json -e BACKEND_HOST=backends -e BIND_HOST=0.0.0.0 \
    -e APP_SYNC="${APP_SYNC:-0}" -e APP_CPU="${APP_CPU:-inline}" \
    "$IMG" "$@" >/dev/null
  wait_ready || { docker rm -f server >/dev/null 2>&1; return; }
  sleep 2
  ( sleep $((DUR / 2)); echo "{\"label\": \"$label\", \"server_mem\": \"$(docker stats --no-stream --format '{{.MemUsage}}' server | cut -d/ -f1 | tr -d ' ')\", \"server_cpu\": \"$(docker stats --no-stream --format '{{.CPUPerc}}' server)\"}" >> "$OUT.rss" ) &
  docker run --rm --network $NET --cpus 3 "$IMG" \
    .venv-ft/bin/python loadgen.py --host server --port 8080 --conc "$CONC" --duration "$DUR" --label "$label" | tee -a "$OUT"
  wait
  docker logs server 2>&1 | grep -i "traceback\|error" | head -3 || true
  docker rm -f server >/dev/null 2>&1
}

for build in gil ft; do
  PY=".venv-$build/bin/python"
  APP_SYNC=0 APP_CPU=inline bench "$build async 1 loop"              $PY uvicorn_serve.py --loops 1
  APP_SYNC=0 APP_CPU=inline bench "$build async 4 loops (threads)"   $PY uvicorn_serve.py --loops 4
  APP_SYNC=0 APP_CPU=inline bench "$build async 4 procs"             $PY uvicorn_serve.py --workers 4
  APP_SYNC=0 APP_CPU=thread bench "$build async 1 loop + to_thread"  $PY uvicorn_serve.py --loops 1
  APP_SYNC=0 APP_CPU=thread bench "$build async 4 loops + to_thread" $PY uvicorn_serve.py --loops 4
  APP_SYNC=1 APP_CPU=inline bench "$build sync fastapi def (64 thr)" $PY uvicorn_serve.py --loops 1
done

docker rm -f backends >/dev/null 2>&1
echo "done -> $OUT"

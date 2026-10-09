#!/usr/bin/env bash
# PROTOTYPE. One extra config against the running (or started) backends container. Usage:
#   APP_SYNC=0 APP_CPU=inline ./docker_bench_one.sh "label" .venv-ft/bin/python uvicorn_serve.py --workers 4 --loop asyncio --http h11
set -uo pipefail
cd "$(dirname "$0")"
IMG=${IMG:-ft-bench}; OUT=${OUT:-results/docker_extra.jsonl}; CONC=${CONC:-64}; DUR=${DUR:-10}; NET=ftnet; SHARED="$PWD/shared"
docker network inspect $NET >/dev/null 2>&1 || docker network create $NET >/dev/null
if ! docker ps --format '{{.Names}}' | grep -q '^backends$'; then
  docker run -d --rm --name backends --network $NET --cpus 2 -v "$SHARED:/shared" -e CONFIG_PATH=/shared/config.json -e BIND_HOST=0.0.0.0 -e ECHO_DELAY="${ECHO_DELAY:-0.001}" -e CPU_ITERS="${CPU_ITERS:-20000}" "$IMG" .venv-gil/bin/python backends.py >/dev/null; sleep 2
fi
label=$1; shift
docker rm -f server >/dev/null 2>&1
docker run -d --rm --name server --network $NET --cpus "${SERVER_CPUS:-4}" -v "$SHARED:/shared" -e CONFIG_PATH=/shared/config.json -e BACKEND_HOST=backends -e BIND_HOST=0.0.0.0 -e APP_SYNC="${APP_SYNC:-0}" -e APP_CPU="${APP_CPU:-inline}" -e CPU_PERMITS="${CPU_PERMITS:-0}" -e EXECUTOR_THREADS="${EXECUTOR_THREADS:-0}" "$IMG" "$@" >/dev/null
for _ in $(seq 1 100); do docker exec server curl -s -o /dev/null http://127.0.0.1:8080/health 2>/dev/null && break; sleep 0.2; done
sleep 2
( sleep $((DUR / 2)); echo "{\"label\": \"$label\", \"server_mem\": \"$(docker stats --no-stream --format '{{.MemUsage}}' server | cut -d/ -f1 | tr -d ' ')\", \"server_cpu\": \"$(docker stats --no-stream --format '{{.CPUPerc}}' server)\"}" >> "$OUT.rss" ) &
docker run --rm --network $NET --cpus 3 "$IMG" .venv-ft/bin/python loadgen.py --host server --port 8080 --conc "$CONC" --duration "$DUR" --label "$label" | tee -a "$OUT"
wait
docker logs server 2>&1 | grep -i "traceback\|error" | head -3 || true
docker rm -f server >/dev/null 2>&1

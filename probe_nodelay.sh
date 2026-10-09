#!/usr/bin/env bash
# Reproduces the TCP_NODELAY finding inside the container (Linux): asyncio create_server(sock=...)
# leaves accepted sockets without TCP_NODELAY in CPython 3.14, uvloop sets it; and the resulting
# 40 ms stall between response headers and body under uvicorn --loop asyncio.
set -euo pipefail
cd "$(dirname "$0")"
IMG=${IMG:-ft-bench}
docker build -q -t "$IMG" . >/dev/null
echo "== TCP_NODELAY on accepted sockets"
docker run --rm "$IMG" .venv-gil/bin/python nodelay_check.py
echo "== segment timing of GET /health under uvicorn (asyncio loop vs uvloop)"
docker run --rm --cpus 4 "$IMG" bash -c '
cd /app; export BIND_HOST=127.0.0.1 APP_SYNC=0 APP_CPU=inline CONFIG_PATH=/tmp/config.json BACKEND_HOST=127.0.0.1
.venv-gil/bin/python backends.py >/dev/null 2>&1 & sleep 2
for cfg in "asyncio h11" "uvloop h11"; do
  set -- $cfg
  .venv-gil/bin/python uvicorn_serve.py --loops 1 --loop $1 --http $2 >/tmp/s.log 2>&1 & SP=$!
  for _ in $(seq 1 50); do curl -s -o /dev/null http://127.0.0.1:8080/health && break; sleep 0.2; done; sleep 0.5
  echo "-- $cfg"; .venv-gil/bin/python stall_timing.py 8080
  kill $SP; wait $SP 2>/dev/null
done'

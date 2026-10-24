"""PROTOTYPE. The same demo service on FastAPI.

Env:
  APP_SYNC=1      -> `def` handler executed in anyio's thread pool (64 tokens); background work and the
                     DB pool are the plain-thread implementation from demo_app/rt_threads (sync_variant.py).
  APP_SYNC=0      -> `async def` handler; DB pool = asyncio streams, ONE POOL PER EVENT LOOP (asyncio
                     connections are loop-bound); shared state is a module-global object (async_variant.py).
  APP_CPU=inline  -> CPU work inside the handler (blocks the loop for ~0.6 ms)
  APP_CPU=thread  -> CPU work via asyncio.to_thread (parallel on the free-threaded build)
"""

from __future__ import annotations

import os

from fastapi import FastAPI, Response

import async_variant
from demo_app import JSON_MEDIA_TYPE
from sync_variant import SyncVariant

SYNC = os.environ.get("APP_SYNC", "0") == "1"

if SYNC:
    _sync = SyncVariant()
    _lifespan, _quote = _sync.lifespan, _sync.quote
else:
    _lifespan, _quote = async_variant.lifespan, async_variant.quote

app = FastAPI(lifespan=_lifespan)
app.add_api_route("/quote", _quote, methods=["GET"])


@app.get("/health")
async def health() -> Response:
    """Liveness probe."""
    return Response(b'{"ok":true}', media_type=JSON_MEDIA_TYPE)

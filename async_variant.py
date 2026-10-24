"""PROTOTYPE. ``async def`` variant of the service (APP_SYNC=0).

DB pool = asyncio streams, ONE POOL PER EVENT LOOP (asyncio connections are loop-bound); shared state is
a module-global object updated by the first loop of the process.
  APP_CPU=inline  -> CPU work inside the handler (blocks the loop for ~0.6 ms)
  APP_CPU=thread  -> CPU work via asyncio.to_thread (parallel on the free-threaded build)
  EXECUTOR_THREADS=N -> size of the to_thread pool per loop (0 = asyncio default)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, Request, Response

from async_pool import open_pools_for_current_loop, pools_for_current_loop
from async_tasks import rotate_pools, run_every, start_process_tasks, state
from backend_protocol import DB_MSG_SIZE, DB_PORTS
from demo_app import JSON_MEDIA_TYPE, NO_RATE, QUERY, ROTATE_PERIOD, cpu_work

EXECUTOR_THREADS = int(os.environ.get("EXECUTOR_THREADS", "0"))
CPU_MODE = os.environ.get("APP_CPU", "inline")


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Per loop: open the pools and rotate them; first loop of the process: update the shared state."""
    if EXECUTOR_THREADS:
        asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(EXECUTOR_THREADS))
    pools = await open_pools_for_current_loop()
    tasks = [asyncio.create_task(run_every(ROTATE_PERIOD, rotate_pools, pools))]  # per loop: pools are per loop
    tasks.extend(start_process_tasks())  # per process: shared state is updated by one loop only
    yield
    for task in tasks:
        task.cancel()


async def quote(request: Request) -> Response:  # noqa: WPS210 - hot path: no extra calls per request
    """GET /quote?sym=N: one DB round-trip, cpu_iters of CPU, JSON with the state versions."""
    sym = int(request.query_params.get("sym", "0"))
    cfg, rates = state.config, state.rates
    pool = pools_for_current_loop()[sym % DB_PORTS]
    async with pool.acquire() as (reader, writer):  # I/O: 1 ms "database"
        writer.write(QUERY)
        await writer.drain()
        await reader.readexactly(DB_MSG_SIZE)
    if CPU_MODE == "thread":
        checksum = await asyncio.to_thread(cpu_work, cfg.cpu_iters)
    else:
        checksum = cpu_work(cfg.cpu_iters)
    body = json.dumps(
        {
            "sym": sym,
            "price": rates.get(sym, NO_RATE) * cfg.multiplier,
            "cfg_version": cfg.version,
            "stream_seq": state.stream_seq,
            "pool_gen": pool.generation,
            "reconnects": state.stream_reconnects,
            "checksum": checksum % 1000,
        },
    ).encode()
    return Response(body, media_type=JSON_MEDIA_TYPE)

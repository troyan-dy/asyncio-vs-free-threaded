"""PROTOTYPE. The same demo service on FastAPI.

Env:
  APP_SYNC=1      -> `def` handler executed in anyio's thread pool (64 tokens); background work and the
                     DB pool are the plain-thread implementation from demo_app/rt_threads.
  APP_SYNC=0      -> `async def` handler; DB pool = asyncio streams, ONE POOL PER EVENT LOOP (asyncio
                     connections are loop-bound); shared state is a module-global object.
  APP_CPU=inline  -> CPU work inside the handler (blocks the loop for ~0.6 ms)
  APP_CPU=thread  -> CPU work via asyncio.to_thread (parallel on the free-threaded build)
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import traceback
from contextlib import asynccontextmanager

import anyio.to_thread
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Request as FRequest
from fastapi.responses import Response

from demo_app import CONFIG_PATH, DB_ADDR_BASE, DB_PORTS, QUERY, STREAM_ADDR, STREAM_MSG, Config, State, cpu_work

SYNC = os.environ.get("APP_SYNC", "0") == "1"
EXECUTOR_THREADS = int(os.environ.get("EXECUTOR_THREADS", "0"))  # size of the to_thread pool per loop (0 = asyncio default)
CPU_MODE = os.environ.get("APP_CPU", "inline")
POOL_SIZE = 8

state = State()  # shared by every loop/thread in this process (not across worker processes)
_once = threading.Lock()
_started = False


# ---------------------------------------------------------------- async implementation
class AsyncPool:
    def __init__(self, addr: tuple[str, int]) -> None:
        self.addr = addr
        self.q: asyncio.Queue = asyncio.Queue()
        self.generation = 0

    async def open(self) -> None:
        for _ in range(POOL_SIZE):
            self.q.put_nowait(await asyncio.open_connection(*self.addr))

    @asynccontextmanager
    async def acquire(self):  # noqa: ANN201
        r, w = await self.q.get()
        try:
            yield r, w
        except BaseException:
            w.close()
            r, w = await asyncio.open_connection(*self.addr)
            raise
        finally:
            self.q.put_nowait((r, w))

    async def rotate(self) -> None:
        for _ in range(POOL_SIZE):
            _r, w = await self.q.get()
            w.close()
            self.q.put_nowait(await asyncio.open_connection(*self.addr))
        self.generation += 1


_pools: dict[asyncio.AbstractEventLoop, list[AsyncPool]] = {}
_pools_lock = threading.Lock()


def pools_for_current_loop() -> list[AsyncPool]:
    with _pools_lock:
        return _pools[asyncio.get_running_loop()]


async def _open_pools_for_this_loop() -> list[AsyncPool]:
    ps = [AsyncPool((DB_ADDR_BASE[0], DB_ADDR_BASE[1] + i)) for i in range(DB_PORTS)]
    for p in ps:
        await p.open()
    with _pools_lock:
        _pools[asyncio.get_running_loop()] = ps
    return ps


async def _every(seconds: float, fn) -> None:  # noqa: ANN001
    while True:
        try:
            await fn()
        except Exception:  # noqa: BLE001
            traceback.print_exc()
        await asyncio.sleep(seconds)


async def _reload_config(mtime: list[float]) -> None:
    st = os.stat(CONFIG_PATH)
    if st.st_mtime == mtime[0]:
        return
    mtime[0] = st.st_mtime
    with open(CONFIG_PATH) as f:
        raw = json.load(f)
    state.config = Config(raw["version"], raw["multiplier"], raw["cpu_iters"])
    state.config_reloads += 1


async def _consume_stream() -> None:
    while True:
        try:
            r, w = await asyncio.open_connection(*STREAM_ADDR)
            state.stream_reconnects += 1
            try:
                while True:
                    seq, sym, value = STREAM_MSG.unpack_from(await r.readexactly(64))
                    state.rates = {**state.rates, sym: value}
                    state.stream_seq = seq
            finally:
                w.close()
        except Exception:  # noqa: BLE001
            traceback.print_exc()
            await asyncio.sleep(0.1)


async def _rotate_pools(ps: list[AsyncPool]) -> None:
    for p in ps:
        await p.rotate()


@asynccontextmanager
async def async_lifespan(_app: FastAPI):  # noqa: ANN201
    global _started  # noqa: PLW0603
    if EXECUTOR_THREADS:
        asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(EXECUTOR_THREADS))
    ps = await _open_pools_for_this_loop()
    tasks = [asyncio.create_task(_every(3.0, lambda: _rotate_pools(ps)))]  # per loop: pools are per loop
    with _once:
        first = not _started
        _started = True
    if first:  # per process: shared state is updated by one loop only
        tasks.append(asyncio.create_task(_every(1.0, lambda m=[0.0]: _reload_config(m))))
        tasks.append(asyncio.create_task(_consume_stream()))
    yield
    for t in tasks:
        t.cancel()


async def async_quote(request: FRequest) -> Response:
    sym = int(request.query_params.get("sym", "0"))
    cfg, rates = state.config, state.rates
    pool = pools_for_current_loop()[sym % DB_PORTS]
    async with pool.acquire() as (r, w):  # I/O: 1 ms "database"
        w.write(QUERY)
        await w.drain()
        await r.readexactly(64)
    if CPU_MODE == "thread":
        checksum = await asyncio.to_thread(cpu_work, cfg.cpu_iters)
    else:
        checksum = cpu_work(cfg.cpu_iters)
    body = json.dumps(
        {
            "sym": sym,
            "price": rates.get(sym, 0.0) * cfg.multiplier,
            "cfg_version": cfg.version,
            "stream_seq": state.stream_seq,
            "pool_gen": pool.generation,
            "reconnects": state.stream_reconnects,
            "checksum": checksum % 1000,
        }
    ).encode()
    return Response(body, media_type="application/json")


# ---------------------------------------------------------------- sync implementation
def _build_sync():  # noqa: ANN202
    from demo_app import build_app
    from rt_threads import ThreadRuntime

    inner = build_app(ThreadRuntime())

    @asynccontextmanager
    async def lifespan(_app: FastAPI):  # noqa: ANN202
        global _started  # noqa: PLW0603
        anyio.to_thread.current_default_thread_limiter().total_tokens = 64  # per loop
        with _once:
            first = not _started
            _started = True
        if first:
            for fn in inner.on_start:
                fn()
            for loop in inner.background:
                threading.Thread(target=loop, daemon=True).start()
        yield

    handler = inner.routes["/quote"]

    def sync_quote(request: FRequest) -> Response:
        from miniflask import Request

        body = handler(Request("/quote", dict(request.query_params)))
        return Response(body, media_type="application/json")

    return lifespan, sync_quote


if SYNC:
    _lifespan, _quote = _build_sync()
else:
    _lifespan, _quote = async_lifespan, async_quote

app = FastAPI(lifespan=_lifespan)
app.add_api_route("/quote", _quote, methods=["GET"])


@app.get("/health")
async def health() -> Response:
    return Response(b'{"ok":true}', media_type="application/json")

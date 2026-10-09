"""PROTOTYPE. The demo service: handlers with I/O + CPU, background state updates.

State updated in background:
  - config.json re-read on mtime change (every 1 s)      -> state.config (immutable snapshot)
  - "gRPC stream" (TCP push stream) consumed in a daemon  -> state.rates / state.stream_seq
  - DB pool connections recreated every 3 s              -> pool.generation
Handler /quote: 1 "DB" round-trip (echo with 1 ms delay) + cpu_iters of pure-Python work.
"""

from __future__ import annotations

import json
import os
import struct
import threading
from dataclasses import dataclass
from typing import Any

from miniflask import App, Pool, Request

BACKEND_HOST = os.environ.get("BACKEND_HOST", "127.0.0.1")
DB_ADDR_BASE = (BACKEND_HOST, 9000)
DB_PORTS = 4
STREAM_ADDR = (BACKEND_HOST, 9100)
CONFIG_PATH = os.environ.get("CONFIG_PATH", os.path.join(os.path.dirname(__file__), "config.json"))
QUERY = b"q" * 64
# Optional: at most N threads execute the CPU part at once (the "GIL sized to the CPU count" idea).
_CPU_PERMITS = int(os.environ.get("CPU_PERMITS", "0"))
STREAM_MSG = struct.Struct("<qid")  # seq, symbol, value (padded to 64 bytes)


@dataclass(frozen=True)
class Config:
    version: int
    multiplier: float
    cpu_iters: int


class State:
    """Mutable holder of immutable snapshots. Attribute assignment is atomic, readers take one ref."""

    config: Config = Config(0, 1.0, 20000)
    rates: dict[int, float] = {}
    stream_seq: int = 0
    stream_reconnects: int = 0
    config_reloads: int = 0


def cpu_work(iters: int) -> int:
    s = 0
    for i in range(iters):
        s += i * i
    return s


def build_app(rt: Any) -> App:
    app = App(rt)
    state = State()
    cpu_permits = threading.BoundedSemaphore(_CPU_PERMITS) if _CPU_PERMITS else None
    pools: list[Pool] = []
    mtime = [0.0]

    @app.startup
    def connect_db() -> None:
        for i in range(DB_PORTS):
            pools.append(Pool(rt, (DB_ADDR_BASE[0], DB_ADDR_BASE[1] + i), size=8))

    @app.every(1.0)
    def reload_config() -> None:
        st = os.stat(CONFIG_PATH)
        if st.st_mtime == mtime[0]:
            return
        mtime[0] = st.st_mtime
        with open(CONFIG_PATH) as f:
            raw = json.load(f)
        state.config = Config(raw["version"], raw["multiplier"], raw["cpu_iters"])
        state.config_reloads += 1

    @app.every(3.0)
    def rotate_db_connections() -> None:
        for p in pools:
            p.rotate()

    @app.daemon
    def consume_stream() -> None:
        s = rt.socket()
        s.connect(STREAM_ADDR)
        state.stream_reconnects += 1
        try:
            while True:
                seq, sym, value = STREAM_MSG.unpack_from(s.recv_exactly(64))
                state.rates = {**state.rates, sym: value}  # publish a new snapshot, never mutate
                state.stream_seq = seq
        finally:
            s.close()

    @app.get("/health")
    def health(_req: Request) -> bytes:
        return b'{"ok":true}'

    @app.get("/quote")
    def quote(req: Request) -> bytes:
        sym = int(req.query.get("sym", "0"))
        cfg, rates = state.config, state.rates  # consistent snapshot for this request
        pool = pools[sym % len(pools)]
        with pool.acquire() as conn:  # I/O: 1 ms "database"
            conn.send_all(QUERY)
            conn.recv_exactly(64)
        if cpu_permits is not None:
            with cpu_permits:  # only N threads compute at once; the rest wait (or are blocked in I/O)
                checksum = cpu_work(cfg.cpu_iters)
        else:
            checksum = cpu_work(cfg.cpu_iters)  # CPU
        return json.dumps(
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

    return app

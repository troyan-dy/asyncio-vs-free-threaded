"""PROTOTYPE. The demo service: handlers with I/O + CPU, background state updates.

State updated in background:
  - config.json re-read on mtime change (every 1 s)      -> state.config (immutable snapshot)
  - "gRPC stream" (TCP push stream) consumed in a daemon  -> state.rates / state.stream_seq
  - DB pool connections recreated every 3 s              -> pool.generation
Handler /quote: 1 "DB" round-trip (echo with 1 ms delay) + cpu_iters of pure-Python work.
"""

from __future__ import annotations

import contextlib
import json
import os
import threading
from dataclasses import dataclass
from typing import Any

from backend_protocol import DB_MSG_SIZE, DB_PORT_BASE, DB_PORTS, STREAM_MSG, STREAM_MSG_SIZE, STREAM_PORT
from miniflask import App, Pool, Request

BACKEND_HOST = os.environ.get("BACKEND_HOST", "127.0.0.1")
DB_PORT_RANGE = range(DB_PORT_BASE, DB_PORT_BASE + DB_PORTS)
DB_ADDRS = tuple((BACKEND_HOST, port) for port in DB_PORT_RANGE)
STREAM_ADDR = (BACKEND_HOST, STREAM_PORT)
HERE = os.path.dirname(__file__)
CONFIG_PATH = os.environ.get("CONFIG_PATH", os.path.join(HERE, "config.json"))
CONFIG_PERIOD = 1  # seconds between config.json mtime checks
ROTATE_PERIOD = 3  # seconds between DB pool rotations
POOL_SIZE = 8  # connections per DB port
QUERY = b"q" * DB_MSG_SIZE
JSON_MEDIA_TYPE = "application/json"
DEFAULT_CPU_ITERS = 20000
NO_RATE = float(0)  # price of a symbol the stream has not sent yet
# Optional: at most N threads execute the CPU part at once (the "GIL sized to the CPU count" idea).
CPU_PERMITS = int(os.environ.get("CPU_PERMITS", "0"))


@dataclass(frozen=True)
class Config:
    """One version of config.json."""

    version: int
    multiplier: float
    cpu_iters: int


class State:
    """Mutable holder of immutable snapshots. Attribute assignment is atomic, readers take one ref."""

    config: Config = Config(0, 1.0, DEFAULT_CPU_ITERS)
    rates: dict[int, float] = {}
    stream_seq: int = 0
    stream_reconnects: int = 0
    config_reloads: int = 0


class ConfigWatcher:
    """Re-reads config.json into ``state.config`` when its mtime changes."""

    def __init__(self, path: str) -> None:
        """Watch the config file at ``path``."""
        self._path = path
        self._mtime: float = 0

    def reload(self, state: State) -> None:
        """Publish a new Config snapshot if the file changed since the last call."""
        mtime = os.stat(self._path).st_mtime
        if mtime == self._mtime:
            return
        self._mtime = mtime
        with open(self._path) as config_file:
            raw = json.load(config_file)
        state.config = Config(raw["version"], raw["multiplier"], raw["cpu_iters"])
        state.config_reloads += 1


def cpu_work(iters: int) -> int:
    """The CPU part of a request: pure-Python loop of ``iters`` iterations."""
    total = 0
    for index in range(iters):  # noqa: WPS519 - hot path: sum() over a generator is slower and changes the workload
        total += index * index
    return total


def health(_req: Request) -> bytes:
    """Liveness probe."""
    return b'{"ok":true}'


class QuoteService:
    """Background state updates and the /quote handler of one App."""

    def __init__(self, rt: Any, stopping: threading.Event) -> None:
        """Create the service on runtime ``rt``; ``stopping`` ends the stream consumer."""
        self.state = State()
        self.pools: list[Pool] = []
        self._rt = rt
        self._stopping = stopping
        self._cpu_permits = threading.BoundedSemaphore(CPU_PERMITS) if CPU_PERMITS else None
        self._config = ConfigWatcher(CONFIG_PATH)

    def connect_db(self) -> None:
        """Open the DB pools (startup)."""
        for addr in DB_ADDRS:
            self.pools.append(Pool(self._rt, addr, size=POOL_SIZE))

    def reload_config(self) -> None:
        """Pick up a new config.json (periodic)."""
        self._config.reload(self.state)

    def rotate_db_connections(self) -> None:
        """Recreate every DB connection (periodic)."""
        for pool in self.pools:
            pool.rotate()

    def consume_stream(self) -> None:
        """Apply push-stream messages to the state until the stream breaks (daemon)."""
        state = self.state
        with contextlib.closing(self._rt.socket()) as sock:
            sock.connect(STREAM_ADDR)
            state.stream_reconnects += 1
            while not self._stopping.is_set():
                seq, sym, rate = STREAM_MSG.unpack_from(sock.recv_exactly(STREAM_MSG_SIZE))
                state.rates = {**state.rates, sym: rate}  # publish a new snapshot, never mutate
                state.stream_seq = seq

    def quote(self, req: Request) -> bytes:  # noqa: WPS210 - hot path: no extra calls per request
        """GET /quote?sym=N: one DB round-trip, cpu_iters of CPU, JSON with the state versions."""
        sym = int(req.query.get("sym", "0"))
        cfg, rates = self.state.config, self.state.rates  # consistent snapshot for this request
        pool = self.pools[sym % len(self.pools)]
        with pool.acquire() as conn:  # I/O: 1 ms "database"
            conn.send_all(QUERY)
            conn.recv_exactly(DB_MSG_SIZE)
        if self._cpu_permits is None:
            checksum = cpu_work(cfg.cpu_iters)  # CPU
        else:
            with self._cpu_permits:  # only N threads compute at once; the rest wait (or are blocked in I/O)
                checksum = cpu_work(cfg.cpu_iters)
        return json.dumps(
            {
                "sym": sym,
                "price": rates.get(sym, NO_RATE) * cfg.multiplier,
                "cfg_version": cfg.version,
                "stream_seq": self.state.stream_seq,
                "pool_gen": pool.generation,
                "reconnects": self.state.stream_reconnects,
                "checksum": checksum % 1000,
            },
        ).encode()


def build_app(rt: Any) -> App:
    """Wire QuoteService into a miniflask App on runtime ``rt``."""
    app = App(rt)
    service = QuoteService(rt, app.stopping)
    app.startup(service.connect_db)
    app.every(CONFIG_PERIOD)(service.reload_config)
    app.every(ROTATE_PERIOD)(service.rotate_db_connections)
    app.daemon(service.consume_stream)
    app.get("/health")(health)
    app.get("/quote")(service.quote)
    return app

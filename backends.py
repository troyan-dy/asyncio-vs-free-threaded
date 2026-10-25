"""PROTOTYPE. Fake backends for the demo: 4 "DB" echo processes (1 ms delay, ports 9000-9003),
a push "gRPC stream" (port 9100, 64B message every 10 ms to every subscriber), and a config writer
that bumps config.json every 2 s. Run with the regular build:  .venv-gil/bin/python backends.py
"""

import asyncio
import contextlib
import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterator

from backend_protocol import DB_PORT_BASE, DB_PORTS, STREAM_MSG, STREAM_MSG_SIZE, STREAM_PORT

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.environ.get("CONFIG_PATH", os.path.join(HERE, "config.json"))
CONFIG_TMP_PATH = f"{CONFIG_PATH}.tmp"
CONFIG_PERIOD = 2  # seconds between config.json versions
BIND_HOST = os.environ.get("BIND_HOST", "127.0.0.1")
ECHO_DELAY = os.environ.get("ECHO_DELAY", "0.001")  # seconds, the "database" latency
ECHO_SCRIPT = os.path.join(HERE, "echo_server.py")
ECHO_CMD = (sys.executable, ECHO_SCRIPT, str(DB_PORT_BASE), str(DB_PORTS), ECHO_DELAY)
DB_PORT_LAST = DB_PORT_BASE + DB_PORTS - 1
CPU_ITERS = int(os.environ.get("CPU_ITERS", "20000"))  # published via config.json
STREAM_PERIOD = 0.01  # seconds between stream messages
STREAM_SYMBOLS = 10
PRICE_RANGE = (90, 110)


class StreamHub:
    """Push stream: every subscriber gets the same message every STREAM_PERIOD seconds."""

    def __init__(self) -> None:
        """Start without subscribers."""
        self._writers: set[asyncio.StreamWriter] = set()

    async def subscribe(self, _reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Connection handler: keep the subscriber until it disconnects."""
        with self._subscribed(writer), contextlib.suppress(ConnectionError):
            await writer.wait_closed()

    async def push_forever(self) -> None:
        """Send a new (seq, symbol, value) message to every subscriber."""
        seq = 0
        while True:
            seq += 1
            message = STREAM_MSG.pack(seq, seq % STREAM_SYMBOLS, random.uniform(*PRICE_RANGE))
            self._broadcast(message.ljust(STREAM_MSG_SIZE, b"\0"))
            await asyncio.sleep(STREAM_PERIOD)

    @contextlib.contextmanager
    def _subscribed(self, writer: asyncio.StreamWriter) -> Iterator[None]:
        self._writers.add(writer)
        try:
            yield
        finally:
            self._writers.discard(writer)

    def _broadcast(self, message: bytes) -> None:
        for subscriber in list(self._writers):
            try:
                subscriber.write(message)
            except ConnectionError:
                self._writers.discard(subscriber)


def config_writer(period: float) -> None:
    """Atomically replace config.json with a new version every ``period`` seconds (own daemon thread)."""
    version = 0
    while True:  # noqa: WPS457 - семантика бенчмарка: daemon-поток пишет конфиг до конца процесса, как в c7120b3
        version += 1
        multiplier = 1.0 + (version % 5) / 10
        with open(CONFIG_TMP_PATH, "w") as config_file:
            json.dump({"version": version, "multiplier": multiplier, "cpu_iters": CPU_ITERS}, config_file)
        os.replace(CONFIG_TMP_PATH, CONFIG_PATH)
        time.sleep(period)


async def stream_server(port: int) -> None:
    """Serve the push stream on ``port`` forever."""
    hub = StreamHub()
    server = await asyncio.start_server(hub.subscribe, BIND_HOST, port)
    async with server:
        await asyncio.gather(server.serve_forever(), hub.push_forever())


def _stop(echo: subprocess.Popen[bytes]) -> None:
    echo.terminate()
    os._exit(0)


def main() -> None:
    """Start the echo processes, the config writer thread and the push stream."""
    echo = subprocess.Popen(ECHO_CMD)
    threading.Thread(target=config_writer, args=(CONFIG_PERIOD,), daemon=True).start()
    signal.signal(signal.SIGTERM, lambda *_: _stop(echo))
    signal.signal(signal.SIGINT, lambda *_: _stop(echo))
    stream = f"stream {STREAM_PORT}, config.json every {CONFIG_PERIOD}s"
    db_ports = f"{DB_PORT_BASE}-{DB_PORT_LAST} (delay {ECHO_DELAY}s)"
    sys.stdout.write(f"backends ready: db {db_ports}, {stream} (cpu_iters={CPU_ITERS})\n")
    sys.stdout.flush()
    asyncio.run(stream_server(STREAM_PORT))


if __name__ == "__main__":
    main()

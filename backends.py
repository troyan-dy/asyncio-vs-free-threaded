"""PROTOTYPE. Fake backends for the demo: 4 "DB" echo processes (1 ms delay, ports 9000-9003),
a push "gRPC stream" (port 9100, 64B message every 10 ms to every subscriber), and a config writer
that bumps config.json every 2 s. Run with the regular build:  .venv-gil/bin/python backends.py
"""

import asyncio
import json
import os
import random
import signal
import struct
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.environ.get("CONFIG_PATH", os.path.join(HERE, "config.json"))
BIND_HOST = os.environ.get("BIND_HOST", "127.0.0.1")
ECHO_DELAY = os.environ.get("ECHO_DELAY", "0.001")  # seconds, the "database" latency
CPU_ITERS = int(os.environ.get("CPU_ITERS", "20000"))  # published via config.json
STREAM_MSG = struct.Struct("<qid")


async def stream_server(port: int) -> None:
    writers: set[asyncio.StreamWriter] = set()

    async def handle(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.add(writer)
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass
        finally:
            writers.discard(writer)

    async def pusher() -> None:
        seq = 0
        while True:
            seq += 1
            msg = STREAM_MSG.pack(seq, seq % 10, random.uniform(90, 110)).ljust(64, b"\0")
            for w in list(writers):
                try:
                    w.write(msg)
                except ConnectionError:
                    writers.discard(w)
            await asyncio.sleep(0.01)

    server = await asyncio.start_server(handle, BIND_HOST, port)
    async with server:
        await asyncio.gather(server.serve_forever(), pusher())


def config_writer(period: float) -> None:
    version = 0
    while True:
        version += 1
        tmp = CONFIG_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"version": version, "multiplier": 1.0 + (version % 5) / 10, "cpu_iters": CPU_ITERS}, f)
        os.replace(tmp, CONFIG_PATH)
        time.sleep(period)


def main() -> None:
    echo = subprocess.Popen([sys.executable, os.path.join(HERE, "echo_server.py"), "9000", "4", ECHO_DELAY])
    threading.Thread(target=config_writer, args=(2.0,), daemon=True).start()

    def stop(*_: object) -> None:
        echo.terminate()
        os._exit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print(f"backends ready: db 9000-9003 (delay {ECHO_DELAY}s), stream 9100, config.json every 2s (cpu_iters={CPU_ITERS})", flush=True)
    asyncio.run(stream_server(9100))


if __name__ == "__main__":
    main()

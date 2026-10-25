"""PROTOTYPE. Fixed-size (64B) echo server, K forked asyncio processes on ports base..base+K-1.
Optional per-request delay emulates a database. Run with the regular (GIL) build.

    python echo_server.py BASE_PORT COUNT [DELAY_SECONDS]
"""

import asyncio
import contextlib
import functools
import os
import signal
import sys

from backend_protocol import DB_MSG_SIZE

BACKLOG = 4096
DELAY_ARG = 3  # index of the optional DELAY_SECONDS argument


async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, delay: float) -> None:
    """Answer every DB_MSG_SIZE-byte query with the same bytes after ``delay`` seconds."""
    with contextlib.closing(writer), contextlib.suppress(ConnectionError):
        while True:
            try:
                query = await reader.readexactly(DB_MSG_SIZE)
            except asyncio.IncompleteReadError:
                return  # the client closed the connection
            if delay:
                await asyncio.sleep(delay)
            writer.write(query)
            await writer.drain()


async def serve(port: int, delay: float) -> None:
    """Run one echo server on ``port`` forever."""
    on_connect = functools.partial(echo, delay=delay)
    host = os.environ.get("BIND_HOST", "127.0.0.1")
    server = await asyncio.start_server(on_connect, host, port, backlog=BACKLOG)
    async with server:
        await server.serve_forever()


def _stop(pids: list[int], *_: object) -> None:
    for pid in pids:
        os.kill(pid, signal.SIGTERM)
    os._exit(0)


def _supervise(pids: list[int]) -> None:
    stop = functools.partial(_stop, pids)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    for pid in pids:
        os.waitpid(pid, 0)


def _fork_servers(ports: range, delay: float) -> list[int]:
    pids = []
    for port in ports:
        pid = os.fork()
        if pid == 0:
            asyncio.run(serve(port, delay))
            os._exit(0)
        pids.append(pid)
    return pids


def _parse_args(argv: list[str]) -> tuple[range, float]:
    base = int(argv[1])
    ports = range(base, base + int(argv[2]))
    delay = argv[DELAY_ARG] if len(argv) > DELAY_ARG else "0"
    return ports, float(delay)


def main() -> None:
    """Fork the echo processes and wait for them."""
    ports, delay = _parse_args(sys.argv)
    pids = _fork_servers(ports, delay)
    last_port = ports.stop - 1
    sys.stdout.write(f"echo ready ports={ports.start}..{last_port} delay={delay}\n")
    sys.stdout.flush()
    _supervise(pids)


if __name__ == "__main__":
    main()

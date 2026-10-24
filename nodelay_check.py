"""PROTOTYPE. TCP_NODELAY on accepted sockets: start_server(host) vs create_server(host) vs create_server(sock=)."""

from __future__ import annotations

import asyncio
import socket
import sys
from collections.abc import Awaitable, Callable

import uvloop

HOST = "127.0.0.1"
SETTLE = 0.05  # seconds for the server side to see the connection
LISTEN_BACKLOG = 8
RESULT_LINE = "  {name:32} accepted-socket TCP_NODELAY = {nodelay}\n"

Started = tuple[asyncio.Server, int]  # server and its port


class NodelayProbe(asyncio.Protocol):
    """Records TCP_NODELAY of the server side of the first accepted connection, then closes it."""

    def __init__(self) -> None:
        """Nothing seen yet."""
        self.nodelay: int | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Protocol callback (create_server)."""
        self.nodelay = _nodelay(transport.get_extra_info("socket"))
        transport.close()

    async def on_stream(self, _reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Stream callback (start_server)."""
        self.nodelay = _nodelay(writer.get_extra_info("socket"))
        writer.close()

    async def start_server_host(self) -> Started:
        """asyncio.start_server(host, port)."""
        server = await asyncio.start_server(self.on_stream, HOST, 0)
        return server, server.sockets[0].getsockname()[1]

    async def create_server_host(self) -> Started:
        """loop.create_server(host, port)."""
        server = await asyncio.get_running_loop().create_server(self._factory, HOST, 0)
        return server, server.sockets[0].getsockname()[1]

    async def create_server_sock(self) -> Started:
        """loop.create_server(sock=...) with a listening socket made by hand."""
        listening = socket.socket()
        listening.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listening.bind((HOST, 0))
        listening.listen(LISTEN_BACKLOG)
        server = await asyncio.get_running_loop().create_server(self._factory, sock=listening)
        return server, listening.getsockname()[1]

    def _factory(self) -> NodelayProbe:
        return self


def _nodelay(sock: socket.socket) -> int:
    return sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)


async def probe(name: str, start: Callable[[NodelayProbe], Awaitable[Started]]) -> None:
    """Start a server the ``start`` way, connect once and print what the accepted socket had."""
    observer = NodelayProbe()
    server, port = await start(observer)
    _reader, writer = await asyncio.open_connection(HOST, port)
    await asyncio.sleep(SETTLE)
    writer.close()
    server.close()
    sys.stdout.write(RESULT_LINE.format(name=name, nodelay=observer.nodelay))


async def probe_all() -> None:
    """The three ways to start a server, on the running event loop."""
    await probe("start_server(host)", NodelayProbe.start_server_host)
    await probe("create_server(host)", NodelayProbe.create_server_host)
    await probe("create_server(sock=)", NodelayProbe.create_server_sock)


def main() -> None:
    """Probe the stock asyncio event loop, then uvloop."""
    python_version = sys.version.split()[0]
    sys.stdout.write(f"asyncio SelectorEventLoop {python_version}\n")
    asyncio.run(probe_all())
    sys.stdout.write("uvloop\n")
    uvloop.run(probe_all())


if __name__ == "__main__":
    main()

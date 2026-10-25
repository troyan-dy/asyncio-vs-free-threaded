"""PROTOTYPE. Flask-like sync micro-framework over a pluggable runtime (rt_threads.ThreadRuntime). One API for handlers AND background work:

    app = App(rt)

    @app.every(1.0)          # periodic background task
    def reload_config(): ...

    @app.daemon              # long-running background loop (stream consumer)
    def consume_stream(): ...

    @app.get("/quote")       # request handler: plain sync function
    def quote(req) -> bytes: ...

Background tasks and handlers are ordinary `def`s; the runtime runs each task in an OS thread. Shared state is published by attribute assignment of immutable snapshots.
Background loops run until ``app.stopping`` is set.
"""

from __future__ import annotations

import contextlib
import functools
import json
import threading
import traceback
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs

Route = Callable[["Request"], bytes]
BackgroundTask = Callable[[], None]
DAEMON_RESTART_PAUSE = 0.1  # seconds before a failed daemon task is restarted
RECV_CHUNK = 65536
HEAD_END = b"\r\n\r\n"
NOT_FOUND = (b"404 Not Found", b'{"error":"not found"}')


@dataclass
class Request:
    """What a handler gets: the path and the query string as a flat dict."""

    path: str
    query: dict[str, str]


class Pool:
    """Connection pool on top of the runtime's cooperative/blocking queue. rotate() recreates
    every connection without dropping in-flight requests."""

    def __init__(self, rt: Any, addr: tuple[str, int], size: int) -> None:
        """Open ``size`` connections to ``addr``."""
        self.rt = rt
        self.addr = addr
        self.size = size
        self.idle = rt.queue(size)
        self.generation = 0
        for _ in range(size):
            self.idle.put(self._connect())

    @contextlib.contextmanager
    def acquire(self) -> Iterator[Any]:
        """Borrow a connection; a connection that saw an exception is replaced with a new one."""
        conn = self.idle.get()
        try:
            yield conn
        except BaseException:
            conn.close()
            conn = self._connect()
            raise
        finally:
            self.idle.put(conn)

    def rotate(self) -> None:
        """Replace every connection, one at a time, and bump the generation."""
        for _ in range(self.size):
            conn = self.idle.get()
            conn.close()
            self.idle.put(self._connect())
        self.generation += 1

    def _connect(self) -> Any:
        conn = self.rt.socket()
        conn.connect(self.addr)
        return conn


class App:
    """Routes, background tasks and startup hooks; run() serves HTTP/1.1 keep-alive."""

    def __init__(self, rt: Any) -> None:
        """Create an empty app on the runtime ``rt``."""
        self.rt = rt
        self.routes: dict[str, Route] = {}
        self.background: list[BackgroundTask] = []
        self.on_start: list[BackgroundTask] = []
        self.stopping = threading.Event()

    # ---- decorators ----
    def get(self, path: str) -> Callable[[Route], Route]:
        """Register a GET handler for ``path``."""
        def decorator(fn: Route) -> Route:
            self.routes[path] = fn
            return fn

        return decorator

    def every(self, seconds: float) -> Callable[[BackgroundTask], BackgroundTask]:
        """Register a periodic background task: run it, then sleep ``seconds``."""
        def decorator(fn: BackgroundTask) -> BackgroundTask:
            self.background.append(functools.partial(_run_every, self, fn, seconds))
            return fn

        return decorator

    def daemon(self, fn: BackgroundTask) -> BackgroundTask:
        """Register a long-running background task, restarted after a failure."""
        self.background.append(functools.partial(_run_daemon, self, fn))
        return fn

    def startup(self, fn: BackgroundTask) -> BackgroundTask:
        """Register a function called once before serving."""
        self.on_start.append(fn)
        return fn

    # ---- server ----
    def run(self, host: str, port: int) -> None:
        """Blocks the calling task. Must be called inside rt.go(...)."""
        for start in self.on_start:
            start()
        for loop in self.background:
            self.rt.go(loop)
        listener = self.rt.socket().listen((host, port))
        while not self.stopping.is_set():
            conn = listener.accept()
            self.rt.go(self._serve_conn, conn)

    def _serve_conn(self, conn: Any) -> None:
        _HttpConnection(conn, self.routes).serve()


class _HttpConnection:
    """One keep-alive client connection of App.run()."""

    def __init__(self, conn: Any, routes: dict[str, Route]) -> None:
        self._conn = conn
        self._routes = routes
        self._buf = b""

    def serve(self) -> None:
        with contextlib.closing(self._conn), contextlib.suppress(ConnectionError, OSError):
            head = self._read_head()
            while head is not None:
                self._conn.send_all(self._respond(head))
                head = self._read_head()

    def _read_head(self) -> bytes | None:
        """Read up to the end of the next request head; None when the client closed the connection."""
        while HEAD_END not in self._buf:
            chunk = self._conn.recv_some(RECV_CHUNK)
            if not chunk:
                return None
            self._buf += chunk
        head, _, rest = self._buf.partition(HEAD_END)
        self._buf = rest
        return head

    def _respond(self, head: bytes) -> bytes:
        """Run the handler for the request ``head`` and build the whole HTTP response."""
        path, _, query_string = _request_target(head).partition("?")
        status, body = self._call(path, query_string)
        return b"".join((
            b"HTTP/1.1 ", status, b"\r\nContent-Type: application/json\r\nContent-Length: ",
            str(len(body)).encode(), b"\r\nConnection: keep-alive\r\n\r\n", body,
        ))

    def _call(self, path: str, query_string: str) -> tuple[bytes, bytes]:
        route = self._routes.get(path)
        if route is None:
            return NOT_FOUND
        fields = parse_qs(query_string)
        query = {name: field_values[0] for name, field_values in fields.items()}
        try:
            return b"200 OK", route(Request(path, query))
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            return b"500 Internal Server Error", json.dumps({"error": repr(exc)}).encode()


def _request_target(head: bytes) -> str:
    request_line = head.split(b"\r\n", 1)[0]
    _method, target, _version = request_line.split(b" ", 2)
    return target.decode()


def _run_every(app: App, fn: BackgroundTask, seconds: float) -> None:
    while not app.stopping.is_set():
        try:
            fn()
        except Exception:  # noqa: BLE001
            traceback.print_exc()
        app.rt.sleep(seconds)


def _run_daemon(app: App, fn: BackgroundTask) -> None:
    while not app.stopping.is_set():
        try:
            fn()
        except Exception:  # noqa: BLE001
            traceback.print_exc()
            app.rt.sleep(DAEMON_RESTART_PAUSE)

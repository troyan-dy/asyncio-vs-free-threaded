"""PROTOTYPE. Flask-like sync micro-framework over a pluggable runtime (gort.GreenletRuntime or
rt_threads.ThreadRuntime). One API for handlers AND background work:

    app = App(rt)

    @app.every(1.0)          # periodic background task
    def reload_config(): ...

    @app.daemon              # long-running background loop (stream consumer)
    def consume_stream(): ...

    @app.get("/quote")       # request handler: plain sync function
    def quote(req) -> bytes: ...

Background tasks and handlers are ordinary `def`s; the runtime decides whether a task is a greenlet
on a hub or an OS thread. Shared state is published by attribute assignment of immutable snapshots.
"""

from __future__ import annotations

import json
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator
from urllib.parse import parse_qs


@dataclass
class Request:
    path: str
    query: dict[str, str]


class Pool:
    """Connection pool on top of the runtime's cooperative/blocking queue. rotate() recreates
    every connection without dropping in-flight requests."""

    def __init__(self, rt: Any, addr: tuple[str, int], size: int) -> None:
        self.rt, self.addr, self.size = rt, addr, size
        self.q = rt.queue(size)
        self.generation = 0
        for _ in range(size):
            self.q.put(self._connect())

    def _connect(self) -> Any:
        s = self.rt.socket()
        s.connect(self.addr)
        return s

    @contextmanager
    def acquire(self) -> Iterator[Any]:
        conn = self.q.get()
        try:
            yield conn
        except BaseException:
            conn.close()
            conn = self._connect()
            raise
        finally:
            self.q.put(conn)

    def rotate(self) -> None:
        for _ in range(self.size):
            conn = self.q.get()
            conn.close()
            self.q.put(self._connect())
        self.generation += 1


class App:
    def __init__(self, rt: Any) -> None:
        self.rt = rt
        self.routes: dict[str, Callable[[Request], bytes]] = {}
        self.background: list[Callable[[], None]] = []
        self.on_start: list[Callable[[], None]] = []

    # ---- decorators ----
    def get(self, path: str) -> Callable:
        def deco(fn: Callable[[Request], bytes]) -> Callable[[Request], bytes]:
            self.routes[path] = fn
            return fn

        return deco

    def every(self, seconds: float) -> Callable:
        def deco(fn: Callable[[], None]) -> Callable[[], None]:
            def loop() -> None:
                while True:
                    try:
                        fn()
                    except Exception:  # noqa: BLE001
                        traceback.print_exc()
                    self.rt.sleep(seconds)

            self.background.append(loop)
            return fn

        return deco

    def daemon(self, fn: Callable[[], None]) -> Callable[[], None]:
        def loop() -> None:
            while True:
                try:
                    fn()
                except Exception:  # noqa: BLE001
                    traceback.print_exc()
                    self.rt.sleep(0.1)

        self.background.append(loop)
        return fn

    def startup(self, fn: Callable[[], None]) -> Callable[[], None]:
        self.on_start.append(fn)
        return fn

    # ---- server ----
    def run(self, host: str, port: int) -> None:
        """Blocks the calling task. Must be called inside rt.go(...)."""
        for fn in self.on_start:
            fn()
        for loop in self.background:
            self.rt.go(loop)
        ls = self.rt.socket()
        ls.listen((host, port))
        while True:
            conn = ls.accept()
            self.rt.go(self._serve_conn, conn)

    def _serve_conn(self, conn: Any) -> None:
        buf = b""
        try:
            while True:
                while b"\r\n\r\n" not in buf:
                    chunk = conn.recv_some(65536)
                    if not chunk:
                        return
                    buf += chunk
                head, _, buf = buf.partition(b"\r\n\r\n")
                line = head.split(b"\r\n", 1)[0]
                _method, target, _ver = line.split(b" ", 2)
                path, _, qs = target.decode().partition("?")
                handler = self.routes.get(path)
                if handler is None:
                    status, body = b"404 Not Found", b'{"error":"not found"}'
                else:
                    try:
                        status, body = b"200 OK", handler(Request(path, {k: v[0] for k, v in parse_qs(qs).items()}))
                    except Exception as e:  # noqa: BLE001
                        traceback.print_exc()
                        status, body = b"500 Internal Server Error", json.dumps({"error": repr(e)}).encode()
                conn.send_all(
                    b"HTTP/1.1 " + status + b"\r\nContent-Type: application/json\r\nContent-Length: "
                    + str(len(body)).encode() + b"\r\nConnection: keep-alive\r\n\r\n" + body
                )
        except (ConnectionError, OSError):
            pass
        finally:
            conn.close()

"""PROTOTYPE. Runtime for the sync variant: plain OS threads and blocking sockets."""

from __future__ import annotations

import queue
import socket
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable


class BlockingSocket:
    def __init__(self, sock: socket.socket | None = None) -> None:
        self.sock = sock or socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def connect(self, addr: tuple[str, int]) -> None:
        self.sock.connect(addr)

    def send_all(self, data: bytes) -> None:
        self.sock.sendall(data)

    def recv_some(self, n: int) -> bytes:
        return self.sock.recv(n)

    def recv_exactly(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("closed")
            buf += chunk
        return bytes(buf)

    def listen(self, addr: tuple[str, int], backlog: int = 1024) -> None:
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(addr)
        self.sock.listen(backlog)

    def accept(self) -> "BlockingSocket":
        conn, _ = self.sock.accept()
        return BlockingSocket(conn)

    def close(self) -> None:
        self.sock.close()


class _Result:
    def __init__(self, fut: Future) -> None:
        self.fut = fut

    def get(self) -> Any:
        return self.fut.result()


class ThreadQueue(queue.Queue):
    """queue.Queue with a no-op close()."""

    def close(self) -> None:
        pass


class ThreadRuntime:
    name = "threads"

    def __init__(self, n_hubs: int = 0, blocking_threads: int = 32) -> None:
        # n_hubs ignored: one OS thread per task, unbounded, Java-style.
        self.pool = ThreadPoolExecutor(blocking_threads, thread_name_prefix="blocking")

    def go(self, fn: Callable, *args: Any) -> _Result:
        fut: Future = Future()

        def run() -> None:
            try:
                fut.set_result(fn(*args))
            except BaseException as e:  # noqa: BLE001
                fut.set_exception(e)

        threading.Thread(target=run, daemon=True).start()
        return _Result(fut)

    def spawn_blocking(self, fn: Callable, *args: Any) -> Any:
        return self.pool.submit(fn, *args).result()

    def sleep(self, dt: float) -> None:
        time.sleep(dt)

    def socket(self) -> BlockingSocket:
        return BlockingSocket()

    def queue(self, maxsize: int) -> ThreadQueue:
        return ThreadQueue(maxsize)

    def shutdown(self) -> None:
        self.pool.shutdown(wait=False)

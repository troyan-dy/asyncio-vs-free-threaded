"""PROTOTYPE. Runtime for the sync variant: plain OS threads and blocking sockets."""

from __future__ import annotations

import queue
import socket
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable

LISTEN_BACKLOG = 1024
Task = Callable[..., Any]


class BlockingSocket:
    """TCP socket with TCP_NODELAY and the small API the framework needs."""

    def __init__(self, sock: socket.socket | None = None) -> None:
        """Wrap ``sock`` or a new TCP socket."""
        self.sock = sock or socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def connect(self, addr: tuple[str, int]) -> None:
        """Connect to ``addr``."""
        self.sock.connect(addr)

    def send_all(self, payload: bytes) -> None:
        """Send the whole ``payload``."""
        self.sock.sendall(payload)

    def recv_some(self, max_size: int) -> bytes:
        """Receive up to ``max_size`` bytes; b"" means the peer closed the connection."""
        return self.sock.recv(max_size)

    def recv_exactly(self, size: int) -> bytes:
        """Receive exactly ``size`` bytes or raise ConnectionError."""
        buf = bytearray()
        while len(buf) < size:
            chunk = self.sock.recv(size - len(buf))
            if not chunk:
                raise ConnectionError("closed")
            buf += chunk
        return bytes(buf)

    def listen(self, addr: tuple[str, int], backlog: int = LISTEN_BACKLOG) -> BlockingListener:
        """Bind to ``addr`` and start listening."""
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(addr)
        self.sock.listen(backlog)
        return BlockingListener(self.sock)

    def close(self) -> None:
        """Close the socket."""
        self.sock.close()


class BlockingListener:
    """Listening socket returned by BlockingSocket.listen()."""

    def __init__(self, sock: socket.socket) -> None:
        """Wrap a listening socket."""
        self.sock = sock

    def accept(self) -> BlockingSocket:
        """Wait for the next connection."""
        conn, _ = self.sock.accept()
        return BlockingSocket(conn)

    def close(self) -> None:
        """Close the socket."""
        self.sock.close()


class TaskHandle:
    """Handle of a task started with ThreadRuntime.go()."""

    def __init__(self, fut: Future[Any]) -> None:
        """Wrap the future that receives the task outcome."""
        self.fut = fut

    def get(self) -> Any:
        """Wait for the task and return its value (or raise its exception)."""
        return self.fut.result()


class ThreadQueue(queue.Queue):
    """queue.Queue with a no-op close()."""

    def close(self) -> None:
        """Do nothing: a thread queue holds no resources."""


class ThreadRuntime:
    """One OS thread per task, unbounded, Java-style."""

    name = "threads"

    def __init__(self, n_hubs: int = 0, blocking_threads: int = 32) -> None:
        """Create the runtime; ``n_hubs`` is ignored (kept for the runtime interface)."""
        self.pool = ThreadPoolExecutor(blocking_threads, thread_name_prefix="blocking")

    def go(self, fn: Task, *args: Any) -> TaskHandle:
        """Run ``fn(*args)`` in a new daemon thread."""
        fut: Future[Any] = Future()
        thread = threading.Thread(target=_run_into, args=(fut, fn, args), daemon=True)
        thread.start()
        return TaskHandle(fut)

    def spawn_blocking(self, fn: Task, *args: Any) -> Any:
        """Run ``fn(*args)`` in the blocking pool and wait for it."""
        return self.pool.submit(fn, *args).result()

    def sleep(self, seconds: float) -> None:
        """Block the calling thread."""
        time.sleep(seconds)

    def socket(self) -> BlockingSocket:
        """Create a TCP socket."""
        return BlockingSocket()

    def queue(self, maxsize: int) -> ThreadQueue:
        """Create a bounded blocking queue."""
        return ThreadQueue(maxsize)

    def shutdown(self) -> None:
        """Stop the blocking pool without waiting."""
        self.pool.shutdown(wait=False)


def _run_into(fut: Future[Any], fn: Task, args: tuple[Any, ...]) -> None:
    try:
        fut.set_result(fn(*args))
    except BaseException as exc:  # noqa: BLE001
        fut.set_exception(exc)

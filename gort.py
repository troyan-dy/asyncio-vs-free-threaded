"""PROTOTYPE, throwaway. Go-like runtime on greenlet: N OS threads, hub per thread,
cooperative sockets, go()/Chan/spawn_blocking. Tasks are pinned to the hub that spawned them.
"""

from __future__ import annotations

import errno
import heapq
import itertools
import os
import selectors
import socket
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

import greenlet

_local = threading.local()
_seq = itertools.count()


def current_hub() -> "Hub | None":
    return getattr(_local, "hub", None)


class Hub:
    def __init__(self, idx: int) -> None:
        self.idx = idx
        self.ready: deque[tuple[greenlet.greenlet, Any]] = deque()
        self.inbox: deque[Callable[[], None]] = deque()
        self.inbox_lock = threading.Lock()
        self.sel = selectors.DefaultSelector()
        self.timers: list[tuple[float, int, greenlet.greenlet]] = []
        self.r, self.w = os.pipe()
        os.set_blocking(self.r, False)
        os.set_blocking(self.w, False)
        self.sel.register(self.r, selectors.EVENT_READ, None)
        self.root: greenlet.greenlet | None = None
        self.stopping = False
        self.tasks = 0
        self.thread = threading.Thread(target=self._run, name=f"hub-{idx}", daemon=True)
        self.started = threading.Event()

    # ---- thread-safe API ----
    def call_threadsafe(self, fn: Callable[[], None]) -> None:
        with self.inbox_lock:
            self.inbox.append(fn)
        try:
            os.write(self.w, b"x")
        except BlockingIOError:
            pass

    def wake(self, gr: greenlet.greenlet, value: Any = None) -> None:
        """Resume a parked greenlet owned by this hub, from any thread."""
        if current_hub() is self:
            self.ready.append((gr, value))
        else:
            self.call_threadsafe(lambda: self.ready.append((gr, value)))

    def spawn(self, fn: Callable, args: tuple, res: "Result") -> None:
        def run(_first_switch_value: Any = None) -> None:
            self.tasks += 1
            try:
                res.set(fn(*args))
            except BaseException as e:  # noqa: BLE001
                res.set_exc(e)
            finally:
                self.tasks -= 1

        self.call_threadsafe(lambda: self.ready.append((greenlet.greenlet(run), None)))

    # ---- hub-thread API ----
    def park(self) -> Any:
        assert self.root is not None
        return self.root.switch()

    def wait_fd(self, fd: int, events: int) -> None:
        gr = greenlet.getcurrent()
        self.sel.register(fd, events, gr)
        try:
            self.park()
        finally:
            self.sel.unregister(fd)

    def sleep(self, dt: float) -> None:
        heapq.heappush(self.timers, (time.monotonic() + dt, next(_seq), greenlet.getcurrent()))
        self.park()

    def _run(self) -> None:
        _local.hub = self
        self.root = greenlet.getcurrent()
        self.started.set()
        while not self.stopping:
            if self.inbox:
                with self.inbox_lock:
                    batch = list(self.inbox)
                    self.inbox.clear()
                for fn in batch:
                    fn()
            while self.ready:
                gr, value = self.ready.popleft()
                gr.switch(value)
            now = time.monotonic()
            while self.timers and self.timers[0][0] <= now:
                _, _, gr = heapq.heappop(self.timers)
                self.ready.append((gr, None))
            if self.ready or self.inbox:
                timeout = 0.0
            elif self.timers:
                timeout = max(0.0, self.timers[0][0] - now)
            else:
                timeout = None
            for key, _mask in self.sel.select(timeout):
                if key.fd == self.r:
                    try:
                        os.read(self.r, 4096)
                    except BlockingIOError:
                        pass
                else:
                    self.ready.append((key.data, None))


class Result:
    """One-shot value; get() parks a greenlet or blocks a plain thread."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.done = False
        self.value: Any = None
        self.exc: BaseException | None = None
        self.waiters: list[tuple[Hub, greenlet.greenlet]] = []
        self.event: threading.Event | None = None

    def set(self, value: Any) -> None:
        self._finish(value, None)

    def set_exc(self, exc: BaseException) -> None:
        self._finish(None, exc)

    def _finish(self, value: Any, exc: BaseException | None) -> None:
        with self.lock:
            self.done = True
            self.value, self.exc = value, exc
            waiters, self.waiters = self.waiters, []
            ev = self.event
        for hub, gr in waiters:
            hub.wake(gr)
        if ev is not None:
            ev.set()

    def get(self) -> Any:
        hub = current_hub()
        with self.lock:
            if not self.done:
                if hub is None:
                    self.event = self.event or threading.Event()
                    ev = self.event
                else:
                    self.waiters.append((hub, greenlet.getcurrent()))
        if not self.done:
            if hub is None:
                ev.wait()
            else:
                hub.park()
        if self.exc is not None:
            raise self.exc
        return self.value


class Chan:
    """Buffered channel, safe across hubs/threads."""

    def __init__(self, maxsize: int = 1) -> None:
        self.maxsize = maxsize
        self.lock = threading.Lock()
        self.items: deque[Any] = deque()
        self.getters: deque[tuple[Hub, greenlet.greenlet]] = deque()
        self.putters: deque[tuple[Hub, greenlet.greenlet]] = deque()
        self.closed = False

    def put(self, item: Any) -> None:
        hub = current_hub()
        assert hub is not None
        while True:
            with self.lock:
                if len(self.items) < self.maxsize:
                    self.items.append(item)
                    if self.getters:
                        h, g = self.getters.popleft()
                        h.wake(g)
                    return
                self.putters.append((hub, greenlet.getcurrent()))
            hub.park()

    def get(self) -> Any:
        hub = current_hub()
        assert hub is not None
        while True:
            with self.lock:
                if self.items:
                    item = self.items.popleft()
                    if self.putters:
                        h, g = self.putters.popleft()
                        h.wake(g)
                    return item
                if self.closed:
                    raise StopIteration
                self.getters.append((hub, greenlet.getcurrent()))
            hub.park()

    def close(self) -> None:
        with self.lock:
            self.closed = True
            getters, self.getters = self.getters, deque()
        for h, g in getters:
            h.wake(g)

    def __iter__(self):
        while True:
            try:
                yield self.get()
            except StopIteration:
                return


class Runtime:
    def __init__(self, n_hubs: int, blocking_threads: int = 32) -> None:
        self.hubs = [Hub(i) for i in range(n_hubs)]
        self._rr = itertools.count()
        self.pool = ThreadPoolExecutor(blocking_threads, thread_name_prefix="blocking")
        for h in self.hubs:
            h.thread.start()
        for h in self.hubs:
            h.started.wait()
        _runtime[0] = self

    def go(self, fn: Callable, *args: Any) -> Result:
        res = Result()
        hub = self.hubs[next(self._rr) % len(self.hubs)]
        hub.spawn(fn, args, res)
        return res

    def spawn_blocking(self, fn: Callable, *args: Any) -> Any:
        hub = current_hub()
        assert hub is not None
        gr = greenlet.getcurrent()
        fut = self.pool.submit(fn, *args)
        fut.add_done_callback(lambda _f: hub.wake(gr))
        hub.park()
        return fut.result()

    def shutdown(self) -> None:
        for h in self.hubs:
            h.stopping = True
            h.call_threadsafe(lambda: None)
        self.pool.shutdown(wait=False)


_runtime: list[Runtime] = [None]  # type: ignore[list-item]


def go(fn: Callable, *args: Any) -> Result:
    return _runtime[0].go(fn, *args)


def spawn_blocking(fn: Callable, *args: Any) -> Any:
    return _runtime[0].spawn_blocking(fn, *args)


def sleep(dt: float) -> None:
    hub = current_hub()
    assert hub is not None
    hub.sleep(dt)


class CoopSocket:
    """Minimal cooperative TCP socket: parks the greenlet instead of blocking the thread."""

    def __init__(self, sock: socket.socket | None = None) -> None:
        self.sock = sock or socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setblocking(False)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def _wait(self, events: int) -> None:
        # Hub is resolved per wait, not per socket: a connection may be used from any hub
        # (one greenlet at a time), which is what lets a pool be shared across threads.
        hub = current_hub()
        assert hub is not None, "cooperative socket used outside a hub"
        hub.wait_fd(self.sock.fileno(), events)

    def connect(self, addr: tuple[str, int]) -> None:
        err = self.sock.connect_ex(addr)
        if err in (0, errno.EISCONN):
            return
        if err not in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EAGAIN):
            raise OSError(err, os.strerror(err))
        self._wait(selectors.EVENT_WRITE)
        err = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if err:
            raise OSError(err, os.strerror(err))

    def send_all(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            try:
                n = self.sock.send(view)
            except BlockingIOError:
                self._wait(selectors.EVENT_WRITE)
                continue
            view = view[n:]

    def recv_some(self, n: int) -> bytes:
        """Up to n bytes; b'' on EOF."""
        while True:
            try:
                return self.sock.recv(n)
            except BlockingIOError:
                self._wait(selectors.EVENT_READ)

    def recv_exactly(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self.recv_some(n - len(buf))
            if not chunk:
                raise ConnectionError("closed")
            buf += chunk
        return bytes(buf)

    def listen(self, addr: tuple[str, int], backlog: int = 1024) -> None:
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(addr)
        self.sock.listen(backlog)

    def accept(self) -> "CoopSocket":
        while True:
            try:
                conn, _ = self.sock.accept()
                return CoopSocket(conn)
            except BlockingIOError:
                self._wait(selectors.EVENT_READ)

    def close(self) -> None:
        self.sock.close()


class GreenletRuntime(Runtime):
    """Runtime facade with the interface shared by rt_threads.ThreadRuntime."""

    name = "greenlet"

    def sleep(self, dt: float) -> None:
        sleep(dt)

    def socket(self) -> CoopSocket:
        return CoopSocket()

    def queue(self, maxsize: int) -> Chan:
        return Chan(maxsize)

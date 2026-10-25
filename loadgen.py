"""PROTOTYPE. HTTP load generator: C keep-alive connections for D seconds, reports rps, latency
percentiles and state freshness (did cfg_version / stream_seq / pool_gen advance during the run?).
Run it with the regular build in a separate process.
python loadgen.py --port 8080 --conc 64 --duration 10 --label "ft async 4 loops"
"""

import argparse
import contextlib
import json
import socket
import sys
import threading
import time

from loadgen_report import Sample, summarize

DEFAULT_PORT = 8080
DEFAULT_CONC = 64
RECV_CHUNK = 65536
HEAD_END = b"\r\n\r\n"
SYMBOLS = 10  # connection i asks for sym = i % SYMBOLS
SAMPLE_FIRST = 2  # responses kept for the freshness check: the first ones ...
SAMPLE_EVERY = 50  # ... and then every SAMPLE_EVERY-th
RECONNECT_PAUSE = 0.05  # seconds
REQUEST_ERRORS = (ConnectionError, OSError, json.JSONDecodeError)

Addr = tuple[str, int]


def read_response(sock: socket.socket, buf: bytes) -> tuple[bytes, bytes, bool]:  # noqa: WPS210, WPS231 - hot path
    """Read one response: (body, bytes after it, server asked to close). Inline on purpose: once per request."""
    while HEAD_END not in buf:
        chunk = sock.recv(RECV_CHUNK)
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
    head, _, rest = buf.partition(HEAD_END)
    headers = head.lower()
    length = 0
    close = b"connection: close" in headers
    for line in headers.split(b"\r\n"):
        if line.startswith(b"content-length:"):
            length = int(line.split(b":")[1])
    while len(rest) < length:
        chunk = sock.recv(RECV_CHUNK)
        if not chunk:
            raise ConnectionError("closed")
        rest += chunk
    return rest[:length], rest[length:], close


def _reopen(sock: socket.socket, addr: Addr) -> socket.socket:
    """Replace a broken connection; an unconnected socket if connecting fails too."""
    with contextlib.suppress(OSError):
        sock.close()
    try:
        return socket.create_connection(addr)
    except OSError:
        time.sleep(RECONNECT_PAUSE)  # e.g. ephemeral port exhaustion; keep the thread alive
        return socket.socket()


class Client:
    """One keep-alive connection that sends the same GET until the deadline."""

    def __init__(self, idx: int, addr: Addr, path: str) -> None:
        """Prepare connection ``idx``; it asks for sym = idx % SYMBOLS."""
        sym = idx % SYMBOLS
        self.addr = addr
        self.request = f"GET {path}&sym={sym} HTTP/1.1\r\nHost: x\r\n\r\n".encode()
        self.latencies: list[float] = []
        self.samples: list[Sample] = []
        self.errors = 0

    def run(self, deadline: float) -> None:
        """Thread body: send requests until ``deadline`` (perf_counter), reconnect after a failure."""
        self.sock = socket.create_connection(self.addr)
        while time.perf_counter() < deadline:
            try:
                self._requests_until_error(deadline)
            except REQUEST_ERRORS:
                self.errors += 1
                self.sock = _reopen(self.sock, self.addr)
        self.sock.close()

    def _requests_until_error(self, deadline: float) -> None:  # noqa: WPS210 - hot path: locals, not attributes
        """The measured loop: per request only the socket I/O and the bookkeeping."""
        sock, request = self.sock, self.request
        latencies, samples = self.latencies, self.samples
        buf = b""
        while time.perf_counter() < deadline:
            started = time.perf_counter()
            sock.sendall(request)
            body, buf, close = read_response(sock, buf)
            latencies.append(time.perf_counter() - started)
            if len(samples) < SAMPLE_FIRST or len(latencies) % SAMPLE_EVERY == 0:
                samples.append(json.loads(body))
            if close:
                sock.close()
                sock = socket.create_connection(self.addr)
                self.sock = sock
                buf = b""


def _run(clients: list[Client], duration: float) -> float:
    """Run every client in its own thread for ``duration`` seconds; return the elapsed time."""
    deadline = time.perf_counter() + duration
    threads = [threading.Thread(target=client.run, args=(deadline,)) for client in clients]
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return time.perf_counter() - started


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--conc", type=int, default=DEFAULT_CONC)
    parser.add_argument("--duration", type=float, default=10)
    parser.add_argument("--path", default="/quote?x=1")
    parser.add_argument("--label", default="")
    return parser.parse_args()


def main() -> None:
    """Run the load, print one JSON line, exit 1 if any request failed."""
    args = _parse_args()
    addr = (args.host, args.port)
    clients = [Client(idx, addr, args.path) for idx in range(args.conc)]
    elapsed = _run(clients, args.duration)
    summary = summarize(args.label, args.conc, clients, elapsed)
    sys.stdout.write(f"{json.dumps(summary)}\n")
    sys.stdout.flush()
    sys.exit(1 if summary["errors"] else 0)


if __name__ == "__main__":
    main()

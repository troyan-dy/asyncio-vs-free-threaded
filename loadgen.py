"""PROTOTYPE. HTTP load generator: C keep-alive connections for D seconds, reports rps, latency
percentiles and state freshness (did cfg_version / stream_seq / pool_gen advance during the run?).
Run it with the regular build in a separate process.
python loadgen.py --port 8080 --conc 64 --duration 10 --label "ft async 4 loops"
"""

import argparse
import json
import socket
import sys
import threading
import time


def read_response(sock: socket.socket, buf: bytes) -> tuple[bytes, bytes, bool]:
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
    head, _, rest = buf.partition(b"\r\n\r\n")
    headers = head.lower()
    length = 0
    close = b"connection: close" in headers
    for line in headers.split(b"\r\n"):
        if line.startswith(b"content-length:"):
            length = int(line.split(b":")[1])
    while len(rest) < length:
        chunk = sock.recv(65536)
        if not chunk:
            raise ConnectionError("closed")
        rest += chunk
    return rest[:length], rest[length:], close


HOST = "127.0.0.1"


def worker(idx: int, port: int, path: str, deadline: float, out: dict) -> None:
    lat: list[float] = []
    seen: list[dict] = []
    errors = 0
    req = f"GET {path}&sym={idx % 10} HTTP/1.1\r\nHost: x\r\n\r\n".encode()
    sock = socket.create_connection((HOST, port))
    buf = b""
    while time.perf_counter() < deadline:
        t0 = time.perf_counter()
        try:
            sock.sendall(req)
            body, buf, close = read_response(sock, buf)
            lat.append(time.perf_counter() - t0)
            if len(seen) < 2 or len(lat) % 50 == 0:
                seen.append(json.loads(body))
            if close:
                sock.close()
                sock = socket.create_connection((HOST, port))
                buf = b""
        except (ConnectionError, OSError, json.JSONDecodeError):
            errors += 1
            try:
                sock.close()
            except OSError:
                pass
            try:
                sock = socket.create_connection((HOST, port))
            except OSError:
                time.sleep(0.05)  # e.g. ephemeral port exhaustion; keep the thread alive
                sock = socket.socket()
            buf = b""
    sock.close()
    out[idx] = (lat, seen, errors)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--conc", type=int, default=64)
    p.add_argument("--duration", type=float, default=10)
    p.add_argument("--path", default="/quote?x=1")
    p.add_argument("--label", default="")
    a = p.parse_args()
    global HOST  # noqa: PLW0603
    HOST = a.host

    deadline = time.perf_counter() + a.duration
    out: dict = {}
    ths = [threading.Thread(target=worker, args=(i, a.port, a.path, deadline, out)) for i in range(a.conc)]
    t0 = time.perf_counter()
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    elapsed = time.perf_counter() - t0

    lat = sorted(x for l, _, _ in out.values() for x in l)
    seen = [s for _, ss, _ in out.values() for s in ss]
    errors = sum(e for _, _, e in out.values())

    def pct(q: float) -> float:
        return round(lat[min(len(lat) - 1, int(q * len(lat)))] * 1000, 2) if lat else -1

    res = {
        "label": a.label,
        "conc": a.conc,
        "rps": round(len(lat) / elapsed),
        "p50_ms": pct(0.50),
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
        "max_ms": round(lat[-1] * 1000, 1) if lat else -1,
        "errors": errors,
        "cfg_version": [min(s.get("cfg_version", 0) for s in seen), max(s.get("cfg_version", 0) for s in seen)] if seen else None,
        "stream_seq": [min(s.get("stream_seq", 0) for s in seen), max(s.get("stream_seq", 0) for s in seen)] if seen else None,
        "pool_gen": [min(s.get("pool_gen", 0) for s in seen), max(s.get("pool_gen", 0) for s in seen)] if seen else None,
    }
    print(json.dumps(res), flush=True)
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()

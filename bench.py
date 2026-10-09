"""PROTOTYPE. Benchmark driver.

  python bench.py --mode greenlet|threads --workers N --scenario cpu|io|mixed --conc C --reqs R
                  [--ports 9000 --nports 4] [--cpu-iters 20000]

mode=greenlet : N hubs (gort), C tasks spread round-robin, cooperative sockets.
mode=threads  : plain OS threads with blocking sockets; for cpu scenario the pool has N threads,
                for io/mixed the pool has C threads (one per in-flight request, Java-style).
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

MSG = b"x" * 64


def cpu_work(iters: int) -> int:
    s = 0
    for i in range(iters):
        s += i * i
    return s


# ---------------- greenlet mode ----------------


def gl_task(args: argparse.Namespace, idx: int) -> int:
    import gort

    sock = None
    if args.scenario != "cpu":
        sock = gort.CoopSocket()
        sock.connect(("127.0.0.1", args.ports + idx % args.nports))
    n = 0
    for _ in range(args.reqs):
        if sock is not None:
            sock.send_all(MSG)
            sock.recv_exactly(len(MSG))
        if args.scenario != "io":
            cpu_work(args.cpu_iters)
        n += 1
    if sock is not None:
        sock.close()
    return n


def run_greenlet(args: argparse.Namespace) -> tuple[int, float]:
    import gort

    rt = gort.Runtime(args.workers)
    t0 = time.perf_counter()
    results = [rt.go(gl_task, args, i) for i in range(args.conc)]
    total = sum(r.get() for r in results)
    dt = time.perf_counter() - t0
    rt.shutdown()
    return total, dt


# ---------------- threads mode ----------------


def th_task(args: argparse.Namespace, idx: int) -> int:
    sock = None
    if args.scenario != "cpu":
        sock = socket.create_connection(("127.0.0.1", args.ports + idx % args.nports))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    n = 0
    for _ in range(args.reqs):
        if sock is not None:
            sock.sendall(MSG)
            buf = b""
            while len(buf) < len(MSG):
                chunk = sock.recv(len(MSG) - len(buf))
                if not chunk:
                    raise ConnectionError
                buf += chunk
        if args.scenario != "io":
            cpu_work(args.cpu_iters)
        n += 1
    if sock is not None:
        sock.close()
    return n


def run_threads(args: argparse.Namespace) -> tuple[int, float]:
    pool_size = args.workers if args.scenario == "cpu" else args.conc
    with ThreadPoolExecutor(pool_size) as pool:
        t0 = time.perf_counter()
        total = sum(pool.map(lambda i: th_task(args, i), range(args.conc)))
        dt = time.perf_counter() - t0
    return total, dt


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["greenlet", "threads"], required=True)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--scenario", choices=["cpu", "io", "mixed"], required=True)
    p.add_argument("--conc", type=int, default=100)
    p.add_argument("--reqs", type=int, default=100)
    p.add_argument("--ports", type=int, default=9000)
    p.add_argument("--nports", type=int, default=4)
    p.add_argument("--cpu-iters", type=int, default=20000)
    args = p.parse_args()

    # warm-up of the interpreter / specializer
    cpu_work(1000)

    rss0 = _rss()
    total, dt = (run_greenlet if args.mode == "greenlet" else run_threads)(args)
    out = {
        "build": "ft" if not sys._is_gil_enabled() else "gil",  # noqa: SLF001
        "mode": args.mode,
        "scenario": args.scenario,
        "workers": args.workers,
        "conc": args.conc,
        "reqs": args.reqs,
        "ops": total,
        "sec": round(dt, 3),
        "ops_per_sec": round(total / dt),
        "rss_mb": round((_rss() - rss0) / 1e6, 1),
        "threads": threading.active_count(),
    }
    print(json.dumps(out), flush=True)


def _rss() -> int:
    import resource

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # bytes on macOS


if __name__ == "__main__":
    main()

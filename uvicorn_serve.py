"""PROTOTYPE. Launch fastapi_app under uvicorn in one of two shapes:

  --loops N    N event loops in N threads of ONE process, all accepting on one shared listening socket
               (only makes sense on the free-threaded build)
  --workers N  N worker processes (uvicorn's standard multiprocess mode)

python uvicorn_serve.py --loops 8 --loop uvloop --http httptools
"""

import argparse
import os
import socket
import sys
import threading
import types
from typing import Any

import uvicorn

DEFAULT_PORT = 8080
LISTEN_BACKLOG = 1024
# uvicorn.Config options shared by every server
SERVER_OPTIONS = types.MappingProxyType({"lifespan": "on", "log_level": "warning", "access_log": False})
REUSEPORT = sys.platform == "linux"  # kernel balances connections across SO_REUSEPORT sockets


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--loops", type=int, default=1)
    parser.add_argument("--workers", type=int, default=0)
    parser.add_argument("--loop", default="uvloop")
    parser.add_argument("--http", default="httptools")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser.parse_args()


class Launcher:
    """Runs uvicorn servers with the same ``options``, each listening on ``addr``."""

    def __init__(self, options: dict[str, Any], addr: tuple[str, int]) -> None:
        """Remember the uvicorn options and the listening address."""
        self.options = options
        self.addr = addr

    def run_workers(self, workers: int) -> None:
        """One server per forked worker process; return when all of them have exited."""
        # fork before any event loop exists; each worker owns an SO_REUSEPORT socket
        # (uvicorn's own --workers shares one socket; asyncio's accept loop then grabs whole bursts
        #  of connections into one worker, which skews the comparison)
        for _ in range(workers):
            if os.fork() == 0:
                self.serve()
                os._exit(0)
        for _ in range(workers):
            os.wait()

    def run_loops(self, loops: int) -> None:
        """One server (event loop) per thread of this process."""
        shared = None if REUSEPORT else self.listen()
        threads = [
            threading.Thread(target=self.serve, args=(shared,), name=f"loop-{index}", daemon=True)
            for index in range(loops)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def serve(self, shared: socket.socket | None = None) -> None:
        """Run one uvicorn server on ``shared`` or on its own new socket."""
        from fastapi_app import app  # imported in the worker process (after fork) or in the loop thread

        server = uvicorn.Server(uvicorn.Config(app, **self.options))
        server.run(sockets=[shared or self.listen()])

    def listen(self) -> socket.socket:
        """New listening socket on ``addr`` (SO_REUSEPORT on Linux)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if REUSEPORT:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(self.addr)
        sock.listen(LISTEN_BACKLOG)
        return sock


def main() -> None:
    """Parse the command line and start the workers or the loops."""
    args = _parse_args()
    addr = (os.environ.get("BIND_HOST", "127.0.0.1"), args.port)
    launcher = Launcher({"loop": args.loop, "http": args.http, **SERVER_OPTIONS}, addr)
    shape = f"loops={args.loops} workers={args.workers} {args.loop}/{args.http}"
    sys.stdout.write(f"serving fastapi {shape} gil={sys._is_gil_enabled()}\n")  # noqa: SLF001
    sys.stdout.flush()
    if args.workers:
        launcher.run_workers(args.workers)
    else:
        launcher.run_loops(args.loops)


if __name__ == "__main__":
    main()

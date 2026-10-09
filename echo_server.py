"""PROTOTYPE. Fixed-size (64B) echo server, K forked asyncio processes on ports base..base+K-1.
Optional per-request delay emulates a database. Run with the regular (GIL) build.
"""

import asyncio
import os
import signal
import sys

MSG = 64


async def serve(port: int, delay: float) -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.readexactly(MSG)
                if delay:
                    await asyncio.sleep(delay)
                writer.write(data)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, os.environ.get("BIND_HOST", "127.0.0.1"), port, backlog=4096)
    async with server:
        await server.serve_forever()


def main() -> None:
    base = int(sys.argv[1])
    count = int(sys.argv[2])
    delay = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
    pids = []
    for i in range(count):
        pid = os.fork()
        if pid == 0:
            asyncio.run(serve(base + i, delay))
            os._exit(0)
        pids.append(pid)
    print(f"echo ready ports={base}..{base + count - 1} delay={delay}", flush=True)

    def stop(*_: object) -> None:
        for p in pids:
            os.kill(p, signal.SIGTERM)
        os._exit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    for p in pids:
        os.waitpid(p, 0)


if __name__ == "__main__":
    main()

"""PROTOTYPE. DB pool on asyncio streams. asyncio connections are loop-bound, so every event loop
opens its own set of pools (open_pools_for_current_loop) and finds it with pools_for_current_loop().
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import AsyncIterator

from demo_app import DB_ADDRS, POOL_SIZE

Connection = tuple[asyncio.StreamReader, asyncio.StreamWriter]

_pools: dict[asyncio.AbstractEventLoop, list[AsyncPool]] = {}
_pools_lock = threading.Lock()


class AsyncPool:
    """POOL_SIZE connections to one "DB" port; rotate() recreates them without dropping requests."""

    def __init__(self, addr: tuple[str, int]) -> None:
        """Create an empty pool for ``addr``; open() fills it."""
        self.addr = addr
        self.idle: asyncio.Queue[Connection] = asyncio.Queue()
        self.generation = 0

    async def open(self) -> None:
        """Open POOL_SIZE connections."""
        opening = (asyncio.open_connection(*self.addr) for _ in range(POOL_SIZE))
        connections = await asyncio.gather(*opening)
        for conn in connections:
            self.idle.put_nowait(conn)

    @contextlib.asynccontextmanager
    async def acquire(self) -> AsyncIterator[Connection]:
        """Borrow a connection; a connection that saw an exception is replaced with a new one."""
        reader, writer = await self.idle.get()
        try:
            yield reader, writer
        except BaseException:
            writer.close()
            reader, writer = await asyncio.open_connection(*self.addr)
            raise
        finally:
            self.idle.put_nowait((reader, writer))

    async def rotate(self) -> None:
        """Replace every connection, one at a time, and bump the generation."""
        for _ in range(POOL_SIZE):
            # one connection at a time: the rest of the pool keeps serving requests (hot path)
            await self._replace_one()  # noqa: WPS476
        self.generation += 1

    async def _replace_one(self) -> None:
        _reader, writer = await self.idle.get()
        writer.close()
        self.idle.put_nowait(await asyncio.open_connection(*self.addr))


def pools_for_current_loop() -> list[AsyncPool]:
    """Pools of the running event loop, one per DB port."""
    with _pools_lock:
        return _pools[asyncio.get_running_loop()]


async def open_pools_for_current_loop() -> list[AsyncPool]:
    """Open one pool per DB port for the running event loop."""
    pools = [AsyncPool(addr) for addr in DB_ADDRS]
    await asyncio.gather(*(pool.open() for pool in pools))
    with _pools_lock:
        _pools[asyncio.get_running_loop()] = pools
    return pools

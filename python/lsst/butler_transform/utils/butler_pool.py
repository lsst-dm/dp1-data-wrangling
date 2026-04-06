from __future__ import annotations

from lsst.daf.butler import Butler
import asyncio
from anyio import to_thread, CancelScope
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager


class ButlerPool:
    def __init__(self, butler: Butler, max_connections: int) -> None:
        self._root_butler = butler
        self._butlers: list[Butler] = []
        self._semaphore = asyncio.BoundedSemaphore(max_connections)
        self.max_connections = max_connections

    def _close(self) -> None:
        for butler in self._butlers:
            butler.close()

    @staticmethod
    @asynccontextmanager
    async def from_config(repo: str, max_connections: int) -> AsyncIterator[ButlerPool]:
        root_butler = await to_thread.run_sync(Butler.from_config, repo)
        try:
            pool = ButlerPool(root_butler, max_connections)
            yield pool
        finally:
            with CancelScope(shield=True):
                await to_thread.run_sync(pool._close)
                await to_thread.run_sync(root_butler.close)

    @asynccontextmanager
    async def get_butler(self) -> AsyncIterator[Butler]:
        async with self._semaphore:
            if self._butlers:
                butler = self._butlers.pop()
            else:
                butler = await to_thread.run_sync(self._root_butler.clone)

            try:
                yield butler
            finally:
                self._butlers.append(butler)

    async def run_with_butler[T](self, func: Callable[[Butler], T]) -> T:
        async with self.get_butler() as butler:
            return await to_thread.run_sync(func, butler)

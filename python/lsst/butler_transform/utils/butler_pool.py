from __future__ import annotations

from lsst.daf.butler import Butler
import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


class ButlerPool:
    def __init__(self, butler: Butler, max_connections: int) -> None:
        self._root_butler = butler
        self._butlers: list[Butler] = []
        self._semaphore = asyncio.BoundedSemaphore(max_connections)

    def _close(self) -> None:
        for butler in self._butlers:
            butler.close()

    @staticmethod
    @asynccontextmanager
    async def from_config(repo: str, max_connections: int) -> AsyncIterator[ButlerPool]:
        root_butler = await asyncio.to_thread(Butler.from_config, repo)
        try:
            pool = ButlerPool(root_butler, max_connections)
            yield pool
        finally:
            await asyncio.to_thread(pool._close)
            await asyncio.to_thread(root_butler.close)

    @asynccontextmanager
    async def get_butler(self) -> AsyncIterator[Butler]:
        async with self._semaphore:
            if self._butlers:
                butler = self._butlers.pop()
            else:
                butler = await asyncio.to_thread(self._root_butler.clone)

            try:
                yield butler
            finally:
                self._butlers.append(butler)

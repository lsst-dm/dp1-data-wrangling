from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
import asyncio


class ProduceConsumeQueue[T]:
    def __init__(self, max_capacity: int) -> None:
        self._queue = asyncio.Queue[T](max_capacity)
        self._loop = asyncio.get_event_loop()
        self._aborted = False

    async def produce(self, item: T) -> None:
        try:
            await self._queue.put(item)
        except asyncio.QueueShutDown:
            if self._aborted:
                raise QueueAbortedError("produce() canceled because queue aborted.")

    def produce_sync(self, item: T) -> None:
        asyncio.run_coroutine_threadsafe(self.produce(item), self._loop).result()

    async def finish_producing(self) -> None:
        self._queue.shutdown()

    async def abort(self) -> None:
        self._aborted = True
        self._queue.shutdown(immediate=True)

    @asynccontextmanager
    async def producing_context(self) -> AsyncIterator[None]:
        try:
            yield
            await self.finish_producing()
        except:
            await self.abort()
            raise

    @contextmanager
    def producing_context_sync(self) -> Iterator[None]:
        try:
            yield
            self.finish_producing_sync()
        except:
            self.abort_sync()
            raise

    def finish_producing_sync(self) -> None:
        asyncio.run_coroutine_threadsafe(self.finish_producing(), self._loop).result()

    def abort_sync(self) -> None:
        asyncio.run_coroutine_threadsafe(self.abort(), self._loop).result()

    async def consume_iter(self) -> AsyncIterator[T]:
        while True:
            try:
                yield await self._queue.get()
            except asyncio.QueueShutDown:
                if self._aborted:
                    raise QueueAbortedError(
                        "consume_iter() canceled because queue aborted."
                    )
                else:
                    # Producer finished, so there is nothing else to read.
                    return


class QueueAbortedError(Exception):
    """Raised by the ``ProduceConsumeQueue`` to cancel all consumers and
    producers if one of the producers or consumers failed unexpectedly.
    """

    pass

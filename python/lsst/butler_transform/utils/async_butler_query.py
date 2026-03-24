from __future__ import annotations

import asyncio
from lsst.daf.butler import DatasetRef, Butler
from lsst.daf.butler.queries import Query
from collections.abc import AsyncIterator, Callable, Iterable

type SyncButlerQueryFunction[T] = Callable[[Query], Iterable[T]]


async def run_butler_query_async[T](
    butler: Butler, query_func: SyncButlerQueryFunction[T]
) -> AsyncIterator[T]:
    queue = asyncio.Queue[Iterable[DatasetRef]](2)
    async with asyncio.TaskGroup() as tg:
        adapter = _SyncQueueAdapter(queue)
        tg.create_task(
            asyncio.to_thread(
                _run_query_sync,
                butler,
                query_func,
                adapter,
            )
        )
        try:
            while True:
                yield await queue.get()
        except asyncio.QueueShutDown:
            # The sync function indicates completion by shutting down the queue
            pass
        finally:
            # Cancel the sync function in the event of cancellation or other
            # error.
            queue.shutdown()


def _run_query_sync[T](
    butler: Butler,
    query_func: SyncButlerQueryFunction,
    queue: _SyncQueueAdapter[Iterable[DatasetRef]],
) -> AsyncIterator[T]:
    try:
        with butler.query() as query:
            results = query_func(query)
            for result_page in results:
                queue.put(result_page)
    except asyncio.QueueShutDown:
        # If an error occurs in the async function reading the queue, it will
        # trigger queue shutdown to cancel the query.
        pass
    finally:
        queue.shutdown()


class _SyncQueueAdapter[T]:
    def __init__(self, queue: asyncio.Queue[T]) -> None:
        self.queue = queue
        self._loop = asyncio.get_event_loop()

    def put(self, item: T) -> None:
        asyncio.run_coroutine_threadsafe(self._queue.put(item), self._loop).result()

    def shutdown(self) -> None:
        asyncio.run_coroutine_threadsafe(self._queue.shutdown(), self._loop).result()

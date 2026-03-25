from __future__ import annotations

import asyncio
from lsst.daf.butler import Butler
from lsst.daf.butler.queries import Query
from collections.abc import AsyncIterator, Callable, Iterable

from .produce_consume_queue import ProduceConsumeQueue

type SyncButlerQueryFunction[T] = Callable[[Query], Iterable[T]]


async def run_butler_query_async[T](
    butler: Butler, query_func: SyncButlerQueryFunction[T]
) -> AsyncIterator[T]:
    async with asyncio.TaskGroup() as tg:
        queue = ProduceConsumeQueue[T](2)
        tg.create_task(
            asyncio.to_thread(
                _run_query_sync,
                butler,
                query_func,
                queue,
            )
        )
        try:
            async for batch in queue.consume_iter():
                yield batch
        except Exception:
            await queue.abort()
            raise


def _run_query_sync[T](
    butler: Butler,
    query_func: SyncButlerQueryFunction[T],
    queue: ProduceConsumeQueue[T],
) -> None:
    with queue.producing_context_sync(), butler.query() as query:
        results = query_func(query)
        for result_page in results:
            queue.produce_sync(result_page)

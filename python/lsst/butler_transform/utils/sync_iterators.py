from collections.abc import Iterator, AsyncIterator
from anyio import to_thread
import enum


class _Sentinel(enum.Enum):
    FINISHED = enum.auto()


async def convert_sync_iterator_to_async[T](iterator: Iterator[T]) -> AsyncIterator[T]:
    while True:
        value = await to_thread.run_sync(next, iterator, _Sentinel.FINISHED)
        if value is _Sentinel.FINISHED:
            return
        yield value

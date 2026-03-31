from collections.abc import Iterator, Callable
from anyio import to_thread
from anyio.abc import ObjectSendStream
import enum


class _Sentinel(enum.Enum):
    FINISHED = enum.auto()


async def transfer_sync_iterator_to_stream[T](
    input: Callable[[], Iterator], output: ObjectSendStream[T]
) -> None:
    async with output:
        iterator = await to_thread.run_sync(lambda: iter(input()))
        while True:
            value = await to_thread.run_sync(next, iterator, _Sentinel.FINISHED)
            if value is _Sentinel.FINISHED:
                return
            await output.send(value)

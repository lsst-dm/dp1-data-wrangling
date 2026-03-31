from __future__ import annotations

from anyio import from_thread
from anyio.streams.memory import MemoryObjectSendStream
from contextlib import AbstractContextManager
from typing import Any


class SyncSendStream[T](AbstractContextManager):
    """Wraps `anyio.ObjectSendStream` for use in a sync function running
    outside the event loop in a separate thread.
    """

    def __init__(self, stream: MemoryObjectSendStream[T]) -> None:
        self._stream = stream

    def __enter__(self) -> SyncSendStream:
        return self

    def send(self, item: T) -> None:
        from_thread.run(self._stream.send, item)

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        from_thread.run_sync(self._stream.close)

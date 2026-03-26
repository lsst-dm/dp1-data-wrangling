import asyncio
from collections.abc import Awaitable


class TaskLimiter:
    def __init__(self, task_group: asyncio.TaskGroup, max_concurrency: int) -> None:
        self._task_group = task_group
        self._semaphore = asyncio.BoundedSemaphore(max_concurrency)

    async def create_task[T](self, awaitable: Awaitable[T]) -> asyncio.Task[T]:
        await self._semaphore.acquire()
        return self._task_group.create_task(
            self._run_task_then_release_semaphore(awaitable)
        )

    async def _run_task_then_release_semaphore[T](self, awaitable: Awaitable[T]) -> T:
        try:
            return await awaitable
        finally:
            self._semaphore.release()

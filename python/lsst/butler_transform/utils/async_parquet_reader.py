from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

from anyio import to_thread, CancelScope

from pyarrow.parquet import ParquetFile
import pyarrow
from .sync_iterators import convert_sync_iterator_to_async


async def read_parquet_async(
    input_file: str | Path, *, batch_size: int = 10000, columns: list[str] | None = None
) -> AsyncIterator[pyarrow.RecordBatch]:
    reader = await to_thread.run_sync(ParquetFile, input_file)
    try:
        iterator = await to_thread.run_sync(
            lambda: reader.iter_batches(batch_size=batch_size, columns=columns)
        )
        async for batch in convert_sync_iterator_to_async(iterator):
            yield batch
    finally:
        with CancelScope(shield=True):
            await to_thread.run_sync(reader.close)

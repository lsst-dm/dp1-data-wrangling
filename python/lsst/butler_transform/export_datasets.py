import asyncio
from itertools import batched
from pathlib import Path

from collections.abc import Iterable

from lsst.daf.butler import DatasetType
from ..dp1_data_wrangling.datasets_parquet import DatasetsParquetWriter
from .utils.async_butler_query import run_butler_query_async
from .utils.butler_pool import ButlerPool


async def export_datasets(
    butler_pool: ButlerPool,
    dataset_type: DatasetType,
    collections: Iterable[str],
    output_directory: str,
) -> None:
    async with butler_pool.get_butler() as butler:
        output_path = Path(output_directory).joinpath(f"{dataset_type.name}.datasets.parquet")
        writer = await asyncio.to_thread(DatasetsParquetWriter, dataset_type, output_path)
        async for refs in run_butler_query_async(
            butler,
            lambda query: batched(
                query.datasets(dataset_type, collections, find_first=True), 50_000
            ),
        ):
            print(f"{dataset_type}: {len(refs)}")
            await asyncio.to_thread(writer.add_refs, refs)
    await asyncio.to_thread(writer.finish)

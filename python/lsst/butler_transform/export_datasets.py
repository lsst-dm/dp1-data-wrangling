import asyncio
from itertools import batched
from pathlib import Path

from collections.abc import Iterable, Mapping

from lsst.daf.butler import DatasetType, DatasetId
from lsst.daf.butler.registry.interfaces import FakeDatasetRef
from lsst.daf.butler.datastore.record_data import DatastoreRecordData
from .utils.task_limiter import TaskLimiter
from ..dp1_data_wrangling.datasets_parquet import (
    DatasetsParquetWriter,
    read_dataset_ids_from_file,
)
from ..dp1_data_wrangling.datastore_parquet import DatastoreParquetWriter
from .utils.async_butler_query import run_butler_query_async
from .utils.butler_pool import ButlerPool
from .utils.produce_consume_queue import ProduceConsumeQueue


async def export_datasets(
    butler_pool: ButlerPool,
    dataset_type: DatasetType,
    collections: Iterable[str],
    output_directory: str,
) -> None:
    dataset_path = Path(output_directory).joinpath(
        f"{dataset_type.name}.datasets.parquet"
    )

    # Export dataset refs to parquet.
    async with butler_pool.get_butler() as butler:
        writer = await asyncio.to_thread(
            DatasetsParquetWriter, dataset_type, dataset_path
        )
        async for refs in run_butler_query_async(
            butler,
            lambda query: batched(
                query.datasets(dataset_type, collections, find_first=True), 50_000
            ),
        ):
            print(f"{dataset_type}: {len(refs)}")
            await asyncio.to_thread(writer.add_refs, refs)
    await asyncio.to_thread(writer.finish)

    # Export datastore records to parquet.
    async with asyncio.TaskGroup() as tg:
        # Read back in the list of IDs from the datasets we exported above.
        dataset_id_queue = ProduceConsumeQueue[Iterable[DatasetId]](4)
        tg.create_task(
            asyncio.to_thread(_read_back_dataset_ids, dataset_path, dataset_id_queue)
        )

        # Look up datastore records associated with the datasets.
        datastore_records_queue = ProduceConsumeQueue[ButlerDatastoreRecords](2)
        tg.create_task(
            _fetch_datastore_records(
                butler_pool, dataset_id_queue, datastore_records_queue
            )
        )

        # Write the datastore records to parquet
        datastore_parquet_path = Path(output_directory).joinpath(
            f"{dataset_type.name}.datastore.parquet"
        )
        tg.create_task(
            _write_datastore_records(datastore_parquet_path, datastore_records_queue)
        )


def _read_back_dataset_ids(
    dataset_file: Path, queue: ProduceConsumeQueue[DatasetId]
) -> None:
    with queue.producing_context_sync():
        for batch in read_dataset_ids_from_file(dataset_file, 50_000):
            queue.produce_sync(batch)


type ButlerDatastoreRecords = Mapping[str, DatastoreRecordData]


async def _fetch_datastore_records(
    butler_pool: ButlerPool,
    input: ProduceConsumeQueue[Iterable[DatasetId]],
    output: ProduceConsumeQueue[ButlerDatastoreRecords],
) -> None:
    async with output.producing_context(), asyncio.TaskGroup() as tg:
        limiter = TaskLimiter(tg, 8)
        async for dataset_ids in input.consume_iter():
            await limiter.create_task(
                _fetch_datastore_record_batch(butler_pool, dataset_ids, output)
            )


async def _fetch_datastore_record_batch(
    butler_pool: ButlerPool,
    dataset_ids: Iterable[DatasetId],
    output: ProduceConsumeQueue[ButlerDatastoreRecords],
) -> None:
    async with butler_pool.get_butler() as butler:
        refs = [FakeDatasetRef(id) for id in dataset_ids]
        records = await asyncio.to_thread(butler._datastore.export_records, refs)
    await output.produce(records)


async def _write_datastore_records(
    output_file: Path, queue: ProduceConsumeQueue[ButlerDatastoreRecords]
) -> None:
    writer = DatastoreParquetWriter(output_file)
    try:
        async for records in queue.consume_iter():
            await asyncio.to_thread(writer.write_records, records)
    finally:
        await asyncio.to_thread(writer.finish)

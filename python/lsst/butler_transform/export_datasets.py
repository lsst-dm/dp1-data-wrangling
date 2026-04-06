from itertools import batched
from anyio import (
    create_task_group,
    create_memory_object_stream,
    to_thread,
    CapacityLimiter,
)
from anyio.abc import ObjectSendStream, ObjectReceiveStream, TaskStatus
from pathlib import Path

from collections.abc import Collection, Iterable, Mapping

from lsst.daf.butler import DatasetRef, DatasetType, DatasetId, Butler
from lsst.daf.butler.registry.interfaces import FakeDatasetRef
from lsst.daf.butler.datastore.record_data import DatastoreRecordData
from .utils.sync_send_stream import SyncSendStream
from .parquet.datasets import DatasetsParquetWriter, read_dataset_ids
from .parquet.datastore import DatastoreParquetWriter
from .utils.butler_pool import ButlerPool

type ButlerDatastoreRecords = Mapping[str, DatastoreRecordData]


async def export_datasets(
    butler_pool: ButlerPool,
    dataset_type: DatasetType,
    collections: Iterable[str],
    output_directory: str,
) -> None:
    dataset_path = Path(output_directory).joinpath(
        f"{dataset_type.name}.datasets.parquet"
    )

    dataset_ref_send, dataset_ref_recv = create_memory_object_stream[
        Collection[DatasetRef]
    ](2)

    # Export dataset refs to parquet.
    async with create_task_group() as tg:
        tg.start_soon(
            butler_pool.run_with_butler,
            lambda butler: _query_datasets(
                SyncSendStream(dataset_ref_send), butler, dataset_type, collections
            ),
        )
        tg.start_soon(
            _write_datasets_to_parquet, dataset_ref_recv, dataset_type, dataset_path
        )

    # Export datastore records to parquet.
    async with create_task_group() as tg:
        # Read back in the list of IDs from the datasets we exported above.
        dataset_id_send, dataset_id_recv = create_memory_object_stream[
            Iterable[DatasetId]
        ]()
        tg.start_soon(
            _read_back_dataset_ids,
            dataset_path,
            dataset_id_send,
        )

        # Look up datastore records associated with the datasets.
        datastore_records_send, datastore_records_recv = create_memory_object_stream[
            ButlerDatastoreRecords
        ](2)
        tg.start_soon(
            _fetch_datastore_records,
            butler_pool,
            dataset_id_recv,
            datastore_records_send,
        )

        # Write the datastore records to parquet
        datastore_parquet_path = Path(output_directory).joinpath(
            f"{dataset_type.name}.datastore.parquet"
        )
        tg.start_soon(
            _write_datastore_records, datastore_parquet_path, datastore_records_recv
        )

    print(f"{dataset_type.name}: complete")


def _query_datasets(
    output: SyncSendStream[Collection[DatasetRef]],
    butler: Butler,
    dataset_type: DatasetType,
    collections: Iterable[str],
) -> None:
    with output, butler.query() as query:
        results = query.datasets(dataset_type, collections, find_first=True)
        for batch in batched(results, 50_000):
            output.send(batch)


async def _write_datasets_to_parquet(
    input: ObjectReceiveStream[Collection[DatasetRef]],
    dataset_type: DatasetType,
    output_path: Path,
) -> None:
    async with input, DatasetsParquetWriter(output_path, dataset_type) as writer:
        async for refs in input:
            print(f"{dataset_type}: {len(refs)} datasets")
            await writer.add_refs(refs)


async def _read_back_dataset_ids(
    dataset_file: Path, output: ObjectSendStream[Collection[DatasetId]]
) -> None:
    async with output:
        async for batch in read_dataset_ids(dataset_file):
            await output.send(batch)


async def _fetch_datastore_records(
    butler_pool: ButlerPool,
    input: ObjectReceiveStream[Iterable[DatasetId]],
    output: ObjectSendStream[ButlerDatastoreRecords],
) -> None:
    async with input, output, create_task_group() as tg:
        async for dataset_ids in input:
            await tg.start(
                _fetch_datastore_record_batch, butler_pool, dataset_ids, output
            )


async def _fetch_datastore_record_batch(
    butler_pool: ButlerPool,
    dataset_ids: Iterable[DatasetId],
    output: ObjectSendStream[ButlerDatastoreRecords],
    task_status: TaskStatus,
) -> None:
    refs = [FakeDatasetRef(id) for id in dataset_ids]
    async with butler_pool.get_butler() as butler:
        task_status.started()
        records = await to_thread.run_sync(butler._datastore.export_records, refs)
        await output.send(records)


async def _write_datastore_records(
    output_file: Path, input: ObjectReceiveStream[ButlerDatastoreRecords]
) -> None:
    async with input, DatastoreParquetWriter(output_file) as writer:
        async for records in input:
            await writer.write_records(records)

from __future__ import annotations

import uuid
from collections.abc import Iterator, Mapping
from pathlib import Path

import pyarrow
from lsst.daf.butler.datastore.record_data import (
    DatastoreRecordData,
    StoredDatastoreItemInfo,
)
from lsst.daf.butler.datastores.fileDatastore import StoredFileInfo
from ..utils.async_parquet_writer import AsyncParquetWriter

# The full structure of the export structure used by
# Datastore.export_records/Datastore.import_records is:
# dict:
#   datastore name -> DatastoreRecordData (class):
#     .records (dict):
#       DatasetId -> dict:
#         table name (str) -> list[StoredDatastoreItemInfo]:


class DatastoreParquetWriter(AsyncParquetWriter):
    def __init__(self, output_path: str | Path) -> None:
        super().__init__(output_path, _create_schema())

    async def write_records(self, records: Mapping[str, DatastoreRecordData]) -> None:
        """Write exported records from Butler Datastore.

        records
            Mapping from Datastore name to the records for that Datastore (as
            returned by `Datastore.export_records`.)
        """
        rows = list(_convert_records_from_rows(records))
        if len(rows) == 0:
            return

        batch = pyarrow.RecordBatch.from_pylist(rows, schema=self._schema)
        await self.write_batch(batch)


def _convert_records_from_rows(
    records: Mapping[str, DatastoreRecordData],
) -> Iterator[dict[str, object]]:
    for datastore in records.keys():
        for dataset_id, record in records[datastore].records.items():
            yield from _convert_record(datastore, dataset_id, record)


def _convert_record(
    datastore_name: str,
    dataset_id: uuid.UUID,
    record: dict[str, list[StoredDatastoreItemInfo]],
) -> Iterator[dict[str, object]]:
    if len(record) > 1:
        # The keys in this dict are a "table" name.  No existing Datastore
        # implementation has more than one key here so it's not clear why
        # this is organized like this.
        raise NotImplementedError(
            "Cannot export datastore records with more than one 'table' entry."
        )
    item_infos = list(record.values())[0]

    for item in item_infos:
        assert isinstance(item, StoredFileInfo), (
            "Exporting records is only supported for FileDatastore"
        )
        row = {
            "datastore_name": datastore_name,
            "dataset_id": dataset_id.bytes,
            **item.to_record(),
        }
        yield row


def _create_schema() -> pyarrow.Schema:
    string_dict = pyarrow.dictionary(pyarrow.int32(), pyarrow.string())
    return pyarrow.schema(
        [
            pyarrow.field("datastore_name", string_dict, nullable=False),
            pyarrow.field("dataset_id", pyarrow.binary(16), nullable=False),
            # These are the fields from the StoredFileInfo Butler datastore
            # records class.
            pyarrow.field("path", pyarrow.string(), nullable=False),
            pyarrow.field("formatter", string_dict, nullable=False),
            pyarrow.field("storage_class", string_dict, nullable=False),
            pyarrow.field("component", string_dict, nullable=True),
            pyarrow.field("checksum", pyarrow.string(), nullable=True),
            pyarrow.field("file_size", pyarrow.int64(), nullable=False),
        ]
    )

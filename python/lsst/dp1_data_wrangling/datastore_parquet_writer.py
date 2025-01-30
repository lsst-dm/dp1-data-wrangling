from __future__ import annotations

import uuid
from collections.abc import Iterator, Mapping

import pyarrow
from lsst.daf.butler import DatasetId
from lsst.daf.butler.datastore.record_data import (
    DatastoreRecordData,
    StoredDatastoreItemInfo,
)
from pyarrow.parquet import ParquetWriter


class DatastoreParquetWriter:
    def __init__(self, output_file: str) -> None:
        self._writer: ParquetWriter | None = None
        self._output_file = output_file

    def write_records(
        self, records: Mapping[str, DatastoreRecordData], datastore_priority: list[str]
    ) -> None:
        """Write exported records from Butler Datastore.

        records
            Mapping from Datastore name to the records for that Datastore (as
            returned by `Datastore.export_records`.)
        datastore_priority
            List of datastore names in priority order.  For each dataset ID, if
            it appears in more than one Datastore only the record from the
            first Datastore in the list will be kept. (Matching the behavior of
            `lsst.daf.butler.ChainedDatastore`)
        """
        assert set(datastore_priority) == set(
            records.keys()
        ), "A priority should be given for all datastores in the mapping."

        rows = list(_convert_records_from_rows(records, datastore_priority))
        if len(rows) == 0:
            return

        table = pyarrow.Table.from_pylist(rows)

        if self._writer is None:
            self._writer = ParquetWriter(self._output_file, table.schema)

        self._writer.write(table)

    def finish(self) -> None:
        if self._writer is not None:
            self._writer.close()


def _convert_records_from_rows(
    records: Mapping[str, DatastoreRecordData], datastore_priority: list[str]
) -> Iterator[dict[str, object]]:
    seen_datasets: set[DatasetId] = set()
    for datastore in datastore_priority:
        for dataset_id, record in records[datastore].records.items():
            if dataset_id in seen_datasets:
                # Skip datasets that existed in higher priority datastores.
                continue
            seen_datasets.add(dataset_id)
            yield from _convert_record(datastore, dataset_id, record)


def _convert_record(
    datastore_name: str, dataset_id: uuid.UUID, record: dict[str, list[StoredDatastoreItemInfo]]
) -> Iterator[dict[str, object]]:
    if len(record) > 1:
        # The keys in this dict are a "table" name.  No existing Datastore
        # implementation has more than one key here so it's not clear why
        # this is organized like this.
        raise NotImplementedError("Cannot export datastore records with more than one 'table' entry.")
    item_infos = list(record.values())[0]

    for item in item_infos:
        row = {"datastore_name": datastore_name, "dataset_id": dataset_id.bytes, **item.to_record()}
        yield row

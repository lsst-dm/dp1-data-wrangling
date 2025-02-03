from __future__ import annotations

from typing import TypeAlias

from lsst.daf.butler import DatasetId, Datastore
from lsst.daf.butler.datastore.record_data import DatastoreRecordData
from lsst.daf.butler.datastores.chainedDatastore import ChainedDatastore
from lsst.daf.butler.datastores.fileDatastore import FileDatastore, StoredFileInfo

from .datastore_parquet import DatastoreRow

DatastoreMappingConfig: TypeAlias = dict[str, str]
"""Keys are datastore names from the source repository, values are
datastore names on the target repository.
"""


class DatastoreMapper:
    """Manages conversion of Datastore records from the source repository
    to the target repository.

    datastore_mapping
    target_datastore
        Datastore to which records will be written in the target repository.
    """

    def __init__(self, datastore_mapping: DatastoreMappingConfig, target_datastore: Datastore) -> None:
        self._mapping = dict(datastore_mapping)
        self._table_names = {}
        for target_name in datastore_mapping.values():
            if target_name not in target_datastore.names:
                raise ValueError(
                    f"Target datastore is not available in the destination repository: {target_name}"
                )
            self._table_names[target_name] = _get_table_name(target_datastore, target_name)

    def map_to_target(self, records: list[DatastoreRow]) -> dict[str, DatastoreRecordData]:
        """Given a flat list of records, generate the nested structure
        expected by ``Datastore.import_records()``.
        """
        # Group rows by datastore and dataset ID.
        # Datastore name -> (dataset UUID -> list of file info objects)
        values: dict[str, dict[DatasetId, list[StoredFileInfo]]] = {}
        for r in records:
            output_datastore_name = self._mapping.get(r.datastore_name)
            if output_datastore_name is None:
                raise RuntimeError(
                    f"No mapping to target datastore defined for source datastore '{r.datastore_name}'"
                )
            datasets = values.setdefault(output_datastore_name, {})
            item_infos = datasets.setdefault(r.dataset_id, [])
            item_infos.append(r.file_info)

        # Add extra intermediate data structures to match format expected by
        # Datastore.import_records.
        out: dict[str, DatastoreRecordData] = {}
        for output_datastore_name, datasets in values.items():
            record_data = DatastoreRecordData()
            table_name = self._table_names[output_datastore_name]
            record_data.records = {k: {table_name: v} for k, v in datasets.items()}
            out[output_datastore_name] = record_data

        return out


def _get_table_name(datastore: Datastore, datastore_name: str) -> str:
    file_datastore = _get_child_datastore(datastore, datastore_name)
    if file_datastore is None:
        raise ValueError(f"Target datastore not found: {datastore_name}")
    tables = file_datastore.get_opaque_table_definitions()
    table_names = list(tables.keys())
    if len(table_names) > 1:
        # FileDatastore currently has exactly one table name, and it's not
        # clear why it would ever need more.
        raise RuntimeError(f"Unexpectedly got more than one table name for Datastore '{datastore_name}'")
    if len(table_names) == 0:
        raise RuntimeError(f"Datastore {datastore_name} does not have a table defined")
    return table_names[0]


def _get_child_datastore(datastore: Datastore, child_name: str) -> FileDatastore | None:
    if datastore.name == child_name:
        if isinstance(datastore, FileDatastore):
            return datastore
        else:
            raise ValueError(
                f"Target datastore must be a FileDatastore, not {type(datastore)}: {datastore.name}"
            )

    if isinstance(datastore, ChainedDatastore):
        for child in datastore.datastores:
            result = _get_child_datastore(child, child_name)
            if result is not None:
                return result

    return None

from __future__ import annotations

import itertools
from collections.abc import Iterable, Iterator
from typing import TypeVar

from lsst.daf.butler import (
    Butler,
    CollectionType,
    DatasetAssociation,
    DatasetType,
    DimensionRecord,
)

from .dataset_types import export_dataset_types
from .datasets_parquet import DatasetAssociationParquetWriter, DatasetsParquetWriter
from .datastore_parquet import DatastoreParquetWriter
from .dimension_record_parquet import DimensionRecordParquetWriter
from .index import ExportIndex
from .paths import ExportPaths
from .utils import write_model_to_file

MAX_ROWS_PER_WRITE = 50000


class Exporter:
    """Export DatasetRefs with associated dimension records to parquet files"""

    def __init__(self, output_path: str, butler: Butler, root_collection: str) -> None:
        self._dimensions: dict[str, DimensionRecordParquetWriter] = {}
        self._butler = butler
        self._paths = ExportPaths(output_path)
        self._paths.create_directories()
        self._root_collection = root_collection

        self._dataset_types_written: set[str] = set()
        self._collections_seen: set[str] = set()
        self._datastore_writer = DatastoreParquetWriter(self._paths.datastore_parquet_path())

    def dump_refs(self, dataset_type_name: str, collections: list[str]) -> None:
        assert (
            dataset_type_name not in self._dataset_types_written
        ), "Each dataset type must be written only once"
        self._dataset_types_written.add(dataset_type_name)

        self._collections_seen.update(collections)

        dataset_type = self._butler.get_dataset_type(dataset_type_name)
        self._generate_association_output(dataset_type, collections)
        self._generate_dataset_output(dataset_type, collections)

    def dump_dimension_records(self, records: Iterable[DimensionRecord]) -> None:
        for record in records:
            self._add_dimension_record(record)

    def did_export_dimension_records(self, dimension: str) -> bool:
        return dimension in self._dimensions

    def close_and_get_dimension_record_output_file(self, dimension: str) -> str:
        """Return the path where the parquet file for a given dimension can be
        found.  No more records may be written for the given dimension after
        this function is called.
        """
        self._dimensions[dimension].finish()
        return self._paths.dimension_parquet_path(dimension)

    def _generate_dataset_output(self, dataset_type: DatasetType, collections: list[str]) -> None:
        """Dump full list of datasets included in the given collections for the
        given dataset type
        """
        writer = DatasetsParquetWriter(dataset_type, self._paths.dataset_parquet_path(dataset_type.name))
        with self._butler.query() as query:
            results = query.datasets(dataset_type, collections, find_first=False).with_dimension_records()
            for refs in _batched(results, MAX_ROWS_PER_WRITE):
                # Sort by data ID to improve compressibility.
                refs.sort(key=lambda ref: ref.dataId)
                writer.add_refs(refs)
                for ref in refs:
                    self._collections_seen.add(ref.run)
                    # Write dimension records from these refs to separate
                    # dimension record files.
                    for record in ref.dataId.records.values():
                        if record is not None:
                            self._add_dimension_record(record)
                # Export datastore records (file paths etc) associated with
                # these refs to a separate file.
                datastore_records = self._butler._datastore.export_records(refs)
                self._datastore_writer.write_records(datastore_records, self._butler._datastore.names)
        writer.finish()

    def _generate_association_output(self, dataset_type: DatasetType, collections: list[str]) -> None:
        """Dump a list of datasets associated with tag and calibration
        collections.
        """
        tag_and_calib_collections = list(
            self._butler.collections.query(
                collections,
                collection_types={CollectionType.TAGGED, CollectionType.CALIBRATION},
                flatten_chains=True,
            )
        )
        writer = DatasetAssociationParquetWriter(
            dataset_type, self._paths.dataset_association_parquet_path(dataset_type.name)
        )
        if len(tag_and_calib_collections) > 0:
            with self._butler.query() as query:
                query = query.join_dataset_search(dataset_type, tag_and_calib_collections)
                result = query.general(
                    dataset_type.dimensions,
                    dataset_fields={dataset_type.name: {"dataset_id", "run", "collection", "timespan"}},
                    find_first=False,
                )
                associations = DatasetAssociation.from_query_result(result, dataset_type)
                for batch in _batched(associations, MAX_ROWS_PER_WRITE):
                    # Sort to group datasets from the same collection together,
                    # then by data ID to improve compressibility.
                    batch.sort(key=lambda association: (association.collection, association.ref.dataId))
                    writer.add_associations(batch)
        writer.finish()

    def finish(self) -> None:
        for writer in self._dimensions.values():
            writer.finish()
        self._datastore_writer.finish()

        self._export_collections()

        dataset_types = self._butler.registry.queryDatasetTypes(self._dataset_types_written)
        export_dataset_types(self._paths.dataset_type_path(), dataset_types)

        index = ExportIndex(
            dimensions=list(self._dimensions.keys()),
            dataset_types=list(self._dataset_types_written),
            root_collection=self._root_collection,
        )
        write_model_to_file(index, self._paths.index_path())

    def _export_collections(self) -> None:
        with self._butler.export(filename=self._paths.collections_path()) as exporter:
            # Export collection structure
            collections = self._butler.collections.query(
                self._collections_seen, flatten_chains=True, include_chains=True
            )
            for collection in collections:
                exporter.saveCollection(collection)

    def _add_dimension_record(self, record: DimensionRecord) -> None:
        dimension = record.definition.name
        writer = self._dimensions.get(dimension)
        if writer is None:
            writer = DimensionRecordParquetWriter(
                record.definition, self._paths.dimension_parquet_path(dimension)
            )
            self._dimensions[dimension] = writer

        writer.add_record(record)


_T = TypeVar("_T")


def _batched(refs: Iterable[_T], batch_size: int) -> Iterator[list[_T]]:
    """Roughly equivalent to Python 3.12's itertools.batched."""
    iterator = iter(refs)
    while batch := list(itertools.islice(iterator, batch_size)):
        yield batch

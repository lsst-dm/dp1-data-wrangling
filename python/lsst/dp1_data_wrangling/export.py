from __future__ import annotations

import itertools
from collections.abc import Iterable, Iterator

from lsst.daf.butler import Butler, DatasetRef, DimensionRecord

from .dataset_types import export_dataset_types
from .datasets_parquet_writer import DatasetsParquetWriter
from .datastore_parquet_writer import DatastoreParquetWriter
from .dimension_record_parquet_writer import DimensionRecordParquetWriter
from .paths import ExportPaths

COLLECTIONS = ["LSSTComCam/runs/DRP/DP1/w_2025_03/DM-48478"]
# Based on a preliminary list provided by Jim Bosch at
# https://rubinobs.atlassian.net/wiki/spaces/~jbosch/pages/423559233/DP1+Dataset+Retention+Removal+Planning
DATASET_TYPES = [
    # "Tier 1" major data products
    "raw",
    "ccdVisitTable",
    "visitTable",
    "pvi",
    "pvi_background",
    "sourceTable_visit",
    "deepCoadd_calexp",
    "deepCoadd_calexp_background",
    "goodSeeingCoadd",
    "objectTable_tract",
    "forcedSourceTable_tract",
    "diaObjectTable_tract",
    "diaSourceTable",
    # "Tier 1b" minor data products.
    # The list asks for all *_metadata, *_log, *_config datasets, but those
    # are not included here yet.
    "finalVisitSummary",
]

OUTPUT_DIRECTORY = "dp1-dump-test"
MAX_ROWS_PER_WRITE = 50000


def main() -> None:
    butler = Butler("/repo/main")

    with butler.registry.caching_context():
        dumper = DatasetsDumper(OUTPUT_DIRECTORY, butler)
        for dt in DATASET_TYPES:
            dumper.dump_refs(dt, COLLECTIONS)
        dumper.finish()


class DatasetsDumper:
    """Export DatasetRefs with associated dimension records to parquet files"""

    def __init__(self, output_path: str, butler: Butler) -> None:
        self._dimensions: dict[str, DimensionRecordParquetWriter] = {}
        self._butler = butler
        self._paths = ExportPaths(output_path)
        self._paths.create_directories()

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
        writer = DatasetsParquetWriter(dataset_type, self._paths.dataset_parquet_path(dataset_type_name))
        with self._butler.query() as query:
            results = query.datasets(dataset_type, collections, find_first=False).with_dimension_records()
            for refs in _batched(results, MAX_ROWS_PER_WRITE):
                writer.add_refs(refs)
                for ref in refs:
                    self._collections_seen.add(ref.run)
                    for key, record in ref.dataId.records.items():
                        self._add_dimension_record(key, record)
                datastore_records = self._butler._datastore.export_records(refs)
                self._datastore_writer.write_records(datastore_records, self._butler._datastore.names)
            writer.finish()

    def finish(self) -> None:
        for writer in self._dimensions.values():
            writer.finish()
        self._datastore_writer.finish()

        self._export_collections()

        dataset_types = self._butler.registry.queryDatasetTypes(self._dataset_types_written)
        export_dataset_types(self._paths.dataset_type_path(), dataset_types)

    def _export_collections(self) -> None:
        with self._butler.export(filename=self._paths.collections_yaml_path()) as exporter:
            # Export collection structure
            collections = self._butler.collections.query(
                self._collections_seen, flatten_chains=True, include_chains=True
            )
            for collection in collections:
                exporter.saveCollection(collection)

    def _add_dimension_record(self, dimension: str, record: DimensionRecord | None) -> None:
        if record is None:
            return

        writer = self._dimensions.get(dimension)
        if writer is None:
            writer = DimensionRecordParquetWriter(
                record.definition, self._paths.dimension_parquet_path(dimension)
            )
            self._dimensions[dimension] = writer

        writer.add_record(record)


def _batched(refs: Iterable[DatasetRef], batch_size: int) -> Iterator[list[DatasetRef]]:
    """Roughly equivalent to Python 3.12's itertools.batched."""
    iterator = iter(refs)
    while batch := list(itertools.islice(iterator, batch_size)):
        yield batch

from __future__ import annotations

import pandas
import pyarrow
from pyarrow.parquet import ParquetWriter

from lsst.daf.butler import (
    Butler,
    DimensionRecordTable,
    DimensionElement,
    DimensionRecord,
    DatasetRef,
    DatasetType,
    DimensionGroup,
)
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
MAX_ROWS_PER_WRITE = 100000


def main() -> None:
    butler = Butler("/repo/main")
    dumper = DatasetsDumper(OUTPUT_DIRECTORY)

    with butler.registry.caching_context():
        for dt in DATASET_TYPES:
            dumper.dump_refs(butler, dt)

    dumper.finish()


class DatasetsDumper:
    """Export DatasetRefs with associated dimension records to parquet files"""

    def __init__(self, output_path: str) -> None:
        self._dimensions: dict[str, DimensionRecordParquetWriter] = {}
        self._paths = ExportPaths(output_path)
        self._paths.create_directories()

        self._dataset_types_written: set[str] = set()

    def dump_refs(self, butler: Butler, dataset_type_name: str) -> None:
        assert (
            dataset_type_name not in self._dataset_types_written
        ), "Each dataset type must be written only once"
        self._dataset_types_written.add(dataset_type_name)

        dataset_type = butler.get_dataset_type(dataset_type_name)
        writer = DatasetsParquetWriter(dataset_type, self._paths.dataset_parquet_path(dataset_type_name))
        with butler.query() as query:
            for ref in query.datasets(dataset_type, COLLECTIONS, find_first=False).with_dimension_records():
                writer.add_ref(ref)
                for key, record in ref.dataId.records.items():
                    self._add_dimension_record(key, record)
            writer.finish()

    def finish(self) -> None:
        for writer in self._dimensions.values():
            writer.finish()

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


class DimensionRecordParquetWriter:
    def __init__(self, dimension: DimensionElement, output_file: str) -> None:
        self._dimension = dimension
        self._output_file = output_file
        self._records: list[DimensionRecord] = []
        self._schema = DimensionRecordTable.make_arrow_schema(dimension)
        self._writer = ParquetWriter(output_file, self._schema)

    def add_record(self, record: DimensionRecord) -> None:
        self._records.append(record)
        if len(self._records) >= MAX_ROWS_PER_WRITE:
            self._flush_records()

    def _flush_records(self) -> None:
        table = DimensionRecordTable(self._dimension, self._records)
        self._writer.write(table.to_arrow())
        self._records.clear()

    def finish(self) -> None:
        self._flush_records()
        self._writer.close()
        data_id_columns = list(self._dimension.schema.required.names)

        df = pandas.read_parquet(self._output_file)
        # De-duplicate the dimension records.  Because the records were
        # inserted from DatasetRefs of multiple dataset types, there is likely
        # to be significant duplication.
        df.drop_duplicates(subset=data_id_columns, inplace=True)
        # The data ID columns are used in indexes and are typically ordered
        # from low to high cardinality, so sorting here should give better
        # compression and insert performance.
        df.sort_values(by=data_id_columns, inplace=True)
        df.to_parquet(self._output_file, schema=self._schema, index=False)


class DatasetsParquetWriter:
    def __init__(self, dataset_type: DatasetType, output_file: str) -> None:
        self._schema = create_dataset_arrow_schema(dataset_type)
        self._writer = ParquetWriter(output_file, self._schema)
        self._rows: list[dict] = []

    def add_ref(self, ref: DatasetRef) -> None:
        row = dict(ref.dataId.required)
        row["dataset_id"] = ref.id.bytes
        row["run"] = ref.run

        self._rows.append(row)
        if len(self._rows) >= MAX_ROWS_PER_WRITE:
            self._flush_rows()

    def _flush_rows(self) -> None:
        batch = pyarrow.RecordBatch.from_pylist(self._rows, schema=self._schema)
        self._writer.write(batch)
        self._rows.clear()

    def finish(self) -> None:
        self._flush_rows()
        self._writer.close()


def create_dataset_arrow_schema(dataset_type: DatasetType) -> pyarrow.Schema:
    fields = [
        pyarrow.field("dataset_id", pyarrow.binary(16)),
        pyarrow.field("run", pyarrow.string()),
        *_get_data_id_column_schemas(dataset_type.dimensions),
    ]
    return pyarrow.schema(fields)


def _get_data_id_column_schemas(dimensions: DimensionGroup) -> list[pyarrow.Field]:
    schema = []
    for dimension in dimensions.required:
        dimension = dimensions.universe.dimensions[dimension]
        field = pyarrow.field(dimension.name, dimension.primary_key.to_arrow().data_type)
        schema.append(field)

    return schema
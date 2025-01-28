import pathlib

from lsst.daf.butler import Butler, DimensionRecordTable, DimensionElement, DimensionRecord, DatasetRef, DatasetType, DimensionGroup
import pyarrow
from pyarrow.parquet import ParquetWriter


COLLECTIONS = [ "LSSTComCam/runs/DRP/DP1/w_2025_03/DM-48478"]
OUTPUT_DIRECTORY = "dp1-dump-test"
DIMENSION_SUBDIRECTORY = "dimensions"
DATASETS_SUBDIRECTORY = "datasets"
MAX_ROWS_PER_WRITE = 100000

def main() -> None:
    output_path = pathlib.Path(OUTPUT_DIRECTORY)
    output_path.mkdir(parents=True, exist_ok=True)

    butler = Butler("/repo/main")
    dumper = DatasetsDumper(output_path)

    with butler.registry.caching_context():
        dumper.dump_refs(butler, "calexp")

    dumper.finish()

class DatasetsDumper:
    """Export DatasetRefs with associated dimension records to parquet files"""

    def __init__(self, output_path: pathlib.Path) -> None:
        self._dimensions: dict[str, DimensionRecordParquetWriter] = {}
        self._output_path = output_path

        dimension_output_directory = output_path.joinpath(DIMENSION_SUBDIRECTORY)
        dimension_output_directory.mkdir(exist_ok=True)
        self._dimension_output_directory = dimension_output_directory

        dataset_output_directory = output_path.joinpath(DATASETS_SUBDIRECTORY)
        dataset_output_directory.mkdir(exist_ok=True)
        self._dataset_output_directory = dataset_output_directory

        self._dataset_types_written: set[str] = set()

    def dump_refs(self, butler: Butler, dataset_type_name: str) -> None:
        assert dataset_type_name not in self._dataset_types_written, "Each dataset type must be written only once"
        self._dataset_types_written.add(dataset_type_name)

        dataset_type = butler.get_dataset_type(dataset_type_name)
        writer = DatasetsParquetWriter(dataset_type, str(self._dataset_output_directory.joinpath(f"{dataset_type_name}.parquet")))
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
            writer = DimensionRecordParquetWriter(record.definition, str(self._dimension_output_directory.joinpath(f"{dimension}.parquet")))
            self._dimensions[dimension] = writer
        writer.add_record(record)

class DimensionRecordParquetWriter:
    def __init__(self, dimension: DimensionElement, output_file: str) -> None:
        self._dimension = dimension
        self._records: list[DimensionRecord] = []
        schema = DimensionRecordTable.make_arrow_schema(dimension)
        self._writer = ParquetWriter(output_file, schema)

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
    print(fields)
    return pyarrow.schema(fields)

def _get_data_id_column_schemas(dimensions: DimensionGroup) -> list[pyarrow.Field]:
    schema = []
    for dimension in dimensions.required:
        dimension = dimensions.universe.dimensions[dimension]
        field = pyarrow.field(dimension.name, dimension.primary_key.to_arrow().data_type)
        schema.append(field)

    return schema

if __name__ == "__main__":
    main()
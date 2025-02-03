from __future__ import annotations

from collections.abc import Iterable, Iterator

import pyarrow
import pyarrow.types
from lsst.daf.butler import DatasetRef, DatasetType, DimensionGroup
from pyarrow.parquet import ParquetFile, ParquetWriter

from .utils import convert_parquet_uuid_to_dataset_id


class DatasetsParquetWriter:
    def __init__(self, dataset_type: DatasetType, output_file: str) -> None:
        self._schema = _create_dataset_arrow_schema(dataset_type)
        self._writer = ParquetWriter(output_file, self._schema)

    def add_refs(self, refs: Iterable[DatasetRef]) -> None:
        rows = [self._to_row(ref) for ref in refs]
        batch = pyarrow.RecordBatch.from_pylist(rows, schema=self._schema)
        self._writer.write(batch)

    def _to_row(self, ref: DatasetRef) -> dict[str, object]:
        row = dict(ref.dataId.required)
        row["dataset_id"] = ref.id.bytes
        row["run"] = ref.run
        return row

    def finish(self) -> None:
        self._writer.close()


def _create_dataset_arrow_schema(dataset_type: DatasetType) -> pyarrow.Schema:
    fields = [
        pyarrow.field("dataset_id", pyarrow.binary(16)),
        pyarrow.field("run", pyarrow.dictionary(pyarrow.int32(), pyarrow.string())),
        *_get_data_id_column_schemas(dataset_type.dimensions),
    ]
    return pyarrow.schema(fields)


def _get_data_id_column_schemas(dimensions: DimensionGroup) -> list[pyarrow.Field]:
    schema = []
    for dimension in dimensions.required:
        dimension = dimensions.universe.dimensions[dimension]
        data_type = dimension.primary_key.to_arrow().data_type
        if pyarrow.types.is_string(data_type):
            # Data ID string values always have low cardinality, so dictionary encoding helps a lot.
            data_type = pyarrow.dictionary(pyarrow.int32(), data_type)
        field = pyarrow.field(dimension.name, data_type)
        schema.append(field)

    return schema


def read_dataset_refs_from_file(dataset_type: DatasetType, input_file: str) -> Iterator[list[DatasetRef]]:
    batch_size = 10000
    reader = ParquetFile(input_file)
    for batch in reader.iter_batches(batch_size=batch_size):
        rows = batch.to_pylist()
        yield [_to_ref(dataset_type, row) for row in rows]


def _to_ref(dataset_type: DatasetType, row: dict[str, object]) -> DatasetRef:
    return DatasetRef(dataset_type, row, row["run"], id=convert_parquet_uuid_to_dataset_id(row["dataset_id"]))

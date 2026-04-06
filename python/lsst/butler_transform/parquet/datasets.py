from __future__ import annotations

import pyarrow
from pathlib import Path
from collections.abc import AsyncIterator, Iterable
from lsst.daf.butler import DatasetType, DimensionGroup, DatasetRef, DatasetId
from ..utils.async_parquet_writer import AsyncParquetWriter
from ..utils.async_parquet_reader import read_parquet_async


class DatasetsParquetWriter(AsyncParquetWriter):
    def __init__(self, output_file: str | Path, dataset_type: DatasetType) -> None:
        super().__init__(output_file, _create_dataset_arrow_schema(dataset_type, []))

    async def add_refs(self, refs: Iterable[DatasetRef]) -> None:
        rows = [_convert_ref_to_row(ref) for ref in refs]
        batch = pyarrow.RecordBatch.from_pylist(rows, schema=self._schema)
        await self.write_batch(batch)


async def read_dataset_ids(input_file: str | Path) -> AsyncIterator[list[DatasetId]]:
    column_name = "dataset_id"
    async for batch in read_parquet_async(
        input_file, batch_size=50000, columns=[column_name]
    ):
        yield [
            _convert_parquet_uuid_to_dataset_id(id.as_py())
            for id in batch.column(column_name)
        ]


def _convert_ref_to_row(ref: DatasetRef) -> dict[str, object]:
    row = dict(ref.dataId.required)
    row["dataset_id"] = ref.id.bytes
    row["run"] = ref.run
    return row


def _create_dataset_arrow_schema(
    dataset_type: DatasetType, additional_columns: list[pyarrow.Field]
) -> pyarrow.Schema:
    fields = [
        pyarrow.field("dataset_id", pyarrow.binary(16), nullable=False),
        pyarrow.field(
            "run", pyarrow.dictionary(pyarrow.int32(), pyarrow.string()), nullable=False
        ),
        *_get_data_id_column_schemas(dataset_type.dimensions),
        *additional_columns,
    ]
    return pyarrow.schema(fields)


def _get_data_id_column_schemas(dimensions: DimensionGroup) -> list[pyarrow.Field]:
    schema = []
    for dimension in dimensions.required:
        dimension = dimensions.universe.dimensions[dimension]
        data_type = dimension.primary_key.to_arrow().data_type
        if pyarrow.types.is_string(data_type):
            # Data ID string values always have low cardinality, so dictionary
            # encoding helps a lot.
            data_type = pyarrow.dictionary(pyarrow.int32(), data_type)
        field = pyarrow.field(dimension.name, data_type, nullable=False)
        schema.append(field)

    return schema


def _convert_parquet_uuid_to_dataset_id(dataset_id_binary: object) -> DatasetId:
    assert isinstance(dataset_id_binary, bytes), (
        f"Dataset ID expected to be serialized as binary bytes, got {type(dataset_id_binary)}"
    )
    return DatasetId(bytes=dataset_id_binary)

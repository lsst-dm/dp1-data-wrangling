from __future__ import annotations

from collections.abc import Iterable, Iterator

import pyarrow
import pyarrow.types
from lsst.daf.butler import (
    DatasetAssociation,
    DatasetRef,
    DatasetType,
    DimensionGroup,
    Timespan,
)
from lsst.daf.butler.arrow_utils import TimespanArrowType
from pyarrow.parquet import ParquetFile, ParquetWriter

from .utils import convert_parquet_uuid_to_dataset_id


class DatasetsParquetWriter:
    def __init__(self, dataset_type: DatasetType, output_file: str) -> None:
        self._schema = _create_dataset_arrow_schema(dataset_type, [])
        self._writer = ParquetWriter(output_file, self._schema)

    def add_refs(self, refs: Iterable[DatasetRef]) -> None:
        rows = [_convert_ref_to_row(ref) for ref in refs]
        batch = pyarrow.RecordBatch.from_pylist(rows, schema=self._schema)
        self._writer.write(batch)

    def finish(self) -> None:
        self._writer.close()


def read_dataset_refs_from_file(dataset_type: DatasetType, input_file: str) -> Iterator[list[DatasetRef]]:
    for batch in _read_rows_from_parquet(input_file):
        yield [_convert_row_to_ref(dataset_type, row) for row in batch]


class DatasetAssociationParquetWriter:
    def __init__(self, dataset_type: DatasetType, output_file: str) -> None:
        self._schema = _create_dataset_arrow_schema(
            dataset_type,
            [
                pyarrow.field(
                    "collection", pyarrow.dictionary(pyarrow.int32(), pyarrow.string()), nullable=False
                ),
                pyarrow.field("timespan", TimespanArrowType(), nullable=True),
            ],
        )
        self._writer = ParquetWriter(output_file, self._schema)

    def add_associations(self, associations: Iterable[DatasetAssociation]) -> None:
        rows = [_convert_association_to_row(association) for association in associations]
        batch = pyarrow.RecordBatch.from_pylist(rows, schema=self._schema)
        self._writer.write(batch)

    def finish(self) -> None:
        self._writer.close()


def read_dataset_associations_from_file(
    dataset_type: DatasetType, input_file: str
) -> Iterator[list[DatasetAssociation]]:
    for batch in _read_rows_from_parquet(input_file):
        yield [_convert_row_to_association(dataset_type, row) for row in batch]


def _convert_ref_to_row(ref: DatasetRef) -> dict[str, object]:
    row = dict(ref.dataId.required)
    row["dataset_id"] = ref.id.bytes
    row["run"] = ref.run
    return row


def _convert_association_to_row(association: DatasetAssociation) -> dict[str, object]:
    row = _convert_ref_to_row(association.ref)
    row["collection"] = association.collection
    row["timespan"] = _convert_timespan_to_dict(association.timespan)
    return row


def _convert_row_to_association(dataset_type: DatasetType, row: dict[str, object]) -> DatasetAssociation:
    ref = _convert_row_to_ref(dataset_type, row)
    timespan = row["timespan"]
    return DatasetAssociation(ref, row["collection"], timespan)


def _convert_row_to_ref(dataset_type: DatasetType, row: dict[str, object]) -> DatasetRef:
    return DatasetRef(dataset_type, row, row["run"], id=convert_parquet_uuid_to_dataset_id(row["dataset_id"]))


def _create_dataset_arrow_schema(
    dataset_type: DatasetType, additional_columns: list[pyarrow.Field]
) -> pyarrow.Schema:
    fields = [
        pyarrow.field("dataset_id", pyarrow.binary(16), nullable=False),
        pyarrow.field("run", pyarrow.dictionary(pyarrow.int32(), pyarrow.string()), nullable=False),
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


def _convert_timespan_to_dict(value: Timespan | None) -> dict[str, int] | None:
    # Convert Timespan to a representation that pyarrow understands.
    return {"begin_nsec": value.nsec[0], "end_nsec": value.nsec[1]} if value is not None else None


def _read_rows_from_parquet(input_file: str) -> Iterator[list[dict[str, object]]]:
    batch_size = 10000
    reader = ParquetFile(input_file)
    try:
        for batch in reader.iter_batches(batch_size=batch_size):
            yield batch.to_pylist()
    finally:
        reader.close()

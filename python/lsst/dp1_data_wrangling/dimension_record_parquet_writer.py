from __future__ import annotations

import pandas
from lsst.daf.butler import DimensionElement, DimensionRecord, DimensionRecordTable
from pyarrow.parquet import ParquetWriter

_MAX_ROWS_PER_WRITE = 50000


class DimensionRecordParquetWriter:
    def __init__(self, dimension: DimensionElement, output_file: str) -> None:
        self._dimension = dimension
        self._output_file = output_file
        self._records: list[DimensionRecord] = []
        self._schema = DimensionRecordTable.make_arrow_schema(dimension)
        self._writer = ParquetWriter(output_file, self._schema)

    def add_record(self, record: DimensionRecord) -> None:
        self._records.append(record)
        if len(self._records) >= _MAX_ROWS_PER_WRITE:
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

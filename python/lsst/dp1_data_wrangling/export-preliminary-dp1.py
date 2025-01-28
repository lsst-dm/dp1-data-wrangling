from collections.abc import Iterable
from contextlib import AbstractContextManager
import pathlib

from lsst.daf.butler import Butler, DimensionRecordTable, DimensionElement, DimensionRecord, DatasetRef, DatasetType
from pyarrow.parquet import ParquetWriter


COLLECTIONS = [ "LSSTComCam/runs/DRP/DP1/w_2025_03/DM-48478"]
OUTPUT_DIRECTORY = "dp1-dump-test"

def main() -> None:
    output_path = pathlib.Path(OUTPUT_DIRECTORY)
    output_path.mkdir(parents=True, exist_ok=True)

    butler = Butler("/repo/main")
    refs = butler.query_datasets("calexp", COLLECTIONS, find_first=False, with_dimension_records=True)

    dumper = DatasetsDumper(output_path)
    dumper.write_refs(refs)
    dumper.finish()

class DatasetsDumper:
    """Export DatasetRefs with associated dimension records to parquet files"""

    def __init__(self, output_path: pathlib.Path) -> None:
        self._dimensions: dict[str, DimensionRecordParquetWriter] = {}
        self._output_path = output_path

    def write_refs(self, refs: Iterable[DatasetRef]) -> None:
        for ref in refs:
            for key, record in ref.dataId.records.items():
                self._add_dimension_record(key, record)

    def finish(self) -> None:
        for writer in self._dimensions.values():
            writer.finish()

    def _add_dimension_record(self, dimension: str, record: DimensionRecord | None) -> None:
        if record is None:
            return

        writer = self._dimensions.get(dimension)
        if writer is None:
            writer = DimensionRecordParquetWriter(record.definition, str(self._output_path.joinpath(f"{dimension}.parquet")))
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
        if len(self._records) > 100000:
            self._flush_records()

    def _flush_records(self) -> None:
        table = DimensionRecordTable(self._dimension, self._records)
        self._writer.write(table.to_arrow())
        self._records.clear()

    def finish(self) -> None:
        self._flush_records()
        self._writer.close()

if __name__ == "__main__":
    main()
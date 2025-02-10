from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

from .datastore_parquet import read_datastore_records_from_file
from .export_dp1 import EXPORT_DIRECTORY
from .import_dp1 import make_datastore_path_relative
from .paths import ExportPaths


def main() -> None:
    DATASTORE_ROOT_PATH = "/sdf/group/rubin/repo/main/"
    OUTPUT_DIRECTORY = "datastore_symlinks"

    output_dir = Path(OUTPUT_DIRECTORY)
    output_dir.mkdir()

    count = 0
    datastore_records_file = ExportPaths(EXPORT_DIRECTORY).datastore_parquet_path()
    for path in _generate_file_list(DATASTORE_ROOT_PATH, datastore_records_file):
        output_path = output_dir.joinpath(path.relative_target)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.symlink_to(path.absolute_source)
        count += 1
        if (count % 10000) == 0:
            print(count)


def _generate_file_list(datastore_root_path: str, datastore_records_file_path: str) -> Iterator[MappedPath]:
    for batch in read_datastore_records_from_file(datastore_records_file_path):
        for row in batch:
            original_path = row.file_info.path
            absolute_path = _make_path_absolute(datastore_root_path, original_path)
            target_path = make_datastore_path_relative(original_path)
            yield MappedPath(absolute_source=absolute_path, relative_target=target_path)


def _make_path_absolute(datastore_root_path: str, file_path: str) -> str:
    if file_path.startswith("file://"):
        return file_path.removeprefix("file://")

    return str(Path(datastore_root_path).joinpath(file_path))


class MappedPath(NamedTuple):
    absolute_source: str
    relative_target: str

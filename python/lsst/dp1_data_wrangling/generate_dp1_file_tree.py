from __future__ import annotations

import concurrent.futures
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

import click

from .datastore_parquet import read_datastore_records_from_file
from .export_dp1 import DEFAULT_EXPORT_DIRECTORY
from .import_dp1 import map_datastore_path_for_rsp
from .paths import ExportPaths


@click.command
@click.option("--input-root", default="/sdf/group/rubin/repo/dp1/")
@click.option("--output-root", default="datastore_symlinks")
@click.option("--export-dir", default=DEFAULT_EXPORT_DIRECTORY)
def main(input_root: str, output_root: str, export_dir: str) -> None:
    output_dir = Path(output_root)
    output_dir.mkdir()

    count = 0
    datastore_records_file = ExportPaths(export_dir).datastore_parquet_path()
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = []
        for path in _generate_file_list(input_root, datastore_records_file):
            futures.append(executor.submit(_create_symlink, output_dir, path))
        for future in concurrent.futures.as_completed(futures):
            future.result()
            count += 1
            if (count % 10000) == 0:
                print(count)


def _create_symlink(output_dir: Path, path: MappedPath) -> None:
    output_path = output_dir.joinpath(path.relative_target)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        output_path.symlink_to(path.absolute_source)
    except FileExistsError:
        # More than one dataset may point to the same file (e.g. log
        # files combined together in a .zip file).  So it's not an error
        # for a file to show up more than once.
        pass


def _generate_file_list(datastore_root_path: str, datastore_records_file_path: str) -> Iterator[MappedPath]:
    for batch in read_datastore_records_from_file(datastore_records_file_path):
        for row in batch:
            original_path = row.file_info.path
            absolute_path = _make_path_absolute(datastore_root_path, original_path)
            target_path = _strip_fragment(map_datastore_path_for_rsp(original_path))
            yield MappedPath(absolute_source=absolute_path, relative_target=target_path)


def _make_path_absolute(datastore_root_path: str, file_path: str) -> str:
    file_path = _strip_fragment(file_path)

    if file_path.startswith("file://"):
        return file_path.removeprefix("file://")

    return str(Path(datastore_root_path).joinpath(file_path))


def _strip_fragment(file_path: str) -> str:
    """Strip a trailing URI fragment like '#unzip=...'.  These are used to
    indicate special loading behaviors for a file, but are not part of the
    actual path.
    """
    return file_path.split("#")[0]


class MappedPath(NamedTuple):
    absolute_source: str
    relative_target: str

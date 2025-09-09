from __future__ import annotations

import os
import re
import uuid
from collections import Counter
from collections.abc import Iterable, Iterator
from glob import glob
from typing import Literal, NamedTuple

import matplotlib.pyplot as plt
from pyarrow.parquet import ParquetFile


def main() -> None:
    accesses = list(access for access in find_file_accesses() if access.repo == "dp1")
    by_endpoint = Counter(access.endpoint for access in accesses)
    print(f"Total: {len(accesses)}")
    print(by_endpoint)
    by_dataset_id = Counter(access.id for access in accesses if access.id is not None)
    print(f"Unique datasets accessed: {len(by_dataset_id)}")

    dataset_type_mapping = find_dataset_types(by_dataset_id.keys())
    by_dataset_type = Counter[str]()
    for id, dataset_type in dataset_type_mapping.items():
        by_dataset_type[dataset_type] += by_dataset_id[id]
    print(by_dataset_type)

    fig, ax = plt.subplots(figsize=(12, 8), ncols=2)
    wedges = list(by_dataset_type.values())
    labels = [f"{k} ({v})" for k, v in by_dataset_type.items()]
    ax[0].pie(wedges, labels=labels)
    handles, labels = ax[0].get_legend_handles_labels()
    ax[1].legend(handles, labels, loc="center right")
    ax[1].set_axis_off()
    plt.show()


def find_file_accesses() -> Iterator[FileAccess]:
    with open("dp1-file-access-log") as fh:
        for line in fh:
            timestamp, rest = line.split("\t")
            if match := re.search(r"GET /api/butler/repo/(\w+)/v1/get_file/([-\w]+)", rest):
                repo = match.group(1)
                id = uuid.UUID(match.group(2))
                yield FileAccess(repo=repo, id=id, endpoint="get_file_by_uuid")
            elif match := re.search(r"POST /api/butler/repo/(\w+)/v1/get_file_by_data_id", rest):
                repo = match.group(1)
                yield FileAccess(repo=repo, id=None, endpoint="get_file_by_data_id")
            elif match := re.search(r"GET /api/butler/repo/(\w+)/v1/dataset/([-\w]+)/download", rest):
                repo = match.group(1)
                id = uuid.UUID(match.group(2))
                yield FileAccess(repo=repo, id=id, endpoint="download")
            else:
                raise ValueError(f"Unhandled log line: {rest}")


def find_dataset_types(search_datasets: Iterable[uuid.UUID]) -> dict[uuid.UUID, str]:
    datasets = set(id.bytes for id in search_datasets)
    root = "dp1-dump/datasets"
    output = {}
    for file in glob("*", root_dir=root):
        dataset_type_name = file
        path = os.path.join(root, file)
        id_column = ParquetFile(path).read(columns=["dataset_id"]).column("dataset_id")
        ids_for_this_type = set(id_column.to_pylist())
        found = datasets.intersection(ids_for_this_type)
        datasets = datasets - found
        for id in found:
            output[uuid.UUID(bytes=id)] = dataset_type_name

    if datasets:
        print(f"No dataset type found for some datasets: {[uuid.UUID(bytes=id) for id in datasets]}")

    return output


class FileAccess(NamedTuple):
    repo: str
    id: uuid.UUID | None
    endpoint: Literal["get_file_by_uuid", "get_file_by_data_id", "download"]


main()

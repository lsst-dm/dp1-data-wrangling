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

    fig, (ax1, ax2) = plt.subplots(figsize=(12, 12), nrows=2, ncols=2)
    plot_dataset_types(ax1[0], ax1[1], by_dataset_type)
    plot_count_histogram(ax2[0], by_dataset_id)
    ax2[1].set_axis_off()

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


def plot_dataset_types(ax: plt.Axes, legend_ax: plt.Axes, dataset_counts: dict[str, int]) -> None:
    dataset_counts = dict(sorted(dataset_counts.items(), key=lambda item: item[1], reverse=True))
    wedges = list(dataset_counts.values())
    labels = [f"{k} ({v})" for k, v in dataset_counts.items()]
    ax.pie(wedges, labels=labels)
    handles, labels = ax.get_legend_handles_labels()
    legend_ax.legend(handles, labels, loc="center right")
    legend_ax.set_axis_off()


def plot_count_histogram(ax: plt.Axes, dataset_counts: dict[uuid.UUID, int]) -> None:
    sorted_counts = dict(sorted(dataset_counts.items(), key=lambda item: item[1], reverse=True))
    print(f"Max: {max(sorted_counts.values())}")
    for min_count in (100, 10):
        print(f">{min_count}: {len([x for x in sorted_counts.values() if x >= min_count])}")
    ax.hist(
        dataset_counts.values(), bins=50
    )  # , bins=[0, 5, 25, 100, 500, 1000, max(dataset_counts.values()) + 1])
    ax.set_yscale("log")
    ax.set_xlabel("Download count")
    ax.set_ylabel("Number of datasets")
    # ax.set_xscale("log")


class FileAccess(NamedTuple):
    repo: str
    id: uuid.UUID | None
    endpoint: Literal["get_file_by_uuid", "get_file_by_data_id", "download"]


main()

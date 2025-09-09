from __future__ import annotations

import re
import uuid
from collections import Counter
from collections.abc import Iterator
from typing import Literal, NamedTuple


def main() -> None:
    accesses = list(access for access in find_file_accesses() if access.repo == "dp1")
    by_endpoint = Counter(access.endpoint for access in accesses)
    print(f"Total: {len(accesses)}")
    print(by_endpoint)


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


class FileAccess(NamedTuple):
    repo: str
    id: uuid.UUID | None
    endpoint: Literal["get_file_by_uuid", "get_file_by_data_id", "download"]


main()

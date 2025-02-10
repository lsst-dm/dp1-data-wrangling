from __future__ import annotations

import re

from lsst.daf.butler import Butler

from .datastore_mapping import DatastoreMappingInput
from .importer import Importer

INPUT_DIRECTORY = "dp1-dump-test"
OUTPUT_REPO = "import-test-repo"


def main() -> None:
    Butler.makeRepo(OUTPUT_REPO)
    butler = Butler(OUTPUT_REPO, writeable=True)
    importer = Importer(INPUT_DIRECTORY, butler)
    importer.import_all(datastore_mapping=_datastore_mapping_function)


def _datastore_mapping_function(input: DatastoreMappingInput) -> DatastoreMappingInput:
    path = input.path
    path = path.replace("file:///sdf/data/rubin/", "external/rubin/")

    if re.match(r"^[\w+]+://", path):
        raise ValueError(f"Unhandled absolute path to datastore file: {path}")

    # /repo/main and target repo both use the default
    # "FileDatastore@<butlerRoot>" datastore name, so we don't need to remap
    # the datastore name.
    return input._replace(path=path)

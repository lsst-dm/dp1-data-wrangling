from __future__ import annotations

import re

import click
from lsst.daf.butler import Butler, Config

from .datastore_mapping import DatastoreMappingInput
from .export_dp1 import EXPORT_DIRECTORY
from .importer import Importer

OUTPUT_REPO = "import-test-repo"


@click.command()
@click.option("--seed", help="Butler seed configuration file to use when creating repository")
def main(seed: str | None) -> None:
    if seed:
        config = Config(seed)
    else:
        config = None
    Butler.makeRepo(OUTPUT_REPO, config=config)
    butler = Butler(OUTPUT_REPO, writeable=True)
    importer = Importer(EXPORT_DIRECTORY, butler)
    importer.import_all(datastore_mapping=_datastore_mapping_function)


def make_datastore_path_relative(path: str) -> str:
    path = path.replace("file:///sdf/data/rubin/", "external/rubin/")

    if re.match(r"^[\w+]+://", path):
        raise ValueError(f"Unhandled absolute path to datastore file: {path}")

    return path


def _datastore_mapping_function(input: DatastoreMappingInput) -> DatastoreMappingInput:
    path = make_datastore_path_relative(input.path)
    # /repo/main and target repo both use the default
    # "FileDatastore@<butlerRoot>" datastore name, so we don't need to remap
    # the datastore name.
    return input._replace(path=path)

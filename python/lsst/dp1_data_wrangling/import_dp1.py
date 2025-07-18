from __future__ import annotations

import re
import tempfile
from contextlib import ExitStack

import click
from lsst.daf.butler import Butler, Config

from .datastore_mapping import DatastoreMappingInput
from .export_dp1 import DEFAULT_EXPORT_DIRECTORY
from .importer import Importer


@click.command()
@click.option("--seed", help="Butler seed configuration file to use when creating repository")
@click.option(
    "--use-existing-repo", is_flag=True, help="Use existing Butler repository instead of creating a new one"
)
@click.option("--db-schema", help="Schema name to use when creating the registry database")
@click.option("--db-connection-string", help="Schema name to use when creating the registry database")
@click.option("--no-datastore-remap", is_flag=True, help="Disable remapping of paths inside the datastore")
def main(
    seed: str | None,
    use_existing_repo: bool,
    no_datastore_remap: bool,
    db_schema: str | None,
    db_connection_string: str | None,
) -> None:
    exit_stack = ExitStack()
    with exit_stack:
        output_repo = "import-test-repo"
        if not use_existing_repo:
            if seed:
                config = Config(seed)
            else:
                config = Config()
            if db_connection_string is not None:
                assert db_schema is not None, "--db-schema is required with --db-connection-string"
                config["registry", "db"] = db_connection_string
            if db_schema is not None:
                assert db_connection_string is not None, "--db-connection-string is required with --db-schema"
                config["registry", "namespace"] = db_schema
            if seed or db_connection_string:
                # User manually specified a target database; use a tempdir
                # for the repository directory so this script can be run
                # more than once.
                output_repo = exit_stack.enter_context(tempfile.TemporaryDirectory())
            print("Initializing repository...")
            Butler.makeRepo(output_repo, config=config)

        print("Connecting to database...")
        butler = Butler(output_repo, writeable=True)
        print("Importing DP1 registry...")
        importer = Importer(DEFAULT_EXPORT_DIRECTORY, butler)
        if no_datastore_remap:
            datastore_mapping = _null_datastore_mapping_function
        else:
            datastore_mapping = _datastore_mapping_function

        importer.import_all(datastore_mapping=datastore_mapping)
        print("Import complete")


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


def _null_datastore_mapping_function(input: DatastoreMappingInput) -> DatastoreMappingInput:
    return input

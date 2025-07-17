import click
from lsst.daf.butler import Butler, progress

from .export_dp1 import _dump_extra_visit_dimensions
from .exporter import Exporter

DEFAULT_EXPORT_DIRECTORY = "dimension-dump"


@click.command
@click.option("--repo", required=True)
@click.option("--collection", required=True)
@click.option("--output-directory", default=DEFAULT_EXPORT_DIRECTORY)
def main(repo: str, collection: str, output_directory: str) -> None:
    butler = Butler(repo)

    with butler.registry.caching_context():
        dumper = Exporter(
            output_directory, butler, root_collection=collection, skip_datastore=True, skip_associations=True
        )

        summary = butler.collections.get_info(collection, include_summary=True)
        dataset_types = summary.dataset_types

        count = 0
        for dt in dataset_types:
            count += 1
            print(f"{count}/{len(dataset_types)}")
            dumper.dump_refs(dt, [collection])
        _dump_extra_visit_dimensions(butler, dumper)
        dumper.finish()

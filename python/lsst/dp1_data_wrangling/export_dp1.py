from __future__ import annotations

import fnmatch
from collections.abc import Iterator

import click
from lsst.daf.butler import Butler, DataCoordinate
from pyarrow.parquet import ParquetFile

from .exporter import MAX_ROWS_PER_WRITE, Exporter

# Based on a preliminary list provided by Jim Bosch at
# https://rubinobs.atlassian.net/wiki/spaces/~jbosch/pages/423559233/DP1+Dataset+Retention+Removal+Planning
DATASET_TYPES = [
    # "Tier 1" major data products.
    "raw",
    "visit_detector_table",
    "visit_table",
    "visit_image",
    "visit_image_background",
    "source",
    "deep_coadd",
    "deep_coadd_background",
    "template_coadd",
    "object",
    "object_scarlet_models",
    "object_forced_source",
    "dia_object_forced_source",
    "dia_object",
    "dia_source",
    "difference_image",
    # 'deepCoadd_*_consolidated_map*' found in _find_extra_dataset_types()
    # below.
    "ss_source",
    "ss_object",
    # "Tier 1b" minor data products.  Additional dataset types from this list
    # are located in _find_extra_dataset_types(), below.
    "visit_summary",
    "deep_coadd_n_image",
    # "Tier 1c" calibration products and ancillary inputs
    "bfk",
    "camera",
    "dark",
    "bias",
    "defects",
    "flat",
    "linearizer",
    "ptc",
    "the_monster_20250219",
    "fgcmLookUpTable",
    "skyMap",
    "crosstalk",
    "cti",
    "illuminationCorrection",
]

DEFAULT_EXPORT_DIRECTORY = "dp1-dump"


@click.command
@click.option("--dataset-type", "-t", multiple=True, help="Override default dataset types to export")
@click.option("--repo", default="/repo/dp1")
@click.option("--collection", default="LSSTComCam/DP1")
@click.option("--output-directory", default=DEFAULT_EXPORT_DIRECTORY)
def main(dataset_type: list[str], repo: str, collection: str, output_directory: str) -> None:
    butler = Butler(repo)

    with butler.registry.caching_context():
        dumper = Exporter(output_directory, butler, root_collection=collection)

        if dataset_type:
            exported_types = set(dataset_type)
        else:
            exported_types = set(DATASET_TYPES).union(_find_extra_dataset_types(butler, collection))
        for dt in exported_types:
            dumper.dump_refs(dt, [collection])
        _dump_extra_visit_dimensions(butler, dumper)
        dumper.finish()


def _find_extra_dataset_types(butler: Butler, collection: str) -> set[str]:
    info = butler.collections.get_info(collection, include_summary=True)
    # Find all metadata, log, and config dataset types. These are included for
    # provenance.
    types = set()
    for dt in info.dataset_types:
        if (
            dt.endswith("_metadata")
            or dt.endswith("_log")
            or dt.endswith("_config")
            or fnmatch.fnmatchcase(dt, "deepCoadd_*_consolidated_map*")
        ):
            types.add(dt)
    return types


def _dump_extra_visit_dimensions(butler: Butler, dumper: Exporter) -> None:
    # Most of the dimension records will have been exported while exporting
    # datasets.
    #
    # However, there are some special visit-related dimensions that are not
    # referenced directly by dataset data IDs, and need to be handled
    # specially.  These are the dimensions listed as "populated_by: visit" in
    # the dimension universe YAML.
    with butler.query() as query:
        dumper.dump_dimension_records(
            query.dimension_records("visit_system").where("instrument='LSSTComCam'")
        )

    for batch in _read_referenced_visits(dumper):
        data_coordinates = [DataCoordinate.standardize(visit, universe=butler.dimensions) for visit in batch]
        with butler.query() as query:
            query = query.join_data_coordinates(data_coordinates)
            dumper.dump_dimension_records(query.dimension_records("visit_system_membership"))
            dumper.dump_dimension_records(query.dimension_records("visit_definition"))


def _read_referenced_visits(dumper: Exporter) -> Iterator[list[dict[str, object]]]:
    if not dumper.did_export_dimension_records("visit"):
        return
    parquet_path = dumper.close_and_get_dimension_record_output_file("visit")
    file = ParquetFile(parquet_path)
    for batch in file.iter_batches(batch_size=MAX_ROWS_PER_WRITE, columns=["instrument", "id"]):
        yield [{"instrument": v["instrument"], "visit": v["id"]} for v in batch.to_pylist()]
    file.close()

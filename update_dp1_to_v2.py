import os
import tempfile
import tarfile

import click

from lsst.daf.butler import Butler, CollectionType
from lsst.dp1_data_wrangling.import_dp1 import do_import


@click.command()
@click.argument("butler_repo")
@click.option(
    "--file_paths",
    help="Selects the directory layout to use for the imported files."
    " Options are 'no-remap', 'rsp' or 'rucio'",
    default="rsp",
)
def main(butler_repo: str, file_paths: str) -> None:
    butler = Butler.from_config(butler_repo, writeable=True)
    with butler.transaction():
        _import_new_datasets(butler, file_paths)
        # set up tagged collection for coadds from previous fixup.
        # This allows us to keep "bad" images out of the main collection chain,
        # instead of relying on users to always use find-first searches.
        coadds = butler.query_all_datasets(
            "LSSTComCam/runs/DRP/DP1/DM-51335", name=["deep_coadd", "template_coadd"]
        )
        coadd_collection = "LSSTComCam/tags/DM-53654/runs/DRP/DP1/DM-51335"
        butler.registry.registerCollection(
            coadd_collection,
            CollectionType.TAGGED,
            "DP1 coadd images with headers updated in DM-51335",
        )
        butler.registry.associate(coadd_collection, coadds)

        common_collections = (
            "LSSTComCam/runs/DRP/DP1/v29_0_0/DM-50260",
            "LSSTComCam/calib/fgcmcal/DM-48089/standard_passbands",
        )

        # set up v1 collection chain
        v1_collection_name = "LSSTComCam/DP1/v1"
        butler.registry.registerCollection(
            v1_collection_name, CollectionType.CHAINED, "Original release of DP1"
        )
        butler.collections.redefine_chain(
            v1_collection_name,
            ("LSSTComCam/runs/DRP/DP1/DM-51335", *common_collections),
        )

        # set up v2/main collection chain
        v2_collection_name = "LSSTComCam/DP1/v2"
        butler.registry.registerCollection(
            v2_collection_name,
            CollectionType.CHAINED,
            "Second release of DP1, including fixes to visit_image and difference_image from DM-53601",
        )
        v2_collections = (
            "LSSTComCam/runs/DRP/DP1/DM-53601",
            coadd_collection,
            *common_collections,
        )
        butler.collections.redefine_chain(v2_collection_name, v2_collections)
        butler.collections.redefine_chain("LSSTComCam/DP1", v2_collections)


def _import_new_datasets(butler: Butler, file_paths: str) -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        update_file = os.path.join(script_dir, "DM-53654.tar.gz")
        with tarfile.open(update_file, "r:*") as tar:
            tar.extractall(tempdir, filter="data")
        input_dir = os.path.join(tempdir, "DM-53654")
        do_import(input_dir, butler, ["visit_image", "difference_image"], file_paths)


if __name__ == "__main__":
    main()
